package distkvs

import (
	"example.org/cpsc416/a5/kvslib"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type StorageAddr string

// this matches the config file format in config/frontend_config.json
type FrontEndConfig struct {
	ClientAPIListenAddr  string
	StorageAPIListenAddr string
	Storage              StorageAddr
	TracerServerAddr     string
	TracerSecret         []byte
}

type FrontEndStorageStarted struct {
	StorageID string
}

type FrontEndStorageFailed struct {
	StorageID string
}

type FrontEndPut struct {
	Key   string
	Value string
}

type FrontEndPutResult struct {
	Err bool
}

type FrontEndGet struct {
	Key string
}

type FrontEndGetResult struct {
	Key   string
	Value *string
	Err   bool
}

type FrontEnd struct {
	// state may go here
	tracer         *tracing.Tracer
	frontendTrace  *tracing.Trace
	storageTimeout uint8
	storageClient  *rpc.Client
	clientMu       sync.RWMutex
	storageStarted bool
	storageMu      sync.RWMutex
	ops            map[string][]StartOpChan
	opsMu          sync.Mutex
	once           sync.Once
}

type StartOpChan chan struct{}

/***** RPC structs *****/
type StorageJoinArgs struct {
	StorageID   string
	StorageAddr string
	Token       tracing.TracingToken
}

type StorageJoinResp struct {
	State    map[string]string
	RetToken tracing.TracingToken
}

func (d *FrontEnd) Start(clientAPIListenAddr string, storageAPIListenAddr string, storageTimeout uint8, ftrace *tracing.Tracer) error {
	if ftrace != nil {
		d.frontendTrace = ftrace.CreateTrace()
	}

	d.tracer = ftrace
	d.storageTimeout = storageTimeout
	d.ops = make(map[string][]StartOpChan)

	server := rpc.NewServer()
	err := server.Register(d)
	if err != nil {
		return fmt.Errorf("format of FrontEnd RPCs aren't correct: %s\n", err)
	}

	clientListener, err := net.Listen("tcp", clientAPIListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %s\n", clientAPIListenAddr, err)
	}

	storageListener, err := net.Listen("tcp", storageAPIListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %s\n", storageAPIListenAddr, err)
	}

	go server.Accept(clientListener)
	server.Accept(storageListener)
	return nil
}

func (d *FrontEnd) Get(args kvslib.FrontendGetArgs, reply *kvslib.FrontendGetResp) error {
	trace := d.AttemptReceiveToken(&args.Token)
	AttemptRecordAction(trace, FrontEndGet{Key: args.Key})

	// wait for start signal
	startCh := make(StartOpChan, 1)
	d.enqueueOperation(args.Key, startCh)
	<-startCh

	// initialize tracing struct to error values
	frontendGetRes := FrontEndGetResult{
		Key:   args.Key,
		Value: nil,
		Err:   true,
	}
	// initialize reply to error values
	reply.Key = args.Key
	reply.Error = true
	reply.Ok = false

	storageArgs := StorageGetArgs{
		Key:   args.Key,
		Token: AttemptGenerateToken(trace),
	}
	storageResp := StorageGetResp{}
	var call *rpc.Call = nil

	d.clientMu.RLock()
	if d.storageClient != nil {
		call = d.storageClient.Go("Storage.Get", storageArgs, &storageResp, nil)
	} else {
		// record error, since no storage connection has been established
		d.once.Do(func() {
			AttemptRecordAction(d.frontendTrace, FrontEndStorageFailed{})
		})
		AttemptRecordAction(trace, frontendGetRes)
	}
	d.clientMu.RUnlock()

	if call == nil {
		// return error
		reply.RetToken = AttemptGenerateToken(trace)
		return nil
	}

	<-call.Done
	if call.Error == nil {
		// success
		d.AttemptReceiveToken(&storageResp.Token)

		var value *string = nil
		if storageResp.Ok {
			value = &storageResp.Value
		}
		frontendGetRes.Value = value
		frontendGetRes.Err = false
		d.recordSuccess(trace, frontendGetRes)

		reply.Value = storageResp.Value
		reply.Ok = storageResp.Ok
		reply.Error = false
		reply.RetToken = AttemptGenerateToken(trace)
		return nil
	}

	// timeout, then retry
	time.Sleep(time.Duration(d.storageTimeout) * time.Second)
	storageArgs.Token = AttemptGenerateToken(trace)

	d.clientMu.RLock()
	call = d.storageClient.Go("Storage.Get", storageArgs, &storageResp, nil)
	d.clientMu.RUnlock()

	<-call.Done
	if call.Error != nil {
		// fail
		d.recordFailure(trace, frontendGetRes)
	} else {
		// success
		d.AttemptReceiveToken(&storageResp.Token)

		var value *string = nil
		if storageResp.Ok {
			value = &storageResp.Value
		}
		frontendGetRes.Value = value
		frontendGetRes.Err = false
		d.recordSuccess(trace, frontendGetRes)

		reply.Value = storageResp.Value
		reply.Ok = storageResp.Ok
		reply.Error = false
	}

	reply.RetToken = AttemptGenerateToken(trace)
	return nil
}

func (d *FrontEnd) Put(args kvslib.FrontendPutArgs, reply *kvslib.FrontendPutResp) error {
	trace := d.AttemptReceiveToken(&args.Token)
	AttemptRecordAction(trace, FrontEndPut{
		Key:   args.Key,
		Value: args.Value,
	})

	// wait for start signal
	startCh := make(StartOpChan, 1)
	defer func() {
		d.dequeueOperation(args.Key)
	}()
	d.enqueueOperation(args.Key, startCh)
	<-startCh

	// initialize tracing struct to error values
	frontendPutRes := FrontEndPutResult{
		Err: true,
	}
	// initialize reply to error values
	reply.Error = true

	storageArgs := StoragePutArgs{
		Key:   args.Key,
		Value: args.Value,
		Token: AttemptGenerateToken(trace),
	}
	storageResp := StoragePutResp{}
	var call *rpc.Call = nil

	d.clientMu.RLock()
	if d.storageClient != nil {
		call = d.storageClient.Go("Storage.Put", storageArgs, &storageResp, nil)
	} else {
		// record error, since no storage connection has been established
		d.once.Do(func() {
			AttemptRecordAction(d.frontendTrace, FrontEndStorageFailed{})
		})
		AttemptRecordAction(trace, frontendPutRes)
	}
	d.clientMu.RUnlock()

	if call == nil {
		// return error
		reply.RetToken = AttemptGenerateToken(trace)
		return nil
	}

	<-call.Done
	if call.Error == nil {
		// success
		d.AttemptReceiveToken(&storageResp.Token)

		frontendPutRes.Err = false
		d.recordSuccess(trace, frontendPutRes)

		reply.Error = false
		reply.RetToken = AttemptGenerateToken(trace)
		return nil
	}

	// timeout, then retry
	time.Sleep(time.Duration(d.storageTimeout) * time.Second)
	storageArgs.Token = AttemptGenerateToken(trace)

	d.clientMu.RLock()
	call = d.storageClient.Go("Storage.Put", storageArgs, &storageResp, nil)
	d.clientMu.RUnlock()

	<-call.Done
	if call.Error != nil {
		// fail
		d.recordFailure(trace, frontendPutRes)
	} else {
		// success
		d.AttemptReceiveToken(&storageResp.Token)

		frontendPutRes.Err = false
		d.recordSuccess(trace, frontendPutRes)

		reply.Error = false
	}

	reply.RetToken = AttemptGenerateToken(trace)
	return nil
}

func (d *FrontEnd) ResultAck(args kvslib.ResultAckArgs, reply *kvslib.ResultAckResp) error {
	trace := d.AttemptReceiveToken(&args.Token)

	d.dequeueOperation(args.Key)

	reply.RetToken = AttemptGenerateToken(trace)
	return nil
}

func (d *FrontEnd) StorageJoin(args StorageJoinArgs, reply *StorageJoinResp) error {
	storageTrace := d.AttemptReceiveToken(&args.Token)

	const numRetries int = 2
	for i := 0; i < numRetries; i++ {
		client, err := rpc.Dial("tcp", args.StorageAddr)
		if err != nil {
			continue
		}

		d.clientMu.Lock()
		d.storageClient = client
		d.clientMu.Unlock()
	}

	reply.State = map[string]string{
		"key1": "val1",
		"key2": "val2",
	}
	reply.RetToken = AttemptGenerateToken(storageTrace)
	return nil
}

func (d *FrontEnd) recordSuccess(trace *tracing.Trace, action interface{}) {
	var storageStarted bool

	d.storageMu.RLock()
	storageStarted = d.storageStarted
	if storageStarted {
		AttemptRecordAction(trace, action)
	}
	d.storageMu.RUnlock()

	if storageStarted {
		return
	}

	// storageStarted == false case, flip to true
	d.storageMu.Lock()
	if d.storageStarted == false {
		d.once.Do(func() {}) // if invoked, the very first storage RPC succeeded
		d.storageStarted = true
		AttemptRecordAction(d.frontendTrace, FrontEndStorageStarted{})
	}
	AttemptRecordAction(trace, action)
	d.storageMu.Unlock()
}

func (d *FrontEnd) recordFailure(trace *tracing.Trace, action interface{}) {
	var storageStarted bool

	d.storageMu.RLock()
	storageStarted = d.storageStarted
	if storageStarted == false {
		// if invoked, the very first storage RPC failed, but storageStarted
		// is initially set to false, so we must explicitly record StorageFailed
		d.once.Do(func() {
			AttemptRecordAction(d.frontendTrace, FrontEndStorageFailed{})
		})
		AttemptRecordAction(trace, action)
	}
	d.storageMu.RUnlock()

	if !storageStarted {
		return
	}

	// storageStarted == true case, flip to false
	d.storageMu.Lock()
	if d.storageStarted {
		d.storageStarted = false
		AttemptRecordAction(d.frontendTrace, FrontEndStorageFailed{})
	}
	AttemptRecordAction(trace, action)
	d.storageMu.Unlock()
}

func (d *FrontEnd) enqueueOperation(key string, ch StartOpChan) {
	d.opsMu.Lock()
	defer d.opsMu.Unlock()

	opQueue, ok := d.ops[key]
	if ok {
		d.ops[key] = append(opQueue, ch)
	} else {
		d.ops[key] = []StartOpChan{ch}
		ch <- struct{}{}
	}
}

func (d *FrontEnd) dequeueOperation(key string) {
	d.opsMu.Lock()
	defer d.opsMu.Unlock()

	chQueue := d.ops[key]
	if len(chQueue) <= 1 {
		delete(d.ops, key)
	} else {
		d.ops[key] = chQueue[1:]
		d.ops[key][0] <- struct{}{}
	}
}

func (d *FrontEnd) AttemptReceiveToken(token *tracing.TracingToken) *tracing.Trace {
	if d.tracer != nil {
		return d.tracer.ReceiveToken(*token)
	}
	return nil
}

func AttemptGenerateToken(trace *tracing.Trace) tracing.TracingToken {
	var token tracing.TracingToken
	if trace != nil {
		token = trace.GenerateToken()
	}
	return token
}

func AttemptRecordAction(trace *tracing.Trace, action interface{}) {
	if trace != nil {
		trace.RecordAction(action)
	}
}

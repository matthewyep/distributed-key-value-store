package distkvs

import (
	"example.org/cpsc416/a5/kvslib"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"log"
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

type FrontEndStorageJoined struct {
	StorageIds []string
}

// TODO: remove these later
// Tracing structs for debugging
type FrontEndGetFailed struct {
	StorageID string
	Key       string
}

type FrontEndGetSucceeded struct {
	StorageID string
	Key       string
	Value     string
}

type FrontEndPutFailed struct {
	StorageID string
	Key       string
	Value     string
}

type FrontEndPutSucceeded struct {
	StorageID string
	Key       string
	Value     string
}

type FailedNodes struct {
	IDs []string
}

type TotalFailure struct{}

type FrontEnd struct {
	// state may go here
	tracer         *tracing.Tracer
	frontendTrace  *tracing.Trace
	storageTimeout uint8
	ops            map[string][]StartOpChan
	opsMu          sync.Mutex

	joinedNodes   map[string]*rpc.Client
	joinedNodesMu sync.RWMutex
	joining       bool
	joiningMu     sync.RWMutex
	joiningOps    map[string][]StartOpChan
	joiningOpsMu  sync.Mutex
	putWg         sync.WaitGroup

	txnQueue []StartOpChan
	txnMu    sync.Mutex
}

type StartOpChan chan struct{}

// StorageReqCall associates an async rpc.Call with the storageID it was sent to
type StorageReqCall struct {
	ID   string
	Call *rpc.Call
}

/***** RPC structs *****/
type StorageJoinArgs struct {
	StorageID   string
	StorageAddr string
	Token       tracing.TracingToken
}

type StorageJoinResp struct {
	RetToken tracing.TracingToken
}

func (d *FrontEnd) Start(clientAPIListenAddr string, storageAPIListenAddr string, storageTimeout uint8, ftrace *tracing.Tracer) error {
	if ftrace != nil {
		d.frontendTrace = ftrace.CreateTrace()
	}

	d.tracer = ftrace
	d.storageTimeout = storageTimeout
	d.ops = make(map[string][]StartOpChan)
	d.joinedNodes = make(map[string]*rpc.Client)
	d.joiningOps = make(map[string][]StartOpChan)
	d.txnQueue = make([]StartOpChan, 0, 25)

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
	defer d.dequeueOperation(args.Key)
	<-startCh

	// using write mutex to enforce request ordering
	d.joinedNodesMu.Lock()
	numNodes := len(d.joinedNodes)
	callResults := make(chan *StorageReqCall, numNodes)
	succeeded := false

	if numNodes == 0 {
		// no storage nodes joined, so abort immediately
		trace.RecordAction(TotalFailure{})
		AttemptRecordAction(trace, FrontEndGetResult{
			Key:   args.Key,
			Value: nil,
			Err:   true,
		})
		d.joinedNodesMu.Unlock()

		reply.Key = args.Key
		reply.Ok = false
		reply.Error = true
		reply.RetToken = AttemptGenerateToken(trace)
		return nil
	}

	queue := make(StartOpChan, 1)
	d.enqueueTransaction(queue)

	var respVal string
	var respOk bool

	for storageID, client := range d.joinedNodes {
		storageArgs := StorageGetArgs{
			Key:   args.Key,
			Token: AttemptGenerateToken(trace),
		}
		storageResp := StorageGetResp{}

		call := client.Go("Storage.Get", storageArgs, &storageResp, nil)
		go func(storageID string) {
			<-call.Done
			if call.Error == nil {
				d.AttemptReceiveToken(&storageResp.Token)
				respVal = storageResp.Value
				respOk = storageResp.Ok
			}
			callResults <- &StorageReqCall{storageID, call}
		}(storageID)
	}
	d.joinedNodesMu.Unlock()

	var failedNodes []string

	for i := 0; i < numNodes; i++ {
		storageCall := <-callResults
		if storageCall.Call.Error != nil {
			failedNodes = append(failedNodes, storageCall.ID)
			trace.RecordAction(FrontEndGetFailed{storageCall.ID, args.Key})
		} else {
			succeeded = true
			trace.RecordAction(FrontEndGetSucceeded{storageCall.ID, args.Key, respVal})
		}
	}

	// if some requests failed, then retry on those nodes
	// since RPC is using the same TCP connection, the retry should always fail
	if len(failedNodes) != 0 {
		trace.RecordAction(FailedNodes{failedNodes})
		time.Sleep(time.Duration(d.storageTimeout) * time.Second)

		d.joinedNodesMu.RLock()
		for _, storageID := range failedNodes {
			if client, ok := d.joinedNodes[storageID]; ok {
				storageArgs := StorageGetArgs{
					Key:   args.Key,
					Token: AttemptGenerateToken(trace),
				}
				storageResp := StorageGetResp{}
				err := client.Call("Storage.Get", storageArgs, storageResp)
				// TODO: remove this assertion later
				if err == nil {
					// the retry should never succeed
					log.Fatalf("retry to %v succeeded...\n", storageID)
				}
			}
		}
		d.joinedNodesMu.RUnlock()
	}

	// wait for turn in transaction queue
	<-queue
	defer d.dequeueTransaction()

	if len(failedNodes) != 0 {
		// remove failed nodes from the joined set
		d.joinedNodesMu.Lock()
		for _, storageID := range failedNodes {
			if _, ok := d.joinedNodes[storageID]; ok {
				delete(d.joinedNodes, storageID)
				AttemptRecordAction(d.frontendTrace, FrontEndStorageFailed{storageID})
				d.UnsafeRecordFEStorageJoined()
			}
		}
		d.joinedNodesMu.Unlock()
	}

	var value *string = nil
	if respOk {
		value = &respVal
	}

	if succeeded {
		AttemptRecordAction(trace, FrontEndGetResult{
			Key:   args.Key,
			Value: value,
			Err:   false,
		})
		reply.Error = false
	} else {
		AttemptRecordAction(trace, FrontEndGetResult{
			Key:   args.Key,
			Value: nil,
			Err:   true,
		})
		reply.Error = true
	}

	reply.Key = args.Key
	reply.Value = respVal
	reply.Ok = respOk
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
	d.enqueueOperation(args.Key, startCh)
	defer d.dequeueOperation(args.Key)
	<-startCh

	// spinlock blocks until no storage nodes are joining
	for {
		d.joiningMu.RLock()
		if d.joining == false {
			d.putWg.Add(1)
			d.joiningMu.RUnlock()
			break
		}
		d.joiningMu.RUnlock()
	}
	defer d.putWg.Done()

	// using write mutex to enforce request ordering
	d.joinedNodesMu.Lock()
	numNodes := len(d.joinedNodes)
	callResults := make(chan *StorageReqCall, numNodes)
	succeeded := false

	if numNodes == 0 {
		// no storage nodes joined, so abort immediately
		trace.RecordAction(TotalFailure{})
		AttemptRecordAction(trace, FrontEndPutResult{true})
		d.joinedNodesMu.Unlock()
		reply.Error = true
		reply.RetToken = AttemptGenerateToken(trace)
		return nil
	}

	queue := make(StartOpChan, 1)
	d.enqueueTransaction(queue)

	for storageID, client := range d.joinedNodes {
		storageArgs := StoragePutArgs{
			Key:   args.Key,
			Value: args.Value,
			Token: AttemptGenerateToken(trace),
		}
		storageResp := StoragePutResp{}

		call := client.Go("Storage.Put", storageArgs, &storageResp, nil)
		go func(storageID string) {
			<-call.Done
			if call.Error == nil {
				d.AttemptReceiveToken(&storageResp.Token)
			}
			callResults <- &StorageReqCall{storageID, call}
		}(storageID)
	}
	d.joinedNodesMu.Unlock()

	var failedNodes []string

	for i := 0; i < numNodes; i++ {
		storageCall := <-callResults
		if storageCall.Call.Error != nil {
			failedNodes = append(failedNodes, storageCall.ID)
			trace.RecordAction(FrontEndPutFailed{storageCall.ID, args.Key, args.Value})
		} else {
			succeeded = true
			trace.RecordAction(FrontEndPutSucceeded{storageCall.ID, args.Key, args.Value})
		}
	}

	// if some requests failed, then retry on those nodes
	// since RPC is using the same TCP connection, the retry should always fail
	if len(failedNodes) != 0 {
		trace.RecordAction(FailedNodes{failedNodes})
		time.Sleep(time.Duration(d.storageTimeout) * time.Second)

		d.joinedNodesMu.RLock()
		for _, storageID := range failedNodes {
			if client, ok := d.joinedNodes[storageID]; ok {
				storageArgs := StoragePutArgs{
					Key:   args.Key,
					Value: args.Value,
					Token: AttemptGenerateToken(trace),
				}
				storageResp := StoragePutResp{}
				err := client.Call("Storage.Put", storageArgs, storageResp)
				// TODO: remove this assertion later
				if err == nil {
					// the retry should never succeed
					log.Fatalf("retry to %v succeeded...\n", storageID)
				}
			}
		}
		d.joinedNodesMu.RUnlock()
	}

	// wait for turn in transaction queue
	<-queue
	defer d.dequeueTransaction()

	if len(failedNodes) != 0 {
		// remove failed nodes from the joined set
		d.joinedNodesMu.Lock()
		for _, storageID := range failedNodes {
			if _, ok := d.joinedNodes[storageID]; ok {
				delete(d.joinedNodes, storageID)
				AttemptRecordAction(d.frontendTrace, FrontEndStorageFailed{storageID})
				d.UnsafeRecordFEStorageJoined()
			}
		}
		d.joinedNodesMu.Unlock()
	}

	if succeeded {
		AttemptRecordAction(trace, FrontEndPutResult{Err: false})
		reply.Error = false
	} else {
		AttemptRecordAction(trace, FrontEndPutResult{Err: true})
		reply.Error = true
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
	storageID := args.StorageID

	client, err := rpc.Dial("tcp", args.StorageAddr)
	if err != nil {
		// storage node is down, so immediately abort
		reply.RetToken = AttemptGenerateToken(storageTrace)
		return nil
	}

	d.joiningOpsMu.Lock()
	if len(d.joiningOps) == 0 {
		// block all put requests
		d.joiningMu.Lock()
		d.joining = true
		d.joiningMu.Unlock()
	}
	start := make(StartOpChan, 1)
	d.unsafeEnqueueJoinOp(storageID, start)
	d.joiningOpsMu.Unlock()

	defer func() {
		d.unsafeDequeueJoinOp(storageID)
		if len(d.joiningOps) == 0 {
			// unblock put requests
			d.joiningMu.Lock()
			d.joining = false
			d.joiningMu.Unlock()
		}
	}()

	// wait for all outstanding put requests to complete
	d.putWg.Wait()

	// if there are other storage join ops with this ID queued up, wait for them to finish
	<-start
	skipUpdate := false

	d.joinedNodesMu.Lock()
	if _, ok := d.joinedNodes[storageID]; ok {
		// record failure if, for some reason, we never detected it earlier
		delete(d.joinedNodes, storageID)
		AttemptRecordAction(d.frontendTrace, FrontEndStorageFailed{storageID})
		d.UnsafeRecordFEStorageJoined()
	}
	AttemptRecordAction(d.frontendTrace, FrontEndStorageStarted{storageID})
	if len(d.joinedNodes) == 0 {
		skipUpdate = true
	}
	d.joinedNodesMu.Unlock()

	var recentState map[string]string

	// get the most recent state from some joined node
	if !skipUpdate {
		d.joinedNodesMu.RLock()
		getStateSucceeded := false
		getStateResp := StorageGetStateResp{}
		for _, joinedClient := range d.joinedNodes {
			getStateArgs := StorageGetStateArgs{AttemptGenerateToken(storageTrace)}

			err := joinedClient.Call("Storage.GetState", getStateArgs, &getStateResp)
			if err == nil {
				d.AttemptReceiveToken(&getStateResp.Token)
				getStateSucceeded = true
				break
			}
		}
		d.joinedNodesMu.RUnlock()

		// TODO: remove this assertion later
		if getStateSucceeded == false {
			storageTrace.RecordAction(TotalFailure{})
			log.Fatalf("total failure should never occur when nodes are joining")
		}

		recentState = getStateResp.State
	}

	// send state to joining node
	updateArgs := StorageUpdateStateArgs{
		State:      recentState,
		SkipUpdate: skipUpdate,
		Token:      AttemptGenerateToken(storageTrace),
	}
	updateResp := StorageUpdateStateResp{}
	err = client.Call("Storage.UpdateState", updateArgs, &updateResp)
	if err != nil {
		reply.RetToken = AttemptGenerateToken(storageTrace)
		AttemptRecordAction(d.frontendTrace, FrontEndStorageFailed{storageID})
		return nil
	}

	// update succeeded, add node to joined nodes
	d.AttemptReceiveToken(&updateResp.RetToken)

	d.joinedNodesMu.Lock()
	d.joinedNodes[storageID] = client
	d.UnsafeRecordFEStorageJoined()
	d.joinedNodesMu.Unlock()

	reply.RetToken = AttemptGenerateToken(storageTrace)
	return nil
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

func (d *FrontEnd) unsafeEnqueueJoinOp(key string, ch StartOpChan) {
	opQueue, ok := d.joiningOps[key]
	if ok {
		d.joiningOps[key] = append(opQueue, ch)
	} else {
		d.joiningOps[key] = []StartOpChan{ch}
		ch <- struct{}{}
	}
}

func (d *FrontEnd) unsafeDequeueJoinOp(key string) {
	chQueue := d.joiningOps[key]
	if len(chQueue) <= 1 {
		delete(d.joiningOps, key)
	} else {
		d.joiningOps[key] = chQueue[1:]
		d.joiningOps[key][0] <- struct{}{}
	}
}

func (d *FrontEnd) enqueueTransaction(ch StartOpChan) {
	d.txnMu.Lock()
	defer d.txnMu.Unlock()

	d.txnQueue = append(d.txnQueue, ch)

	if len(d.txnQueue) == 1 {
		ch <- struct{}{}
	}
}

func (d *FrontEnd) dequeueTransaction() {
	d.txnMu.Lock()
	defer d.txnMu.Unlock()

	d.txnQueue = d.txnQueue[1:]

	if len(d.txnQueue) > 0 {
		d.txnQueue[0] <- struct{}{}
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

func (d *FrontEnd) UnsafeRecordFEStorageJoined() {
	// Note: this function is NOT thread safe;
	// The mutex for joinedNodes must be held by the caller!!
	if d.frontendTrace != nil {
		set := make([]string, len(d.joinedNodes))
		i := 0

		for storageID := range d.joinedNodes {
			set[i] = storageID
			i += 1
		}

		d.frontendTrace.RecordAction(FrontEndStorageJoined{set})
	}
}

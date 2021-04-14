// Package kvslib provides an API which is a wrapper around RPC calls to the
// frontend.
package kvslib

import (
	"fmt"
	"log"
	"net/rpc"
	"sync"

	"github.com/DistributedClocks/tracing"
)

type KvslibBegin struct {
	ClientId string
}

type KvslibPut struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type KvslibGet struct {
	ClientId string
	OpId     uint32
	Key      string
}

type KvslibPutResult struct {
	OpId uint32
	Err  bool
}

type KvslibGetResult struct {
	OpId  uint32
	Key   string
	Value *string
	Err   bool
}

type KvslibComplete struct {
	ClientId string
}

// NotifyChannel is used for notifying the client about a mining result.
type NotifyChannel chan ResultStruct

type ResultStruct struct {
	OpId        uint32
	StorageFail bool
	Result      *string
}

type KVS struct {
	notifyCh    NotifyChannel
	clientId    string
	tracer      *tracing.Tracer
	kvslibTrace *tracing.Trace
	frontend    *rpc.Client
	nextOpId    uint32
	getMu       sync.Mutex
	getOps      map[string][]StartOpChan
	putMu       sync.Mutex
	putOps      map[string][]StartOpChan
	closeWg     sync.WaitGroup
	// Add more KVS instance state here.
}

type CloseChannel chan struct{}
type StartOpChan chan struct{}

// RPC structs
type FrontendGetArgs struct {
	Token tracing.TracingToken
	Key   string
}

type FrontendGetResp struct {
	RetToken tracing.TracingToken
	Key      string
	Value    string
	Error    bool
	Ok       bool
}

type FrontendPutArgs struct {
	Token tracing.TracingToken
	Key   string
	Value string
}

type FrontendPutResp struct {
	RetToken tracing.TracingToken
	Error    bool
}

const (
	Get = "get"
	Put = "put"
)

func NewKVS() *KVS {
	return &KVS{
		notifyCh: nil,
	}
}

// Initialize Initializes the instance of KVS to use for connecting to the frontend,
// and the frontends IP:port. The returned notify-channel channel must
// have capacity ChCapacity and must be used by kvslib to deliver all solution
// notifications. If there is an issue with connecting, this should return
// an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Initialize(localTracer *tracing.Tracer, clientId string, frontEndAddr string, chCapacity uint) (NotifyChannel, error) {
	if localTracer != nil {
		d.kvslibTrace = localTracer.CreateTrace()
		d.kvslibTrace.RecordAction(KvslibBegin{ClientId: clientId})
	}

	// connect to Frontend
	log.Printf("dialing frontend at %s\n", frontEndAddr)
	frontendClient, err := rpc.Dial("tcp", frontEndAddr)
	if err != nil {
		return nil, fmt.Errorf("error dialing frontend: %s\n", err)
	}
	d.frontend = frontendClient

	d.nextOpId = 0
	d.clientId = clientId
	d.tracer = localTracer
	d.getOps = make(map[string][]StartOpChan)
	d.putOps = make(map[string][]StartOpChan)
	d.notifyCh = make(NotifyChannel, chCapacity)

	return d.notifyCh, nil
}

// Get is a non-blocking request from the client to the system. This call is used by
// the client when it wants to get value for a key.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) (uint32, error) {
	opId := d.nextOpId
	d.nextOpId += 1

	var trace *tracing.Trace = nil
	if tracer != nil {
		trace = tracer.CreateTrace()
		trace.RecordAction(KvslibGet{
			ClientId: clientId,
			OpId:     opId,
			Key:      key,
		})
	}

	startCh := make(StartOpChan, 1)
	d.enqueueOperation(key, Get, startCh)
	d.closeWg.Add(1)
	go d.getAsync(trace, key, opId, startCh)

	return opId, nil
}

// Put is a non-blocking request from the client to the system. This call is used by
// the client when it wants to update the value of an existing key or add add a new
// key and value pair.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	opId := d.nextOpId
	d.nextOpId += 1

	var trace *tracing.Trace = nil
	if tracer != nil {
		trace = tracer.CreateTrace()
		trace.RecordAction(KvslibPut{
			ClientId: clientId,
			OpId:     opId,
			Key:      key,
			Value:    value,
		})
	}

	startCh := make(chan struct{}, 1)
	d.enqueueOperation(key, Put, startCh)
	d.closeWg.Add(1)
	go d.putAsync(trace, key, value, opId, startCh)

	return opId, nil
}

// Close Stops the KVS instance from communicating with the frontend and
// from delivering any solutions via the notify-channel. If there is an issue
// with stopping, this should return an appropriate err value, otherwise err
// should be set to nil.
func (d *KVS) Close() error {
	d.closeWg.Wait()

	AttemptRecordAction(d.kvslibTrace, KvslibComplete{ClientId: d.clientId})

	// close frontend connection
	err := d.frontend.Close()
	if err != nil {
		return err
	}

	d.frontend = nil
	d.tracer = nil
	d.kvslibTrace = nil
	d.getOps = nil
	d.putOps = nil

	return nil
}

func (d *KVS) getAsync(trace *tracing.Trace, key string, opId uint32, startCh StartOpChan) {
	defer func() {
		log.Printf("operation #%d finished\n", opId)
		d.closeWg.Done()
	}()

	<-startCh

	args := FrontendGetArgs{
		Token: AttemptGenerateToken(trace),
		Key:   key,
	}
	response := FrontendGetResp{}

	call := d.frontend.Go("FrontEnd.Get", args, &response, nil)

	<-call.Done
	if call.Error != nil {
		log.Fatalln(call.Error)
	}

	d.AttemptReceiveToken(&response.RetToken)
	d.dequeueOperation(key, Get)

	var value *string = nil
	if response.Ok {
		value = &response.Value
	}

	AttemptRecordAction(trace, KvslibGetResult{
		OpId:  opId,
		Key:   key,
		Value: value,
		Err:   response.Error,
	})

	d.notifyCh <- ResultStruct{
		OpId:        opId,
		StorageFail: response.Error,
		Result:      value,
	}
}

func (d *KVS) putAsync(trace *tracing.Trace, key, value string, opId uint32, startCh StartOpChan) {
	defer func() {
		log.Printf("operation #%d finished\n", opId)
		d.closeWg.Done()
	}()

	<-startCh

	args := FrontendPutArgs{
		Token: AttemptGenerateToken(trace),
		Key:   key,
		Value: value,
	}
	response := FrontendGetResp{}

	call := d.frontend.Go("FrontEnd.Put", args, &response, nil)

	<-call.Done
	if call.Error != nil {
		log.Fatalln(call.Error)
	}

	d.AttemptReceiveToken(&response.RetToken)
	d.dequeueOperation(key, Put)

	AttemptRecordAction(trace, KvslibPutResult{
		OpId: opId,
		Err:  response.Error,
	})

	d.notifyCh <- ResultStruct{
		OpId:        opId,
		StorageFail: response.Error,
		Result:      &value,
	}
}

// enqueueOperation appends the channel to the operation queue associated with its key.
// If this is the first operation with given key, it also sends to channel to acknowledge
// that this operation can proceed immediately.
func (d *KVS) enqueueOperation(key, op string, ch StartOpChan) {
	var mu *sync.Mutex
	var ops map[string][]StartOpChan

	if op == Get {
		mu = &d.getMu
		ops = d.getOps
	} else {
		mu = &d.putMu
		ops = d.putOps
	}

	mu.Lock()
	defer mu.Unlock()

	opQueue, ok := ops[key]
	if ok {
		ops[key] = append(opQueue, ch)
	} else {
		ops[key] = []StartOpChan{ch}
		ch <- struct{}{}
	}
}

// dequeueOperation removes the first operation from the queue associated with its key.
// If there are remaining operations with given key, it notifies the next operation to begin.
// If there are no remaining operations queued, it deletes the key entry from the map.
func (d *KVS) dequeueOperation(key, op string) {
	var mu *sync.Mutex
	var ops map[string][]StartOpChan

	if op == Get {
		mu = &d.getMu
		ops = d.getOps
	} else {
		mu = &d.putMu
		ops = d.putOps
	}

	mu.Lock()
	defer mu.Unlock()

	chQueue := ops[key]
	if len(chQueue) <= 1 {
		delete(ops, key)
	} else {
		ops[key] = chQueue[1:]
		ops[key][0] <- struct{}{}
	}
}

func (d *KVS) AttemptReceiveToken(token *tracing.TracingToken) {
	if d.tracer != nil {
		d.tracer.ReceiveToken(*token)
	}
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

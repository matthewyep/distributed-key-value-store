package distkvs

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/DistributedClocks/tracing"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
)

type StorageConfig struct {
	StorageID        string
	StorageAdd       StorageAddr
	ListenAddr       string
	FrontEndAddr     string
	DiskPath         string
	TracerServerAddr string
	TracerSecret     []byte
}

type StorageLoadSuccess struct {
	StorageID string
	State     map[string]string
}

type StoragePut struct {
	StorageID string
	Key       string
	Value     string
}

type StorageSaveData struct {
	StorageID string
	Key       string
	Value     string
}

type StorageGet struct {
	StorageID string
	Key       string
}

type StorageGetResult struct {
	StorageID string
	Key       string
	Value     *string
}

type StorageJoining struct {
	StorageID string
}

type StorageJoined struct {
	StorageID string
	State     map[string]string
}

type Storage struct {
	storageID string
	stracer   *tracing.Tracer
	strace    *tracing.Trace
	recorder  *StoreRecorder
	store     *Store
}

type Store struct {
	mu    sync.RWMutex
	kv    map[string]string
	ready bool
}

type StoreRecorder struct {
	file    *os.File
	size    int64
	encoder *json.Encoder
}

type StoreRecord struct {
	Key   string
	Value string
}

/***** RPC structs *****/
type StorageGetArgs struct {
	Key   string
	Token tracing.TracingToken
}

type StorageGetResp struct {
	Value string
	Ok    bool
	Token tracing.TracingToken
}

type StoragePutArgs struct {
	Key   string
	Value string
	Token tracing.TracingToken
}

type StoragePutResp struct {
	Value string
	Token tracing.TracingToken
}

type StorageGetStateArgs struct {
	Token tracing.TracingToken
}

type StorageGetStateResp struct {
	State map[string]string
	Token tracing.TracingToken
}

type StorageUpdateStateArgs struct {
	State      map[string]string
	SkipUpdate bool
	Token      tracing.TracingToken
}

type StorageUpdateStateResp struct {
	Token tracing.TracingToken
}

func (s *Storage) Start(storageId string, frontEndAddr string, storageAddr string, diskPath string, stracer *tracing.Tracer) error {
	// create local strace
	s.stracer = stracer
	s.storageID = storageId

	if stracer != nil {
		s.strace = stracer.CreateTrace()
	}

	// initialize/recover state
	recorder := &StoreRecorder{}
	err := recorder.init(diskPath, storageId)
	if err != nil {
		return fmt.Errorf("failed to initialize store: %w", err)
	}
	s.recorder = recorder
	if err := s.initStore(); err != nil {
		return fmt.Errorf("failed to recover store: %w", err)
	}

	// dial Frontend
	frontend, err := rpc.Dial("tcp", frontEndAddr)
	if err != nil {
		return fmt.Errorf("failed to dial Frontend: %w", err)
	}

	// initialize RPCs
	server := rpc.NewServer()
	err = server.Register(s)
	if err != nil {
		return fmt.Errorf("failed to initialize Storage RPCs: %w", err)
	}
	listener, err := net.Listen("tcp", storageAddr)
	if err != nil {
		return fmt.Errorf("failed to listen at port %s: %w", storageAddr, err)
	}
	// start accept loop for Frontend
	log.Printf("serving Storage RPCs on port %s", storageAddr)
	go server.Accept(listener)

	// join request to Frontend
	AttemptRecordAction(s.strace, StorageJoining{StorageID: storageId})
	args := StorageJoinArgs{
		StorageID:   storageId,
		StorageAddr: storageAddr,
		Token:       AttemptGenerateToken(s.strace),
	}
	reply := StorageJoinResp{}
	err = frontend.Call("FrontEnd.StorageJoin", args, &reply)
	if err != nil {
		return fmt.Errorf("failed to notify Frontend: %w", err)
	}
	s.AttemptReceiveToken(&reply.RetToken)

	// run forever
	for {
		ch := make(chan struct{})
		<-ch
	}
}

func (s *Storage) initStore() error {
	s.store = &Store{
		mu:    sync.RWMutex{},
		kv:    make(map[string]string),
		ready: false,
	}

	if s.recorder.size > 0 {
		f := s.recorder.file
		if f == nil {
			return fmt.Errorf("recorder not initialized")
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			if !strings.HasPrefix(line, "{") {
				log.Printf("not an object: %s", line)
				continue
			}
			var record StoreRecord
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				log.Printf("failed to read: %s", err)
				continue
			}
			s.store.put(record.Key, record.Value)
		}

		// bring record file to fresh state if last line corrupt
		last := make([]byte, 1)
		if _, err := f.ReadAt(last, s.recorder.size-1); err != nil {
			return fmt.Errorf("failed to initialize record: %w", err)
		}
		if string(last) != "\n" {
			log.Printf("inserting a new line")
			if _, err := f.WriteString("\n"); err != nil {
				return fmt.Errorf("failed to initialize record: %w", err)
			}
		}
	}
	AttemptRecordAction(s.strace, StorageLoadSuccess{
		StorageID: s.storageID,
		State:     s.store.kv,
	})

	return nil
}

func (s *Storage) mergeStore(that map[string]string) error {
	for k, thatV := range that {
		thisV, ok := s.store.get(k)
		if !ok || thisV != thatV {
			s.store.put(k, thatV)
			err := s.recorder.record(StoreRecord{
				Key:   k,
				Value: thatV,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Storage) Get(args StorageGetArgs, reply *StorageGetResp) error {
	trace := s.AttemptReceiveToken(&args.Token)

	s.store.mu.RLock()
	defer s.store.mu.RUnlock()

	if !s.store.ready {
		return fmt.Errorf("storage %s not joined", s.storageID)
	}

	AttemptRecordAction(trace, StorageGet{
		StorageID: s.storageID,
		Key:       args.Key,
	})
	value, ok := s.store.get(args.Key)

	var resultVal *string = nil
	if ok {
		resultVal = &value
	}
	AttemptRecordAction(trace, StorageGetResult{
		StorageID: s.storageID,
		Key:       args.Key,
		Value:     resultVal,
	})

	reply.Value = value
	reply.Ok = ok
	reply.Token = AttemptGenerateToken(trace)
	return nil
}

func (s *Storage) Put(args StoragePutArgs, reply *StoragePutResp) error {
	trace := s.AttemptReceiveToken(&args.Token)

	s.store.mu.Lock()
	defer s.store.mu.Unlock()

	if !s.store.ready {
		return fmt.Errorf("storage %s not joined", s.storageID)
	}

	AttemptRecordAction(trace, StoragePut{
		StorageID: s.storageID,
		Key:       args.Key,
		Value:     args.Value,
	})
	value := s.store.put(args.Key, args.Value)
	err := s.recorder.record(StoreRecord{
		Key:   args.Key,
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("failed to save to disk: %w", err)
	}
	AttemptRecordAction(trace, StorageSaveData{
		StorageID: s.storageID,
		Key:       args.Key,
		Value:     value,
	})

	reply.Value = value
	reply.Token = AttemptGenerateToken(trace)
	return nil
}

func (s *Storage) GetState(args StorageGetStateArgs, reply *StorageGetStateResp) error {
	trace := s.AttemptReceiveToken(&args.Token)

	s.store.mu.RLock()
	defer s.store.mu.RUnlock()

	if !s.store.ready {
		return fmt.Errorf("storage %s not joined", s.storageID)
	}

	reply.State = s.store.kv
	reply.Token = AttemptGenerateToken(trace)
	return nil
}

func (s *Storage) UpdateState(args StorageUpdateStateArgs, reply *StorageUpdateStateResp) error {
	trace := s.AttemptReceiveToken(&args.Token)

	s.store.mu.Lock()
	defer s.store.mu.Unlock()

	if !args.SkipUpdate {
		// merge with latest store
		if err := s.mergeStore(args.State); err != nil {
			return fmt.Errorf("failed to merge state from Frontend: %w", err)
		}
	}
	s.store.ready = true
	AttemptRecordAction(s.strace, StorageJoined{
		StorageID: s.storageID,
		State:     s.store.kv,
	})

	reply.Token = AttemptGenerateToken(trace)
	return nil
}

func (r *StoreRecorder) init(diskPath string, storageId string) error {
	if err := os.MkdirAll(diskPath, os.ModePerm); err != nil {
		return err
	}
	filePath := fmt.Sprintf("%s%s.log", diskPath, storageId)
	file, openErr := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	stat, statErr := os.Stat(filePath)
	if openErr != nil || statErr != nil {
		return fmt.Errorf("failed to initialize record file")
	}
	r.file = file
	r.size = stat.Size()
	r.encoder = json.NewEncoder(file)
	return nil
}

func (r *StoreRecorder) close() error {
	if err := r.file.Close(); err != nil {
		return err
	}
	r.file = nil
	r.encoder = nil
	return nil
}

func (r *StoreRecorder) record(record StoreRecord) error {
	if r.encoder == nil {
		return fmt.Errorf("recorder not initialized")
	}
	if err := r.encoder.Encode(record); err != nil {
		return err
	}
	return nil
}

func (s *Store) put(key string, value string) string {
	s.kv[key] = value
	return value
}

func (s *Store) get(key string) (string, bool) {
	value, ok := s.kv[key]
	return value, ok
}

func (s *Storage) AttemptReceiveToken(token *tracing.TracingToken) *tracing.Trace {
	if s.stracer != nil {
		return s.stracer.ReceiveToken(*token)
	}
	return nil
}

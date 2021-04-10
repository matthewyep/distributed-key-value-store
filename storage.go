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
	State map[string]string
}

type StoragePut struct {
	Key   string
	Value string
}

type StorageSaveData struct {
	Key   string
	Value string
}

type StorageGet struct {
	Key string
}

type StorageGetResult struct {
	Key   string
	Value *string
}

type Storage struct {
	stracer  *tracing.Tracer
	strace   *tracing.Trace
	recorder *StoreRecorder
	store    *Store
}

type Store struct {
	mu sync.RWMutex
	kv map[string]string
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

func (s *Storage) Start(frontEndAddr string, storageAddr string, diskPath string, stracer *tracing.Tracer) error {
	// create local strace
	s.stracer = stracer
	if stracer != nil {
		s.strace = stracer.CreateTrace()
	}

	// initialize/recover state
	recorder := &StoreRecorder{}
	err := recorder.init(diskPath)
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

	// notify Frontend
	args := StorageConnectArgs{
		StorageAddr: storageAddr,
		Token:       AttemptGenerateToken(s.strace),
	}

	reply := StorageConnectResp{}
	err = frontend.Call("FrontEnd.StorageConnect", args, &reply)
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
		mu: sync.RWMutex{},
		kv: make(map[string]string),
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
	AttemptRecordAction(s.strace, StorageLoadSuccess{State: s.store.kv})

	return nil
}

func (s *Storage) Get(args StorageGetArgs, reply *StorageGetResp) error {
	trace := s.AttemptReceiveToken(&args.Token)

	s.store.mu.RLock()
	AttemptRecordAction(trace, StorageGet{Key: args.Key})
	value, ok := s.store.get(args.Key)

	var resultVal *string = nil
	if ok {
		resultVal = &value
	}
	AttemptRecordAction(trace, StorageGetResult{
		Key:   args.Key,
		Value: resultVal,
	})
	s.store.mu.RUnlock()

	reply.Value = value
	reply.Ok = ok
	reply.Token = AttemptGenerateToken(trace)
	return nil
}

func (s *Storage) Put(args StoragePutArgs, reply *StoragePutResp) error {
	trace := s.AttemptReceiveToken(&args.Token)

	s.store.mu.Lock()
	AttemptRecordAction(trace, StoragePut{
		Key:   args.Key,
		Value: args.Value,
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
		Key:   args.Key,
		Value: value,
	})
	s.store.mu.Unlock()

	reply.Value = value
	reply.Token = AttemptGenerateToken(trace)
	return nil
}

func (r *StoreRecorder) init(diskPath string) error {
	if err := os.MkdirAll(diskPath, os.ModePerm); err != nil {
		return err
	}
	filePath := diskPath + "store_record.log"
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

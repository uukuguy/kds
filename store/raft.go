package store

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
	log "github.com/uukuguy/kds/utils/logger"
	"golang.org/x/net/context"
)

// GenerateSnapshot ()
// ======== RaftNode::GenerateSnapshot() ========
func (_this *RaftNode) GenerateSnapshot(data interface{}) ([]byte, error) {
	_this.dataRWLock.Lock()
	defer _this.dataRWLock.Unlock()
	return json.Marshal(data)
}

// RecoverFromSnapshot ()
// ======== RaftNode::RecoverFromSnapshot() ========
func (_this *RaftNode) RecoverFromSnapshot(snapshot []byte, data interface{}) error {
	_this.dataRWLock.Lock()
	defer _this.dataRWLock.Unlock()
	if err := json.Unmarshal(snapshot, data); err != nil {
		return err
	}
	return nil
}

type kv struct {
	Key string
	Val string
}

// ReadCommits ()
// ======== RaftNode::ReadCommits() ========
func (_this *RaftNode) ReadCommits(commitC <-chan *string, errorC <-chan error, data interface{}) {
	for data := range commitC {
		if data == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapshot
			var snapshotter *snap.Snapshotter
			snapshot, err := snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil {
				log.Panicf("snapshotter.Load() failed. %v", err)
			}
			log.Infof("Loading snapshot at term %d and index &d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := _this.RecoverFromSnapshot(snapshot.Data, data); err != nil {
				log.Panicf("RaftNode.RecoverFromSnapshot() failed. %v", err)
			}
		} else {

			dec := gob.NewDecoder(bytes.NewBufferString(*data))

			var datakv kv
			if err := dec.Decode(&datakv); err != nil {
				log.Fatalf("could not decode message. %v", err)
			}
			// s.rwlock.Lock()
			// s.kvStore[dataKv.Key] = dataKv.Val
			// s.rwlock.Unlock()
		}
	}
	if err, ok := <-errorC; ok {
		log.Fatalf("There is a error in errorC chan. %v", err)
	}
}

// RaftNode ********************************
// Distributed node backed by raft.
type RaftNode struct {
	proposeC    <-chan string            // proposed messages (k,v)
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes.
	commitC     chan<- *string           // entries commited to log(k,v)
	errorC      chan<- error             // errors from raft session

	id               int      // client ID for raft session
	peers            []string // raft peer URLs
	joined           bool     // node is joining an existing cluster
	waldir           string   // path to WAL directory
	snapdir          string   // path to snapshot directory
	lastIndex        uint64   // index of log at start
	generateSnapshot func() ([]byte, error)

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready

	snapCount uint64
	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete

	dataRWLock sync.RWMutex
}

var defaultSnapCount uint64 = 10000

// NewRaftNode -
// ======== NewRaftNode() ========
// NewRaftNode initializes a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func NewRaftNode(id int, peers []string, joined bool, generateSnapshot func() ([]byte, error), proposeC <-chan string, confChangeC <-chan raftpb.ConfChange) (commitC chan<- *string, errorC chan<- error, snapshotterReady <-chan *snap.Snapshotter) {
	raftNode := &RaftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     make(chan *string),
		errorC:      make(chan error),

		id:               id,
		peers:            peers,
		joined:           joined,
		waldir:           fmt.Sprintf("raftexample-%d", id),
		snapdir:          fmt.Sprintf("raftexample-%d-snap", id),
		generateSnapshot: generateSnapshot,

		raftStorage: raft.NewMemoryStorage(),

		snapshotterReady: make(chan *snap.Snapshotter, 1),

		snapCount: defaultSnapCount,
		stopc:     make(chan struct{}),
		httpstopc: make(chan struct{}),
		httpdonec: make(chan struct{}),
	}

	commitC = raftNode.commitC
	errorC = raftNode.errorC
	snapshotterReady = raftNode.snapshotterReady

	go raftNode.startRaft()

	return commitC, errorC, snapshotterReady
}

// Interface methods for rafthttp.Raft

// Process -
// ======== RaftNode::Process() ========
func (_this *RaftNode) Process(ctx context.Context, m raftpb.Message) error {
	return _this.node.Step(ctx, m)
}

// IsIDRemoved -
// ======== RaftNode::IsIDRemoved() ========
func (_this *RaftNode) IsIDRemoved(id uint64) bool { return false }

// ReportUnreachable -
// ======== RaftNode::ReportUnreachable() ========
func (_this *RaftNode) ReportUnreachable(id uint64) {}

// ReportSnapshot -
// ======== RaftNode()::ReportSnapshot() ========
func (_this *RaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

// -------- RaftNode::startRaft() --------
// startRaft  main entry
func (_this *RaftNode) startRaft() {
	if !fileutil.Exist(_this.snapdir) {
		if err := os.Mkdir(_this.snapdir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for snapshot. %v", err)
		}
	}
	_this.snapshotter = snap.New(_this.snapdir)
	_this.snapshotterReady <- _this.snapshotter

	oldwal := wal.Exist(_this.waldir)
	_this.wal = _this.replayWAL()

	peers := make([]raft.Peer, len(_this.peers))
	for i := range peers {
		peers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	c := &raft.Config{
		ID:              uint64(_this.id),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         _this.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}

	if oldwal {
		_this.node = raft.RestartNode(c)
	} else {
		startPeers := peers
		if _this.joined {
			startPeers = nil
		}
		_this.node = raft.StartNode(c, startPeers)
	}

	serverStats := &stats.ServerStats{}
	serverStats.Initialize()

	_this.transport = &rafthttp.Transport{
		ID:          types.ID(_this.id),
		ClusterID:   0x1000,
		Raft:        _this,
		ServerStats: serverStats,
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(_this.id)),
		ErrorC:      make(chan error),
	}

	_this.transport.Start()
	for i := range _this.peers {
		if i+1 != _this.id {
			_this.transport.AddPeer(types.ID(i+1), []string{_this.peers[i]})
		}
	}

	go _this.serveRaft()
	go _this.serveChannels()
}

// -------- RaftNode::stop() --------
func (_this *RaftNode) stop() {
	_this.stopHTTP()
	close(_this.commitC)
	close(_this.errorC)
	_this.node.Stop()
}

// -------- RaftNode::writeError() --------
func (_this *RaftNode) writeError(err error) {
	_this.stopHTTP()
	close(_this.commitC)
	_this.errorC <- err
	close(_this.errorC)
	_this.node.Stop()
}

// -------- RaftNode::stopHTTP() --------
func (_this *RaftNode) stopHTTP() {
	_this.transport.Stop()
	close(_this.httpstopc)
	<-_this.httpdonec
}

// -------- RaftNode::openWAL() --------
// openWAL returns a WAL ready for reading
func (_this *RaftNode) openWAL() *wal.WAL {
	if !wal.Exist(_this.waldir) {
		if err := os.Mkdir(_this.waldir, 0750); err != nil {
			log.Fatalf("Cannot create dir for wal. %v", err)
		}
		w, err := wal.Create(_this.waldir, nil)
		if err != nil {
			log.Fatalf("Create wal error. %v", err)
		}
		w.Close()
	}

	w, err := wal.Open(_this.waldir, walpb.Snapshot{})
	if err != nil {
		log.Fatalf("Error loading wal. %v", err)
	}

	return w
}

// -------- RaftNode::replayWAL() --------
// replayWAL replays WAL entries into the raft instance.
func (_this *RaftNode) replayWAL() *wal.WAL {
	w := _this.openWAL()
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("Failed to read WAL. %v", err)
	}

	// append to storage so raft starts at the right place in log.
	_this.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		_this.lastIndex = ents[len(ents)-1].Index
	} else {
		_this.commitC <- nil
	}
	_this.raftStorage.SetHardState(st)
	return w
}

// StoppableListener -
// **************** StoppableListener ****************
type StoppableListener struct {
	*net.TCPListener
	stopc <-chan struct{}
}

// NewStoppableListener -
// ======== NewStoppableListener() ========
func NewStoppableListener(addr string, stopc <-chan struct{}) (*StoppableListener, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &StoppableListener{listener.(*net.TCPListener), stopc}, nil
}

// Accept -
// ======== StoppableListener::Accept() ========
func (_this *StoppableListener) Accept() (conn net.Conn, err error) {
	connc := make(chan *net.TCPConn, 1)
	errc := make(chan error, 1)

	go func() {
		tc, err := _this.AcceptTCP()
		if err != nil {
			errc <- err
			return
		}
		connc <- tc
	}()

	select {
	case <-_this.stopc:
		return nil, errors.New("Server stopped.")
	case err := <-errc:
		return nil, err
	case tc := <-connc:
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(3 * time.Minute)
		return tc, nil
	}
}

// -------- RaftNode::serveRaft() --------
func (_this *RaftNode) serveRaft() {
	url, err := url.Parse(_this.peers[_this.id-1])
	if err != nil {
		log.Fatalf("Failed parsing URL. %v", err)
	}

	ln, err := NewStoppableListener(url.Host, _this.httpstopc)
	if err != nil {
		log.Fatalf("Failed to listen rafthttp. %v", err)
	}

	err = (&http.Server{Handler: _this.transport.Handler()}).Serve(ln)
	select {
	case <-_this.httpstopc:
	default:
		log.Fatalf("Failed to serve rafthttp. %v", err)
	}
	close(_this.httpdonec)
}

// -------- RaftNode::serveChannels() --------
func (_this *RaftNode) serveChannels() {
	snap, err := _this.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}

	_this.confState = snap.Metadata.ConfState
	_this.snapshotIndex = snap.Metadata.Index
	_this.appliedIndex = snap.Metadata.Index

	defer _this.wal.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		var confChangeCount uint64
		for _this.proposeC != nil && _this.confChangeC != nil {
			select {
			case prop, ok := <-_this.proposeC:
				if !ok {
					_this.proposeC = nil
				} else {
					_this.node.Propose(context.TODO(), []byte(prop))
				}

			case cc, ok := <-_this.confChangeC:
				if !ok {
					_this.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					_this.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already.
		close(_this.stopc)
	}()

	// event loop on raft state machine updates.
	for {
		select {
		case <-ticker.C:
			_this.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-_this.node.Ready():
			_this.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				_this.saveSnap(rd.Snapshot)
				_this.raftStorage.ApplySnapshot(rd.Snapshot)
				_this.publishSnapshot(rd.Snapshot)
			}
			_this.raftStorage.Append(rd.Entries)
			_this.transport.Send(rd.Messages)
			if ok := _this.publishEntries(_this.getEntriesToApply(rd.CommittedEntries)); !ok {
				_this.stop()
				return
			}
			_this.maybeTriggerSnapshot()
			_this.node.Advance()

		case err := <-_this.transport.ErrorC:
			_this.writeError(err)
			return

		case <-_this.stopc:
			_this.stop()
			return
		}

	}
}

// -------- RaftNode::saveSnap() --------
func (_this *RaftNode) saveSnap(snap raftpb.Snapshot) error {
	if err := _this.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := _this.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	return _this.wal.ReleaseLockTo(snap.Metadata.Index)
}

// -------- RaftNode::publishSnapshot() --------
func (_this *RaftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Infof("Publishing snapshot at index %d", _this.snapshotIndex)
	defer log.Infof("Finished publishing snapshot at index %d", _this.snapshotIndex)

	if snapshotToSave.Metadata.Index <= _this.appliedIndex {
		log.Fatalf("Snapshot index [%d] should > progress.appliedIndex[%d] + 1", snapshotToSave.Metadata.Index, _this.appliedIndex)
	}
	_this.commitC <- nil // trigger kvstore to load snapshot

	_this.confState = snapshotToSave.Metadata.ConfState
	_this.snapshotIndex = snapshotToSave.Metadata.Index
	_this.appliedIndex = snapshotToSave.Metadata.Index
}

// -------- RaftNode::publishEntries() --------
// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (_this *RaftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			select {
			case _this.commitC <- &s:
			case <-_this.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			_this.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					_this.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(_this.id) {
					log.Infof("I've been removed from the cluster! Shutting down.")
					return false
				}
				_this.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}

		// after commit, update appliedIndex
		_this.appliedIndex = ents[i].Index

		// special nil commit to signal replay has finished
		if ents[i].Index == _this.lastIndex {
			select {
			case _this.commitC <- nil:
			case <-_this.stopc:
				return false
			}
		}
	}
	return true
}

// -------- getEntriesToApply() --------
func (_this *RaftNode) getEntriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}

	firstIdx := ents[0].Index
	if firstIdx > _this.appliedIndex+1 {
		log.Fatalf("First index of committed entry[%d] should <= progress.appliedIndex[%d] 1", firstIdx, _this.appliedIndex)
	}
	if _this.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[_this.appliedIndex-firstIdx+1:]
	}
	return
}

const (
	snapshotCatchUpEntriesN uint64 = 10000
)

// -------- myabeTriggerSnapshot() --------
func (_this *RaftNode) maybeTriggerSnapshot() {
	if _this.appliedIndex-_this.snapshotIndex <= _this.snapCount {
		return
	}

	log.Infof("Start snapshot [applied index: %d | last snapshot index: %d]", _this.appliedIndex, _this.snapshotIndex)
	data, err := _this.generateSnapshot()
	if err != nil {
		log.Panicf("RaftNode.generateSnapshot() failed. %v", err)
	}
	snapshot, err := _this.raftStorage.CreateSnapshot(_this.appliedIndex, &_this.confState, data)
	if err != nil {
		log.Panicf("RaftNode.raftStorage.CreateSnapshot() failed. %v", err)
	}
	if err := _this.saveSnap(snapshot); err != nil {
		log.Panicf("RaftNode.saveSnap() failed. %v", err)
	}

	compactIndex := uint64(1)
	if _this.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = _this.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := _this.raftStorage.Compact(compactIndex); err != nil {
		log.Panicf("RaftNode.raftStorage.Compact() failed. %v", err)
	}

	log.Infof("Compacted log at index %d", compactIndex)
	_this.snapshotIndex = _this.appliedIndex
}

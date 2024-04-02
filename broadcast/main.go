package main

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastMsg struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type ReadOkMsg struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type TopologyMsg struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type ReplicateMsg struct {
	Type         string          `json:"type"`
	Message      int             `json:"message"`
	ReplicatedTo map[string]bool `json:"replicated_to"`
}

type ReplicateOkMsg struct {
	Type string `json:"type"`
}

func main() {
	tracker := newTracker()
	ti := newTopologyInfo()
	node := maelstrom.NewNode()
	replicator := NewReplicator(node, ti, tracker)
	th := newTopoHandler(node, ti)
	rh := newReadHandler(tracker, node)
	bh := newBroadcastHandler(node, tracker, replicator)
	node.Handle("topology", th.handleMsg)
	node.Handle("read", rh.handleMsg)
	node.Handle("broadcast", bh.handleMsg)
	node.Handle("replicate", replicator.handleMsg)

	node.Run()
}

type topohandler struct {
	ti   *topologyInfo
	node *maelstrom.Node
}

func newTopoHandler(node *maelstrom.Node, ti *topologyInfo) *topohandler {
	return &topohandler{
		ti:   ti,
		node: node,
	}
}

func (th *topohandler) handleMsg(msg maelstrom.Message) error {
	var topologyMsg TopologyMsg
	if err := json.Unmarshal(msg.Body, &topologyMsg); err != nil {
		return err
	}

	th.ti.set(th.node.ID(), topologyMsg.Topology)

	response := map[string]string{
		"type": "topology_ok",
	}
	return th.node.Reply(msg, response)
}

type readHandler struct {
	tracker *tracker
	node    *maelstrom.Node
}

func newReadHandler(tracker *tracker, node *maelstrom.Node) *readHandler {
	return &readHandler{
		tracker: tracker,
		node:    node,
	}
}

func (rh *readHandler) handleMsg(msg maelstrom.Message) error {
	vals := rh.tracker.values()
	response := ReadOkMsg{
		Type:     "read_ok",
		Messages: vals,
	}
	return rh.node.Reply(msg, response)
}

type broadcastHandler struct {
	node       *maelstrom.Node
	tracker    *tracker
	replicator *replicator
}

func newBroadcastHandler(node *maelstrom.Node, tracker *tracker, replicator *replicator) *broadcastHandler {
	return &broadcastHandler{
		node:       node,
		tracker:    tracker,
		replicator: replicator,
	}
}

func (bh *broadcastHandler) handleMsg(msg maelstrom.Message) error {
	var broadcastMsg BroadcastMsg
	if err := json.Unmarshal(msg.Body, &broadcastMsg); err != nil {
		return err
	}

	bh.tracker.add(broadcastMsg.Message)
	bh.replicator.startReplicate(broadcastMsg.Message)

	response := map[string]any{
		"type": "broadcast_ok",
	}
	return bh.node.Reply(msg, response)
}

type replicator struct {
	node    *maelstrom.Node
	ti      *topologyInfo
	tracker *tracker
}

func NewReplicator(node *maelstrom.Node, ti *topologyInfo, tracker *tracker) *replicator {
	return &replicator{
		node:    node,
		ti:      ti,
		tracker: tracker,
	}
}

func (r *replicator) startReplicate(val int) {
	msg := ReplicateMsg{
		Type:         "replicate",
		Message:      val,
		ReplicatedTo: map[string]bool{r.node.ID(): true},
	}

	for _, neighbor := range r.ti.neighbors() {
		go r.replicateToNeighbor(msg, neighbor)
	}
}

func (r *replicator) replicateToNeighbor(msg ReplicateMsg, neighbor string) {
	backoff := 200
	for {
		timeout, _ := context.WithTimeout(context.Background(), 500*time.Millisecond)
		reply, err := r.node.SyncRPC(timeout, neighbor, msg)
		var response map[string]any
		if err := json.Unmarshal(reply.Body, &response); err != nil {
			continue
		}
		if response["type"] != "replicate_ok" {
			continue
		}
		if err == nil {
			break
		}

		time.Sleep(time.Duration(backoff) * time.Millisecond)
		backoff *= 2
	}
}

func (r *replicator) handleMsg(msg maelstrom.Message) error {
	var replicateMsg ReplicateMsg
	if err := json.Unmarshal(msg.Body, &replicateMsg); err != nil {
		return err
	}

	r.tracker.add(replicateMsg.Message)
	replicateMsg.ReplicatedTo[r.node.ID()] = true
	for _, neighbor := range r.ti.neighbors() {
		if replicateMsg.ReplicatedTo[neighbor] {
			continue
		}
		r.replicateToNeighbor(replicateMsg, neighbor)
	}

	r.node.Reply(msg, &ReplicateOkMsg{
		Type: "replicate_ok",
	})

	return nil
}

type tracker struct {
	seen atomic.Value
	mu   sync.Mutex
}

func newTracker() *tracker {
	t := &tracker{}
	t.seen.Store(make(map[int]bool))
	return t
}

func (t *tracker) add(i int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	current := t.seen.Load().(map[int]bool)
	newSeen := make(map[int]bool, len(current))
	for k, v := range current {
		newSeen[k] = v
	}
	newSeen[i] = true
	t.seen.Store(newSeen)
}

func (t *tracker) values() []int {
	seen := t.seen.Load().(map[int]bool)
	vals := make([]int, 0, len(seen))
	for key, val := range seen {
		if val {
			vals = append(vals, key)
		}
	}
	return vals
}

type topologyInfo struct {
	myNeighboors []string
	ready        sync.WaitGroup
	init         sync.Once
}

func newTopologyInfo() *topologyInfo {
	ti := topologyInfo{}
	ti.ready.Add(1)
	return &ti
}

func (ti *topologyInfo) set(me string, topology map[string][]string) {
	ti.myNeighboors = topology[me]
	ti.init.Do(func() {
		ti.ready.Done()
	})
}

func (ti *topologyInfo) neighbors() []string {
	ti.ready.Wait()
	return ti.myNeighboors
}

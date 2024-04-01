package main

import (
	"encoding/json"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Tracker struct {
	mu   sync.Mutex
	seen map[int]bool
}

func NewTracker() *Tracker {
	return &Tracker{
		seen: make(map[int]bool),
	}
}

func (t *Tracker) AddValue(i int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seen[i] = true
}

func (t *Tracker) Values() []int {
	t.mu.Lock()
	defer t.mu.Unlock()
	vals := make([]int, 0, len(t.seen))
	for key, val := range t.seen {
		if val {
			vals = append(vals, key)
		}
	}
	return vals
}

type TopologyInfo struct {
	myNeighboors []string
	ready        sync.WaitGroup
	init         sync.Once
}

func NewTopologyInfo() *TopologyInfo {
	ti := TopologyInfo{}
	ti.ready.Add(1)
	return &ti
}

func (ti *TopologyInfo) Set(me string, topology map[string][]string) {
	ti.myNeighboors = topology[me]
	ti.init.Do(func() {
		ti.ready.Done()
	})
}

func (ti *TopologyInfo) Neighbors() []string {
	ti.ready.Wait()
	return ti.myNeighboors
}

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

func main() {
	tracker := NewTracker()
	topologyInfo := NewTopologyInfo()
	node := maelstrom.NewNode()
	th := newTopoHandler(topologyInfo, node)
	rh := newReadHandler(tracker, node)
	rb := newRebroadcaster(node, topologyInfo)
	bh := newBroadcastHandler(node, tracker, rb)
	node.Handle("topology", th.handleMsg)
	node.Handle("read", rh.handleMsg)
	node.Handle("broadcast", bh.handleMsg)

	node.Run()
}

type topohandler struct {
	ti   *TopologyInfo
	node *maelstrom.Node
}

func newTopoHandler(ti *TopologyInfo, node *maelstrom.Node) *topohandler {
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

	th.ti.Set(th.node.ID(), topologyMsg.Topology)

	response := map[string]string{
		"type": "topology_ok",
	}
	return th.node.Reply(msg, response)
}

type readHandler struct {
	tracker *Tracker
	node    *maelstrom.Node
}

func newReadHandler(tracker *Tracker, node *maelstrom.Node) *readHandler {
	return &readHandler{
		tracker: tracker,
		node:    node,
	}

}

func (rh *readHandler) handleMsg(msg maelstrom.Message) error {
	vals := rh.tracker.Values()
	response := ReadOkMsg{
		Type:     "read_ok",
		Messages: vals,
	}
	return rh.node.Reply(msg, response)
}

type rebroadcaster struct {
	node *maelstrom.Node
	ti   *TopologyInfo
}

func newRebroadcaster(node *maelstrom.Node, ti *TopologyInfo) *rebroadcaster {
	return &rebroadcaster{
		node: node,
		ti:   ti,
	}
}

func (r *rebroadcaster) broadcastToNeightbors(message BroadcastMsg) {
	for _, neighbor := range r.ti.Neighbors() {
		go r.broadcastToNeighbor(message, neighbor)
	}
}

func (r *rebroadcaster) broadcastToNeighbor(message BroadcastMsg, neighbor string) {
	r.node.RPC(neighbor, message, func(reply maelstrom.Message) error {
		var response map[string]any
		if err := json.Unmarshal(reply.Body, &response); err != nil {
            time.Sleep(100 * time.Millisecond)
			r.broadcastToNeighbor(message, neighbor)
		}
		if response["type"] != "broadcast_ok" {
            time.Sleep(100 * time.Millisecond)
			r.broadcastToNeighbor(message, neighbor)
		}
		return nil
	})
}

type broadcastHandler struct {
	node          *maelstrom.Node
	tracker       *Tracker
	rebroadcaster *rebroadcaster
}

func newBroadcastHandler(node *maelstrom.Node, tracker *Tracker, rebrebroadcaster *rebroadcaster) *broadcastHandler {
	return &broadcastHandler{
		node:          node,
		tracker:       tracker,
		rebroadcaster: rebrebroadcaster,
	}
}

func (bh *broadcastHandler) handleMsg(msg maelstrom.Message) error {
	var broadcastMsg BroadcastMsg
	if err := json.Unmarshal(msg.Body, &broadcastMsg); err != nil {
		return err
	}

	bh.tracker.AddValue(broadcastMsg.Message)
	bh.rebroadcaster.broadcastToNeightbors(broadcastMsg)

	response := map[string]any{
		"type": "broadcast_ok",
	}
	return bh.node.Reply(msg, response)
}

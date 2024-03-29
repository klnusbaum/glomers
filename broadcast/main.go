package main

import (
	"encoding/json"
	"fmt"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Tracker struct {
	mu   sync.Mutex
	seen []int
}

func NewTracker() *Tracker {
	return &Tracker{
		seen: make([]int, 0),
	}
}

func (t *Tracker) AddValue(i int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seen = append(t.seen, i)
}

func (t *Tracker) Values() []int {
	t.mu.Lock()
	defer t.mu.Unlock()
	vals := make([]int, len(t.seen))
	copy(vals, t.seen)
	return vals
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
	topology := make(map[string][]string)
	node := maelstrom.NewNode()

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var broadcastMsg BroadcastMsg
		if err := json.Unmarshal(msg.Body, &broadcastMsg); err != nil {
			return err
		}

		tracker.AddValue(broadcastMsg.Message)

		response := map[string]any{
			"type": "broadcast_ok",
		}
		return node.Reply(msg, response)
	})

	node.Handle("read", func(msg maelstrom.Message) error {
        vals := tracker.Values()
		response := ReadOkMsg{
			Type:     "read_ok",
			Messages: vals,
		}
		return node.Reply(msg, response)
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		var topologyMsg TopologyMsg
		if err := json.Unmarshal(msg.Body, &topologyMsg); err != nil {
			return err
		}

		topology = topologyMsg.Topology

		response := map[string]string{
			"type": "topology_ok",
		}
		return node.Reply(msg, response)
	})

	node.Run()
	fmt.Printf("Topology %s\n", topology)
}

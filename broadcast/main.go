package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const replicateTimeout = 500 * time.Millisecond
const typeField = "type"
const typeTopology = "topology"
const typeTopologyOk = "topology_ok"
const typeRead = "read"
const typeReadOk = "read_ok"
const typeBroadcast = "broadcast"
const typeBroadcastOk = "broadcast_ok"
const typeReplicate = "replicate"
const typeReplicateOk = "replicate_ok"

type BroadcastMsg struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type ReadOkMsg struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type ReplicateMsg struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

func main() {
	tracker := newTracker()
	node := maelstrom.NewNode()
	th := newTopoHandler(node)
	rh := newReadHandler(node, tracker)
	bh := newBroadcastHandler(node, tracker)
	reph := newReplicateHandler(node, tracker)
	node.Handle(typeTopology, th.handleMsg)
	node.Handle(typeRead, rh.handleMsg)
	node.Handle(typeBroadcast, bh.handleMsg)
	node.Handle(typeReplicate, reph.handleMsg)

	node.Run()
}

type topoHandler struct {
	node *maelstrom.Node
}

func newTopoHandler(node *maelstrom.Node) *topoHandler {
	return &topoHandler{node: node}
}

func (th *topoHandler) handleMsg(msg maelstrom.Message) error {
	response := map[string]string{
		typeField: typeTopologyOk,
	}
	return th.node.Reply(msg, response)
}

type readHandler struct {
	tracker *tracker
	node    *maelstrom.Node
}

func newReadHandler(node *maelstrom.Node, tracker *tracker) *readHandler {
	return &readHandler{
		tracker: tracker,
		node:    node,
	}
}

func (rh *readHandler) handleMsg(msg maelstrom.Message) error {
	vals := rh.tracker.values()
	response := ReadOkMsg{
		Type:     typeReadOk,
		Messages: vals,
	}
	return rh.node.Reply(msg, response)
}

type broadcastHandler struct {
	node    *maelstrom.Node
	tracker *tracker
}

func newBroadcastHandler(node *maelstrom.Node, tracker *tracker) *broadcastHandler {
	return &broadcastHandler{
		node:    node,
		tracker: tracker,
	}
}

func (bh *broadcastHandler) handleMsg(msg maelstrom.Message) error {
	var broadcastMsg BroadcastMsg
	if err := json.Unmarshal(msg.Body, &broadcastMsg); err != nil {
		return err
	}

	bh.tracker.add(broadcastMsg.Message)
	bh.replicate(broadcastMsg.Message)

	response := map[string]any{
		typeField: typeBroadcastOk,
	}
	return bh.node.Reply(msg, response)
}

func (bh *broadcastHandler) replicate(val int) {
	msg := ReplicateMsg{
		Type:    typeReplicate,
		Message: val,
	}

	me := bh.node.ID()
	for _, other := range bh.node.NodeIDs() {
		if other == me {
			continue
		}
		go bh.replicateTo(msg, other)
	}
}

func (bh *broadcastHandler) replicateTo(msg ReplicateMsg, neighbor string) {
	op := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), replicateTimeout)
		defer cancel()

		reply, err := bh.node.SyncRPC(ctx, neighbor, msg)
		var response map[string]any
		if err != nil {
			return fmt.Errorf("rpc: %s", err)
		}

		if err := json.Unmarshal(reply.Body, &response); err != nil {
			return fmt.Errorf("parse response: %s", err)
		}
		if response[typeField] != typeReplicateOk {
			return fmt.Errorf("unexpected response type: %s", response["type"])
		}
		return nil
	}

	backoff.Retry(op, backoff.NewExponentialBackOff())
}

type replicateHandler struct {
	node    *maelstrom.Node
	tracker *tracker
}

func newReplicateHandler(node *maelstrom.Node, tracker *tracker) *replicateHandler {
	return &replicateHandler{
		node:    node,
		tracker: tracker,
	}
}

func (rh *replicateHandler) handleMsg(msg maelstrom.Message) error {
	var replicateMsg ReplicateMsg
	if err := json.Unmarshal(msg.Body, &replicateMsg); err != nil {
		return err
	}

	rh.tracker.add(replicateMsg.Message)
	reply := map[string]string{
        typeField: typeReplicateOk,
	}
	if err := rh.node.Reply(msg, reply); err != nil {
		return fmt.Errorf("reply: %s", err)
	}

	return nil
}

type tracker struct {
	curValues map[int]bool
	mu        sync.RWMutex
}

func newTracker() *tracker {
	t := &tracker{
		curValues: make(map[int]bool),
	}
	return t
}

func (t *tracker) add(i int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.curValues[i] = true
}

func (t *tracker) values() []int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	vals := make([]int, 0, len(t.curValues))
	for key, val := range t.curValues {
		if val {
			vals = append(vals, key)
		}
	}
	return vals
}

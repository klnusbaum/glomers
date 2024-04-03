package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const counterKey = "counter"
const typeReadOk = "read_ok"
const typeAddOk = "add_ok"
const readTimeout = 500 * time.Millisecond
const casTimeout = 500 * time.Millisecond

type ReadOk struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

type Add struct {
	Type  string `json:"type"`
	Delta int    `json:"delta"`
}

type AddOk struct {
	Type string `json:"type"`
}

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)
    reader := newReader(kv)
	rh := newReadHandler(node, reader)
	ah := newAddHandler(node, kv, reader)

	node.Handle("read", rh.handleMsg)
	node.Handle("add", ah.handleMsg)

	node.Run()
}

type readHandler struct {
	node *maelstrom.Node
    reader *reader
}

func newReadHandler(node *maelstrom.Node, reader *reader) *readHandler {
	return &readHandler{
		node: node,
        reader: reader,
	}
}

func (h *readHandler) handleMsg(msg maelstrom.Message) error {
    val, err := h.reader.read()
    if err != nil {
        return fmt.Errorf("read counter: %s", err)
    }

	reply := ReadOk{
		Type:  typeReadOk,
		Value: val,
	}

	if err := h.node.Reply(msg, reply); err != nil {
		return fmt.Errorf("reply: %s", err)
	}
	return nil
}

type addHandler struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
    reader *reader
}

func newAddHandler(node *maelstrom.Node, kv *maelstrom.KV, reader *reader) *addHandler {
	return &addHandler{
		node: node,
		kv:   kv,
        reader: reader,
	}
}

func (h *addHandler) handleMsg(msg maelstrom.Message) error {
	var add Add
	if err := json.Unmarshal(msg.Body, &add); err != nil {
		return fmt.Errorf("parse add body: %s", err)
	}

	addOp := func() error {
		curVal, err := h.reader.read()
		if err != nil {
			return fmt.Errorf("read counter: %s", err)
		}

		newVal := curVal + add.Delta
		ctx, cancel := casContext()
		defer cancel()
		err = h.kv.CompareAndSwap(ctx, counterKey, curVal, newVal, true)
		if err != nil {
			return fmt.Errorf("compare and swap counter: %s", err)
		}

		return nil
	}

	if err := backoff.Retry(addOp, expBackoff()); err != nil {
		return fmt.Errorf("failed after backoff: %s", err)
	}

	reply := AddOk{
		Type: typeAddOk,
	}
	if err := h.node.Reply(msg, reply); err != nil {
		return fmt.Errorf("reply: %s", err)
	}

	return nil
}

type reader struct {
    kv *maelstrom.KV
}

func newReader(kv *maelstrom.KV) *reader {
    return &reader{
        kv: kv,
    }
}

func (r * reader) read() (int, error) {
	ctx, cancel := readCtx()
	defer cancel()
	val, err := r.kv.ReadInt(ctx, counterKey)

	var rpcError *maelstrom.RPCError
	if errors.As(err, &rpcError) && rpcError.Code == maelstrom.KeyDoesNotExist {
		val = 0
	} else if err != nil {
		return 0, fmt.Errorf("read error: %s", err)
	}

    return val, nil
}

func readCtx() (context.Context, func()) {
	return context.WithTimeout(context.Background(), readTimeout)
}

func casContext() (context.Context, func()) {
	return context.WithTimeout(context.Background(), casTimeout)
}

func expBackoff() *backoff.ExponentialBackOff {
    return backoff.NewExponentialBackOff()
}

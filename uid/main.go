package main

import (
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type LocalCounter struct {
	curCounter int
}

func (c *LocalCounter) Next() int {
	next := c.curCounter
	c.curCounter += 1
	return next
}

func main() {
	node := maelstrom.NewNode()
	localCounter := LocalCounter{}

	node.Handle("generate", func(msg maelstrom.Message) error {
		body := make(map[string]any)
		body["type"] = "generate_ok"
		body["id"] = fmt.Sprintf("%s:%d", node.ID(), localCounter.Next())

		return node.Reply(msg, body)
	})
	node.Run()
}

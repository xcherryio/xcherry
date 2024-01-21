package memberlist

import (
	"context"
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/serialx/hashring"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type EventDelegate struct {
	consistent *hashring.HashRing
}

func (d *EventDelegate) NotifyJoin(node *memberlist.Node) {
	hostPort := fmt.Sprintf("%s:%d", node.Addr.To4().String(), node.Port)
	log.Printf("EventDelegate: join %s", hostPort)

	if d.consistent == nil {
		d.consistent = hashring.New([]string{hostPort})
	} else {
		d.consistent = d.consistent.AddNode(hostPort)
	}
}

func (d *EventDelegate) NotifyLeave(node *memberlist.Node) {
	hostPort := fmt.Sprintf("%s:%d", node.Addr.To4().String(), node.Port)
	log.Printf("EventDelegate: leave %s", hostPort)

	if d.consistent != nil {
		d.consistent = d.consistent.RemoveNode(hostPort)
	}
}

func (d *EventDelegate) NotifyUpdate(node *memberlist.Node) {
	// skip
}

func wait_signal(cancel context.CancelFunc) {
	signal_chan := make(chan os.Signal, 1)
	signal.Notify(signal_chan, syscall.SIGINT)
	for {
		select {
		case s := <-signal_chan:
			log.Printf("signal %s happen", s.String())
			cancel()
		}
	}
}

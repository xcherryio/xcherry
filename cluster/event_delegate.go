// Copyright 2023 xCherryIO organization

// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/serialx/hashring"
	"log"
	"strconv"
)

type ClusterEventDelegate struct {
	consistent    *hashring.HashRing
	ServerAddress string
}

func (d *ClusterEventDelegate) NotifyJoin(node *memberlist.Node) {
	hostPort := BuildHostAddress(node)
	log.Printf("ClusterEvent JOIN %s with server address %s", hostPort, d.ServerAddress)

	if d.consistent == nil {
		d.consistent = hashring.New([]string{hostPort})
	} else {
		d.consistent = d.consistent.AddNode(hostPort)
	}
}

func (d *ClusterEventDelegate) NotifyLeave(node *memberlist.Node) {
	hostPort := BuildHostAddress(node)
	log.Printf("ClusterEvent LEAVE %s with server address %s", hostPort, d.ServerAddress)

	if d.consistent != nil {
		d.consistent = d.consistent.RemoveNode(hostPort)
	}
}

func (d *ClusterEventDelegate) NotifyUpdate(node *memberlist.Node) {
	// skip
}

func (d *ClusterEventDelegate) GetNodeFor(shardId int32) string {
	node, ok := d.consistent.GetNode(strconv.Itoa(int(shardId)))
	if !ok {
		log.Fatalf("Failed to search shardId %d", shardId)
	}
	return node
}

func BuildHostAddress(node *memberlist.Node) string {
	return fmt.Sprintf("%s:%d", node.Addr.To4().String(), node.Port)
}

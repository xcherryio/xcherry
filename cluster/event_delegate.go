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
	meta := ParseClusterDelegateMetaData(node.Meta)

	hostPort := BuildHostAddress(node)
	log.Printf("ClusterEvent JOIN %s: advertise address %s, server address %s", d.ServerAddress, hostPort, meta.ServerAddress)

	if d.consistent == nil {
		d.consistent = hashring.New([]string{meta.ServerAddress})
	} else {
		d.consistent = d.consistent.AddNode(meta.ServerAddress)
	}
}

func (d *ClusterEventDelegate) NotifyLeave(node *memberlist.Node) {
	meta := ParseClusterDelegateMetaData(node.Meta)

	hostPort := BuildHostAddress(node)
	log.Printf("ClusterEvent LEAVE %s: advertise address %s, server address %s", d.ServerAddress, hostPort, meta.ServerAddress)

	if d.consistent != nil {
		d.consistent = d.consistent.RemoveNode(meta.ServerAddress)
	}
}

func (d *ClusterEventDelegate) NotifyUpdate(node *memberlist.Node) {
	// skip
}

func (d *ClusterEventDelegate) GetServerAddressFor(shardId int32) string {
	node, ok := d.consistent.GetNode(strconv.Itoa(int(shardId)))
	if !ok {
		log.Fatalf("Failed to search shardId %d", shardId)
	}
	return node
}

func BuildHostAddress(node *memberlist.Node) string {
	return fmt.Sprintf("%s:%d", node.Addr.To4().String(), node.Port)
}

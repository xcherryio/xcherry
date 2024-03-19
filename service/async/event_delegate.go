// Copyright 2023 xCherryIO organization

// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package async

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/serialx/hashring"
	"github.com/xcherryio/xcherry/common/log"
	"strconv"
)

type ClusterEventDelegate struct {
	consistent    *hashring.HashRing
	Logger        log.Logger
	Shard         int
	ServerAddress string
	AsyncService  Service
}

func (d *ClusterEventDelegate) NotifyJoin(node *memberlist.Node) {
	meta, err := ParseClusterDelegateMetaData(node.Meta)
	if err != nil {
		d.Logger.Fatal(fmt.Sprintf("failed to parse ClusterDelegateMetaData %s", node.Meta))
	}

	hostPort := BuildHostAddress(node)
	d.Logger.Info(fmt.Sprintf("ClusterEvent JOIN %s: advertise address %s, server address %s", d.ServerAddress, hostPort, meta.ServerAddress))

	if d.consistent == nil {
		d.consistent = hashring.New([]string{meta.ServerAddress})
	} else {
		d.consistent = d.consistent.AddNode(meta.ServerAddress)
	}

	d.reBalance()
}

func (d *ClusterEventDelegate) NotifyLeave(node *memberlist.Node) {
	meta, err := ParseClusterDelegateMetaData(node.Meta)
	if err != nil {
		d.Logger.Fatal(fmt.Sprintf("failed to parse ClusterDelegateMetaData %s", node.Meta))
	}

	hostPort := BuildHostAddress(node)
	d.Logger.Info(fmt.Sprintf("ClusterEvent LEAVE %s: advertise address %s, server address %s", d.ServerAddress, hostPort, meta.ServerAddress))

	if d.consistent != nil {
		d.consistent = d.consistent.RemoveNode(meta.ServerAddress)
	}

	d.reBalance()
}

func (d *ClusterEventDelegate) NotifyUpdate(node *memberlist.Node) {
	// skip
}

func (d *ClusterEventDelegate) GetServerAddressFor(shardId int32) string {
	node, ok := d.consistent.GetNode(strconv.Itoa(int(shardId)))
	if !ok {
		d.Logger.Fatal(fmt.Sprintf("Failed to search shardId %d", shardId))
	}
	return node
}

func (d *ClusterEventDelegate) reBalance() {
	var assignedShardIds []int32

	for i := 0; i < d.Shard; i++ {
		if d.GetServerAddressFor(int32(i)) == d.ServerAddress {
			assignedShardIds = append(assignedShardIds, int32(i))
		}
	}

	d.AsyncService.ReBalance(assignedShardIds)
}

func BuildHostAddress(node *memberlist.Node) string {
	return fmt.Sprintf("%s:%d", node.Addr.To4().String(), node.Port)
}

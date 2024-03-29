// Copyright 2023 xCherryIO organization

// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package async

import (
	"context"
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/xcherryio/xcherry/common/log"
	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/config"
	"strconv"
	"strings"
)

type membership struct {
	rootCtx context.Context

	memberlistCfg *memberlist.Config

	cfg    config.Config
	logger log.Logger
}

func NewMembershipImpl(rootCtx context.Context, cfg config.Config, logger log.Logger, svc Service) Membership {
	if cfg.AsyncService.Mode != config.AsyncServiceModeCluster {
		return nil
	}

	bindAddress := cfg.AsyncService.Membership.BindAddress
	advertiseAddress := cfg.AsyncService.Membership.AdvertiseAddress
	advertiseAddressToJoin := cfg.AsyncService.Membership.AdvertiseAddressToJoin

	serverAddress := cfg.AsyncService.InternalHttpServer.Address

	bindParts := strings.Split(bindAddress, ":")
	bindPort, err := strconv.Atoi(bindParts[1])
	if err != nil {
		logger.Fatal(fmt.Sprintf("fail to get port from bind address %s", bindAddress), tag.Error(err))
	}

	advertiseParts := strings.Split(advertiseAddress, ":")
	advertisePort, err := strconv.Atoi(advertiseParts[1])
	if err != nil {
		logger.Fatal(fmt.Sprintf("fail to get port from advertise address %s", advertiseAddress), tag.Error(err))
	}

	memberlistConf := memberlist.DefaultLocalConfig()
	memberlistConf.Name = "async_" + advertiseAddress
	memberlistConf.BindAddr = bindParts[0]
	memberlistConf.BindPort = bindPort
	memberlistConf.AdvertiseAddr = advertiseParts[0]
	memberlistConf.AdvertisePort = advertisePort

	memberlistConf.Events = &ClusterEventDelegate{
		Logger:        logger,
		Shard:         cfg.Database.Shards,
		ServerAddress: serverAddress,
		AsyncService:  svc,
	}

	memberlistConf.Delegate = &ClusterDelegate{
		Meta: ClusterDelegateMetaData{
			ServerAddress: serverAddress,
		},
	}

	list, err := memberlist.Create(memberlistConf)
	if err != nil {
		logger.Fatal("fail to create member with config", tag.Error(err))
	}

	if advertiseAddressToJoin != "" {
		_, err = list.Join([]string{advertiseAddressToJoin})
		if err != nil {
			logger.Fatal(fmt.Sprintf("fail to join %s in %s", advertiseAddressToJoin, advertiseAddress), tag.Error(err))
		}
	}

	return membership{
		rootCtx: rootCtx,

		memberlistCfg: memberlistConf,

		cfg:    cfg,
		logger: logger,
	}
}

func (m membership) GetServerAddress() string {
	return m.cfg.AsyncService.InternalHttpServer.Address
}

func (m membership) GetServerAddressForShard(shardId int32) string {
	eventDelegate, ok := m.memberlistCfg.Events.(*ClusterEventDelegate)
	if !ok {
		m.logger.Fatal("failed to get delegate")
	}

	return eventDelegate.GetServerAddressFor(shardId)
}

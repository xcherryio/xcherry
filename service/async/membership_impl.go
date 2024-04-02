// Copyright 2023 xCherryIO organization

// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package async

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/xcherryio/xcherry/common/log"
	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/config"
	"strconv"
	"strings"
)

const (
	ServerTypeApi   = "api"
	ServerTypeAsync = "async"
)

type membership struct {
	memberlistCfg *memberlist.Config

	serverType    string
	serverAddress string

	cfg    config.Config
	logger log.Logger
}

func NewMembershipImpl(cfg config.Config, logger log.Logger, asyncService *Service, serverType string) Membership {
	if serverType == ServerTypeApi && cfg.Membership == nil {
		return nil
	}
	if serverType == ServerTypeAsync && cfg.AsyncService.Mode != config.AsyncServiceModeCluster {
		return nil
	}

	bindAddress := cfg.Membership.BindAddress
	advertiseAddress := cfg.Membership.AdvertiseAddress
	advertiseAddressToJoin := cfg.Membership.AdvertiseAddressToJoin

	serverAddress := ""
	if serverType == ServerTypeApi {
		serverAddress = cfg.ApiService.HttpServer.Address
	}
	if serverType == ServerTypeAsync {
		serverAddress = cfg.AsyncService.InternalHttpServer.Address
	}

	if !strings.HasPrefix(serverAddress, "http") {
		serverAddress = "http://" + serverAddress
	}

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
	memberlistConf.Name = serverType + "_" + advertiseAddress
	memberlistConf.BindAddr = bindParts[0]
	memberlistConf.BindPort = bindPort
	memberlistConf.AdvertiseAddr = advertiseParts[0]
	memberlistConf.AdvertisePort = advertisePort

	memberlistConf.Events = &ClusterEventDelegate{
		Logger:        logger,
		Shard:         cfg.Database.Shards,
		ServerAddress: serverAddress,
		AsyncService:  asyncService,
	}

	memberlistConf.Delegate = &ClusterDelegate{
		Meta: ClusterDelegateMetaData{
			ServerType:    serverType,
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
		memberlistCfg: memberlistConf,
		serverType:    serverType,
		serverAddress: serverAddress,
		cfg:           cfg,
		logger:        logger,
	}
}

func (m membership) GetServerAddress() string {
	return m.serverAddress
}

func (m membership) GetAsyncServerAddressForShard(shardId int32) string {
	eventDelegate, ok := m.memberlistCfg.Events.(*ClusterEventDelegate)
	if !ok {
		m.logger.Fatal("failed to get delegate")
	}

	return eventDelegate.GetAsyncServerAddressFor(shardId)
}

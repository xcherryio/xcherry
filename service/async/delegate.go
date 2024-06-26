// Copyright 2023 xCherryIO organization

// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package async

import (
	"encoding/json"
)

type ClusterDelegate struct {
	Meta ClusterDelegateMetaData
}

func (d *ClusterDelegate) NodeMeta(limit int) []byte {
	return d.Meta.Bytes()
}
func (d *ClusterDelegate) LocalState(join bool) []byte {
	// not use, noop
	return []byte("")
}
func (d *ClusterDelegate) NotifyMsg(msg []byte) {
	// not use
}
func (d *ClusterDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	// not use, noop
	return nil
}
func (d *ClusterDelegate) MergeRemoteState(buf []byte, join bool) {
	// not use
}

type ClusterDelegateMetaData struct {
	ServerType    string
	ServerAddress string
}

func (m ClusterDelegateMetaData) Bytes() []byte {
	data, err := json.Marshal(m)
	if err != nil {
		return []byte("")
	}
	return data
}

func ParseClusterDelegateMetaData(data []byte) (ClusterDelegateMetaData, error) {
	meta := ClusterDelegateMetaData{}

	err := json.Unmarshal(data, &meta)
	if err != nil {
		return ClusterDelegateMetaData{}, err
	}
	return meta, err
}

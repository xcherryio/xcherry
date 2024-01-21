package memberlist

import (
	"context"
	"github.com/hashicorp/memberlist"
)

func NewMember(name string, bindPort int, advertisePort int, addrToJoin string) (*EventDelegate, error) {
	conf := memberlist.DefaultLocalConfig()
	conf.Name = name
	conf.BindPort = bindPort
	conf.AdvertisePort = advertisePort
	conf.Events = new(EventDelegate)

	list, err := memberlist.Create(conf)
	if err != nil {
		return nil, err
	}

	_, err = list.Join([]string{addrToJoin})
	if err != nil {
		return nil, err
	}

	_, cancel := context.WithCancel(context.TODO())
	go wait_signal(cancel)

	return conf.Events.(*EventDelegate), nil
}

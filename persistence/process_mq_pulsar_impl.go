package persistence

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/xdblab/xdb/config"
)

type processMQPulsar struct {
	cfg      config.Config
	prcOrm   ProcessORM
	consumer pulsar.Consumer
	client   pulsar.Client
}

func NewPulsarProcessMQ(cfg config.Config, prcOrm ProcessORM) ProcessMQ {
	return &processMQPulsar{
		cfg:    cfg,
		prcOrm: prcOrm,
	}
}

func (p processMQPulsar) Start(prcOrm ProcessORM, cfg config.Config) error {
	pulsarCfg := cfg.AsyncService.MessageQueue.Pulsar
	client, err := pulsar.NewClient(pulsarCfg.PulsarClientOptions)
	if err != nil {
		return err
	}
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            pulsarCfg.CDCTopic,
		SubscriptionName: pulsarCfg.DefaultCDCTopicSubscription,
		Type:             pulsar.Shared,
	})
	if err != nil {
		return err
	}
	p.client = client
	p.consumer = consumer
	// processing logic in a goroutine
	
	return nil
}

func (p processMQPulsar) Stop() error {

	p.consumer.Close()
	p.client.Close()
	return nil
}

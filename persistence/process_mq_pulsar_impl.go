package persistence

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
)

type processMQPulsar struct {
	cfg      config.Config
	prcOrm   ProcessORM
	consumer pulsar.Consumer
	client   pulsar.Client
	stopCh   chan struct{}
	logger   log.Logger
}

func NewPulsarProcessMQ(cfg config.Config, prcOrm ProcessORM, logger log.Logger) ProcessMQ {
	return &processMQPulsar{
		cfg:    cfg,
		prcOrm: prcOrm,
		stopCh: make(chan struct{}),
		logger: logger,
	}
}

func (p processMQPulsar) Start() error {
	pulsarCfg := p.cfg.AsyncService.MessageQueue.Pulsar
	client, err := pulsar.NewClient(pulsarCfg.PulsarClientOptions)
	if err != nil {
		return err
	}
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		TopicsPattern:    pulsarCfg.CDCTopicsPrefix + ".*",
		SubscriptionName: pulsarCfg.DefaultCDCTopicSubscription,
		Type:             pulsar.Shared,
	})
	if err != nil {
		return err
	}
	p.client = client
	p.consumer = consumer
	// processing logic in a goroutine
	go p.processMessages()
	return nil
}

func (p processMQPulsar) Stop() error {
	close(p.stopCh)
	p.consumer.Close()
	p.client.Close()
	return nil
}

func (p processMQPulsar) processMessages() {
	msgCh := p.consumer.Chan()
	for {
		select {
		case msg, ok := <-msgCh:
			if !ok {
				p.logger.Info("message channel is closed")
				return
			}
			p.logger.Info("test, test received value",
				tag.ID(msg.Message.ID().String()),
				tag.Key(msg.Message.Key()),
				tag.Value(string(msg.Message.Payload())))
			err := p.consumer.Ack(msg)
			if err != nil {
				p.logger.Error("failed to ack the message after processing",
					tag.Error(err),
					tag.ID(msg.Message.ID().String()),
					tag.Key(msg.Message.Key()),
					tag.Value(string(msg.Message.Payload())))
			}
		case <-p.stopCh:
			p.logger.Info("message processor is closed")
			return
		}
	}
}

package persistence

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	"time"
)

type processMQPulsar struct {
	cfg      config.Config
	prcOrm   ProcessORM
	consumer pulsar.Consumer
	client   pulsar.Client
	logger   log.Logger
	rootCtx  context.Context
}

func NewPulsarProcessMQ(rootCtx context.Context, cfg config.Config, prcOrm ProcessORM, logger log.Logger) ProcessMQ {
	return &processMQPulsar{
		cfg:     cfg,
		prcOrm:  prcOrm,
		logger:  logger,
		rootCtx: rootCtx,
	}
}

func (p processMQPulsar) Start() error {
	pulsarCfg := p.cfg.AsyncService.MessageQueue.Pulsar
	client, err := pulsar.NewClient(pulsarCfg.PulsarClientOptions)
	if err != nil {
		return err
	}
	var consumer pulsar.Consumer
	for i := 0; i < 60; i++ {
		// using a loop with wait because there is a racing condition
		// where the topics are not created yet when subscribing.
		// The topics are created by CDC connector
		consumer, err = client.Subscribe(pulsar.ConsumerOptions{
			Topics: []string{
				pulsarCfg.CDCTopicsPrefix + "xdb_sys_process_executions",
			},
			SubscriptionName: pulsarCfg.DefaultCDCTopicSubscription,
			Type:             pulsar.Shared,
		})
		if err == nil {
			break
		} else {
			p.logger.Warn("error on subscribing, maybe topics are not ready? wait and retry", tag.Error(err))
			time.Sleep(time.Second)
		}
	}

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
		case <-p.rootCtx.Done():
			p.logger.Info("message processor is being closed")
			return
		}
	}
}

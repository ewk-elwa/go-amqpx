package main

import (
	"net"
	"time"

	"github.com/ewk-elwa/go-amqpx/amqpx"
)

type amqpConnInfo struct {
	openParams  amqpx.ConnectionParameters
	session     amqpx.SessionParameters
	attach      amqpx.AttachParameters
	disposition amqpx.DispositionParameters
	unsettled   map[uint32]uint32
	flow        amqpx.FlowParameters
	transfer    amqpx.TransferParameters
	// Stats
	messageCount uint64
}

type amqpClient struct {
	conn        net.Conn
	containerID string
	hostname    string
	channelMax  uint16
	idleTimeout uint32
	readTimeout time.Duration
	rx          amqpConnInfo
	tx          amqpConnInfo
}

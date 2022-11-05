package main

import (
	"encoding/binary"
)

// AMQP 1.0 Values
const (
	amqpOpen        byte = 0x10
	amqpBegin       byte = 0x11
	amqpAttach      byte = 0x12
	amqpFlow        byte = 0x13
	amqpTransfer    byte = 0x14
	amqpDisposition byte = 0x15
	amqpDetach      byte = 0x16
	amqpEnd         byte = 0x17
	amqpClose       byte = 0x18
)

type amqpVersionStruct struct {
	protocol   [4]byte
	protocolID byte
	major      byte
	minor      byte
	revision   byte
}

const sizeAmqpVersionStruct = 8

var amqpVersion0100 = amqpVersionStruct{protocol: [4]byte{0x41, 0x4d, 0x51, 0x50}, protocolID: 0, major: 1, minor: 0, revision: 0}

type amqpHeaderStruct struct {
	length     uint32
	doff       byte
	theType    byte
	theChannel uint16
	perfTag    uint16
	perfValue  byte
}

const sizeAmqpHeaderStruct = 11

const (
	doff    byte   = 2
	theType byte   = 0
	perfTag uint16 = 83
)

type amqpArgumentsStruct struct {
	unknownField     [9]byte
	a1TagContainerID byte
	containerLength  byte
	containerID      string
	a1TagHostname    byte
	hostnameLength   byte
	hostname         string
	tagChannelMax    [2]byte
	channelMax       uint16
	tagIdleTimeout   byte
	idleTimeout      uint32
}

const fixedSizeOfArgumentsStruct = 13
const a1TagContainerID byte = 0xa1
const a1TagHostname byte = 0xa1
const tagIdleTimeout byte = 0x70

var unknownArgumentsField = [9]byte{0xd0, 0x00, 0x00, 0x00, 0x39, 0x00, 0x00, 0x00, 0x0a}
var tagChannelMax = [2]byte{0x40, 0x60}

func serializeArgumentsStruct(arg amqpArgumentsStruct) []byte {
	length := len(unknownArgumentsField) + fixedSizeOfArgumentsStruct + len(arg.containerID) + len(arg.hostname)
	buf := make([]byte, length)
	copy(buf, arg.unknownField[:])
	inx := len(arg.unknownField)
	buf[inx] = a1TagContainerID
	inx++
	buf[inx] = arg.containerLength
	inx++
	copy(buf[inx:], []byte(arg.containerID))
	inx += len(arg.containerID)
	buf[inx] = a1TagHostname
	inx++
	buf[inx] = arg.hostnameLength
	inx++
	copy(buf[inx:], arg.hostname)
	inx += len(arg.hostname)
	copy(buf[inx:], tagChannelMax[:])
	inx += 2
	binary.BigEndian.PutUint16(buf[inx:], arg.channelMax)
	inx += 2
	buf[inx] = tagIdleTimeout
	inx++
	binary.BigEndian.PutUint32(buf[inx:], arg.idleTimeout)
	return buf
}

var amqpEndFrame = []byte{0x40, 0x40, 0x40, 0x40, 0x40}

// constants for amqpHeaderBlock
const (
	indexDoff       = 4
	sizeDoff        = 1
	indexTheType    = 5
	sizeTheType     = 1
	indexTheChannel = 6
	sizeTheChannel  = 2
	indexPerfTag    = 8
	sizePerfTag     = 2
	inxPerfValue    = 10
	sizePerfValue   = 1
	headerSize      = 11
)

var argumentTag = [2]byte{0x00, 0x53}

const (
	sizeContainerLength      = 1
	hostnameTag         byte = 0xa1
	sizeHostnameTag          = 1
	sizeHostname             = 1
)

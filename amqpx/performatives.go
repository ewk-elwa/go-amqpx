package amqpx

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	log "github.com/mgutz/logxi/v1"
)

// ProtoocolVersion ... gathered in the Protocol header message
type ProtoocolVersion struct {
	Protocol   [4]byte `json:"protocol"`
	ProtocolId byte    `json:"protocolId"`
	Major      byte    `json:"major"`
	Minor      byte    `json:"minor"`
	Revision   byte    `json:"revision"`
}

const (
	PerfOpen        byte = 0x10
	PerfBegin       byte = 0x11
	PerfAttach      byte = 0x12
	PerfFlow        byte = 0x13
	PerfTransfer    byte = 0x14
	PerfDisposition byte = 0x15
	PerfHeader      byte = 0x70
	PerfProperties  byte = 0x73
	PerfAmqpValue   byte = 0x77
)

const (
	szFrameHeader = 8
	szInt32       = 4
	szInt16       = 2
	szByte        = 1
)

var amqp100 = []byte{0x41, 0x4d, 0x51, 0x50, 0x00, 0x01, 0x00, 0x00}

// SerializePerformative a List from given buffers
func SerializePerformative(performative byte, bufs ...[]byte) (retBuf []byte) {
	listCount := len(bufs)
	listSize := 0
	for _, buf := range bufs {
		listSize += len(buf)
	}
	retBuf = make([]byte, listSize+5+4) // 5: 0xd0-list code plus 4byte for items count
	retBuf[0] = 0xd0
	binary.BigEndian.PutUint32(retBuf[1:], uint32(performative))
	binary.BigEndian.PutUint32(retBuf[5:], uint32(listCount))
	inx := 9
	for _, buf := range bufs {
		copy(retBuf[inx:], buf)
		inx += len(buf)
	}
	return retBuf
}

// ParseProtocolHeader reads the Protocol version
func ParseProtocolHeader(buffer []byte) (protocolVersion ProtoocolVersion, bytesUsed uint32, err error) {
	bytesUsed = 0
	if len(buffer) < szFrameHeader {
		return protocolVersion, bytesUsed, errors.New("amqllib: Protocol header does not include enough data")
	}

	for i, v := range amqp100 {
		if v != buffer[i] {
			return protocolVersion, bytesUsed, errors.New("amqpx Protocol version mismatch")
		}
	}

	copy(protocolVersion.Protocol[:], buffer[0:4])
	protocolVersion.ProtocolId = buffer[4]
	protocolVersion.Major = buffer[5]
	protocolVersion.Minor = buffer[6]
	protocolVersion.Revision = buffer[7]
	bytesUsed = szFrameHeader
	return protocolVersion, bytesUsed, nil
}

// Frame ... gather in the beginning of a frame
type Frame struct {
	Size     uint32 `json:"size"`
	Doff     byte   `json:"Doff"`
	TypeCode byte   `json:"typeCode"`
	Channel  uint16 `json:"Channel"`
}

// ParseFraming ... reads the frame header
func ParseFraming(buffer []byte) (frame Frame, bytesUsed uint32, performative byte, err error) {
	bytesUsed = 0
	performative = 0
	inx := uint32(0)
	// Read required header: fixed 8 bytes
	if len(buffer) < szFrameHeader {
		return frame, bytesUsed, performative, errors.New("amqpx: not enough buffer for frame header")
	}

	frame.Size = binary.BigEndian.Uint32(buffer[inx:])
	inx += szInt32
	if frame.Size < szFrameHeader {
		return frame, bytesUsed, performative, errors.New("amqpx: Size can not be less than the Size of require frame header: 8")
	}

	frame.Doff = buffer[inx]
	inx++
	if frame.Doff < 2 {
		return frame, bytesUsed, performative, errors.New("amqpx: Doff can not be less than '2' the Size of require frame header: 2x4")
	}

	frame.TypeCode = buffer[inx]
	inx++
	if frame.TypeCode != 0 {
		return frame, bytesUsed, performative, errors.New("amqpx: we only Handle AMQP types for now")
	}

	frame.Channel = binary.BigEndian.Uint16(buffer[inx:])
	inx += szInt16
	// nothing To check with Channel

	// Skipping ..Read optional extended header: variable
	fmt.Printf("Checking buffer[%d] for ParseBlockType\n", inx)
	performative, used, _ := ParseBlockType(buffer[inx:])
	inx += used

	fmt.Printf("performative is 0x% x used:%d\n", performative, used)

	// Read optional frame body: variable
	// Returning the bytesUsed so the framebody can get handled there
	//bytesUsed = 4 * uint32(frame.Doff)
	bytesUsed = inx
	return frame, bytesUsed, performative, nil
}

func displayJsonStruct(v interface{}) {
	jsonData, _ := json.Marshal(v)
	log.Debug(string(jsonData))
}

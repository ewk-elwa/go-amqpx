package amqpx

import (
	"errors"

	log "github.com/mgutz/logxi/v1"
)

// ConnectionParameters .. gathered in the Open performative
// Spec section 2.7.1 Open
//
//	<type name="open" class="composite" Source="list" provides="frame">
//	<descriptor name="amqp:open:list" code="0x00000000:0x00000010"/>
//	<field name="container-id" type="string" mandatory="true"/>
//	<field name="hostname" type="string"/>
//	<field name="max-frame-Size" type="uint" default="4294967295"/>
//	<field name="Channel-max" type="ushort" default="65535"/>
//	<field name="idle-time-out" type="milliseconds"/>
//	<field name="outgoing-locales" type="ietf-language-tag" multiple="true"/>
//	<field name="incoming-locales" type="ietf-language-tag" multiple="true"/>
//	<field name="offered-capabilities" type="symbol" multiple="true"/>
//	<field name="desired-capabilities" type="symbol" multiple="true"/>
//	<field name="properties" type="fields"/>
//	</type>
type ConnectionParameters struct {
	ContainerId   string `json:"containerId"`
	Hostname      string `json:"hostname"`
	MaxFrameSize  uint32 `json:"maxFrameSize"`
	ChannelMax    uint16 `json:"channelMax"`
	IdleTimeoutMs uint32 `json:"idleTimeoutMs"`
	//outgoingLocales string // ietf-language-tag
	//incomingLocales string // ietf-language-tag
	//offeredCapabilities ?? // symbol
	//desiredCapabilities ?? // symbol
	//properties ?? // fields
}

// Serialize a connection parameter block
func (connParameters ConnectionParameters) Serialize() (buf []byte) {
	// list primitive

	// serialize containerId string
	buf1 := SerializeStringPrimitive(connParameters.ContainerId)

	// serialize hostname string
	buf2 := SerializeStringPrimitive(connParameters.Hostname)

	// Set max-frame-Size To null ... optional
	buf3 := make([]byte, 1)
	buf3[0] = nullCode

	// channelMax Tag
	// serialize channelMax
	buf4 := SerializeUshortPrimitive(connParameters.ChannelMax)

	// idleTimeout Tag
	// serialize idleTimeout
	buf5 := SerializeUintPrimitive(connParameters.IdleTimeoutMs)

	return SerializeList(buf1, buf2, buf3, buf4, buf5)
}

// ParsePerformativeOpen reads a open performative from buffer.
func ParsePerformativeOpen(buffer []byte) (connParameters ConnectionParameters, inx uint32, err error) {
	err = nil
	inx = uint32(0)
	advanceInx := uint32(0)
	// header

	_, countItems, inxFirstItem, advanceInx, err := ParseListPrimitive(buffer[inx:])
	if err != nil {
		return connParameters, inx, errors.New(err.Error() + "\nReadOpenPerformative() failed compound list")
	}
	inx += advanceInx
	if inxFirstItem != advanceInx {
		log.Debug(" Strange! inxFirstItem not equal advanceInx")
	}

	connParameters.ContainerId, advanceInx, err = ParseStringPrimitive(buffer[inx:])
	if err != nil {
		return connParameters, inx, errors.New(err.Error() + "\nReadOpenPerformative() failed reading container id from list")
	}
	inx += advanceInx
	countItems--

	advanceInx = 0
	// Hostname is optional field , can be nullcode
	if buffer[inx] != nullCode {
		connParameters.Hostname, advanceInx, err = ParseStringPrimitive(buffer[inx:])
		if err != nil {
			return connParameters, inx, errors.New(err.Error() + "\nReadOpenPerformative() failed reading Hostname from list")
		}
		inx += advanceInx
	} else {
		inx++
	}
	countItems--

	advanceInx = 0
	// MaxFrameSize is optional field , can be nullcode
	if buffer[inx] != nullCode {
		connParameters.MaxFrameSize, advanceInx, err = ParseUintPrimitive(buffer[inx:])
		if err != nil {
			return connParameters, inx, errors.New(err.Error() + "\nReadOpenPerformative() failed reading MaxFrameSize from list")
		}
		inx += advanceInx
	} else {
		inx++
		//log.Debug("skipping MaxFrameSize .. is nullCode inx:", inx)
	}
	countItems--

	advanceInx = 0
	// ChannelMax is optional field , can be nullcode
	if buffer[inx] != nullCode {
		connParameters.ChannelMax, advanceInx, err = PraseUshortPrimitive(buffer[inx:])
		if err != nil {
			return connParameters, inx, errors.New(err.Error() + "\nReadOpenPerformative() failed reading ChannelMax from list")
		}
		inx += advanceInx
	} else {
		inx++
		//log.Debug("skipping ChannelMax .. is nullCode inx:", inx)
	}
	countItems--

	advanceInx = 0
	// IdleTimeoutMs is optional field , can be nullcode
	if buffer[inx] != nullCode {
		connParameters.IdleTimeoutMs, advanceInx, err = ParseUintPrimitive(buffer[inx:])
		if err != nil {
			return connParameters, inx, errors.New(err.Error() + "\nReadOpenPerformative() failed reading IdleTimeoutMs from list")
		}
		inx += advanceInx
	} else {
		inx++
		//log.Debug("skipping IdleTimeoutMs .. is nullCode inx:", inx)
	}
	countItems--

	// TODO: process { outgoing, incoming, offered, desired and properties ... expecting null}
	if buffer[inx] == nullCode {
		inx++
		countItems--
	}
	if buffer[inx] == nullCode {
		inx++
		countItems--
	}
	if buffer[inx] == nullCode {
		inx++
		countItems--
	}
	if buffer[inx] == nullCode {
		inx++
		countItems--
	}
	if buffer[inx] == nullCode {
		inx++
		countItems--
	}

	if countItems != 0 {
		log.Debug("Were all items read: left with countItem: ", countItems)
	}

	return connParameters, inx, nil
}

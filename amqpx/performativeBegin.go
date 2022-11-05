package amqpx

import (
	"errors"

	log "github.com/mgutz/logxi/v1"
)

// SessionParameters .. gathered in the Begin performative
// <type name="begin" class="composite" Source="list" provides="frame">
//
//	<descriptor name="amqp:begin:list" code="0x00000000:0x00000011"/>
//	<field name="remote-Channel" type="ushort"/>
//	<field name="next-outgoing-id" type="transfer-number" mandatory="true"/>
//	<field name="incoming-window" type="uint" mandatory="true"/>
//	<field name="outgoing-window" type="uint" mandatory="true"/>
//	<field name="Handle-max" type="Handle" default="4294967295"/>
//	<field name="offered-capabilities" type="symbol" multiple="true"/>
//	<field name="desired-capabilities" type="symbol" multiple="true"/>
//	<field name="properties" type="fields"/>
//
// </type>
type SessionParameters struct {
	RemoteChannel  uint16     `json:"remoteChannel,omitempty"` //optional
	NextOutgoing   SequenceNo `json:"nextOutgoing"`            // mandatory
	IncomingWindow uint32     `json:"incomingWindow"`          // mandatory
	OutgoingWindow uint32     `json:"outgoingWindow"`          //mandatory
	//HandleMax      uint32     // default 4294967295
	//offeredCapabilities ?? // symbol
	//desiredCapabilities ?? // symbol
	//properties ?? // fields
}

// Serialize a session parameter block for BEGIN performative
func (session SessionParameters) Serialize() (buf []byte) {
	remoteChannel := SerializeNullPrimitive()
	nextOutgoing := SerializeSequenceNoPrimitive(session.NextOutgoing)
	incomingWindow := SerializeUintPrimitive(session.IncomingWindow)
	outgoingWindow := SerializeUintPrimitive(session.OutgoingWindow)

	return SerializePerformative(PerfBegin, remoteChannel, nextOutgoing, incomingWindow, outgoingWindow)
}

// ParsePerformativeBegin reads a open performative from buffer.
func ParsePerformativeBegin(buffer []byte) (sessionParameters SessionParameters, inx uint32, err error) {
	err = nil
	inx = uint32(0)
	advanceInx := uint32(0)
	// header

	_, countItems, _, advanceInx, err := ParseListPrimitive(buffer[inx:])
	if err != nil {
		return sessionParameters, inx, errors.New(err.Error() + "\nReadBeginPerformative() failed compound list")
	}
	inx += advanceInx
	advanceInx = 0

	// remote-Channel is optional field , can be nullcode
	if buffer[inx] != nullCode {
		sessionParameters.RemoteChannel, advanceInx, err = PraseUshortPrimitive(buffer[inx:])
		if err != nil {
			return sessionParameters, inx, errors.New(err.Error() + "\nReadOpenPerformative() failed reading RemoteChannel from list")
		}
		inx += advanceInx
		advanceInx = 0
	} else {
		inx++
		//log.Debug("skipping RemoteChannel .. is nullCode inx:", inx)
	}
	countItems--

	sessionParameters.NextOutgoing, advanceInx, err = ParseSequenceNoPrimitive(buffer[inx:])
	if err != nil {
		return sessionParameters, inx, errors.New(err.Error() + "\nReadOpenPerformative() failed reading nextoutgoing from list")
	}
	inx += advanceInx
	advanceInx = 0
	countItems--

	sessionParameters.IncomingWindow, advanceInx, err = ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return sessionParameters, inx, errors.New(err.Error() + "\nReadOpenPerformative() failed reading IncomingWindow from list")
	}
	inx += advanceInx
	advanceInx = 0
	countItems--

	sessionParameters.OutgoingWindow, advanceInx, err = ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return sessionParameters, inx, errors.New(err.Error() + "\nReadOpenPerformative() failed reading OutgoingWindow from list")
	}
	inx += advanceInx
	advanceInx = 0
	countItems--

	if countItems != 0 {
		log.Debug("Were all items read: left with countItem: ", countItems)
	}

	return sessionParameters, inx, nil
}

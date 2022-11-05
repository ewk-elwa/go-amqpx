package amqpx

import (
	"errors"

	log "github.com/mgutz/logxi/v1"
)

// Milliseconds Source is uint32 : ParseUintPrimitive
type Milliseconds uint32

// MessageHeader is a message format
// <type Name="header" class="composite" Source="list" provides="section">
//
//	<descriptor Name="amqp:header:list" code="0x00000000:0x00000070"/>
//	<field Name="Durable" type="boolean"/>
//	<field Name="Priority" type="ubyte"/>
//	<field Name="Ttl" type="milliseconds"/>
//	<field Name="First-acquirer" type="boolean"/>
//	<field Name="delivery-count" type="uint"/>
//
// </type>
type MessageHeader struct {
	Durable       BooleanChoice `json:"Durable"`
	Priority      byte          `json:"priority"`
	Ttl           Milliseconds  `json:"ttl"`
	FirstAcquirer BooleanChoice `json:"firstAcquirer"`
	DeliveryCount uint32        `json:"deliveryCount"`
}

// ParseMessageHeader message header after transport.
func ParseMessageHeader(buffer []byte) (header MessageHeader, bytesUsed uint32, err error) {
	err = nil
	inx := uint32(0)
	bytesUsed = uint32(0)
	advanceInx := uint32(0)

	// header
	_, countItems, _, advanceInx, err := ParseListPrimitive(buffer[inx:])
	if err != nil {
		return header, bytesUsed, errors.New(err.Error() + "\nReadMessageHeader() failed compound list")
	}
	inx += advanceInx
	advanceInx = 0
	countItems--

	durable, advanceInx, err := ParseBooleanPrimitive(buffer[inx:])
	if err != nil {
		return header, bytesUsed, errors.New(err.Error() + "\nReadMessageHeader() failed reading Durable")
	}
	header.Durable = BooleanChoice(durable)
	log.Debug("header.Durable:", header.Durable)
	inx += advanceInx
	advanceInx = 0
	countItems--

	header.Priority, advanceInx, err = ParseUbytePrimitive(buffer[inx:])
	if err != nil {
		return header, bytesUsed, errors.New(err.Error() + "\nReadMessageHeader() failed reading Priority")
	}
	log.Debug("header.Priority:", header.Priority)
	inx += advanceInx
	advanceInx = 0
	countItems--

	ttl, advanceInx, err := ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return header, bytesUsed, errors.New(err.Error() + "\nReadMessageHeader() failed reading Ttl")
	}
	header.Ttl = Milliseconds(ttl)
	log.Debug("header.Ttl:", header.Ttl)
	inx += advanceInx
	advanceInx = 0
	countItems--

	firstAcquirer, advanceInx, err := ParseBooleanPrimitive(buffer[inx:])
	if err != nil {
		return header, bytesUsed, errors.New(err.Error() + "\nReadMessageHeader() failed reading FirstAcquirer")
	}
	header.FirstAcquirer = BooleanChoice(firstAcquirer)
	log.Debug("header.FirstAcquirer:", header.FirstAcquirer)
	inx += advanceInx
	advanceInx = 0
	countItems--

	header.DeliveryCount, advanceInx, err = ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return header, bytesUsed, errors.New(err.Error() + "\nReadMessageHeader() failed reading DeliveryCount")
	}
	log.Debug("header.DeliveryCount:", header.DeliveryCount)
	inx += advanceInx
	advanceInx = 0
	countItems--

	bytesUsed = inx
	return header, bytesUsed, nil

}

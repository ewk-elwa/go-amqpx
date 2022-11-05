package amqpx

import (
	"errors"

	log "github.com/mgutz/logxi/v1"
)

// DeliveryNumber Source is SequenceNo : ParseSequenceNoPrimitive
type DeliveryNumber SequenceNo

// DeliveryTag Source is Binary8 or Binary32 : ParseBinaryPrimitive
type DeliveryTag []byte

// DeliveryState is the delivery State byte
type DeliveryState byte

// MessageFormat Source is uintCode : ParseUintPrimitive()
type MessageFormat uint32

// TransferParameters .. gathered in the Transfer performative
// <type Name="transfer" class="composite" Source="list" provides="frame">
//
//	<descriptor Name="amqp:transfer:list" code="0x00000000:0x00000014"/>
//	<field Name="Handle" type="Handle" mandatory="true"/>
//	<field Name="delivery-id" type="delivery-number"/>
//	<field Name="delivery-tag" type="delivery-tag"/>
//	<field Name="message-format" type="message-format"/>
//	<field Name="Settled" type="boolean"/>
//	<field Name="More" type="boolean" default="false"/>
//	<field Name="rcv-settle-mode" type="receiver-settle-mode"/>
//	<field Name="State" type="*" requires="delivery-State"/>
//	<field Name="Resume" type="boolean" default="false"/>
//	<field Name="Aborted" type="boolean" default="false"/>
//	<field Name="Batchable" type="boolean" default="false"/>
//
// </type>
type TransferParameters struct {
	Handle        Handle                   `json:"handle"`
	DeliveryId    DeliveryNumber           `json:"deliveryId"`
	DeliveryTag   DeliveryTag              `json:"deliveryTag"`
	MessageFormat MessageFormat            `json:"messageFormat"`
	Settled       BooleanChoice            `json:"settled"`
	More          BooleanChoice            `json:"more"`
	RcvSettleMode ReceiverSettleModeChoice `json:"rcvSettleMode"`
	State         DeliveryState            `json:"state"`
	Resume        BooleanChoice            `json:"resume"`
	Aborted       BooleanChoice            `json:"aborted"`
	Batchable     BooleanChoice            `json:"batchable"`
}

// ParsePerformativeTransfer reads a transfer performative from buffer.
func ParsePerformativeTransfer(buffer []byte) (transfer TransferParameters, bytesUsed uint32, err error) {
	err = nil
	inx := uint32(0)
	advanceInx, bytesUsed := uint32(0), uint32(0)
	// header

	_, countItems, _, advanceInx, err := ParseListPrimitive(buffer[inx:])
	if err != nil {
		return transfer, bytesUsed, errors.New(err.Error() + "\nReadTransferPerformative() failed compound list")
	}
	inx += advanceInx
	advanceInx = 0

	handle, advanceInx, err := ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return transfer, bytesUsed, errors.New(err.Error() + "\nReadTransferPerformative() failed reading Handle from list")
	}
	transfer.Handle = Handle(handle)
	log.Debug("transfer.Handle:", transfer.Handle)
	inx += advanceInx
	advanceInx = 0
	countItems--

	deliveryID, advanceInx, err := ParseSequenceNoPrimitive(buffer[inx:])
	if err != nil {
		return transfer, bytesUsed, errors.New(err.Error() + "\nReadTransferPerformative() failed reading DeliveryId from list")
	}
	transfer.DeliveryId = DeliveryNumber(deliveryID)
	log.Debug("transfer.DeliveryId:", transfer.DeliveryId)
	inx += advanceInx
	advanceInx = 0
	countItems--

	deliveryTag, advanceInx, err := ParseBinaryPrimitive(buffer[inx:])
	if err != nil {
		return transfer, bytesUsed, errors.New(err.Error() + "\nReadTransferPerformative() failed reading DeliveryTag from list")
	}
	transfer.DeliveryTag = deliveryTag
	log.Debug("transfer.DeliveryTag:", transfer.DeliveryTag)
	inx += advanceInx
	advanceInx = 0
	countItems--

	messageformat, advanceInx, err := ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return transfer, bytesUsed, errors.New(err.Error() + "\nReadTransferPerformative() failed reading MessageFormat from list")
	}
	transfer.MessageFormat = MessageFormat(messageformat)
	log.Debug("transfer.MessageFormat:", transfer.MessageFormat)
	inx += advanceInx
	advanceInx = 0
	countItems--

	settled, advanceInx, err := ParseBooleanPrimitive(buffer[inx:])
	if err != nil {
		return transfer, bytesUsed, errors.New(err.Error() + "\nReadTransferPerformative() failed reading Settled from list")
	}
	transfer.Settled = BooleanChoice(settled)
	log.Debug("transfer.Settled:", transfer.Settled)
	inx += advanceInx
	advanceInx = 0
	countItems--

	more, advanceInx, err := ParseBooleanPrimitive(buffer[inx:])
	if err != nil {
		return transfer, bytesUsed, errors.New(err.Error() + "\nReadTransferPerformative() failed reading 'More' from list")
	}
	transfer.More = BooleanChoice(more)
	log.Debug("transfer.More:", transfer.More)
	inx += advanceInx
	advanceInx = 0
	countItems--

	rcvSettleMode, advanceInx, err := ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return transfer, bytesUsed, errors.New(err.Error() + "\nReadTransferPerformative() failed reading RcvSettleMode from list")
	}
	transfer.RcvSettleMode = ReceiverSettleModeChoice(rcvSettleMode)
	log.Debug("transfer.RcvSettleMode:", transfer.RcvSettleMode)
	inx += advanceInx
	advanceInx = 0
	countItems--

	transfer.State, advanceInx, err = ParseDeliveryStatePrimitive(buffer[inx:])
	if err != nil {
		return transfer, bytesUsed, errors.New(err.Error() + "\nReadTransferPerformative() failed reading 'State' from list")
	}
	log.Debug("transfer.State:", transfer.State)
	inx += advanceInx
	advanceInx = 0
	countItems--

	bytesUsed = inx
	return transfer, bytesUsed, nil
}

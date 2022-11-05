package amqpx

import (
	"errors"

	log "github.com/mgutz/logxi/v1"
)

// TransferNumber is a sequenceno which is a uint
type TransferNumber SequenceNo

// FlowParameters .. gathered in the Flow performative
type FlowParameters struct {
	NextIncoming   TransferNumber `json:"nextIncoming"`
	IncomingWindow uint32         `json:"nextIncoming"`
	NextOutgoing   TransferNumber `json:"nextOutgoing"`
	OutgoingWindow uint32         `json:"outgoingWindow"`
	Handle         Handle         `json:"Handle"`
	DeliveryCount  SequenceNo     `json:"DeliveryCount"`
	LinkCredit     uint32         `json:"linkCredit"`
	Available      uint32         `json:"available"`
	Drain          BooleanChoice  `json:"drain"`
	// echo           BooleanChoice
	//properties
}

// ParsePerformativeFlow reads a flow performative from buffer.
func ParsePerformativeFlow(buffer []byte) (flowParameters FlowParameters, inx uint32, err error) {
	err = nil
	inx = uint32(0)
	advanceInx := uint32(0)
	// header

	_, countItems, _, advanceInx, err := ParseListPrimitive(buffer[inx:])
	if err != nil {
		return flowParameters, inx, errors.New(err.Error() + "\nReadFlowPerformative() failed compound list")
	}
	inx += advanceInx
	advanceInx = 0

	transferNumber, advanceInx, err := ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return flowParameters, inx, errors.New(err.Error() + "\nReadFlowPerformative() failed reading NextIncoming from list")
	}
	flowParameters.NextIncoming = TransferNumber(transferNumber)
	log.Debug("flow.NextIncoming:", flowParameters.NextIncoming)
	inx += advanceInx
	advanceInx = 0
	countItems--

	flowParameters.IncomingWindow, advanceInx, err = ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return flowParameters, inx, errors.New(err.Error() + "\nReadFlowPerformative() failed reading IncomingWindow from list")
	}
	log.Debug("flow.IncomingWindow:", flowParameters.IncomingWindow)
	inx += advanceInx
	advanceInx = 0
	countItems--

	transferNumber, advanceInx, err = ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return flowParameters, inx, errors.New(err.Error() + "\nReadFlowPerformative() failed reading NextOutgoing from list")
	}
	flowParameters.NextOutgoing = TransferNumber(transferNumber)
	log.Debug("flow.NextOutgoing:", flowParameters.NextOutgoing)
	inx += advanceInx
	advanceInx = 0
	countItems--

	flowParameters.OutgoingWindow, advanceInx, err = ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return flowParameters, inx, errors.New(err.Error() + "\nReadFlowPerformative() failed reading OutgoingWindow from list")
	}
	log.Debug("flow.OutgoingWindow:", flowParameters.OutgoingWindow)
	inx += advanceInx
	advanceInx = 0
	countItems--

	handle, advanceInx, err := ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return flowParameters, inx, errors.New(err.Error() + "\nReadFlowPerformative() failed reading Handle from list")
	}
	flowParameters.Handle = Handle(handle)
	log.Debug("flow.Handle:", flowParameters.Handle)
	inx += advanceInx
	advanceInx = 0
	countItems--

	sequenceNo, advanceInx, err := ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return flowParameters, inx, errors.New(err.Error() + "\nReadFlowPerformative() failed reading DeliveryCount from list")
	}
	flowParameters.DeliveryCount = SequenceNo(sequenceNo)
	log.Debug("flow.DeliveryCount:", flowParameters.DeliveryCount)
	inx += advanceInx
	advanceInx = 0
	countItems--

	flowParameters.LinkCredit, advanceInx, err = ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return flowParameters, inx, errors.New(err.Error() + "\nReadFlowPerformative() failed reading LinkCredit from list")
	}
	log.Debug("flow.LinkCredit:", flowParameters.LinkCredit)
	inx += advanceInx
	advanceInx = 0
	countItems--

	flowParameters.Available, advanceInx, err = ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return flowParameters, inx, errors.New(err.Error() + "\nReadFlowPerformative() failed reading Available from list")
	}
	log.Debug("flow.Available:", flowParameters.Available)
	inx += advanceInx
	advanceInx = 0
	countItems--

	choiceBoolean, advanceInx, err := ParseBooleanPrimitive(buffer[inx:])
	if err != nil {
		return flowParameters, inx, errors.New(err.Error() + "\nReadFlowPerformative() failed reading Drain from list")
	}
	flowParameters.Drain = BooleanChoice(choiceBoolean)
	log.Debug("flow.Drain:", flowParameters.Drain)
	inx += advanceInx
	advanceInx = 0
	countItems--

	// choiceBoolean, advanceInx, err = ParseBooleanPrimitive(buffer[inx:])
	// if err != nil {
	// 	return flowParameters, errors.New(err.Error() + "\nReadFlowPerformative() failed reading echo from list")
	// }
	// flowParameters.echo = BooleanChoice(choiceBoolean)
	// log.Debug("flow.echo:", flowParameters.echo)
	// inx += advanceInx
	// advanceInx = 0
	// countItems--

	return flowParameters, inx, nil
}

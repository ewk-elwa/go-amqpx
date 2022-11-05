package amqpx

import (
	"errors"
	"fmt"

	log "github.com/mgutz/logxi/v1"
)

const (
	amqpTrue  byte = 0x41
	amqpFalse byte = 0x42
	receiver       = amqpTrue
	sender         = amqpFalse
)

// RoleChoice should be either {Receiver, Sender}
type RoleChoice bool

// Handle Source is Uint : ParseUintPrimitive
type Handle uint32

const (
	unsettled byte = 0x00
	settled   byte = 0x01
	mix       byte = 0x02
	first     byte = 0x00
	second    byte = 0x01
)

// SenderSettleModeChoice should be either {Unsettled, Settled, Mixed}
type SenderSettleModeChoice byte

// ReceiverSettleModeChoice should be either { First:0, second:1}
type ReceiverSettleModeChoice byte

// TerminusDurabilityChoice should be { None:, Configuration:1, UnsettleState:2}
type TerminusDurabilityChoice byte

// TerminusExpiryPolicyChoice should be { link-detach, session-end, connection-close, never }
type TerminusExpiryPolicyChoice byte

// BooleanChoice should be { amqpTrue, amqpFalse }
type BooleanChoice bool

// SequenceNo Source is uintCode
type SequenceNo uint32

// Ulong is uint32
type Ulong uint32

// Map .. using byte for now
type Map byte

// Source is a composite list
type Source struct {
	Address      string                     `json:"address,omitempty"`
	Durable      TerminusDurabilityChoice   `json:"durable,omitempty"`
	ExpiryPolicy TerminusExpiryPolicyChoice `json:"expiryPolicy,omitempty"`
	Timeout      uint32                     `json:"timeout,omitempty"`
	Dynamic      BooleanChoice              `json:"dynamic,omitempty"`

	// dynamicNodeProperties NodeProperties // optional
	// distributionMode Symbol // optional
	// filter FilterSet // optional
	// defaultOutcome // optional
	// outcomes Symbol  // optional
	// capabilities Symbol // optional
}

// Target is a composite list
type Target struct {
	Address      string                     `json:"address,omitempty"`
	Durable      TerminusDurabilityChoice   `json:"durable,omitempty"`
	ExpiryPolicy TerminusExpiryPolicyChoice `json:"expiryPolicy,omitempty"`
	Timeout      uint32                     `json:"timeout,omitempty"`
	Dynamic      BooleanChoice              `json:"dynamic,omitempty"`

	// dynamicNodeProperties NodeProperties // optional
	// capabilities Symbol // optional
}

// AttachParameters .. gathered in the Begin performative
type AttachParameters struct {
	Name                 string                   `json:"name"`   // mandatory
	Handle               Handle                   `json:"Handle"` // mandatory
	Role                 RoleChoice               `json:"Role"`   // mandatory
	SndSettleMode        SenderSettleModeChoice   `json:"sndSettleMode,omitempty"`
	RcvSettleMode        ReceiverSettleModeChoice `json:"RcvSettleMode,omitempty"`
	Source               Source                   `json:"source"`
	Target               Target                   `json:"target"`
	Unsettled            Map                      `json:"unsettled,omitempty"`
	IncompleteUnsettled  BooleanChoice            `json:"incompleteUnsettled,omitempty"`
	InitialDeliveryCount SequenceNo               `json:"initialDeliveryCount"`
	MaxMessageSize       Ulong                    `json:"maxMessageSize"`
	// offeredCapabilities
	// desiredCapabilities
	// properties
}

// ReadSourceList reads the Source list
func ReadSourceList(buffer []byte) (source Source, bytesUsed uint32, err error) {
	bytesUsed = 0
	inx := uint32(0)

	// read compoundlist
	_, countItems, _, advanceInx, err := ParseListPrimitive(buffer[inx:])
	if err != nil {
		return source, bytesUsed, errors.New(err.Error() + "\nReadSourceList() failed compound list")
	}
	fmt.Printf("Compoundlist expects:0x% x (%d)\n", countItems, countItems)
	inx += advanceInx
	advanceInx = 0

	// Get Source list
	// sourcelist.Address is optional
	if buffer[inx] != nullCode {
		source.Address, advanceInx, err = ParseStringPrimitive(buffer[inx:])
		if err != nil {
			return source, bytesUsed, errors.New(err.Error() + "\nReadSourceList() failed reading Address from list")
		}
		inx += advanceInx
		advanceInx = 0
	} else {
		log.Debug("skipping Source.Address .. is nullCode inx:", inx)
		inx++
	}
	countItems--

	choiceSmallunit, advanceInx, err := ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return source, bytesUsed, errors.New(err.Error() + "\nReadSourceList() failed reading Durable from list")
	}
	source.Durable = TerminusDurabilityChoice(choiceSmallunit)
	inx += advanceInx
	advanceInx = 0
	countItems--

	// ExpiryPolicy is optional
	if buffer[inx] != nullCode {
		choiceByte, advanceInx, err := ParseBytePrimitive(buffer[inx:])
		if err != nil {
			return source, bytesUsed, errors.New(err.Error() + "\nReadSourceList() failed reading ExpiryPolicy from list")
		}
		source.ExpiryPolicy = TerminusExpiryPolicyChoice(choiceByte)
		inx += advanceInx
		advanceInx = 0
	} else {
		log.Debug("skipping Source.ExpiryPolicy .. is nullCode inx:", inx)
		inx++
	}
	countItems--

	source.Timeout, advanceInx, err = ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return source, bytesUsed, errors.New(err.Error() + "\nReadSourceList() failed reading Timeout from list")
	}
	inx += advanceInx
	advanceInx = 0
	countItems--

	choiceBoolean, advanceInx, err := ParseBooleanPrimitive(buffer[inx:])
	if err != nil {
		return source, bytesUsed, errors.New(err.Error() + "\nReadSourceList() failed reading Dynamic from list")
	}
	source.Dynamic = BooleanChoice(choiceBoolean)
	inx += advanceInx
	advanceInx = 0
	countItems--

	for i := countItems; i > 0; i-- {
		if buffer[inx] != nullCode {
			fmt.Printf("unexpected non-null item:0x%x inx:%d\n", buffer[inx], inx)
			inx++
		} else {
			log.Debug("skipping Source.ITEMS .. is nullCode inx:", inx)
			inx++
		}
		countItems--
	}

	log.Debug("ReadSourceList result: countItems left:", countItems)
	displayJsonStruct(source)

	bytesUsed = inx
	return source, bytesUsed, nil
}

// ReadTargetList reads the Source list
func ReadTargetList(buffer []byte) (target Target, bytesUsed uint32, err error) {
	bytesUsed = 0
	inx := uint32(0)

	// read compoundlist
	_, countItems, _, advanceInx, err := ParseListPrimitive(buffer[inx:])
	if err != nil {
		return target, bytesUsed, errors.New(err.Error() + "\nReadTargetList() failed compound list")
	}
	fmt.Printf("Compoundlist expects:0x% x (%d)\n", countItems, countItems)
	inx += advanceInx
	advanceInx = 0

	// Get Source list
	// targetList.Address is optional
	if buffer[inx] != nullCode {
		target.Address, advanceInx, err = ParseStringPrimitive(buffer[inx:])
		if err != nil {
			return target, bytesUsed, errors.New(err.Error() + "\nReadTargetList() failed reading Address from list")
		}
		inx += advanceInx
		advanceInx = 0
	} else {
		log.Debug("skipping Target.Address .. is nullCode inx:", inx)
		inx++
	}
	countItems--

	choiceSmallunit, advanceInx, err := ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return target, bytesUsed, errors.New(err.Error() + "\nReadTargetList() failed reading Durable from list")
	}
	target.Durable = TerminusDurabilityChoice(choiceSmallunit)
	inx += advanceInx
	advanceInx = 0
	countItems--

	// ExpiryPolicy is optional
	if buffer[inx] != nullCode {
		choiceByte, advanceInx, err := ParseBytePrimitive(buffer[inx:])
		if err != nil {
			return target, bytesUsed, errors.New(err.Error() + "\nReadTargetList() failed reading ExpiryPolicy from list")
		}
		target.ExpiryPolicy = TerminusExpiryPolicyChoice(choiceByte)
		inx += advanceInx
		advanceInx = 0
	} else {
		log.Debug("skipping Target.ExpiryPolicy .. is nullCode inx:", inx)
		inx++
	}
	countItems--

	target.Timeout, advanceInx, err = ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return target, bytesUsed, errors.New(err.Error() + "\nReadTargetList() failed reading Timeout from list")
	}
	inx += advanceInx
	advanceInx = 0
	countItems--

	choiceBoolean, advanceInx, err := ParseBooleanPrimitive(buffer[inx:])
	if err != nil {
		return target, bytesUsed, errors.New(err.Error() + "\nReadTargetList() failed reading Dynamic from list")
	}
	target.Dynamic = BooleanChoice(choiceBoolean)
	inx += advanceInx
	advanceInx = 0
	countItems--

	for i := countItems; i > 0; i-- {
		if buffer[inx] != nullCode {
			fmt.Printf("unexpected non-null item:0x%x inx:%d\n", buffer[inx], inx)
			inx++
		} else {
			log.Debug("skipping Source.ITEMS .. is nullCode inx:", inx)
			inx++
		}
		countItems--
	}

	log.Debug("ReadTargetList result: countItems left:", countItems)
	displayJsonStruct(target)

	bytesUsed = inx
	return target, bytesUsed, nil
}

// ParsePerformativeAttach reads a attach performative from buffer.
func ParsePerformativeAttach(buffer []byte) (attachParameters AttachParameters, inx uint32, err error) {
	err = nil
	inx = uint32(0)
	advanceInx := uint32(0)
	// header

	_, countItems, _, advanceInx, err := ParseListPrimitive(buffer[inx:])
	if err != nil {
		return attachParameters, inx, errors.New(err.Error() + "\nReadAttachPerformative() failed compound list")
	}
	inx += advanceInx
	advanceInx = 0

	attachParameters.Name, advanceInx, err = ParseStringPrimitive(buffer[inx:])
	if err != nil {
		return attachParameters, inx, errors.New(err.Error() + "\nReadAttachPerformative() failed reading Name from list")
	}
	log.Debug("attach.Name:", attachParameters.Name)
	inx += advanceInx
	advanceInx = 0
	countItems--

	handle, advanceInx, err := ParseUintPrimitive(buffer[inx:])
	if err != nil {
		return attachParameters, inx, errors.New(err.Error() + "\nReadAttachPerformative() failed reading Handle from list")
	}
	attachParameters.Handle = Handle(handle)
	fmt.Printf("attach.Handle:0x%x\n", attachParameters.Handle)
	inx += advanceInx
	advanceInx = 0
	countItems--

	role, advanceInx, err := ParseBooleanPrimitive(buffer[inx:])
	if err != nil {
		return attachParameters, inx, errors.New(err.Error() + "\nReadAttachPerformative() failed reading Role from list")
	}
	attachParameters.Role = RoleChoice(role)
	fmt.Printf("attach.Role: %v\n", attachParameters.Role)
	inx += advanceInx
	advanceInx = 0
	countItems--

	senderSettleModeChoice, advanceInx, err := ParseUbytePrimitive(buffer[inx:])
	if err != nil {
		return attachParameters, inx, errors.New(err.Error() + "\nReadAttachPerformative() failed reading SenderSettleModeChoice from list")
	}
	attachParameters.SndSettleMode = SenderSettleModeChoice(senderSettleModeChoice)
	fmt.Printf("attach.SndSettleMode:0x%x\n", attachParameters.SndSettleMode)
	inx += advanceInx
	advanceInx = 0
	countItems--

	receiverSettleModeChoice, advanceInx, err := ParseUbytePrimitive(buffer[inx:])
	if err != nil {
		return attachParameters, inx, errors.New(err.Error() + "\nReadAttachPerformative() failed reading receiverSettleModeChoice from list")
	}
	attachParameters.RcvSettleMode = ReceiverSettleModeChoice(receiverSettleModeChoice)
	fmt.Printf("attach.RcvSettleMode:0x%x\n", attachParameters.RcvSettleMode)
	inx += advanceInx
	advanceInx = 0
	countItems--

	// read expected composite 0x00
	expectedComposite, advanceInx, err := ParseRawBytePrimitive(buffer[inx:])
	if err != nil {
		return attachParameters, inx, errors.New(err.Error() + "\nReadAttachPerformative() failed reading expectComposite from list")
	}
	if expectedComposite != 0x00 {
		fmt.Printf("expectedComposite[%d] not 0x00: 0x%x\n", inx, expectedComposite)
	}
	inx += advanceInx
	advanceInx = 0

	// read expected ulong = 0x28
	expectedUlong, advanceInx, err := ParseUlongPrimitive(buffer[inx:])
	if err != nil {
		return attachParameters, inx, errors.New(err.Error() + "\nReadAttachPerformative() failed reading expectedUlong from list")
	}
	if expectedUlong != 0x28 {
		fmt.Printf("expectedUlong[%d] not 0x28: 0x%x\n", inx, expectedUlong)
	}
	inx += advanceInx
	advanceInx = 0

	// read expected Source type0x28
	attachParameters.Source, advanceInx, err = ReadSourceList(buffer[inx:])
	if err != nil {
		return attachParameters, inx, errors.New(err.Error() + "\nReadAttachPerformative() failed reading SourceList")
	}
	inx += advanceInx
	advanceInx = 0
	countItems--

	// read expected composite 0x00
	expectedComposite, advanceInx, err = ParseRawBytePrimitive(buffer[inx:])
	if err != nil {
		return attachParameters, inx, errors.New(err.Error() + "\nReadAttachPerformative() failed reading expectComposite from list")
	}
	if expectedComposite != 0x00 {
		fmt.Printf("expectedComposite[%d] not 0x00: 0x%x\n", inx, expectedComposite)
	}
	inx += advanceInx
	advanceInx = 0

	// read expected ulong = 0x29
	expectedUlong, advanceInx, err = ParseUlongPrimitive(buffer[inx:])
	if err != nil {
		return attachParameters, inx, errors.New(err.Error() + "\nReadAttachPerformative() failed reading expectedUlong from list")
	}
	if expectedUlong != 0x29 {
		fmt.Printf("expectedUlong[%d] not 0x29: 0x%x\n", inx, expectedUlong)
	}
	inx += advanceInx
	advanceInx = 0

	// read expected Target type 0x29
	attachParameters.Target, advanceInx, err = ReadTargetList(buffer[inx:])
	if err != nil {
		return attachParameters, inx, errors.New(err.Error() + "\nReadAttachPerformative() failed reading SourceList")
	}
	inx += advanceInx
	advanceInx = 0
	countItems--

	if buffer[inx] != nullCode {
		choiceByte, advanceInx, err := ParseBytePrimitive(buffer[inx:])
		if err != nil {
			return attachParameters, inx, errors.New(err.Error() + "\nReadAttachPerformative() failed reading Unsettled from list")
		}
		attachParameters.Unsettled = Map(choiceByte)
		inx += advanceInx
		advanceInx = 0
	} else {
		log.Debug("skipping Unsettled MAP .. is nullCode inx:", inx)
		inx++
	}
	countItems--

	if buffer[inx] != nullCode {
		choiceBoolean, advanceInx, err := ParseBooleanPrimitive(buffer[inx:])
		if err != nil {
			return attachParameters, inx, errors.New(err.Error() + "\nReadAttachPerformative() failed reading IncompleteUnsettled from list")
		}
		attachParameters.IncompleteUnsettled = BooleanChoice(choiceBoolean)
		inx += advanceInx
		advanceInx = 0
	} else {
		log.Debug("skipping IncompleteUnsettled  .. is nullCode inx:", inx)
		inx++
	}
	countItems--

	if buffer[inx] != nullCode {
		smalluint, advanceInx, err := ParseUintPrimitive(buffer[inx:])
		if err != nil {
			return attachParameters, inx, errors.New(err.Error() + "\nReadAttachPerformative() failed reading InitialDeliveryCount from list")
		}
		attachParameters.InitialDeliveryCount = SequenceNo(smalluint)
		inx += advanceInx
		advanceInx = 0
	} else {
		log.Debug("skipping InitialDeliveryCount  .. is nullCode inx:", inx)
		inx++
	}
	countItems--

	if buffer[inx] != nullCode {
		smallulong, advanceInx, err := ParseUlongPrimitive(buffer[inx:])
		if err != nil {
			return attachParameters, inx, errors.New(err.Error() + "\nReadAttachPerformative() failed reading MaxMessageSize from list")
		}
		attachParameters.MaxMessageSize = Ulong(smallulong)
		inx += advanceInx
		advanceInx = 0
	} else {
		log.Debug("skipping MaxMessageSize  .. is nullCode inx:", inx)
		inx++
	}
	countItems--

	if countItems != 0 {
		log.Debug("Were all items read: left with countItem: ", countItems)
	}

	return attachParameters, inx, nil
}

package amqpx

import (
	"errors"

	log "github.com/mgutz/logxi/v1"
)

// DispositionParameters .. gathered in the Disposition performative
// <type Name="disposition" class="composite" Source="list" provides="frame">
//
//	<descriptor Name="amqp:disposition:list" code="0x00000000:0x00000015"/>
//	<field Name="Role" type="Role" mandatory="true"/>
//	<field Name="First" type="delivery-number" mandatory="true"/>
//	<field Name="Last" type="delivery-number"/>
//	<field Name="Settled" type="boolean" default="false"/>
//	<field Name="State" type="*" requires="delivery-State"/>
//	<field Name="Batchable" type="boolean" default="false"/>
//
// </type>
type DispositionParameters struct {
	Role      RoleChoice     `json:"role"`  // mandatory
	First     DeliveryNumber `json:"first"` // mandatory
	Last      DeliveryNumber `json:"last,omitempty"`
	Settled   BooleanChoice  `json:"Settled,omitempty"`
	State     DeliveryState  `json:"State,omitempty"`
	Batchable BooleanChoice  `json:"Batchable,omitempty"`
}

// ParsePerformativeDisposition reads a disposition performative from buffer.
// noinspection ALL
func ParsePerformativeDisposition(buffer []byte) (disposition DispositionParameters, bytesUsed uint32, err error) {
	err = nil
	inx := uint32(0)
	advanceInx := uint32(0)
	// header

	_, countItems, _, advanceInx, err := ParseListPrimitive(buffer[inx:])
	if err != nil {
		inx += advanceInx
		return disposition, inx, errors.New(err.Error() + "\nReadDispositionPerformative() failed compound list")
	}
	inx += advanceInx
	advanceInx = 0

	disposition.Role, advanceInx, err = ParseRolePrimitive(buffer[inx:])
	if err != nil {
		inx += advanceInx
		return disposition, inx, errors.New(err.Error() + "\nReadDispositionPerformative() failed reading Role from list")
	}
	log.Debug("disposition.Role:", disposition.Role)
	inx += advanceInx
	advanceInx = 0
	countItems--

	disposition.First, advanceInx, err = ParseDeliveryNumberPrimitive(buffer[inx:])
	if err != nil {
		inx += advanceInx
		return disposition, inx, errors.New(err.Error() + "\nReadDispositionPerformative() failed reading First from list")
	}
	log.Debug("disposition.First:", disposition.First)
	inx += advanceInx
	advanceInx = 0
	countItems--

	disposition.Last, advanceInx, err = ParseDeliveryNumberPrimitive(buffer[inx:])
	if err != nil {
		inx += advanceInx
		return disposition, inx, errors.New(err.Error() + "\nReadDispositionPerformative() failed reading last from list")
	}
	log.Debug("disposition.Last:", disposition.First)
	inx += advanceInx
	advanceInx = 0
	countItems--

	disposition.Settled, advanceInx, err = ParseBooleanChoicePrimitive(buffer[inx:])
	if err != nil {
		inx += advanceInx
		return disposition, inx, errors.New(err.Error() + "\nReadDispositionPerformative() failed reading Settled from list")
	}
	log.Debug("disposition.Settled:", disposition.Settled)
	inx += advanceInx
	advanceInx = 0
	countItems--

	disposition.State, advanceInx, err = ParseDeliveryStatePrimitive(buffer[inx:])
	if err != nil {
		inx += advanceInx
		return disposition, inx, errors.New(err.Error() + "\nReadDispositionPerformative() failed reading State from list")
	}
	log.Debug("disposition.State:", disposition.State)
	inx += advanceInx
	advanceInx = 0
	countItems--

	return disposition, inx, nil
}

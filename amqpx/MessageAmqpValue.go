package amqpx

import (
	"errors"

	log "github.com/mgutz/logxi/v1"
)

// MessageAmqpValue is a message format
// <type Name="amqp-Value" class="restricted" Source="*" provides="section">
//
//	<descriptor Name="amqp:amqp-Value:*" code="0x00000000:0x00000077"/>
//
// </type>
type MessageAmqpValue struct {
	Value Binary `json:"value"`
}

// ParseMessageAmqpValue message properties after transport.
func ParseMessageAmqpValue(buffer []byte) (messageAmqpValue MessageAmqpValue, bytesUsed uint32, err error) {
	err = nil
	inx := uint32(0)
	bytesUsed = uint32(0)
	advanceInx := uint32(0)

	if buffer[inx] != nullCode {
		messageAmqpValue.Value, advanceInx, err = ParseBinaryPrimitive(buffer[inx:])
		if err != nil {
			return messageAmqpValue, bytesUsed, errors.New(err.Error() + "\nReadMessageAmqpValue() failed reading Value")
		}
		log.Debug("messageAmqpValue.Value:", messageAmqpValue.Value)
		inx += advanceInx
		advanceInx = 0
	} else {
		inx++
		//log.Debug("skipping Value .. is nullCode inx:", inx)
	}

	bytesUsed = inx
	return messageAmqpValue, bytesUsed, nil

}

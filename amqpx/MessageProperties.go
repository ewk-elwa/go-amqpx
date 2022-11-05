package amqpx

import (
	"errors"

	log "github.com/mgutz/logxi/v1"
)

// MessageProperties is a message format
// <type Name="properties" class="composite" Source="list" provides="section">
//
//	<descriptor Name="amqp:properties:list" code="0x00000000:0x00000073"/>
//	<field Name="message-id" type="*" requires="message-id"/>
//	<field Name="user-id" type="binary"/>
//	<field Name="To" type="*" requires="Address"/>
//	<field Name="Subject" type="string"/>
//	<field Name="reply-To" type="*" requires="Address"/>
//	<field Name="correlation-id" type="*" requires="message-id"/>
//	<field Name="content-type" type="symbol"/>
//	<field Name="content-encoding" type="symbol"/>
//	<field Name="absolute-expiry-time" type="timestamp"/>
//	<field Name="creation-time" type="timestamp"/>
//	<field Name="group-id" type="string"/>
//	<field Name="group-sequence" type="sequence-no"/>
//	<field Name="reply-To-group-id" type="string"/>
//
// </type>
type MessageProperties struct {
	MessageId       Binary     `json:"messageId"`
	UserId          uint32     `json:"userId"`
	To              string     `json:"to"`
	Subject         string     `json:"subject"`
	ReplyTo         string     `json:"replyTo"`
	CorrelationId   Binary     `json:"correlationId"`
	ContentType     Symbol     `json:"contentType"`
	ContentEncoding Symbol     `json:"contentEncoding"`
	AbsExpiryTime   Timestamp  `json:"absExpiryTime"`
	CreationTime    Timestamp  `json:"creationTime"`
	GroupId         string     `json:"groupId"`
	GroupSequence   SequenceNo `json:"groupSequence"`
	ReplyToGroupID  string     `json:"replyToGroupId"`
}

// ParseMessageProperties message properties after transport.
func ParseMessageProperties(buffer []byte) (properties MessageProperties, bytesUsed uint32, err error) {
	err = nil
	inx := uint32(0)
	bytesUsed = uint32(0)
	advanceInx := uint32(0)

	// header
	_, countItems, _, advanceInx, err := ParseListPrimitive(buffer[inx:])
	if err != nil {
		return properties, bytesUsed, errors.New(err.Error() + "\nReadMessageProperties() failed compound list")
	}
	inx += advanceInx
	advanceInx = 0
	countItems--

	if buffer[inx] != nullCode {
		properties.MessageId, advanceInx, err = ParseBinaryPrimitive(buffer[inx:])
		if err != nil {
			return properties, bytesUsed, errors.New(err.Error() + "\nReadBinaryPrimitive() failed reading MessageId")
		}
		log.Debug("properties.MessageId:", properties.MessageId)
		inx += advanceInx
		advanceInx = 0
	} else {
		inx++
		//log.Debug("skipping MessageId .. is nullCode inx:", inx)
	}
	countItems--

	if buffer[inx] != nullCode {
		properties.UserId, advanceInx, err = ParseUintPrimitive(buffer[inx:])
		if err != nil {
			return properties, bytesUsed, errors.New(err.Error() + "\nReadUintPrimitive() failed reading UserId")
		}
		log.Debug("properties.UserId:", properties.UserId)
		inx += advanceInx
		advanceInx = 0
	} else {
		inx++
		//log.Debug("skipping UserId .. is nullCode inx:", inx)
	}
	countItems--

	if buffer[inx] != nullCode {
		properties.To, advanceInx, err = ParseStringPrimitive(buffer[inx:])
		if err != nil {
			return properties, bytesUsed, errors.New(err.Error() + "\nReadStringPrimitive() failed reading 'To'")
		}
		log.Debug("properties.To:", properties.To)
		inx += advanceInx
		advanceInx = 0
	} else {
		inx++
		//log.Debug("skipping 'To' .. is nullCode inx:", inx)
	}
	countItems--

	if buffer[inx] != nullCode {
		properties.Subject, advanceInx, err = ParseStringPrimitive(buffer[inx:])
		if err != nil {
			return properties, bytesUsed, errors.New(err.Error() + "\nReadStringPrimitive() failed reading 'Subject'")
		}
		log.Debug("properties.Subject:", properties.Subject)
		inx += advanceInx
		advanceInx = 0
	} else {
		inx++
		//log.Debug("skipping 'Subject' .. is nullCode inx:", inx)
	}
	countItems--

	if buffer[inx] != nullCode {
		properties.ReplyTo, advanceInx, err = ParseStringPrimitive(buffer[inx:])
		if err != nil {
			return properties, bytesUsed, errors.New(err.Error() + "\nReadStringPrimitive() failed reading 'ReplyTo'")
		}
		log.Debug("properties.ReplyTo:", properties.ReplyTo)
		inx += advanceInx
		advanceInx = 0
	} else {
		inx++
		//log.Debug("skipping 'ReplyTo' .. is nullCode inx:", inx)
	}
	countItems--

	if buffer[inx] != nullCode {
		properties.CorrelationId, advanceInx, err = ParseBinaryPrimitive(buffer[inx:])
		if err != nil {
			return properties, bytesUsed, errors.New(err.Error() + "\nReadBinaryPrimitive() failed reading 'CorrelationId'")
		}
		log.Debug("properties.CorrelationId:", properties.CorrelationId)
		inx += advanceInx
		advanceInx = 0
	} else {
		inx++
		//log.Debug("skipping 'ReplyTo' .. is nullCode inx:", inx)
	}
	countItems--

	if buffer[inx] != nullCode {
		properties.ContentType, advanceInx, err = ParseSymbolPrimitive(buffer[inx:])
		if err != nil {
			return properties, bytesUsed, errors.New(err.Error() + "\nReadBinaryPrimitive() failed reading 'ContentType'")
		}
		log.Debug("properties.ContentType:", properties.ContentType)
		inx += advanceInx
		advanceInx = 0
	} else {
		inx++
		//log.Debug("skipping 'ContentType' .. is nullCode inx:", inx)
	}
	countItems--

	if buffer[inx] != nullCode {
		properties.ContentEncoding, advanceInx, err = ParseSymbolPrimitive(buffer[inx:])
		if err != nil {
			return properties, bytesUsed, errors.New(err.Error() + "\nReadBinaryPrimitive() failed reading 'ContentEncoding'")
		}
		log.Debug("properties.ContentEncoding:", properties.ContentEncoding)
		inx += advanceInx
		advanceInx = 0
	} else {
		inx++
		//log.Debug("skipping 'ContentEncoding' .. is nullCode inx:", inx)
	}
	countItems--

	if buffer[inx] != nullCode {
		properties.AbsExpiryTime, advanceInx, err = ParseTimestampPrimitive(buffer[inx:])
		if err != nil {
			return properties, bytesUsed, errors.New(err.Error() + "\nReadTimestampPrimitive() failed reading 'AbsExpiryTime'")
		}
		log.Debug("properties.AbsExpiryTime:", properties.AbsExpiryTime)
		inx += advanceInx
		advanceInx = 0
	} else {
		inx++
		//log.Debug("skipping 'AbsExpiryTime' .. is nullCode inx:", inx)
	}
	countItems--

	if buffer[inx] != nullCode {
		properties.CreationTime, advanceInx, err = ParseTimestampPrimitive(buffer[inx:])
		if err != nil {
			return properties, bytesUsed, errors.New(err.Error() + "\nReadTimestampPrimitive() failed reading 'CreationTime'")
		}
		log.Debug("properties.CreationTime:", properties.CreationTime)
		inx += advanceInx
		advanceInx = 0
	} else {
		inx++
		//log.Debug("skipping 'CreationTime' .. is nullCode inx:", inx)
	}
	countItems--

	if buffer[inx] != nullCode {
		properties.GroupId, advanceInx, err = ParseStringPrimitive(buffer[inx:])
		if err != nil {
			return properties, bytesUsed, errors.New(err.Error() + "\nReadStringPrimitive() failed reading 'GroupId'")
		}
		log.Debug("properties.GroupId:", properties.GroupId)
		inx += advanceInx
		advanceInx = 0
	} else {
		inx++
		//log.Debug("skipping 'GroupId' .. is nullCode inx:", inx)
	}
	countItems--

	if buffer[inx] != nullCode {
		properties.GroupSequence, advanceInx, err = ParseSequenceNoPrimitive(buffer[inx:])
		if err != nil {
			return properties, bytesUsed, errors.New(err.Error() + "\nReadSequenceNoPrimitive() failed reading GroupSequence")
		}
		log.Debug("properties.GroupSequence:", properties.GroupSequence)
		inx += advanceInx
		advanceInx = 0
	} else {
		inx++
		//log.Debug("skipping GroupSequence .. is nullCode inx:", inx)
	}
	countItems--

	if buffer[inx] != nullCode {
		properties.ReplyToGroupID, advanceInx, err = ParseStringPrimitive(buffer[inx:])
		if err != nil {
			return properties, bytesUsed, errors.New(err.Error() + "\nReadStringPrimitive() failed reading 'ReplyToGroupID'")
		}
		log.Debug("properties.ReplyToGroupID:", properties.ReplyToGroupID)
		inx += advanceInx
		advanceInx = 0
	} else {
		inx++
		//log.Debug("skipping 'ReplyToGroupID' .. is nullCode inx:", inx)
	}
	countItems--

	bytesUsed = inx
	return properties, bytesUsed, nil

}

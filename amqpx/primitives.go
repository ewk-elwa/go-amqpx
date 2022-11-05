package amqpx

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	log "github.com/mgutz/logxi/v1"
)

// noinspection ALL
const (
	nullCode       byte = 0x40 // "fixed-width="0" label="the null Value"
	booleanCode    byte = 0x56 // "fixed-width="1" label="boolean with the octet 0x00 being false and octet 0x01 being true"
	booleanTrue    byte = 0x41 // "fixed-width="0" label="the boolean Value true"
	booleanFalse   byte = 0x42 // "fixed-width="0" label="the boolean Value false"
	ubyteCode      byte = 0x50 // "fixed-width="1" label="8-bit unsigned integer"
	ushortCode     byte = 0x60 // "fixed-width="2" label="16-bit unsigned integer in network byte order
	uintCode       byte = 0x70 // "fixed-width="4" label="32-bit unsigned integer in network byte order"
	uintSmallCode  byte = 0x52 // "fixed-width="1" label="unsigned integer Value in the range 0 To 255 inclusive"
	uint0Code      byte = 0x43 // "fixed-width="0" label="the uint Value 0"
	ulongCode      byte = 0x80 // "fixed-width="8" label="64-bit unsigned integer in network byte order"
	ulongSmallCode byte = 0x53 // "fixed-width="1" label="unsigned long Value in the range 0 To 255 inclusive"
	ulong0Code     byte = 0x44 // "fixed-width="0" label="the ulong Value 0"
	byteCode       byte = 0x51 // "fixed-width="1" label="8-bit two's-complement integer"
	shortCode      byte = 0x61 // "fixed-width="2" label="16-bit two's-complement integer in network byte order"
	intCode        byte = 0x71 // "fixed-width="4" label="32-bit two's-complement integer in network byte order"
	intSmallCode   byte = 0x54 // "fixed-width="1" label="8-bit two's-complement integer"
	longCode       byte = 0x81 // "fixed-width="8" label="64-bit two's-complement integer in network byte order"
	longSmallCode  byte = 0x55 // "fixed-width="1" label="8-bit two's-complement integer"
	floatCode      byte = 0x72 // "fixed-width="4" label="IEEE 754-2008 binary32"
	decimal32Code  byte = 0x74 // "fixed-width="4" label="IEEE 754-2008 decimal32 using the Binary Integer Decimal encoding"
	decimal64Code  byte = 0x84 // "fixed-width="8" label="IEEE 754-2008 decimal64 using the Binary Integer Decimal encoding"
	decimal128Code byte = 0x94 // "fixed-width="16" label="IEEE 754-2008 decimal128 using the Binary Integer Decimal encoding"
	charCode       byte = 0x73 // "fixed-width="4" label="a UTF-32BE encoded Unicode character"
	timestampCode  byte = 0x83 // "fixed-width="8" label="64-bit two's-complement integer representing msec since the unix epoch"
	uuidCode       byte = 0x98 // "fixed-width="16" label="UUID as defined in section 4.1.2 of RFC-4122"
	binary8Code    byte = 0xa0 // "variable-width="1" label="up To 2^8 - 1 octets of binary data"
	binary32Code   byte = 0xb0 // "variable-width="4" label="up To 2^32 - 1 octets of binary data"
	string8Code    byte = 0xa1 // "variable-width="1" label="up To 2^8 - 1 octets worth of UTF-8 Unicode (with no byte order mark)"
	string32Code   byte = 0xb1 // "variable-width="4" label="up To 2^32 - 1 octets worth of UTF-8 Unicode (with no byte order mark)"
	symbol8Code    byte = 0xa3 // "variable-width="1" label="up To 2^8 - 1 seven bit ASCII chars representing a symbolic Value"
	symbol32Code   byte = 0xb3 // "variable-width="4" label="up To 2^32 - 1 seven bit ASCII chars representing a symbolic Value"
	list0Code      byte = 0x45 // "fixed-width="0" label="the empty list (i.e. the list with no elements)"
	list8Code      byte = 0xc0 // "compound-width="1" label="up To 2^8 - 1 list elements with total Size less than 2^8 octets"
	list32Code     byte = 0xd0 // "compound-width="4" label="up To 2^32 - 1 list elements with total Size less than 2^32 octets"
	map8Code       byte = 0xc1 // "compound-width="1" label="up To 2^8 - 1 octets of encoded map data"
	map32Code      byte = 0xd1 // "compound-width="4" label="up To 2^32 - 1 octets of encoded map data"
	array8Code     byte = 0xe0 // "array-width="1" label="up To 2^8 - 1 array elements with total Size less than 2^8 octets"
	array32Code    byte = 0xf0 // "array-width="4" label="up To 2^32 - 1 array elements with total Size less than 2^32 octets"
)

// booleanCodeTrue/False applies when code is booleanCode
const (
	booleanCodeFalse byte = 0x00
	booleanCodeTrue  byte = 0x01
)

// Timestamp is an int64 Value
type Timestamp int64

// Binary is a byte array max from binary32Code
type Binary []byte

// Symbol is a string using string8Code
type Symbol string

// noinspection ALL
type frameHeader struct {
	Length       uint32  `json:"length"`
	Doff         byte    `json:"doff"`
	ProtocolType byte    `json:"protocolType"`
	Channel      uint16  `json:"channel"`
	BlockType    [3]byte `json:"blockType"`
}

// SerializeStruct serializes a struct To byte array
func SerializeStruct(myStruct interface{}) []byte {
	var binBuf bytes.Buffer
	_ = binary.Write(&binBuf, binary.BigEndian, myStruct)
	return binBuf.Bytes()
}

// SerializeNullPrimitive serialized null .. for optionals
func SerializeNullPrimitive() (buf []byte) {
	buf = make([]byte, 1)
	buf[0] = nullCode
	return buf
}

// SerializeBooleanPrimitive serialized boolean
func SerializeBooleanPrimitive(value bool) (buf []byte) {
	buf = make([]byte, 2)
	buf[0] = booleanCode
	if value {
		buf[1] = 0x01
	} else {
		buf[1] = 0x00
	}
	return buf
}

// ParseBooleanPrimitive reads a boolean code or a boolean Value. Returns true or false
func ParseBooleanPrimitive(buffer []byte) (retVal bool, bytesUsed uint32, err error) {
	retVal = false
	bytesUsed = 0
	inx := 0
	if buffer[inx] == booleanCode {
		if len(buffer) < 2 {
			return retVal, bytesUsed, errors.New("amqpx: buffer len must be 2 or More for booleanCode")
		}
		inx++
		if buffer[inx] == 0x00 {
			retVal = false
		} else if buffer[inx] == 0x01 {
			retVal = true
		} else {
			return retVal, bytesUsed, errors.New("amqpx: booleanCode expected false(0x00) or true(0x01) got :" + fmt.Sprintf("0x%x", buffer[inx]))
		}
		inx++
		bytesUsed = uint32(inx)
		return retVal, bytesUsed, nil
	}

	if buffer[inx] == booleanTrue {
		retVal = true
	} else if buffer[inx] == booleanFalse {
		retVal = false
	} else {
		return retVal, bytesUsed, errors.New("amqpx: expected true(0x41) or false(0x42) got :" + fmt.Sprintf("0x%x", buffer[inx]))
	}
	inx++
	bytesUsed = uint32(inx)
	return retVal, bytesUsed, nil
}

// SerializeBooleanChoicePrimitive serialized boolean choice
func SerializeBooleanChoicePrimitive(value BooleanChoice) (buf []byte) {
	return SerializeBooleanPrimitive(bool(value))
}

// ParseBooleanChoicePrimitive reads a BooleanChoice primitive
func ParseBooleanChoicePrimitive(buffer []byte) (retVal BooleanChoice, bytesUsed uint32, err error) {
	booleanChoice, bytesUsed, err := ParseBooleanPrimitive(buffer)
	return BooleanChoice(booleanChoice), bytesUsed, err
}

// SerializeRoleChoicePrimitive serialized Role choice
func SerializeRoleChoicePrimitive(value RoleChoice) (buf []byte) {
	return SerializeBooleanPrimitive(bool(value))
}

// ParseRolePrimitive reads a Role primitive
func ParseRolePrimitive(buffer []byte) (retVal RoleChoice, bytesUsed uint32, err error) {
	role, bytesUsed, err := ParseBooleanPrimitive(buffer)
	return RoleChoice(role), bytesUsed, err
}

// SerializeUbytePrimitive serialized Ubyte
func SerializeUbytePrimitive(value byte) (buf []byte) {
	buf = make([]byte, 2)
	buf[0] = ubyteCode
	buf[1] = value
	return buf
}

// ParseUbytePrimitive reads a byte from the buffer
func ParseUbytePrimitive(buffer []byte) (retVal byte, bytesUsed uint32, err error) {
	retVal = 0
	bytesUsed = 0
	inx := uint32(0)

	switch buffer[inx] {
	case ubyteCode:
		if len(buffer) < 2 {
			return retVal, bytesUsed, errors.New("amqpx: buffer len must be 2 or More for ubyteCode")
		}
		inx++
		retVal = buffer[inx]
		inx++
		bytesUsed = inx
		return retVal, bytesUsed, nil
	default:
		return retVal, bytesUsed, errors.New("amqpx: contructor not ubyteCode")
	}
}

// SerializeUshortPrimitive serialized Ushort
func SerializeUshortPrimitive(value uint16) (buf []byte) {
	buf = make([]byte, szInt16+1)
	buf[0] = ushortCode
	binary.BigEndian.PutUint16(buf[1:], value)
	return buf
}

// PraseUshortPrimitive reads a binary encoded Value from buffer
func PraseUshortPrimitive(buffer []byte) (retVal uint16, bytesUsed uint32, err error) {
	retVal = uint16(0)
	bytesUsed = 0

	inx := uint32(0)
	switch buffer[inx] {
	case ushortCode:
		if len(buffer) < 3 {
			return retVal, bytesUsed, errors.New("amqpx: buffer len must be 3 or More for ushortCode")
		}
		inx++
		retVal = binary.BigEndian.Uint16(buffer[inx:])
		inx += 2
		bytesUsed = inx
		return retVal, bytesUsed, nil
	default:
		return retVal, bytesUsed, errors.New("amqpx: contructor not ushortCode")
	}
}

// SerializeUintPrimitive serialized uint
func SerializeUintPrimitive(value uint32) (buf []byte) {
	if value == 0 {
		buf = make([]byte, 2)
		buf[0] = uint0Code
		buf[1] = 0
	} else if value <= 0xff {
		buf = make([]byte, 2)
		buf[0] = uintSmallCode
		buf[1] = byte(value)
	} else {
		buf = make([]byte, 5)
		buf[0] = uintCode
		binary.BigEndian.PutUint32(buf[1:], value)
	}
	return buf
}

// ParseUintPrimitive reads 4 byte, 1, byte or 0 byte for unsigned int
func ParseUintPrimitive(buffer []byte) (retVal uint32, bytesUsed uint32, err error) {
	retVal = uint32(0)
	bytesUsed = 0
	inx := 0

	switch buffer[inx] {
	case uintCode:
		if len(buffer) < 5 {
			return retVal, bytesUsed, errors.New("amqpx: buffer len must be 5 or More for uintCode")
		}
		inx++
		retVal = binary.BigEndian.Uint32(buffer[inx:])
		inx += 4
		bytesUsed = uint32(inx)
		return retVal, bytesUsed, nil
	case uintSmallCode:
		if len(buffer) < 2 {
			return retVal, bytesUsed, errors.New("amqpx: buffer len must be 2 or More for uintSmallCode")
		}
		inx++
		retVal = uint32(buffer[inx])
		inx++
		bytesUsed = uint32(inx)
		return retVal, bytesUsed, nil
	case uint0Code:
		inx++
		retVal = uint32(0)
		bytesUsed = uint32(inx)
		return retVal, bytesUsed, nil
	case nullCode:
		inx++
		retVal = uint32(0)
		bytesUsed = uint32(inx)
		return retVal, bytesUsed, nil
	default:
		return retVal, bytesUsed, errors.New("amqpx: contructor not uintCode, uintSmallCode or uint0Code")
	}
}

// SerializeDeliveryStatePrimitive serialized DeliveryState
func SerializeDeliveryStatePrimitive(value DeliveryState) (buf []byte) {
	return SerializeUintPrimitive(uint32(value))
}

// ParseDeliveryStatePrimitive reads a uint
func ParseDeliveryStatePrimitive(buffer []byte) (retVal DeliveryState, bytesUsed uint32, err error) {
	state, advanceInx, err := ParseUintPrimitive(buffer[0:])
	return DeliveryState(state), advanceInx, err
}

// SerializeUlongPrimitive serialized ulong
func SerializeUlongPrimitive(value uint64) (buf []byte) {
	if value == 0 {
		buf = make([]byte, 2)
		buf[0] = ulong0Code
		buf[1] = 0
	} else if value <= 0xff {
		buf = make([]byte, 2)
		buf[0] = ulongSmallCode
		buf[1] = byte(value)
	} else {
		buf = make([]byte, 9)
		buf[0] = ulongCode
		binary.BigEndian.PutUint64(buf[1:], value)
	}
	return buf
}

// ParseUlongPrimitive reads 8 byte, 1, byte or 0 byte for unsigned int
func ParseUlongPrimitive(buffer []byte) (retVal uint64, bytesUsed uint32, err error) {
	retVal = uint64(0)
	bytesUsed = 0
	inx := uint32(0)

	switch buffer[inx] {
	case ulongCode:
		if len(buffer) < 9 {
			return retVal, bytesUsed, errors.New("amqpx: buffer len must be 9 or More for ulongCode")
		}
		inx++
		retVal = binary.BigEndian.Uint64(buffer[inx:])
		inx += 8
		bytesUsed = inx
		return retVal, bytesUsed, nil
	case ulongSmallCode:
		if len(buffer) < 2 {
			return retVal, bytesUsed, errors.New("amqpx: buffer len must be 2 or More for ulongSmallCode")
		}
		inx++
		retVal = uint64(buffer[inx])
		inx++
		bytesUsed = inx
		return retVal, bytesUsed, nil
	case ulong0Code:
		inx++
		retVal = uint64(0)
		bytesUsed = inx
		return retVal, bytesUsed, nil
	default:
		return retVal, bytesUsed, errors.New("amqpx: contructor not ulongCode, ulongSmallCode or ulong0Code")
	}
}

// SerializeBytePrimitive serialized byte
func SerializeBytePrimitive(value byte) (buf []byte) {
	buf = make([]byte, 2)
	buf[0] = byteCode
	buf[1] = value
	return buf
}

// ParseBytePrimitive reads a byte from the buffer
func ParseBytePrimitive(buffer []byte) (retVal byte, bytesUsed uint32, err error) {
	retVal = 0
	bytesUsed = 0
	inx := uint32(0)

	switch buffer[inx] {
	case byteCode:
		if len(buffer) < 2 {
			return retVal, bytesUsed, errors.New("amqpx: buffer len must be 2 or More for byteCode")
		}
		inx++
		retVal = buffer[inx]
		inx++
		bytesUsed = inx
		return retVal, bytesUsed, nil
	default:
		return retVal, bytesUsed, errors.New("amqpx: contructor not byteCode")
	}
}

// SerializeRawBytePrimitive serialized raw byte
func SerializeRawBytePrimitive(value byte) (buf []byte) {
	buf = make([]byte, 1)
	buf[0] = value
	return buf
}

// ParseRawBytePrimitive reads a byte from the buffer
func ParseRawBytePrimitive(buffer []byte) (retVal byte, bytesUsed uint32, err error) {
	retVal = 0
	bytesUsed = 0
	inx := uint32(0)

	retVal = buffer[inx]
	inx++
	bytesUsed = inx
	return retVal, bytesUsed, nil

}

// SerializeShortPrimitive serialized short
func SerializeShortPrimitive(value int16) (buf []byte) {
	buf = make([]byte, 3)
	buf[0] = shortCode
	binary.BigEndian.PutUint16(buf[1:], uint16(value))
	return buf
}

// ParseShortPrimitive reads a binary encoded Value from buffer
func ParseShortPrimitive(buffer []byte) (retVal int16, bytesUsed uint32, err error) {
	retVal = int16(0)
	bytesUsed = 0

	inx := uint32(0)
	switch buffer[inx] {
	case shortCode:
		if len(buffer) < 3 {
			return retVal, bytesUsed, errors.New("amqpx: buffer len must be 3 or More for shortCode")
		}
		inx++
		retVal = int16(binary.BigEndian.Uint16(buffer[inx:]))
		inx += 2
		bytesUsed = inx
		return retVal, bytesUsed, nil
	default:
		return retVal, bytesUsed, errors.New("amqpx: contructor not shortCode")
	}
}

// SerializeIntPrimitive serialized int
func SerializeIntPrimitive(value int32) (buf []byte) {
	uvalue := uint32(value)
	if uvalue <= 0xff {
		buf = make([]byte, 2)
		buf[0] = intSmallCode
		buf[1] = byte(uvalue)
	} else {
		buf = make([]byte, 5)
		buf[0] = intCode
		binary.BigEndian.PutUint32(buf[1:], uvalue)
	}
	return buf
}

// ParseIntPrimitive reads a binary encoded Value from buffer
func ParseIntPrimitive(buffer []byte) (retVal int32, bytesUsed uint32, err error) {
	retVal = int32(0)
	bytesUsed = 0

	inx := uint32(0)
	switch buffer[inx] {
	case intCode:
		if len(buffer) < 5 {
			return retVal, bytesUsed, errors.New("amqpx: buffer len must be 5 or More for intCode")
		}
		inx++
		retVal = int32(binary.BigEndian.Uint32(buffer[inx:]))
		inx += 4
		bytesUsed = inx
		return retVal, bytesUsed, nil
	case intSmallCode:
		if len(buffer) < 2 {
			return retVal, bytesUsed, errors.New("amqpx: buffer len must be 2 or More for intSmallCode")
		}
		inx++
		retVal = int32(buffer[inx])
		inx++
		bytesUsed = inx
		return retVal, bytesUsed, nil
	default:
		return retVal, bytesUsed, errors.New("amqpx: contructor not intCode or intSmallCode")
	}
}

// SerializeLongPrimitive serialized int
func SerializeLongPrimitive(value int64) (buf []byte) {
	uvalue := uint64(value)
	if uvalue <= 0xff {
		buf = make([]byte, 2)
		buf[0] = longSmallCode
		buf[1] = byte(uvalue)
	} else {
		buf = make([]byte, 9)
		buf[0] = longCode
		binary.BigEndian.PutUint64(buf[1:], uvalue)
	}
	return buf
}

// ParseLongPrimitive reads a binary encoded Value from buffer
func ParseLongPrimitive(buffer []byte) (retVal int64, bytesUsed uint32, err error) {
	retVal = int64(0)
	bytesUsed = 0

	inx := uint32(0)
	switch buffer[inx] {
	case longCode:
		if len(buffer) < 9 {
			return retVal, bytesUsed, errors.New("amqpx: buffer len must be 9 or More for longCode")
		}
		inx++
		retVal = int64(binary.BigEndian.Uint64(buffer[inx:]))
		inx += 8
		bytesUsed = inx
		return retVal, bytesUsed, nil
	case longSmallCode:
		if len(buffer) < 2 {
			return retVal, bytesUsed, errors.New("amqpx: buffer len must be 2 or More for intSmallCode")
		}
		inx++
		retVal = int64(buffer[inx])
		inx++
		bytesUsed = inx
		return retVal, bytesUsed, nil
	default:
		return retVal, bytesUsed, errors.New("amqpx: contructor not longCode or longSmallCode")
	}
}

// Skipping
// ReadFloatPrimitive
// ReadDoublePrimitive
// ReadDecimal32Primitive
// ReadDecimal64Primitive
// ReadDecimal128Primitive
// ReadCharPrimitive

// SerializeTimestampPrimitive serialized timestamp
func SerializeTimestampPrimitive(value Timestamp) (buf []byte) {
	buf = make([]byte, 9)
	buf[0] = timestampCode
	binary.BigEndian.PutUint64(buf[1:], uint64(value))
	return buf
}

// ParseTimestampPrimitive reads
func ParseTimestampPrimitive(buffer []byte) (retVal Timestamp, bytesUsed uint32, err error) {
	retVal = Timestamp(0)
	bytesUsed = 0

	inx := uint32(0)
	switch buffer[inx] {
	case timestampCode:
		if len(buffer) < 9 {
			return retVal, bytesUsed, errors.New("amqpx: buffer len must be 9 or More for timestampCode")
		}
		inx++
		retVal = Timestamp(binary.BigEndian.Uint64(buffer[inx:]))
		inx += 8
		bytesUsed = inx
		return retVal, bytesUsed, nil

	default:
		return retVal, bytesUsed, errors.New("amqpx: contructor not timestampCode")
	}
}

// Skipping
// ReadUuidPrimitive

// SerializeBinaryPrimitive serialized binary
func SerializeBinaryPrimitive(value []byte) (buf []byte) {
	if len(value) <= 0xff {
		buf = make([]byte, len(value)+1)
		buf[0] = binary8Code
	} else {
		buf = make([]byte, len(value)+1)
		buf[0] = binary32Code
	}
	copy(buf[1:], value)
	return buf
}

// ParseBinaryPrimitive reads
func ParseBinaryPrimitive(buffer []byte) (retVal []byte, bytesUsed uint32, err error) {
	retVal = []byte{}
	bytesUsed = 0

	if len(buffer) < 2 {
		return retVal, bytesUsed, errors.New("amqpx: buffer must include at least constructor, Length byte, and one character")
	}

	inx := uint32(0)
	binaryLen := uint32(0)
	switch buffer[inx] {
	case binary8Code:
		if len(buffer) < 2 {
			return retVal, bytesUsed, errors.New("amqpx: buffer len must be 2 or More for binary8Code")
		}
		inx++
		binaryLen = uint32(buffer[inx])
		inx++
		bytesUsed = inx
	case binary32Code:
		if len(buffer) < 5 {
			return retVal, bytesUsed, errors.New("amqpx: buffer len must be 5 or More for binary32Code")
		}
		inx++
		binaryLen = binary.BigEndian.Uint32(buffer[inx:])
		inx += 4
		bytesUsed = inx
	default:
		return retVal, bytesUsed, errors.New("amqpx: contructor not binary8Code or binary32Code")
	}

	if uint32(len(buffer)) < (binaryLen + inx) {
		return retVal, bytesUsed, errors.New("amqpx: buffer not large enough To contain binary")
	}

	retVal = buffer[inx:(binaryLen + inx)]
	bytesUsed = inx + uint32(len(retVal))
	return retVal, bytesUsed, nil
}

// SerializeStringPrimitive serializes string
func SerializeStringPrimitive(value string) (buf []byte) {
	length := len(value)
	inx := uint32(0)
	if length == 0 {
		buf = make([]byte, 1)
		buf[0] = nullCode
	} else if length < 0xff {
		buf = make([]byte, length+1)
		buf[inx] = byte(length)
		inx++
	} else {
		buf = make([]byte, length+szInt32)
		binary.BigEndian.PutUint32(buf[inx:], uint32(length))
		inx += szInt32
	}

	copy(buf[inx:], value)
	return buf
}

// ParseStringPrimitive reads
func ParseStringPrimitive(buffer []byte) (retString string, bytesUsed uint32, err error) {
	retString = ""
	bytesUsed = 0

	stringLen := uint32(0)
	inx := uint32(0)
	switch buffer[inx] {
	case string8Code:
		if len(buffer) < 3 {
			return "", bytesUsed, errors.New("amqpx: buffer len must be 2 or More for string8Code")
		}
		inx++
		stringLen = uint32(buffer[inx])
		inx++
	case string32Code:
		if len(buffer) < 5 {
			return "", bytesUsed, errors.New("amqpx: buffer len must be 5 or More for string32Code")
		}
		inx++
		stringLen = binary.BigEndian.Uint32(buffer[inx:])
		inx += 4

	default:
		return retString, bytesUsed, errors.New("amqpx: contructor not string8Code or string32Code")
	}

	if uint32(len(buffer)) < (stringLen + inx) {
		return retString, bytesUsed, errors.New("amqpx: buffer not large enough To contain string")
	}
	retString = string(buffer[inx:(stringLen + inx)])
	bytesUsed = inx + uint32(len(retString))
	return retString, bytesUsed, nil
}

// SerializeSymbolPrimitive serialized Symbol
func SerializeSymbolPrimitive(symValue Symbol) (buf []byte) {
	value := string(symValue)
	length := len(value)
	if length <= 0xff {
		buf = make([]byte, length+2)
		buf[0] = symbol8Code
		buf[1] = byte(length)
		// copy Value into buf[2:]
		copy(buf[2:], symValue)
	} else {
		buf = make([]byte, length+5)
		buf[0] = symbol8Code
		binary.BigEndian.PutUint32(buf[1:], uint32(length))
		// copy Value into buf[6:]
		copy(buf[6:], symValue)
	}
	return buf
}

// ParseSymbolPrimitive reads
func ParseSymbolPrimitive(buffer []byte) (retString Symbol, bytesUsed uint32, err error) {
	retString = ""
	bytesUsed = 0

	stringLen := uint32(0)
	inx := uint32(0)
	switch buffer[inx] {
	case symbol8Code:
		if len(buffer) < 3 {
			return "", bytesUsed, errors.New("amqpx: buffer len must be 2 or More for symbol8Code")
		}
		inx++
		stringLen = uint32(buffer[inx])
		inx++
	case symbol32Code:
		if len(buffer) < 5 {
			return "", bytesUsed, errors.New("amqpx: buffer len must be 5 or More for symbol32Code")
		}
		inx++
		stringLen = binary.BigEndian.Uint32(buffer[inx:])
		inx += 4

	default:
		return retString, bytesUsed, errors.New("amqpx: contructor not symbol8Code or symbol32Code")
	}

	if uint32(len(buffer)) < (stringLen + inx) {
		return retString, bytesUsed, errors.New("amqpx: buffer not large enough To contain string")
	}
	retString = Symbol(buffer[inx:(stringLen + inx)])
	bytesUsed = inx + uint32(len(retString))
	return retString, bytesUsed, nil
}

// SerializeList a List from given buffers
func SerializeList(bufs ...[]byte) (retBuf []byte) {
	listSize := 0
	for _, buf := range bufs {
		listSize += len(buf)
	}
	retBuf = make([]byte, listSize+1) // plus 1 for list code

	if listSize == 0 {
		retBuf[0] = list0Code
	} else if listSize < 0xff {
		retBuf[0] = list8Code
	} else {
		retBuf[0] = list32Code
	}
	inx := 1
	for _, buf := range bufs {
		copy(retBuf[inx:], buf)
		inx += len(buf)
	}
	return retBuf
}

// ParseListPrimitive reads a compound primitive from buffer.
func ParseListPrimitive(buffer []byte) (size uint32, countItems uint32, inxFirstItem uint32, bytesUsed uint32, err error) {
	size, countItems, inxFirstItem, bytesUsed = 0, 0, 0, 0
	err = nil

	inx := uint32(0)
	switch buffer[inx] {
	case list0Code:
		inx++
		bytesUsed = inx
		return size, countItems, inxFirstItem, bytesUsed, nil
	case list8Code:
		inx++
		size = uint32(buffer[inx])
		inx++
		countItems = uint32(buffer[inx])
		inx++
		inxFirstItem = inx
		bytesUsed = inx
		return size, countItems, inxFirstItem, bytesUsed, nil
	case list32Code:
		inx++
		size = binary.BigEndian.Uint32(buffer[inx:])
		inx += 4
		countItems = binary.BigEndian.Uint32(buffer[inx:])
		inx += 4
		inxFirstItem = inx
		bytesUsed = inx
		return size, countItems, inxFirstItem, bytesUsed, nil
	default:
		return size, countItems, inxFirstItem, bytesUsed, errors.New("amqpx: contructor not list0Code, list8Code or list32Code")
	}
}

// Skipping
// ReadMapPrimitive
// ReadArrayPrimitive

///////////////

// SerializeSequenceNoPrimitive serialized SequenceNo
func SerializeSequenceNoPrimitive(value SequenceNo) (buf []byte) {
	return SerializeUintPrimitive(uint32(value))
}

// ParseSequenceNoPrimitive reads a unit encoded Value from buffer
func ParseSequenceNoPrimitive(buffer []byte) (retVal SequenceNo, bytesUsed uint32, err error) {
	seqNumber, bytesUsed, err := ParseUintPrimitive(buffer)
	return SequenceNo(seqNumber), bytesUsed, err
}

// SerializeDeliveryNumberPrimitive serialized DeliveryNumber
func SerializeDeliveryNumberPrimitive(value DeliveryNumber) (buf []byte) {
	return SerializeUintPrimitive(uint32(value))
}

// ParseDeliveryNumberPrimitive reads a unit encoded Value from buffer
func ParseDeliveryNumberPrimitive(buffer []byte) (retVal DeliveryNumber, bytesUsed uint32, err error) {
	seqNumber, bytesUsed, err := ParseUintPrimitive(buffer)
	return DeliveryNumber(seqNumber), bytesUsed, err
}

// SerializeBlockType serializes a blocktype
func SerializeBlockType() (retBuf []byte) {
	retBuf = make([]byte, 2)
	retBuf[0] = 0x00
	retBuf[1] = 0x53
	return retBuf
}

// ParseBlockType reads a boolean code or a boolean Value. Returns true or false
func ParseBlockType(buffer []byte) (retVal byte, bytesUsed uint32, err error) {
	retVal = 0x00
	bytesUsed = 0
	if len(buffer) < 3 {
		log.Error("*** ParseBlockType: buffer < 3")
		return retVal, bytesUsed, errors.New("amqpx: Not enough bytes in buffer To determine BlockType")
	}

	inx := uint32(0)
	if buffer[inx] != 0x00 {
		errStr := fmt.Sprintf("amqpx: Expected First byte To be zero, got 0x%x", buffer[inx])
		return retVal, bytesUsed, errors.New(errStr)
	}
	inx++

	if buffer[inx] != 0x53 {
		errStr := fmt.Sprintf("amqpx: Expected First byte To be 0x53, got 0x%x", buffer[inx])
		return retVal, bytesUsed, errors.New(errStr)
	}
	inx++

	retVal = buffer[inx]
	inx++
	bytesUsed = inx

	return retVal, bytesUsed, nil
}

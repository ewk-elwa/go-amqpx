package amqpx

import (
	"strings"
	"testing"
)

func TestReadBooleanPrimitive(t *testing.T) {
	booleanCodeTrue := []byte{0x56, 0x01}
	booleanCodeFalse := []byte{0x56, 0x00}
	booleanByteTrue := []byte{0x41}
	booleanByteFalse := []byte{0x42}
	val, _, err := ParseBooleanPrimitive(booleanCodeTrue)
	if err != nil {
		t.Errorf("ParseBooleanPrimitive was incorrect, expected no errors")
	} else {
		if val != true {
			t.Errorf("ParseBooleanPrimitive (booleanCodeTrue) was incorrect, \n\texpected: \"%v\" \n\tgot:\"%v\"", true, val)
		}
	}

	val, _, err = ParseBooleanPrimitive(booleanCodeFalse)
	if err != nil {
		t.Errorf("ParseBooleanPrimitive was incorrect, expected no errors")
	} else {
		if val != false {
			t.Errorf("ParseBooleanPrimitive (booleanCodeFalse) was incorrect, \n\texpected: \"%v\" \n\tgot:\"%v\"", false, val)
		}
	}

	val, _, err = ParseBooleanPrimitive(booleanByteTrue)
	if err != nil {
		t.Errorf("ParseBooleanPrimitive was incorrect, expected no errors")
	} else {
		if val != true {
			t.Errorf("ParseBooleanPrimitive (booleanByteTrue) was incorrect, \n\texpected: \"%v\" \n\tgot:\"%v\"", true, val)
		}
	}

	val, _, err = ParseBooleanPrimitive(booleanByteFalse)
	if err != nil {
		t.Errorf("ParseBooleanPrimitive was incorrect, expected no errors")
	} else {
		if val != false {
			t.Errorf("ParseBooleanPrimitive (booleanByteFalse) was incorrect, \n\texpected: \"%v\" \n\tgot:\"%v\"", false, val)
		}
	}
}

func TestReadStringPrimitive(t *testing.T) {
	valConstructor := []byte{0xa1, 0x1e}
	valBuf := []byte("Hello Glorious Messaging World")
	goodBuffer := append(valConstructor, valBuf...)

	val, _, err := ParseStringPrimitive(goodBuffer)
	if err != nil {
		t.Errorf("ParseStringPrimitive was incorrect, expected no errors")
	} else {
		if strings.Compare(string(valBuf), val) != 0 {
			t.Errorf("ParseStringPrimitive was incorrect string, \n\texpected: \"%s\" \n\tgot:\"%s\"", string(valBuf), val)
		} else {
			t.Logf("ParseStringPrimitive str8 pass")
		}
	}

	valConstructor = []byte{0xb1, 0x00, 0x00, 0x00, 0x1e}
	goodBuffer = append(valConstructor, valBuf...)
	val, _, err = ParseStringPrimitive(goodBuffer)
	if err != nil {
		t.Errorf("ParseStringPrimitive was incorrect, expected no errors")
	} else {
		if strings.Compare(string(valBuf), val) != 0 {
			t.Errorf("ParseStringPrimitive was incorrect string, \n\texpected: \"%s\" \n\tgot:\"%s\"", string(valBuf), val)
		} else {
			t.Logf("ParseStringPrimitive str32 pass")
		}
	}
}

func TestReadBinaryPrimitive(t *testing.T) {
	valConstructor := []byte{0xa0, 0x01}
	valBuf := []byte{0x12}
	goodBuffer := append(valConstructor, valBuf...)

	val, _, err := ParseBinaryPrimitive(goodBuffer)
	if err != nil {
		t.Errorf("ParseBinaryPrimitive was incorrect, expected no errors")
	} else {
		if val[0] != valBuf[0] {
			t.Errorf("ParseBinaryPrimitive binary8 was incorrect, \n\texpected: \"%s\" \n\tgot:\"%s\"", valBuf, val)
		}
		t.Logf("ParseBinaryPrimitive binary8 pass")
	}

	valConstructor = []byte{0xb0, 0x00, 0x00, 0x00, 0x04}
	valBuf = []byte{0x12, 0x34, 0x56, 0x78}
	goodBuffer = append(valConstructor, valBuf...)

	val, _, err = ParseBinaryPrimitive(goodBuffer)
	if err != nil {
		t.Errorf("ParseBinaryPrimitive was incorrect, expected no errors")
	} else {
		if val[0] != valBuf[0] {
			t.Errorf("ParseBinaryPrimitive binary32 was incorrect, \n\texpected: \"%s\" \n\tgot:\"%s\"", valBuf, val)
		}
		t.Logf("ParseBinaryPrimitive binary32 pass")
	}
}

func TestReadListPrimitive(t *testing.T) {
	valConstructor := []byte{0xd0}
	valBuf := []byte{0x00, 0x00, 0x00, 0x42, 0x00, 0x00, 0x00, 0x0a, 0xa1, 0x24, 0x63, 0x62, 0x64, 0x35, 0x65, 0x36, 0x33, 0x63, 0x2d, 0x33, 0x66, 0x34, 0x34, 0x2d, 0x34, 0x39, 0x32, 0x66, 0x2d, 0x38, 0x30, 0x37, 0x62, 0x2d, 0x65, 0x37, 0x35, 0x33, 0x36, 0x64, 0x39, 0x64, 0x62, 0x35, 0x30, 0x65, 0xa1, 0x08, 0x74, 0x65, 0x73, 0x74, 0x68, 0x6f, 0x73, 0x74, 0x40, 0x60, 0x7f, 0xff, 0x70, 0x00, 0x00, 0x75, 0x30, 0x40, 0x40, 0x40, 0x40, 0x40}
	goodBuffer := append(valConstructor, valBuf...)

	size, countItems, inxFirstItem, _, err := ParseListPrimitive(goodBuffer)
	if err != nil {
		t.Errorf("ParseListPrimitive was incorrect, expected no errors")
	} else {
		if size != 0x42 {
			t.Errorf("ParseListPrimitive list32 was incorrect Size, \n\texpected: \"%s\" \n\tgot:\"0x%x\"", "0x42", size)
		}
		if countItems != 0x0a {
			t.Errorf("ParseListPrimitive list32 was incorrect countItems, \n\texpected: \"%s\" \n\tgot:\"0x%x\"", "0x0a", countItems)
		}
		if inxFirstItem != 9 {
			t.Errorf("ParseListPrimitive list32 was incorrect inxFirstItem, \n\texpected: \"%s\" \n\tgot:\"%d\"", "0", inxFirstItem)
		}

		// Try it out...
		// conatinerID, err := ParseStringPrimitive(goodBuffer[inxFirstItem:])
		// if err != nil {
		// 	log.Debug("FAILED containerid:", err.Error())
		// } else {
		// 	fmt.Printf("PASS containerid: %s\n", conatinerID)
		// }
	}
}

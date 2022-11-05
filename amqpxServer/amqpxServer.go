package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"time"

	amqpx "github.com/ewk-elwa/go-amqp/amqpx"

	"github.com/ewk-elwa/go-play/utils"
	log "github.com/mgutz/logxi/v1"
)

// Example AMQPx TCP server using "net" package
// ... Using  Listen, and Accept... Conn and Listener interfaces
const (
	tagSize    = 4
	lengthSize = 8
)

func serializeStruct(myStruct interface{}) []byte {
	var binBuf bytes.Buffer
	binary.Write(&binBuf, binary.BigEndian, myStruct)
	return binBuf.Bytes()
}

// Read reads up 'til buffer length
func (client *amqpClient) Read(rxBuf []byte) (n int, err error) {
	client.conn.SetReadDeadline(time.Now().Add(client.readTimeout))
	return client.conn.Read(rxBuf)
}

// ReadUntilFull reads until the rx buffer is full
func (client *amqpClient) ReadUntilFull(rxBuf []byte) (n int, err error) {
	byteCount := 0
	size := len(rxBuf)
	for byteCount < size {
		client.conn.SetReadDeadline(time.Now().Add(client.readTimeout))
		bytesRead, err := client.conn.Read(rxBuf[byteCount:])
		byteCount += bytesRead
		if err != nil || byteCount >= size {
			return byteCount, err
		}
	}
	return byteCount, err
}

func (client *amqpClient) Write(rxBuf []byte) (n int, err error) {
	client.conn.SetWriteDeadline(time.Now().Add(client.readTimeout))
	return client.conn.Write(rxBuf)
}

func handleAmqpVersion(client amqpClient) (err error) {
	// Make a buffer to hold the Version message
	log.Debug("handleAmqpVersion():Entered")
	rxBuf := make([]byte, sizeAmqpVersionStruct)

	_, err = client.ReadUntilFull(rxBuf)
	if err != nil {
		log.Debug("handleAmqpVersion():Error reading AMQP Version:", err.Error())
		return err
	}

	protocolVersion, _, err := amqpx.ParseProtocolHeader(rxBuf)
	if err != nil {
		return err
	}

	myAmqpVersion := serializeStruct(amqpVersion0100)
	recvAmqpVersion := serializeStruct(protocolVersion)
	if !bytes.Equal(myAmqpVersion, recvAmqpVersion) {
		fmt.Printf("handleAmqpVersion():Error bad AMQP Version received: % x", recvAmqpVersion)
		return errors.New("Bad AMQP Protocol Version")
	}
	return nil
}

func handleAmqpOpen(client amqpClient) (err error) {
	log.Debug("handleAmqpOpen():Entered")
	// First read 4 bytes of length
	amqpLengthBuf := make([]byte, 4)
	_, err = client.ReadUntilFull(amqpLengthBuf)
	if err != nil {
		log.Debug("handleAmqpOpen():Error reading AMQP Length: ", err.Error())
		return err
	}
	amqpLength := binary.BigEndian.Uint32(amqpLengthBuf[:])

	frameBuf := make([]byte, amqpLength)
	copy(frameBuf[:], amqpLengthBuf)
	_, err = client.Read(frameBuf[4:])
	if err != nil {
		log.Debug("handleAmqpOpen():Error reading AMQP Open Frame: ", err.Error())
		return err
	}

	_, bytesUsed, performative, err := amqpx.ParseFraming(frameBuf)
	if err != nil {
		log.Debug("handleAmqpOpen():Error OPEN performative:", err.Error())
		return err
	}

	if performative == amqpx.PerfOpen {
		connParameters, _, err := amqpx.ParsePerformativeOpen(frameBuf[bytesUsed:])

		if err != nil {
			log.Debug("handleAmqpOpen():ReadOpenPerformative was incorrect, expected no errors", err.Error())
		} else {
			log.Debug("connection parameters")
			log.Debug("\tcontainer-id:", connParameters.ContainerId)
			log.Debug("\thostname:", connParameters.Hostname)
			log.Debug("\tmaxFrameSize:", connParameters.MaxFrameSize)
			log.Debug("\tchannelMax:", connParameters.ChannelMax)
			log.Debug("\tidleTimeoutMs", connParameters.IdleTimeoutMs)
		}
	} else {
		return errors.New("handleAmqpOpen():Error did not read expected OPEN performative")
	}
	sendAmqpVersionAndOpen(client)
	return nil
}

func sendAmqpVersionAndOpen(client amqpClient) {
	log.Debug("sendAmqpVersionAndOpen():Entered")
	versionBuf := serializeStruct(amqpVersion0100)

	// Build arguments block
	arguments := amqpArgumentsStruct{
		unknownField:     unknownArgumentsField,
		a1TagContainerID: a1TagContainerID,
		containerLength:  byte(len(client.containerID)),
		containerID:      client.containerID,
		a1TagHostname:    a1TagHostname,
		hostnameLength:   byte(len(client.hostname)),
		hostname:         client.hostname,
		tagChannelMax:    tagChannelMax,
		channelMax:       client.channelMax,
		tagIdleTimeout:   tagIdleTimeout,
		idleTimeout:      client.idleTimeout,
	}
	argumentsBuf := serializeArgumentsStruct(arguments)
	fmt.Printf("sendAmqpVersionAndOpen():serializeArgumentsStruct(%v): % x\n", len(argumentsBuf), argumentsBuf)

	// Build header block last since it needs the length
	var frameLength = uint32(sizeAmqpHeaderStruct + len(argumentsBuf) + len(amqpEndFrame))

	writeBuf := make([]byte, frameLength+uint32(len(versionBuf)))

	hdrBuf := serializeStruct(amqpHeaderStruct{frameLength, doff, theType, 0, perfTag, amqpOpen})

	inx := 0
	copy(writeBuf[inx:], versionBuf)
	inx += len(versionBuf)
	copy(writeBuf[inx:], hdrBuf)
	inx += len(hdrBuf)
	copy(writeBuf[inx:], argumentsBuf)
	inx += len(argumentsBuf)
	copy(writeBuf[inx:], amqpEndFrame)

	client.Write(writeBuf)
}

func handleAmqpBeginAttachFlow(client amqpClient) (err error) {
	log.Debug("handleAmqpBeginAttachFlow():Entered")

	for i := 0; i < 4; i++ {
		// First read 4 bytes of length
		amqpLengthBuf := make([]byte, 4)
		_, err := client.Read(amqpLengthBuf)
		if err != nil {
			log.Debug("handleAmqpBeginAttachFlow():Error reading AMQP Length: ", err.Error())
			return err
		}
		amqpLength := binary.BigEndian.Uint32(amqpLengthBuf[:])

		frameBuf := make([]byte, amqpLength)
		copy(frameBuf[:], amqpLengthBuf)
		_, err = client.Read(frameBuf[4:])
		if err != nil {
			log.Debug("handleAmqpBeginAttachFlow():Error reading AMQP Frame: ", err.Error())
			return err
		}

		frameInx := uint32(0)
		_, bytesUsed, performative, err := amqpx.ParseFraming(frameBuf[frameInx:])
		if err != nil {
			log.Debug("handleAmqpBeginAttachFlow():Error parsing performative: ", err.Error())
			return err
		}
		frameInx += bytesUsed
		switch performative {
		case amqpx.PerfBegin:
			sessionParameters, bytesUsed, err := amqpx.ParsePerformativeBegin(frameBuf[frameInx:])
			frameInx += bytesUsed
			if err != nil {
				log.Debug("handleAmqpBeginAttachFlow():Error ParsePerformativeBegin: ", err.Error())
				return err
			}
			log.Debug("session parameters")
			log.Debug("\t remoteChannel:", sessionParameters.RemoteChannel)
			log.Debug("\t nextOutgoing:", sessionParameters.NextOutgoing)
			log.Debug("\t incomingWindow:", sessionParameters.IncomingWindow)
			log.Debug("\t outgoingWindow:", sessionParameters.OutgoingWindow)

		case amqpx.PerfAttach:
			attach, bytesUsed, err := amqpx.ParsePerformativeAttach(frameBuf[frameInx:])
			frameInx += bytesUsed
			if err != nil {
				log.Debug("handleAmqpBeginAttachFlow():Error ParsePerformativeAttach: ", err.Error())
				return err
			}
			log.Debug("Attach parameters")
			log.Debug("\t name:", attach.Name)
			log.Debug("\t handle:", attach.Handle)
			log.Debug("\t role:", attach.Role)
			log.Debug("\t sndSettleMode:", attach.SndSettleMode)

		case amqpx.PerfFlow:
			flow, bytesUsed, err := amqpx.ParsePerformativeFlow(frameBuf[frameInx:])
			frameInx += bytesUsed
			if err != nil {
				log.Debug("handleAmqpBeginAttachFlow():Error ParsePerformativeFlow: ", err.Error())
				return err
			}
			log.Debug("Flow parameters")
			log.Debug("\t NextIncoming:", flow.NextIncoming)
			log.Debug("\t IncomingWindow:", flow.IncomingWindow)
		}
	}

	sendAmqpBeginAttachFlow(client)
	return nil
}

func sendAmqpBeginAttachFlow(client amqpClient) {
	log.Debug("sendAmqpBeginAttachFlow():Entered")
	client.tx.session.NextOutgoing = 1
	client.tx.session.IncomingWindow = 0x12345678
	client.tx.session.OutgoingWindow = 0x87654321
	beginBuf := client.tx.session.Serialize()

	var frameLength = uint32(sizeAmqpHeaderStruct + len(beginBuf))

	writeBuf := make([]byte, frameLength)
	hdrBuf := serializeStruct(amqpHeaderStruct{frameLength, doff, theType, 0, perfTag, amqpx.PerfBegin})

	inx := 0
	copy(writeBuf[inx:], hdrBuf)
	inx += len(hdrBuf)
	copy(writeBuf[inx:], beginBuf)

	client.Write(writeBuf)
}

func handleAmqpLifecycle(client amqpClient) (err error) {
	log.Debug("handleAmqpLifecycle():Entered")

	for {
		// First read length from net client to know buffer size
		amqpLengthBuf := make([]byte, 4)
		_, err := client.ReadUntilFull(amqpLengthBuf)
		if err != nil {
			log.Debug("handleAmqpLifecycle():Error reading AMQP Length: ", err.Error())
			return err
		}
		amqpLength := binary.BigEndian.Uint32(amqpLengthBuf[:])

		// Create properly sized buffer, and read that number of bytes from net client
		frameBuf := make([]byte, amqpLength)
		copy(frameBuf[:], amqpLengthBuf)
		_, err = client.ReadUntilFull(frameBuf[4:])
		if err != nil {
			log.Debug("handleAmqpLifecycle():Error reading AMQP Frame: ", err.Error())
			return err
		}

		// From here on, we will just parse from the frame buffer we just read from the net client
		frameInx := uint32(0)
		_, bytesUsed, performative, err := amqpx.ParseFraming(frameBuf)
		frameInx += bytesUsed
		if err != nil {
			log.Debug("handleAmqpLifecycle():Error parsing frame:", err.Error())
			return err
		}

		switch performative {
		case amqpx.PerfOpen:
			connParameters, bytesUsed, err := amqpx.ParsePerformativeOpen(frameBuf[frameInx:])
			frameInx += bytesUsed

			if err != nil {
				log.Debug("handleAmqpLifecycle():Error ReadOpenPerformative", err.Error())
				return err
			}
			client.rx.openParams = connParameters
			// TODO(eking) the "idleTimeout field should be used to set a keepAliveInterval"
			log.Debug("connection parameters:", client.rx.openParams.ContainerId)

		case amqpx.PerfBegin:
			sessionParameters, bytesUsed, err := amqpx.ParsePerformativeBegin(frameBuf[frameInx:])
			frameInx += bytesUsed
			if err != nil {
				log.Debug("handleAmqpLifecycle():Error ParsePerformativeBegin", err.Error())
				return err
			}
			client.rx.session = sessionParameters
			log.Debug("session parameters:", client.rx.session.IncomingWindow)

		case amqpx.PerfAttach:
			attach, bytesUsed, err := amqpx.ParsePerformativeAttach(frameBuf[frameInx:])
			frameInx += bytesUsed
			if err != nil {
				log.Debug("handleAmqpLifecycle():Error ParsePerformativeAttach", err.Error())
				return err
			}
			client.rx.attach = attach
			log.Debug("Attach parameters:", client.rx.attach.Name)

		case amqpx.PerfFlow:
			flow, bytesUsed, err := amqpx.ParsePerformativeFlow(frameBuf[frameInx:])
			frameInx += bytesUsed
			if err != nil {
				log.Debug("handleAmqpLifecycle():Error ParsePerformativeFlow", err.Error())
				return err
			}
			client.rx.flow = flow
			log.Debug("Flow parameters:", client.rx.flow.IncomingWindow)

		case amqpx.PerfTransfer:
			_, bytesUsed, err := amqpx.ParsePerformativeTransfer(frameBuf[frameInx:])
			frameInx += bytesUsed
			if err != nil {
				log.Debug("handleAmqpLifecycle():Error ParsePerformativeTransfer", err.Error())
				return err
			}
			// TODO(eking) Apply AMQP section 2.5.6 session flow control here.
			log.Debug("Transfer parameters:")

			_, bytesUsed, err = amqpx.ParseBlockType(frameBuf[frameInx:])
			frameInx += bytesUsed
			_, bytesUsed, err = amqpx.ParseMessageHeader(frameBuf[frameInx:])
			frameInx += bytesUsed
			if err != nil {
				log.Debug("handleAmqpLifecycle():Error ParseMessageHeader", err.Error())
				return err
			}
			log.Debug("MessageHeader parameters:")

			_, bytesUsed, err = amqpx.ParseBlockType(frameBuf[frameInx:])
			frameInx += bytesUsed
			_, bytesUsed, err = amqpx.ParseMessageProperties(frameBuf[frameInx:])
			frameInx += bytesUsed
			if err != nil {
				log.Debug("handleAmqpLifecycle():Error ParseMessageProperties", err.Error())
				return err
			}
			log.Debug("MessageProperties parameters:")

			_, bytesUsed, err = amqpx.ParseBlockType(frameBuf[frameInx:])
			frameInx += bytesUsed
			amqpValue, bytesUsed, err := amqpx.ParseMessageAmqpValue(frameBuf[frameInx:])
			frameInx += bytesUsed
			if err != nil {
				log.Debug("handleAmqpLifecycle():Error ParseMessageAmqpValue", err.Error())
				return err
			}
			fmt.Printf("MessageAmqpValue parameters: %v", amqpValue)

		case amqpx.PerfDisposition:
			disposition, bytesUsed, err := amqpx.ParsePerformativeDisposition(frameBuf[frameInx:])
			frameInx += bytesUsed
			if err != nil {
				log.Debug("handleAmqpLifecycle():Error ParsePerformativeDisposition", err.Error())
				return err
			}
			client.rx.disposition = disposition
			// TODO(eking) remove messages that have been dispositioned from the client.tx.unsettled list
			log.Debug("Disposition parameters")

		default:
			log.Debug("handleAmqpLifecycle():Error Not ready for this performative yet ;)")
		}
	}

	//sendAmqpBeginAttachFlow(client)
	//return nil
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func initRand() {
	rand.Seed(time.Now().UnixNano())
}

// RandString generates a random string of alphabets with n characters
func RandString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func handleClientConnection(conn net.Conn) {
	var client amqpClient
	client.conn = conn
	client.containerID = "amqpxServer-" + RandString(12)
	tmp, _ := strconv.ParseUint(utils.GetEnv("AMQPX_SERVER_CHANNELMAX", "1"), 10, 16)
	client.channelMax = uint16(tmp)
	tmp, _ = strconv.ParseUint(utils.GetEnv("AMQPX_SERVER_IDLETIMEOUT", "1111"), 10, 32)
	client.idleTimeout = uint32(tmp)
	client.hostname = utils.GetEnv("AMQPX_SERVER_HOSTNAME", "amqpxServer")
	tmpInt, _ := strconv.ParseInt(utils.GetEnv("AMQPX_SERVER_READTIMEOUT", "5"), 10, 32)
	client.readTimeout = time.Duration(tmpInt) * time.Second

	defer func() {
		log.Debug("Closing client connection TBD details about connection")
		conn.Close()
	}()

	err := handleAmqpVersion(client)
	if err != nil {
		log.Debug("Closing client connection after AmqpVersion")
		return
	}

	err = handleAmqpOpen(client)
	if err != nil {
		log.Debug("Closing client connection after AmqpOpen")
		return
	}

	err = handleAmqpBeginAttachFlow(client)
	if err != nil {
		log.Debug("Closing client connection after handleAmqpBeginAttachFlow")
		return
	}

	err = handleAmqpLifecycle(client)
	if err != nil {
		log.Debug("Closing client connection after handleAmqpLifecycle")
		return
	}
}

const (
	connHost = "0.0.0.0"
	connType = "tcp"
)

func server(serverListenPort string) {
	defer log.Debug("Exiting server")

	// Listen for incomming connections
	serverHost := utils.GetEnv("AMQPX_SERVER_HOSTIP", "0.0.0.0")
	ln, err := net.Listen(connType, serverHost+":"+serverListenPort)
	if err != nil {
		log.Debug("Server error :", err)
		return
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Debug("Accept error :", err)
			return
		}
		go handleClientConnection(conn)
	}
}

func main() {
	initRand()
	serverListenPort := utils.GetEnv("AMQPX_SERVER_PORT", "10010")
	log.Debug("Server listening on port ", serverListenPort)
	server(serverListenPort)
}

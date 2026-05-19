package main

import (
	"bufio"
	"fmt"
)

const (
	ECHO                    = 1
	ProducerRegister        = 2
	ProducerConsumerMessage = 3

	// ResponseEcho Response
	ResponseEcho                    = 101
	ResponseProducerRegister        = 102
	ResponseProducerConsumerMessage = 103
)

type Message struct {
	ECHO                    *string
	ProducerRegister        *ProducerRegisterMessage
	ProducerConsumerMessage []byte // nil-able
	//RESPONSE
	ResponseEcho                    *string
	ResponseProducerRegister        *byte // 0 -> 7
	ResponseProducerConsumerMessage *byte
	//Other type here...
}

type ProducerRegisterMessage struct {
	port    uint16
	topicID uint16
}

func (m *ProducerRegisterMessage) fromByte(streamMessage []byte) {
	// First 2 bytes: port
	// Next 2 bytes: topicID
	m.port = uint16(streamMessage[0])<<8 + uint16(streamMessage[1])
	m.topicID = uint16(streamMessage[2])<<8 + uint16(streamMessage[3])
}

func (m *ProducerRegisterMessage) toByte() []byte {
	var data [4]byte
	// First 2 bytes: port
	// Next 2 bytes: topicID

	// 244 131
	data[0] = byte(m.port >> 8)
	data[1] = byte(m.port % 256)

	data[2] = byte(m.topicID >> 8)
	data[3] = byte(m.topicID % 256)
	return data[0:4]
}

// Message format:
// - stream[0]: size
// -> stream[1:]: []byte
func readFromStream(streamRw *bufio.ReadWriter) ([]byte, error) {
	var err error
	// Read
	header, err := streamRw.ReadByte() // Block
	if err != nil {
		return nil, err
	}

	buf, err := streamRw.Peek(int(header)) // Block
	if err != nil {
		return nil, err
	}
	data := make([]byte, int(header))
	copy(data, buf)

	_, err = streamRw.Discard(int(header))
	if err != nil {
		return nil, err
	}

	return data, err
}

func parseMessage(streamMessage []byte) *Message {
	if streamMessage == nil {
		return nil
	}
	switch streamMessage[0] {
	case ECHO:
		//fmt.Printf("%p\n", streamMessage)
		var st = string(streamMessage[1:])
		//fmt.Printf("%p\n", &st)
		return &Message{ECHO: &st}
	case ResponseEcho:
		var st = string(streamMessage[1:])
		return &Message{ResponseEcho: &st}
	case ProducerRegister:
		p := ProducerRegisterMessage{}
		p.fromByte(streamMessage[1:])
		return &Message{ProducerRegister: &p}
	case ResponseProducerRegister:
		var st = streamMessage[1]
		return &Message{ResponseProducerRegister: &st}
	case ProducerConsumerMessage:
		return &Message{ProducerConsumerMessage: streamMessage[1:]}
	case ResponseProducerConsumerMessage:
		var st = streamMessage[1]
		return &Message{ResponseProducerConsumerMessage: &st}
	default:
		return nil
	}
}

func readMessageFromStream(streamRW *bufio.ReadWriter) (*Message, error) {
	data, err := readFromStream(streamRW)
	if err != nil {
		return nil, err
	}
	return parseMessage(data), nil
}

func writeDataToStreamWithType(streamRw *bufio.ReadWriter, mType byte, data string) error {
	var err error
	// Write length
	err = streamRw.WriteByte(byte(len(data) + 1))
	if err != nil {
		return err
	}
	// Write type
	err = streamRw.WriteByte(mType)
	if err != nil {
		return err
	}
	// Write data
	_, err = streamRw.WriteString(data)
	if err != nil {
		return err
	}
	err = streamRw.Flush()
	if err != nil {
		return err
	}
	return nil
}

// [ 7  1  h e l l o o ]
func writeMessageToStream(streamRW *bufio.ReadWriter, message Message) error {
	if message.ECHO != nil {
		if err := writeDataToStreamWithType(streamRW, ECHO, *message.ECHO); err != nil {
			return err
		}
	} else if message.ResponseEcho != nil {
		if err := writeDataToStreamWithType(streamRW, ResponseEcho, *message.ResponseEcho); err != nil {
			return err
		}
	}
	if message.ProducerRegister != nil {
		data := string(message.ProducerRegister.toByte())
		if err := writeDataToStreamWithType(streamRW, ProducerRegister, data); err != nil {
			return err
		}
	}
	if message.ResponseProducerRegister != nil {
		data := fmt.Sprintf("%d", *message.ResponseProducerRegister)
		if err := writeDataToStreamWithType(streamRW, ResponseProducerRegister, data); err != nil {
			return err
		}
	}
	if message.ProducerConsumerMessage != nil {
		if err := writeDataToStreamWithType(streamRW, ProducerConsumerMessage, string(message.ProducerConsumerMessage)); err != nil {
			return err
		}
	}
	if message.ResponseProducerConsumerMessage != nil {
		data := fmt.Sprintf("%d", *message.ResponseProducerConsumerMessage)
		if err := writeDataToStreamWithType(streamRW, ResponseProducerConsumerMessage, data); err != nil {
			return err
		}
	}
	return nil
}

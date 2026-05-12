package main

import (
	"bufio"
	"fmt"
)

const (
	ECHO             = 1
	ProducerRegister = 2

	//ACK
	ResponseEcho             = 101
	ResponseProducerRegister = 102
)

type Message struct {
	ECHO             *string
	ProducerRegister *string
	//RESPONSE
	ResponseEcho             *string
	ResponseProducerRegister *byte // 0 -> 7
	//Other type here...
}

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
		var st = string(streamMessage[1:])
		return &Message{ECHO: &st}
	case ResponseEcho:
		var st = string(streamMessage[1:])
		return &Message{ResponseEcho: &st}
	case ProducerRegister:
		var st = string(streamMessage[1:])
		return &Message{ProducerRegister: &st}
	case ResponseProducerRegister:
		var st = streamMessage[1]
		return &Message{ResponseProducerRegister: &st}
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
		if err := writeDataToStreamWithType(streamRW, ProducerRegister, *message.ProducerRegister); err != nil {
			return err
		}
	}
	if message.ResponseProducerRegister != nil {
		data := fmt.Sprintf("%d", *message.ResponseProducerRegister)
		if err := writeDataToStreamWithType(streamRW, ResponseProducerRegister, data); err != nil {
			return err
		}
	}
	return nil
}

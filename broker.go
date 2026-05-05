package main

import (
	"bufio"
	"fmt"
	"net"
)

const BrokerPort = 10000

const (
	ECHO = 1
	// Other message types
)

type Message struct {
	ECHO *string
	// Other type here...
}

func readFromStream(streamRw *bufio.ReadWriter) ([]byte, error) {
	var err error
	// Read
	header, err := streamRw.ReadByte() // Block
	if err != nil {
		return nil, err
	}

	data, err := streamRw.Peek(int(header)) // Block
	if err != nil {
		return nil, err
	}

	_, err = streamRw.Discard(int(header))
	if err != nil {
		return nil, err
	}

	return data, err
}

func writeToStream(streamRw *bufio.ReadWriter, data string) error {
	var err error
	// Write
	err = streamRw.WriteByte(byte(len(data)))
	if err != nil {
		return err
	}
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

type Broker struct {
}

func (b *Broker) startBrokerServer() error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", BrokerPort))
	if err != nil {
		panic(err)
	}
	for {
		conn, _ := ln.Accept() // Block until can
		streamRw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

		var err error
		data, err := readFromStream(streamRw)
		if err != nil {
			return err
		}

		// Process
		parsedMessage := b.parseBrokerMessage(data)
		if parsedMessage != nil {
			resp, err := b.processBrokerMessage(parsedMessage)
			if err != nil {
				return err
			}
			// Write it back
			err = writeToStream(streamRw, resp)
			if err != nil {
				return err
			}
		}

		err = conn.Close()
		if err != nil {
			return err
		}
	}
}

func (b *Broker) parseBrokerMessage(message []byte) *Message {
	switch message[0] {
	case ECHO:
		var st = string(message[1:])
		return &Message{ECHO: &st}
	default:
		return nil
	}
}

func (b *Broker) processBrokerMessage(message *Message) (string, error) {
	var err error
	var resp string

	if message.ECHO != nil {
		resp = fmt.Sprintf("I have receiver: %s", *message.ECHO)
	}
	return resp, err
}

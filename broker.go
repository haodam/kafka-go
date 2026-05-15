package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
)

const BrokerPort = 10000

type Broker struct {
	mq Queue
}

func (b *Broker) startBrokerServer() error {
	var err error
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", BrokerPort))
	if err != nil {
		panic(err)
	}
	for {
		conn, err := ln.Accept() // Block until can
		if err != nil {
			return err
		}
		go b.handleConnection(conn)
	}
}

func (b *Broker) handleConnection(conn net.Conn) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			return
		}
	}(conn)
	streamRw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	for {
		parsedMessage, err := readMessageFromStream(streamRw)
		if err != nil {
			fmt.Println("read error:", err)
			return
		}

		if parsedMessage == nil {
			fmt.Println("invalid message")
			return
		}

		resp, err := b.processBrokerMessage(parsedMessage)
		if err != nil {
			fmt.Println("process error:", err)
			return
		}

		if resp == nil {
			fmt.Println("nil response")
			return
		}

		err = writeMessageToStream(streamRw, *resp)
		if err != nil {
			fmt.Println("write error:", err)
			return
		}
	}
}

// Process:
// - Call inner process function for each message type
// - Response correct Message
func (b *Broker) processBrokerMessage(message *Message) (*Message, error) {
	if message.ECHO != nil {
		resp, err := b.processEchoMessage(message.ECHO)
		if err != nil {
			return nil, err
		}
		return &Message{ResponseEcho: &resp}, nil
	}
	if message.ProducerRegister != nil {
		resp, err := b.processProducerRegisterMessage(message.ProducerRegister)
		if err != nil {
			return nil, err
		}
		return &Message{ResponseProducerRegister: resp}, nil
	}
	if message.ProducerConsumerMessage != nil {
		resp, err := b.processProducerConsumerMessage(message.ProducerConsumerMessage)
		if err != nil {
			return nil, err
		}
		return &Message{ResponseProducerConsumerMessage: &resp}, nil
	}
	return nil, nil
}

func (b *Broker) processProducerConsumerMessage(pcm []byte) (byte, error) {
	b.mq.push(pcm)
	b.mq.debug()
	return 0, nil
}

func (b *Broker) processEchoMessage(echoMessage *string) (string, error) {
	return fmt.Sprintf("I have receiver: %s", *echoMessage), nil
}

func (b *Broker) processProducerRegisterMessage(pRegMessage *string) (*byte, error) {
	port, err := strconv.ParseInt(*pRegMessage, 10, 32)
	if err != nil {
		return nil, err
	}
	go func() {
		conn, err := net.Dial("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			panic(err)
		}
		fmt.Printf("Connected to server at port %v\n", port)
		// Read input from stdin and write to stream.
		streamRw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		for {
			parsedMessage, err := readMessageFromStream(streamRw)
			if parsedMessage == nil || err != nil {
				panic(err)
			}
			// Process something here
			resp, err := b.processBrokerMessage(parsedMessage)
			if err != nil {
				panic(err)
			}
			err = writeMessageToStream(streamRw, *resp)
			if err != nil {
				panic(err)
			}
		}
	}()
	var resp byte = 0
	return &resp, err
}

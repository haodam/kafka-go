package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
)

type Producer struct {
}

// Connect to Broker to send register
func (b *Producer) sendPortDataToBroker(port int16) error {
	var err error
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", BrokerPort))
	if err != nil {
		return err
	}
	streamRw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	portStr := fmt.Sprintf("%d", port)
	message := Message{
		ProducerRegister: &portStr,
	}
	err = writeMessageToStream(streamRw, message)
	if err != nil {
		panic(err)
	}

	// Try to read back from the stream
	resp, err := readMessageFromStream(streamRw)
	if err != nil {
		panic(err)
	}

	if resp == nil || resp.ResponseProducerRegister == nil {
		return fmt.Errorf("invalid broker response")
	}

	fmt.Printf("Receive response from broker: %v\n", *resp.ResponseProducerRegister)
	return nil
}

func (b *Producer) startProducerServer(port int16) error {
	var err error

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}

	err = b.sendPortDataToBroker(port)
	if err != nil {
		panic(err)
	}

	conn, err := ln.Accept() // Block until can
	if err != nil {
		panic(err)
	}
	streamRw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	rd := bufio.NewReader(os.Stdin)

	for {
		// Read from stdin
		line, err := rd.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				// Probably panic here
			}
		}
		// Write ProducerConsumerMessage
		err = writeMessageToStream(streamRw, Message{
			ProducerConsumerMessage: []byte(line),
		})
		if err != nil {
			break
		}
		// Try to read back from the stream
		resp, err := readMessageFromStream(streamRw)
		if err != nil {
			break
		}

		fmt.Printf("Receive producer consumer message from broker: %d\n", *resp.ResponseProducerConsumerMessage)

	}
	err = conn.Close()
	if err != nil {
		return err
	}
	return nil
}

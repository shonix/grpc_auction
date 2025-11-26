package main

import (
	proto "auction/grpc"
	"fmt"
	"log"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	lamportClock int64
	clockMu      sync.Mutex
	clientId     string
}

func (c *Client) incrementClock() int64 {
	c.clockMu.Lock()
	defer c.clockMu.Unlock()
	c.lamportClock++
	return c.lamportClock
}

func (c *Client) updateClock(receivedTime int64) int64 {
	c.clockMu.Lock()
	defer c.clockMu.Unlock()
	if receivedTime > c.lamportClock {
		c.lamportClock = receivedTime
	}
	c.lamportClock++
	return c.lamportClock
}

func main() {
	serverAddress := "localhost:5050"

	conn, err := grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("could not connect to server: %v", err)
	}
	defer conn.Close()

	//biddingClient := proto.NewBiddingClient(conn)
	//auctionClient := proto.NewAuctionClient(conn)

	//create bidding stream

}

func setupBiddingStream(stream proto.Bidding_BidClient) {
	for {
		ack, err := stream.Recv()
		if err != nil {
			log.Fatalf("Bidding stream closed:", err)
			return
		}
		fmt.Println("Recieved ACK:", ack)
	}
}

func setupSubscribeStream(stream proto.Auction_SubscribeClient) {
	for {
		result, err := stream.Recv()
		if err != nil {
			log.Fatalf("Subscribe stream closed:", err)
			return
		}
		fmt.Println("Recieved ACK:", result)
	}
}

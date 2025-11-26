package main

import (
	proto "auction/grpc"
	"context"
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

	biddingClient := proto.NewBiddingClient(conn)
	auctionClient := proto.NewAuctionClient(conn)
	queryClient := proto.NewQueryClient(conn)

	client := &Client{clientId: "client1"}
	ctx := context.Background()

	//Setting up bidding stream
	bidStream, err := biddingClient.Bid(ctx)
	if err != nil {
		log.Fatalf("could not open bidding stream: %v", err)
	}
	go setupBiddingStream(client, bidStream)

	//Setting up subscription to server auction push notifications
	subReq := &proto.SubscribeRequest{
		LamportTimestamp: client.incrementClock(),
		ClientId:         client.clientId,
	}

	subStream, err := auctionClient.Subscribe(ctx, subReq)
	if err != nil {
		log.Fatalf("could not open auction stream: %v", err)
	}
	go setupSubscribeStream(client, subStream)

	//console controls
	for {
		var cmd string
		fmt.Println("> Enter command (bid/query)")
		fmt.Scanln(&cmd)

		switch cmd {
		case "bid":
			var amount int64
			fmt.Println("> Enter bid amount: ")
			fmt.Scanln(&amount)

			if err := bid(client, bidStream, amount); err != nil {
				log.Fatalf("could not send bid: %v", err)
			}
		case "query":
			ts := client.incrementClock()
			status, err := queryClient.GetAuctionStatus(ctx, &proto.QueryRequest{
				LamportTimestamp: ts,
				ClientId:         client.clientId,
			})
			if err != nil {
				fmt.Printf("could not get auction status: %v", err)
				continue
			}
			client.updateClock(status.LamportTimestamp)
			fmt.Println("STATUS: ", status)
		default:
			fmt.Println("Invalid command. Use 'bid' or 'query'.")
		}

	}

}

func setupBiddingStream(client *Client, stream proto.Bidding_BidClient) {
	for {
		ack, err := stream.Recv()
		if err != nil {
			log.Fatalf("Bidding stream closed:", err)
			return
		}
		client.updateClock(ack.LamportTimestamp)
		fmt.Println("ACK:", ack)
	}
}

func setupSubscribeStream(client *Client, stream proto.Auction_SubscribeClient) {
	for {
		result, err := stream.Recv()
		if err != nil {
			log.Fatalf("Subscribe stream closed:", err)
			return
		}
		client.updateClock(result.LamportTimestamp)
		fmt.Println("AUCTION UPDATE:", result)
	}
}

func bid(c *Client, stream proto.Bidding_BidClient, amount int64) error {
	ts := c.incrementClock()

	bid := &proto.BidRequest{
		LamportTimestamp: ts,
		ClientId:         c.clientId,
		Amount:           amount,
	}

	return stream.Send(bid)
}

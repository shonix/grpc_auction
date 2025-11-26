package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	proto "auction/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type BidderClient struct {
	clientID    string
	lamportTime int64
	timeLock    sync.Mutex

	conn          *grpc.ClientConn
	biddingClient proto.BiddingClient
	auctionClient proto.AuctionClient
	queryClient   proto.QueryClient

	bidStream proto.Bidding_BidClient
}

func NewBidderClient(clientID, serverAddress string) (*BidderClient, error) {
	conn, err := grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to the server: %v", err)
	}

	client := &BidderClient{
		clientID:      clientID,
		lamportTime:   0,
		conn:          conn,
		biddingClient: proto.NewBiddingClient(conn),
		auctionClient: proto.NewAuctionClient(conn),
		queryClient:   proto.NewQueryClient(conn),
	}

	return client, nil
}

func (c *BidderClient) incrementTime() int64 {
	c.timeLock.Lock()
	defer c.timeLock.Unlock()
	c.lamportTime++
	return c.lamportTime
}

func (c *BidderClient) syncTime(receivedTime int64) int64 {
	c.timeLock.Lock()
	defer c.timeLock.Unlock()
	if receivedTime > c.lamportTime {
		c.lamportTime = receivedTime
	}
	c.lamportTime++
	return c.lamportTime
}

func (c *BidderClient) startBidStream(ctx context.Context) error {
	stream, err := c.biddingClient.Bid(ctx)
	if err != nil {
		return fmt.Errorf("couldn't start the bid stream: %v", err)
	}
	c.bidStream = stream

	// listneing forr bid responses
	go c.listenForBidResponses()

	return nil
}

func (c *BidderClient) listenForBidResponses() {
	for {
		ack, err := c.bidStream.Recv()
		if err == io.EOF {
			log.Println("The bid stream has been closed by the server")
			return
		}
		if err != nil {
			log.Printf("Error receiving bid response: %v", err)
			return
		}

		c.syncTime(ack.LamportTimestamp)

		switch ack.Status {
		case "SUCCESS":
			fmt.Println("The bid is accepted!")
		case "FAIL":
			fmt.Println("The bid is rejected: amount too low")
		case "EXCEPTION":
			fmt.Println("The bid is rejected: the auction is over")
		}
	}
}

func (c *BidderClient) placeBid(amount int64) error {
	if c.bidStream == nil {
		return fmt.Errorf("the bid stream is not started")
	}

	request := &proto.BidRequest{
		LamportTimestamp: c.incrementTime(),
		ClientId:         c.clientID,
		Amount:           amount,
	}

	err := c.bidStream.Send(request)
	if err != nil {
		return fmt.Errorf("couldn't send the bid: %v", err)
	}

	log.Printf("Sent bid: %d", amount)
	return nil
}

func (c *BidderClient) getAuctionStatus(ctx context.Context) (*proto.AuctionStatus, error) {
	request := &proto.QueryRequest{
		LamportTimestamp: c.incrementTime(),
		ClientId:         c.clientID,
	}

	status, err := c.queryClient.GetAuctionStatus(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("couldn't get the auction status: %v", err)
	}

	c.syncTime(status.LamportTimestamp)
	return status, nil
}

func (c *BidderClient) subscribeToResults(ctx context.Context) error {
	request := &proto.SubscribeRequest{
		LamportTimestamp: c.incrementTime(),
		ClientId:         c.clientID,
	}

	stream, err := c.auctionClient.Subscribe(ctx, request)
	if err != nil {
		return fmt.Errorf("couldn't subscribe subscribe: %v", err)
	}

	// listneing for auction results
	go c.listenForAuctionResult(stream)

	return nil
}

func (c *BidderClient) listenForAuctionResult(stream proto.Auction_SubscribeClient) {
	for {
		result, err := stream.Recv()
		if err == io.EOF {
			log.Println("The result stream is closed")
			return
		}
		if err != nil {
			log.Printf("Error receiving the result: %v", err)
			return
		}

		c.syncTime(result.LamportTimestamp)

		fmt.Println("\n***** THE AUCTION HAS ENDED *****")
		if result.ClientId == "" {
			fmt.Println("No winner (since no bids were placed)")
		} else {
			fmt.Printf("The winner: %s\n", result.ClientId)
			fmt.Printf("The winning bid: %d\n", result.Amount)
		}
		fmt.Println("---------------------------------")
	}
}

func (c *BidderClient) close() {
	if c.bidStream != nil {
		c.bidStream.CloseSend()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}

func main() {
	clientID := flag.String("id", "", "Client ID (required)")
	serverAddr := flag.String("server", "localhost:5050", "Server address")
	flag.Parse()

	if *clientID == "" {
		log.Fatal("Client ID is required. Use -id=<your_id>")
	}

	client, err := NewBidderClient(*clientID, *serverAddr)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.close()

	ctx := context.Background()

	// starting bid stream
	err = client.startBidStream(ctx)
	if err != nil {
		log.Fatalf("Failed to start the bid stream: %v", err)
	}

	// subscribing to to the auction results
	err = client.subscribeToResults(ctx)
	if err != nil {
		log.Fatalf("Failed to subscribe to the auction results: %v", err)
	}

	fmt.Printf("You are connected as %s to %s\n", *clientID, *serverAddr)
	fmt.Println("Commands: bid <amount>, status, quit")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		parts := strings.Fields(input)

		if len(parts) == 0 {
			continue
		}

		command := strings.ToLower(parts[0])

		switch command {
		case "bid":
			if len(parts) < 2 {
				fmt.Println("Usage: bid <amount>")
				continue
			}
			amount, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				fmt.Println("Invalid amount")
				continue
			}
			err = client.placeBid(amount)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			}

		case "status":
			status, err := client.getAuctionStatus(ctx)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			fmt.Printf("Highest bid: %d by %s\n", status.HighestBid, status.ClientId)

		case "quit", "exit":
			fmt.Println("You have disconnected. See you next time! :D")
			return

		default:
			fmt.Println("This is an unknown command. You can use these commands: bid <amount>, status, quit")
		}
	}
}

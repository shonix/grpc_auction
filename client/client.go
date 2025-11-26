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

	// connection to the current server
	conn          *grpc.ClientConn
	biddingClient proto.BiddingClient
	auctionClient proto.AuctionClient
	queryClient   proto.QueryClient
	bidStream     proto.Bidding_BidClient

	// failover
	serverAddresses  []string // list of all of the server addresses
	currentServerIdx int      // index of all thee currently connectted seerver
	connLock         sync.Mutex
}

func NewBidderClient(clientID string, serverAddresses []string) (*BidderClient, error) {
	if len(serverAddresses) == 0 {
		return nil, fmt.Errorf("at least one server address is required")
	}

	client := &BidderClient{
		clientID:         clientID,
		lamportTime:      0,
		serverAddresses:  serverAddresses,
		currentServerIdx: 0,
	}

	// first we try to connect to the first availlbable server
	err := client.connectToServer(serverAddresses[0])
	if err != nil {
		// then we try other servers
		connected := false
		for i := 1; i < len(serverAddresses); i++ {
			if err := client.connectToServer(serverAddresses[i]); err == nil {
				client.currentServerIdx = i
				connected = true
				break
			}
		}
		if !connected {
			return nil, fmt.Errorf("couldn't connect to any server")
		}
	}

	return client, nil
}

// connectToServer makes a conection to a specific server
func (c *BidderClient) connectToServer(address string) error {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("couldn't connect to %s: %v", address, err)
	}

	c.conn = conn
	c.biddingClient = proto.NewBiddingClient(conn)
	c.auctionClient = proto.NewAuctionClient(conn)
	c.queryClient = proto.NewQueryClient(conn)

	log.Printf("Connected to server at %s", address)
	return nil
}

// switchToNextServer then tries to connect to the next available server that there is
func (c *BidderClient) switchToNextServer(ctx context.Context) error {
	c.connLock.Lock()
	defer c.connLock.Unlock()

	// closing the current connection
	if c.conn != nil {
		c.conn.Close()
	}
	c.bidStream = nil

	// we try each server  in order
	startIdx := c.currentServerIdx
	for i := 0; i < len(c.serverAddresses); i++ {
		nextIdx := (startIdx + i + 1) % len(c.serverAddresses)
		addr := c.serverAddresses[nextIdx]

		log.Printf("Trying to connect to %s...", addr)

		if err := c.connectToServer(addr); err != nil {
			log.Printf("Failed to connect to %s: %v", addr, err)
			continue
		}

		c.currentServerIdx = nextIdx

		// re establishing of the bid stream
		if err := c.startBidStream(ctx); err != nil {
			log.Printf("Failed to start the bid stream on %s: %v", addr, err)
			continue
		}

		// re subscribing to the results
		if err := c.subscribeToResults(ctx); err != nil {
			log.Printf("Failed to subscribe on %s: %v", addr, err)
			continue
		}

		log.Printf("Successfully switched to server %s", addr)
		return nil
	}

	return fmt.Errorf("couldn't connect to any server")
}

// getCurrentServer returns the address of the currently connectedd server
func (c *BidderClient) getCurrentServer() string {
	return c.serverAddresses[c.currentServerIdx]
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
	servers := flag.String("servers", "localhost:5050", "list (comma seperated) of server addresses (for example localhost:5050,localhost:5051,localhost:5052 and so on)")
	flag.Parse()

	if *clientID == "" {
		log.Fatal("Client ID is required. Use -id=<your_id>")
	}

	// parsing the server addresses
	serverAddresses := strings.Split(*servers, ",")

	client, err := NewBidderClient(*clientID, serverAddresses)
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

	fmt.Printf("You are connected as %s to %s\n", *clientID, client.getCurrentServer())
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
				// trying to switch  to another server
				fmt.Println("Trying to switch to another server...")
				if switchErr := client.switchToNextServer(ctx); switchErr != nil {
					fmt.Printf("Failed to switch to another server: %v\n", switchErr)
				} else {
					fmt.Printf("Successfullyy switched to %s. Please try to do your bid again.\n", client.getCurrentServer())
				}
			}

		case "status":
			status, err := client.getAuctionStatus(ctx)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				// trying to switch to another server
				fmt.Println("Trying to switch to another server...")
				if switchErr := client.switchToNextServer(ctx); switchErr != nil {
					fmt.Printf("Failed to switch to another server: %v\n", switchErr)
				} else {
					fmt.Printf("Successfully switched to  %s. Please try to do your bid again.\n", client.getCurrentServer())
				}
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

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	proto "auction/grpc"

	"google.golang.org/grpc"
)

type AuctionNode struct {
	proto.UnimplementedBiddingServer
	proto.UnimplementedAuctionServer
	proto.UnimplementedQueryServer

	mu sync.RWMutex

	// Auction state
	highestBid     int64
	highestBidder  string
	auctionOver    bool
	auctionEndTime time.Time

	// Known bidders
	knownBidders map[string]bool

	// Lamport time
	lamportTime int64

	// Server identity
	serverID string
	port     string

	// Subscribers for auction results
	subscribers     []chan *proto.Result
	subscribersLock sync.Mutex
}

func NewAuctionNode(serverID, port string, auctionDuration time.Duration) *AuctionNode {
	node := &AuctionNode{
		highestBid:     0,
		highestBidder:  "",
		auctionOver:    false,
		auctionEndTime: time.Now().Add(auctionDuration),
		knownBidders:   make(map[string]bool),
		lamportTime:    0,
		serverID:       serverID,
		port:           port,
		subscribers:    make([]chan *proto.Result, 0),
	}

	// Start auction countdown
	go node.waitForAuctionEnd(auctionDuration)

	return node
}

func (s *AuctionNode) incrementTime() int64 {
	s.lamportTime++
	return s.lamportTime
}

func (s *AuctionNode) syncTime(receivedTime int64) int64 {
	if receivedTime > s.lamportTime {
		s.lamportTime = receivedTime
	}
	s.lamportTime++
	return s.lamportTime
}

func (s *AuctionNode) waitForAuctionEnd(duration time.Duration) {
	time.Sleep(duration)

	s.mu.Lock()
	s.auctionOver = true
	result := &proto.Result{
		LamportTimestamp: s.incrementTime(),
		ServerId:         s.serverID,
		Amount:           s.highestBid,
		ClientId:         s.highestBidder,
	}
	s.mu.Unlock()

	log.Printf("Auction ended! Winner: %s with bid: %d", s.highestBidder, s.highestBid)

	// Notify all subscribers
	s.subscribersLock.Lock()
	for _, ch := range s.subscribers {
		select {
		case ch <- result:
		default:
			// Channel full or closed, skip
		}
	}
	s.subscribersLock.Unlock()
}

// Bid implements bidirectional streaming for bids
func (s *AuctionNode) Bid(stream proto.Bidding_BidServer) error {
	for {
		bidRequest, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		s.mu.Lock()

		// Sync Lamport time with incoming request
		s.syncTime(bidRequest.LamportTimestamp)

		var status string

		// Check if auction is over
		if s.auctionOver {
			status = "EXCEPTION"
			log.Printf("Bid rejected from %s: auction is over", bidRequest.ClientId)
		} else {
			// Register bidder on first bid
			if !s.knownBidders[bidRequest.ClientId] {
				s.knownBidders[bidRequest.ClientId] = true
				log.Printf("New bidder joined: %s", bidRequest.ClientId)
			}

			// Check if bid is higher than current highest
			if bidRequest.Amount > s.highestBid {
				s.highestBid = bidRequest.Amount
				s.highestBidder = bidRequest.ClientId
				status = "SUCCESS"
				log.Printf("New highest bid: %d from %s", bidRequest.Amount, bidRequest.ClientId)
			} else {
				status = "FAIL"
				log.Printf("Bid rejected from %s: %d <= current highest %d", bidRequest.ClientId, bidRequest.Amount, s.highestBid)
			}
		}

		ack := &proto.Acknowledgement{
			LamportTimestamp: s.incrementTime(),
			ServerId:         s.serverID,
			Status:           status,
		}

		s.mu.Unlock()

		if err := stream.Send(ack); err != nil {
			return err
		}
	}
}

// Subscribe allows clients to receive auction results
func (s *AuctionNode) Subscribe(req *proto.SubscribeRequest, stream proto.Auction_SubscribeServer) error {
	s.mu.Lock()
	s.syncTime(req.LamportTimestamp)

	// If auction is already over, send result immediately
	if s.auctionOver {
		result := &proto.Result{
			LamportTimestamp: s.incrementTime(),
			ServerId:         s.serverID,
			Amount:           s.highestBid,
			ClientId:         s.highestBidder,
		}
		s.mu.Unlock()
		return stream.Send(result)
	}
	s.mu.Unlock()

	// Create channel for this subscriber
	resultCh := make(chan *proto.Result, 1)

	s.subscribersLock.Lock()
	s.subscribers = append(s.subscribers, resultCh)
	s.subscribersLock.Unlock()

	log.Printf("Client %s subscribed to auction results", req.ClientId)

	// Wait for result
	result := <-resultCh

	return stream.Send(result)
}

// GetAuctionStatus returns current auction state
func (s *AuctionNode) GetAuctionStatus(ctx context.Context, req *proto.QueryRequest) (*proto.AuctionStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.syncTime(req.LamportTimestamp)
	timestamp := s.incrementTime()

	return &proto.AuctionStatus{
		LamportTimestamp: timestamp,
		ServerId:         s.serverID,
		ClientId:         s.highestBidder,
		HighestBid:       s.highestBid,
		LeaderId:         s.serverID, // For now, single server is always leader
	}, nil
}

func main() {
	port := flag.String("port", "5050", "Server port")
	duration := flag.Int("duration", 60, "Auction duration in seconds")
	flag.Parse()

	serverID := fmt.Sprintf("server-%s", *port)

	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", *port, err)
	}

	grpcServer := grpc.NewServer()
	auctionNode := NewAuctionNode(serverID, *port, time.Duration(*duration)*time.Second)

	proto.RegisterBiddingServer(grpcServer, auctionNode)
	proto.RegisterAuctionServer(grpcServer, auctionNode)
	proto.RegisterQueryServer(grpcServer, auctionNode)

	log.Printf("Server %s starting on port %s", serverID, *port)
	log.Printf("Auction will end in %d seconds", *duration)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

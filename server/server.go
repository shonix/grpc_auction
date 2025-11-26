package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	proto "auction/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type AuctionNode struct {
	proto.UnimplementedBiddingServer
	proto.UnimplementedAuctionServer
	proto.UnimplementedQueryServer
	proto.UnimplementedNodeToNodeServer

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

	// THe leader election, and also the replication
	peerAddresses    []string                          // this is addresses of other nodes (such as for example "localhost:5051")
	peerClients      map[string]proto.NodeToNodeClient // This is gRPC clients to other nodes
	peerConns        map[string]*grpc.ClientConn       // this is connections to other nodes
	isLeader         bool                              // this is wwhether this node is the leader
	currentLeader    string                            // this is the ID of current leader
	lastHeartbeat    time.Time                         // this is when we last heard from leader
	heartbeatTimeout time.Duration                     // This is how long to wait before we will consider a new leader (and it seems to work good with 5sec )
}

func NewAuctionNode(serverID, port string, auctionDuration time.Duration, peerAddresses []string) *AuctionNode {
	node := &AuctionNode{
		highestBid:       0,
		highestBidder:    "",
		auctionOver:      false,
		auctionEndTime:   time.Now().Add(auctionDuration),
		knownBidders:     make(map[string]bool),
		lamportTime:      0,
		serverID:         serverID,
		port:             port,
		subscribers:      make([]chan *proto.Result, 0),
		peerAddresses:    peerAddresses,
		peerClients:      make(map[string]proto.NodeToNodeClient),
		peerConns:        make(map[string]*grpc.ClientConn),
		isLeader:         false,
		currentLeader:    "",
		lastHeartbeat:    time.Now(),
		heartbeatTimeout: 5 * time.Second, // we see the leader as "dead" after 5 secs of not being heartbeat, maybe we should adjustit but it seems to work good
	}

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

		var ack *proto.Acknowledgement

		s.mu.Lock()
		isLeader := s.isLeader
		leaderAddr := s.getLeaderAddress()
		s.syncTime(bidRequest.LamportTimestamp)
		s.mu.Unlock()

		// here, if we are not the leader, then we just forward the bid to the leader
		if !isLeader && leaderAddr != "" {
			log.Printf("Forwarding the bid from %s to the leader at %s", bidRequest.ClientId, leaderAddr)
			ack, err = s.forwardBidToLeader(bidRequest, leaderAddr)
			if err != nil {
				log.Printf("Failed to forward the bid to the leader: %v", err)
				ack = &proto.Acknowledgement{
					LamportTimestamp: s.incrementTime(),
					ServerId:         s.serverID,
					Status:           "FAIL",
				}
			}
		} else {
			// and else if we are the leader, then we just process the bid, wohoo ;D
			ack = s.processBid(bidRequest)
		}

		if err := stream.Send(ack); err != nil {
			return err
		}
	}
}

// processBid handles the processing of the bid (which is called by the leader)
func (s *AuctionNode) processBid(bidRequest *proto.BidRequest) *proto.Acknowledgement {
	s.mu.Lock()

	var status string

	// we check if the auction is over
	if s.auctionOver {
		status = "EXCEPTION"
		log.Printf("Bid rejected from %s: the auction is over", bidRequest.ClientId)
	} else {
		// we register the bidder on the first bid
		if !s.knownBidders[bidRequest.ClientId] {
			s.knownBidders[bidRequest.ClientId] = true
			log.Printf("A new bidder has joined: %s", bidRequest.ClientId)
		}

		// we check if if the bid is higher than the current highest bid
		if bidRequest.Amount > s.highestBid {
			s.highestBid = bidRequest.Amount
			s.highestBidder = bidRequest.ClientId
			status = "SUCCESS"
			log.Printf("New highest bid: %d from %s", bidRequest.Amount, bidRequest.ClientId)

			// we replicate the state to followers
			s.mu.Unlock()
			if !s.replicateState() {
				log.Printf("WARNING : Couldn't repplicate to followers")
			}
			s.mu.Lock()
		} else {
			status = "FAIL"
			log.Printf("The b9id is rejected from %s: %d <= the current highest %d", bidRequest.ClientId, bidRequest.Amount, s.highestBid)
		}
	}

	ack := &proto.Acknowledgement{
		LamportTimestamp: s.incrementTime(),
		ServerId:         s.serverID,
		Status:           status,
	}

	s.mu.Unlock()
	return ack
}

// ForwardBid is handling forwarded bids from followers (which are ofc called on the leader)
func (s *AuctionNode) ForwardBid(ctx context.Context, req *proto.BidRequest) (*proto.Acknowledgement, error) {
	s.mu.Lock()
	s.syncTime(req.LamportTimestamp)
	isLeader := s.isLeader
	s.mu.Unlock()

	if !isLeader {
		// since we are not the leader, we reject it
		return &proto.Acknowledgement{
			LamportTimestamp: s.incrementTime(),
			ServerId:         s.serverID,
			Status:           "FAIL",
		}, nil
	}

	log.Printf("Received forwarded bid from %s: %d", req.ClientId, req.Amount)
	return s.processBid(req), nil
}

// getLeaderAddress is returns the address of the current leader
func (s *AuctionNode) getLeaderAddress() string {
	if s.currentLeader == "" {
		return ""
	}
	// extracting port from leader ID (like for eksample "server-5052" --> "localhost:5052")
	parts := strings.Split(s.currentLeader, "-")
	if len(parts) < 2 {
		return ""
	}
	return "localhost:" + parts[len(parts)-1]
}

// forwardBidToLeader sends a bid to the leader node
func (s *AuctionNode) forwardBidToLeader(bid *proto.BidRequest, leaderAddr string) (*proto.Acknowledgement, error) {
	client, ok := s.peerClients[leaderAddr]
	if !ok {
		return nil, fmt.Errorf("no connection to leader at %s", leaderAddr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s.mu.Lock()
	bid.LamportTimestamp = s.incrementTime()
	s.mu.Unlock()

	return client.ForwardBid(ctx, bid)
}

// replicateState sends the current state to all followers and then waits for majoority ack
func (s *AuctionNode) replicateState() bool {
	s.mu.Lock()
	update := &proto.StateUpdate{
		LamportTimestamp: s.incrementTime(),
		LeaderId:         s.serverID,
		HighestBid:       s.highestBid,
		HighestBidder:    s.highestBidder,
		AuctionOver:      s.auctionOver,
	}
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	acksReceived := 1 // counting acks received our selfs
	totalNodes := len(s.peerAddresses) + 1

	for addr, client := range s.peerClients {
		reply, err := client.SendStateUpdate(ctx, update)
		if err != nil {
			log.Printf("Failed to replicate to %s: %v", addr, err)
			continue
		}

		s.mu.Lock()
		s.syncTime(reply.LamportTimestamp)
		s.mu.Unlock()

		if reply.Accepted {
			acksReceived++
			log.Printf("State replicated to %s", reply.NodeId)
		}
	}

	// we check if we got majority acks
	gotMajority := acksReceived > totalNodes/2
	if gotMajority {
		log.Printf("State replicated to majority (%d/%d)", acksReceived, totalNodes)
	}
	return gotMajority
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
		LeaderId:         s.currentLeader,
	}, nil
}

/*
service implementations for NodeToNode
*/

// SendPing iis handling the heartbeaet from the leader
func (s *AuctionNode) SendPing(ctx context.Context, ping *proto.Ping) (*proto.PingReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.syncTime(ping.LamportTimestamp)

	// we update the leader info and then we reset the hearbtetat timer
	s.currentLeader = ping.LeaderId
	s.lastHeartbeat = time.Now()

	log.Printf("Received heartbeat from the leader %s", ping.LeaderId)

	return &proto.PingReply{
		LamportTimestamp: s.incrementTime(),
		NodeId:           s.serverID,
		Alive:            true,
	}, nil
}

// AskForVote is handling the vote requests durigng the leader "election"
func (s *AuctionNode) AskForVote(ctx context.Context, req *proto.VoteRequest) (*proto.VoteReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.syncTime(req.LamportTimestamp)

	// and here we use very simple rule -- and that is that we vote for the Candidate if we don't have a leader
	// OR if the candidate has higher ID than the current leader, then :
	voteGranted := false
	if s.currentLeader == "" || req.CandidateId > s.currentLeader {
		voteGranted = true
		s.currentLeader = req.CandidateId
		log.Printf("Voted for %s as the leader", req.CandidateId)
	} else {
		log.Printf("Rejected vote for %s (the current leader: %s)", req.CandidateId, s.currentLeader)
	}

	return &proto.VoteReply{
		LamportTimestamp: s.incrementTime(),
		NodeId:           s.serverID,
		VoteGranted:      voteGranted,
	}, nil
}

// SendStateUpdate is handling the state replication from the leader
func (s *AuctionNode) SendStateUpdate(ctx context.Context, update *proto.StateUpdate) (*proto.StateAck, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.syncTime(update.LamportTimestamp)

	// we only accept updates from the current leader
	if update.LeaderId != s.currentLeader {
		log.Printf("Rejected state update from %s ( thecurrent leader: %s)", update.LeaderId, s.currentLeader)
		return &proto.StateAck{
			LamportTimestamp: s.incrementTime(),
			NodeId:           s.serverID,
			Accepted:         false,
		}, nil
	}

	// aaaaand applying the state update
	s.highestBid = update.HighestBid
	s.highestBidder = update.HighestBidder
	s.auctionOver = update.AuctionOver

	log.Printf("applied state update: highest bid %d by %s", update.HighestBid, update.HighestBidder)

	return &proto.StateAck{
		LamportTimestamp: s.incrementTime(),
		NodeId:           s.serverID,
		Accepted:         true,
	}, nil
}

/*
HElpers for leader election
*/

// connectToPeers is making grpc connections to all the peer nodes
func (s *AuctionNode) connectToPeers() {
	for _, addr := range s.peerAddresses {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Couldn't connect to peer %s: %v", addr, err)
			continue
		}
		s.peerConns[addr] = conn
		s.peerClients[addr] = proto.NewNodeToNodeClient(conn)
		log.Printf("Connected to the peer at %s", addr)
	}
}

// getPortNumber extracts the port number from server ID (such as for example "server-5050" --> 5050 just like further up :D )
func (s *AuctionNode) getPortNumber(serverID string) int {
	parts := strings.Split(serverID, "-")
	if len(parts) < 2 {
		return 0
	}
	port, _ := strconv.Atoi(parts[len(parts)-1])
	return port
}

// startElection is beginning the leader election  "process"
func (s *AuctionNode) startElection() {
	s.mu.Lock()
	myPort := s.getPortNumber(s.serverID)
	s.mu.Unlock()

	log.Printf("Starting the leader election (my port is: %d)", myPort)

	// ask all the peerrrs for votes
	votesReceived := 1 // and ofc we vote for our selfs
	totalNodes := len(s.peerAddresses) + 1

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s.mu.Lock()
	timestamp := s.incrementTime()
	s.mu.Unlock()

	for addr, client := range s.peerClients {
		reply, err := client.AskForVote(ctx, &proto.VoteRequest{
			LamportTimestamp: timestamp,
			CandidateId:      s.serverID,
		})
		if err != nil {
			log.Printf("Couldn't get vote from %s: %v", addr, err)
			continue
		}

		s.mu.Lock()
		s.syncTime(reply.LamportTimestamp)
		s.mu.Unlock()

		if reply.VoteGranted {
			votesReceived++
			log.Printf("Received vote from %s", reply.NodeId)
		}
	}

	// check if we got majorityy votes
	if votesReceived > totalNodes/2 {
		s.becomeLeader()
	} else {
		log.Printf("Didn't get majority votes (%d/%d)", votesReceived, totalNodes)
	}
}

// becomeLeader makes this node the leader
func (s *AuctionNode) becomeLeader() {
	s.mu.Lock()
	s.isLeader = true
	s.currentLeader = s.serverID
	s.mu.Unlock()

	log.Printf("*** This node is now the leader ! :D wohoo ***")

	// we start sending sending heartbeats
	go s.sendHeartbeats()

	// then we start auction countdown ( and only the leader manages the timer)
	go s.waitForAuctionEnd(time.Until(s.auctionEndTime))
}

// sendHeartbeats sends pings/heaertbeatts to all thje followers
func (s *AuctionNode) sendHeartbeats() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		s.mu.RLock()
		if !s.isLeader {
			s.mu.RUnlock()
			return
		}
		s.mu.RUnlock()

		s.mu.Lock()
		timestamp := s.incrementTime()
		s.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

		for addr, client := range s.peerClients {
			_, err := client.SendPing(ctx, &proto.Ping{
				LamportTimestamp: timestamp,
				LeaderId:         s.serverID,
			})
			if err != nil {
				log.Printf("Heartbeat to %s has failed: %v", addr, err)
			}
		}

		cancel()
		<-ticker.C
	}
}

// monitorLeader is watching for for leader failure (amnd is run by the followers)
func (s *AuctionNode) monitorLeader() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.RLock()
		isLeader := s.isLeader
		lastHeartbeat := s.lastHeartbeat
		timeout := s.heartbeatTimeout
		currentLeader := s.currentLeader
		s.mu.RUnlock()

		// if we're the leader, then we dont need to watch
		if isLeader {
			continue
		}

		// we check if leader has timed out
		if currentLeader != "" && time.Since(lastHeartbeat) > timeout {
			log.Printf("Leader %s looks like it is dead (since there are no heartbeat for %v)", currentLeader, time.Since(lastHeartbeat))
			s.mu.Lock()
			s.currentLeader = ""
			s.mu.Unlock()
			s.startElection()
		}
	}
}

// determineInitialLeader is picking the initial/starting leader, which is the highest port number
func (s *AuctionNode) determineInitialLeader() {
	// we give the nodes time to start up
	time.Sleep(3 * time.Second)

	s.mu.Lock()
	myPort := s.getPortNumber(s.serverID)
	s.mu.Unlock()

	// then we find the highest port between all of the nodes
	highestPort := myPort
	for _, addr := range s.peerAddresses {
		parts := strings.Split(addr, ":")
		if len(parts) == 2 {
			port, _ := strconv.Atoi(parts[1])
			if port > highestPort {
				highestPort = port
			}
		}
	}

	// if we haave the highest port, then we become leadererr
	if myPort == highestPort {
		log.Printf("This node has the highest port (%d), and is becoming leader", myPort)
		s.becomeLeader()
	} else {
		log.Printf("Waiting for the leader (my port: %d, highest port: %d)", myPort, highestPort)
		// aaaand we start monitoring for the leader heartbeats/pings
		go s.monitorLeader()
	}
}

func main() {
	port := flag.String("port", "5050", "Server port")
	duration := flag.Int("duration", 60, "Auction duration in seconds")
	peers := flag.String("peers", "", "This is a list (and is comma seperated) of peer addresses (such as for example localhost:5051,localhost:5052 etc...)")
	flag.Parse()

	serverID := fmt.Sprintf("server-%s", *port)

	// parsing peer addresses
	var peerAddresses []string
	if *peers != "" {
		peerAddresses = strings.Split(*peers, ",")
	}

	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", *port, err)
	}

	grpcServer := grpc.NewServer()
	auctionNode := NewAuctionNode(serverID, *port, time.Duration(*duration)*time.Second, peerAddresses)

	// registering all the grpc services
	proto.RegisterBiddingServer(grpcServer, auctionNode)
	proto.RegisterAuctionServer(grpcServer, auctionNode)
	proto.RegisterQueryServer(grpcServer, auctionNode)
	proto.RegisterNodeToNodeServer(grpcServer, auctionNode)

	log.Printf("Server %s starting on port %s", serverID, *port)
	log.Printf("Auction will end in %d seconds", *duration)
	if len(peerAddresses) > 0 {
		log.Printf("Peers: %v", peerAddresses)
	}

	// connecting to the peers and figuring out the leader (in the background)
	go func() {
		// waiting a sec for the severr to start
		time.Sleep(1 * time.Second)
		auctionNode.connectToPeers()
		auctionNode.determineInitialLeader()
	}()

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

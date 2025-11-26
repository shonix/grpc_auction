# Assignment 5 -- Distributed Auction System

"You must implement a **distributed auction system** using replication:
a distributed component which handles auctions, and provides
operations for bidding and querying the state of an auction. The
component must faithfully implement the semantics of the system
described below, and must at least be resilient to one (1) crash
failure."

## Running the program:

### 1. Start 3 servers (in separate terminals)

```bash
go run server/server.go -port=5050 -peers=localhost:5051,localhost:5052 -duration=60
go run server/server.go -port=5051 -peers=localhost:5050,localhost:5052 -duration=60
go run server/server.go -port=5052 -peers=localhost:5050,localhost:5051 -duration=60
```

### 2. Start a client

```bash
go run client/client.go -id=alice -servers=localhost:5050,localhost:5051,localhost:5052
```

### 3. The client commands are the following:

- `bid <amount>` - Place a bid
- `status` - Check current highest bid
- `quit` - Exit

## How does it work? :D

- The node with the highest port become the leader
- The leader replicates state to followers after each and every bid
- If the leader crashes, then the followers "elect" a new leader
- The client are able to connect to any server (and the bids are forwarded to the leader)

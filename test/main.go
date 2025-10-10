package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	//"sync"
	"time"


	pb "peertopeer/test/test_proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var leaderID string = "1"

type node struct {
	id string
	port string

	nodelist map[string]string
}

var thisnode node

type networkServer struct {
	pb.UnimplementedNetworkServer
	kvStore map[string]string
}

// -- RPC Implementations --

// -- gRPC server startup
func runGRPCServer(s *networkServer) error {
    lis, err := net.Listen("tcp", ":"+thisnode.port)
    if err != nil {
        return fmt.Errorf("failed to listen: %v", err)
    }

    grpcServer := grpc.NewServer()
    pb.RegisterNetworkServer(grpcServer, s)
    log.Printf("gRPC server listening on %v", lis.Addr())

    return grpcServer.Serve(lis)
}



//JoinNetwork: when a new node joins and gets a list of known nodes
func (s *networkServer) JoinNetwork(ctx context.Context, req *pb.Node) (*pb.SeedResponse, error) {
	incomingID := req.Id
	incomingAddr := req.Address
	log.Printf("Received Join req from: %v at location %v", incomingID, incomingAddr)

	thisnode.nodelist[incomingID] = incomingAddr
	response := &pb.SeedResponse{
		Nodelist: thisnode.nodelist,
		Acknowledgement: "Successfully joined the network",
	}

	return response, nil
}
func connectAndJoinNetwork(serverAddress, nodeID, nodeAddress string) {
	if !strings.Contains(serverAddress, ":") {
		serverAddress = "localhost:" + serverAddress
	}
	if !strings.Contains(nodeAddress, ":") {
		nodeAddress = "localhost:" + nodeAddress
	}

	conn, err := grpc.Dial(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	// Create a new client for the Network service.
	client := pb.NewNetworkClient(conn)

	// Create the request payload.
	req := &pb.Node{
		Id:      nodeID,
		Address: nodeAddress,
	}

	// Set a timeout for the RPC call.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Call the JoinNetwork RPC.
	log.Printf("Attempting to join network at %s...", serverAddress)
	res, err := client.JoinNetwork(ctx, req)
	if err != nil {
		log.Fatalf("RPC call failed: %v", err)
	}

	// Log the successful response.
	log.Println("Successfully joined the network!")
	log.Printf("Acknowledgement from server: %s", res.Acknowledgement)
	log.Printf("Received node list:")
	thisnode.nodelist = res.Nodelist
}


func (s *networkServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if thisnode.id != leaderID {
		return &pb.PutResponse{
			Success: false,
			Message: "Writes are only allowed at the leader",
		}, nil
	}

	key := req.Key
	val := req.Value

	log.Printf("[Leader] stored key=%s value=%s", key, val)

	log.Print("Beginning propagation to all nodes")

	for peerID, peerAddr := range thisnode.nodelist {
		if peerID == thisnode.id {
			continue
		}

		replicateToNode(peerID, peerAddr, key, val)
	}

	return &pb.PutResponse{
		Success: true,
		Message: fmt.Sprintf("Key %s stored and replicated successfully", key),
	}, nil

}

func replicateToNode(node_id string, node_addr string, key string, val string) {
	peer_addr := "localhost:" + node_addr
	conn, err := grpc.Dial(peer_addr, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to connect to peer %s: %v", peer_addr, err)
	}

	client := pb.NewNetworkClient(conn)
	_, err = client.Replicate(context.Background(), &pb.ReplicateRequest{Key: key, Value: val})
	if err != nil {
		log.Printf("Replication to %s failed: %v", peer_addr, err)
	}
	conn.Close()
}

func (s *networkServer) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	s.kvStore[req.Key] = req.Value

	log.Printf("[Follower] Replicated key=%s  value=%s", req.Key, req.Value)

	return  &pb.ReplicateResponse{
		Success: true,
	}, nil
}


//http server and functions related to that

func runHTTPServer(port string) {
	http.HandleFunc("/network", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(thisnode.nodelist)
	})

	portnum, _ := strconv.Atoi(thisnode.port)

	httpPort := portnum+1000
	addr := fmt.Sprintf(":%d", httpPort)

	log.Fatal(http.ListenAndServe(addr, nil))
	
}

func main() {
	thisnode.nodelist = make(map[string]string)

	args := os.Args[1:]
	if len(args) < 2 {
		fmt.Print("Usage: go run main.go <id> <grpc_port> <seed_address>")
		return
	}

	thisnode.id = args[0]
	thisnode.port = args[1]
	seed_address := args[2]
	connectAndJoinNetwork(seed_address, thisnode.id, thisnode.port)

	srv := &networkServer{
		kvStore: make(map[string]string),
	}

	go func() {
		if err := runGRPCServer(srv); err != nil {
			log.Fatal(err)
		}
	}()

}
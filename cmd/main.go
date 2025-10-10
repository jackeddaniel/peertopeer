package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	pb "peertopeer/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var my_map = make(map[string]string)

var node_list = map[string]string{
	"1": ":8001",
	"2": ":8002",
	"3": ":8003",
	"4": ":8004",
	"5": ":8005",
}

var leader = "1"
var NodeId string

type Node struct {
	pb.UnimplementedNodeServer
	id string
}

// gRPC handler (unchanged)
func (n *Node) PutReq(ctx context.Context, req *pb.Req) (*pb.Res, error) {
	key := req.Key
	val := req.Val
	my_map[key] = val
	response := fmt.Sprintf("Stored key=%s val=%s", key, val)
	return &pb.Res{Content: response}, nil
}

// run gRPC server
func run_server(n *Node) error {
	port, ok := node_list[NodeId]
	if !ok {
		return fmt.Errorf("invalid NodeID: %s", NodeId)
	}

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterNodeServer(grpcServer, n)
	log.Printf("gRPC Server listening on %v", lis.Addr())

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
	return nil
}

// broadcast writes from leader to other nodes
func broadcast_write(key, val string) {
	for id, port := range node_list {
		if id == NodeId {
			continue
		}
		conn, err := grpc.Dial(port, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("failed to dial node %s: %v", id, err)
			continue
		}
		client := pb.NewNodeClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		req := &pb.Req{Key: key, Val: val}
		res, err := client.PutReq(ctx, req)
		if err != nil {
			log.Printf("Failed to send message to node %s: %v", id, err)
		} else {
			log.Printf("Response from node %s: %s", id, res.Content)
		}
		conn.Close()
	}
}

// leader-only write enforcement
func write_if_leader(key, val string) {
	if NodeId != leader {
		fmt.Println("Error: Writes are only allowed on the leader node.")
		return
	}

	// store locally
	my_map[key] = val
	fmt.Printf("Stored key=%s val=%s on leader.\n", key, val)

	// broadcast to other nodes
	broadcast_write(key, val)
}

// HTTP handler for /status
func statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(my_map)
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run main.go <NodeID>")
	}
	NodeId = os.Args[1]
	node := &Node{id: NodeId}

	// run gRPC server
	go func() {
		if err := run_server(node); err != nil {
			log.Fatal(err)
		}
	}()

	// run HTTP /status endpoint
	go func() {
		http.HandleFunc("/status", statusHandler)
		port := ":900" + NodeId // Node1 → 9001, Node2 → 9002
		log.Printf("HTTP status server running on %s", port)
		log.Fatal(http.ListenAndServe(port, nil))
	}()

	// simple CLI loop
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)

		if strings.HasPrefix(line, "put") {
			// expected format: put key=x val=y
			parts := strings.Fields(line)
			var key, val string
			for _, p := range parts {
				if strings.HasPrefix(p, "key=") {
					key = strings.TrimPrefix(p, "key=")
				} else if strings.HasPrefix(p, "val=") {
					val = strings.TrimPrefix(p, "val=")
				}
			}
			if key != "" && val != "" {
				write_if_leader(key, val)
			} else {
				fmt.Println("Usage: put key=x val=y")
			}
		} else if line == "exit" {
			fmt.Println("Exiting...")
			return
		} else {
			fmt.Println("Commands: put key=x val=y | exit")
		}
	}
}

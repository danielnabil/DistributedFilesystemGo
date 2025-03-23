package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	pb "DistributedFileSystem/dfs"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// dataKeeperServer implements the DistributedFileSystem gRPC service for the data keeper.
type dataKeeperServer struct {
	pb.UnimplementedDistributedFileSystemServer
	id          string // unique identifier for the data keeper
	masterIP    string
	masterPort  string
	ports       []string // available ports for this data keeper
	storagePath string
}

// UploadFile receives the file data from the client.
func (d *dataKeeperServer) UploadFile(ctx context.Context, req *pb.UploadFileRequest) (*pb.UploadFileResponse, error) {
	fmt.Printf("DataKeeper %s: Received file upload request for '%s' (size: %d bytes).\n",
		d.id, req.FileName, len(req.FileData))
	storagePath := d.storagePath + "/" + d.id + "/"
	// Create file storage path if it doesn't exist
	if err := os.MkdirAll(storagePath, 0755); err != nil {
		log.Printf("DataKeeper %s: Error creating storage directory: %v", d.id, err)
		return nil, fmt.Errorf("storage error: %v", err)
	}

	// Save the file to disk
	filePath := fmt.Sprintf("%s/%s", storagePath, req.FileName)
	if err := os.WriteFile(filePath, req.FileData, 0644); err != nil {
		log.Printf("DataKeeper %s: Error writing file: %v", d.id, err)
		return nil, fmt.Errorf("file write error: %v", err)
	}

	// Notify the master tracker of the upload
	go notifyMasterOfUpload(d.masterIP, d.masterPort, req.FileName, d.id, filePath, req.UploadToken)

	return &pb.UploadFileResponse{Message: "File successfully received and stored."}, nil
}

// InitiateReplication handles replication requests from the master tracker
func (d *dataKeeperServer) InitiateReplication(ctx context.Context, req *pb.ReplicationRequest) (*pb.Ack, error) {
	fmt.Printf("\n[NODE %s] Replication request received:\n", d.id)
	fmt.Printf("  ├── File: '%s'\n", req.FileName)
	fmt.Printf("  ├── Source: %s\n", req.SourceDataKeeper)
	fmt.Printf("  ├── Destination Port: %s\n", req.DestinationDataKeeper)

	dest_port := req.DestinationDataKeeper
	file_name := req.FileName
	filePath := fmt.Sprintf("%s/%s/%s", d.storagePath, d.id, file_name) // Use the configured storagePath

	// upload file to the destination port
	fmt.Printf("  ├── Reading file from: %s\n", filePath)
	file, err := os.ReadFile(filePath)
	if err != nil {
		errMsg := fmt.Sprintf("  └── ERROR: Failed to read file: %v", err)
		log.Println(errMsg)
		return &pb.Ack{Success: false, Message: errMsg}, err
	}
	fmt.Printf("  ├── Successfully read %d bytes\n", len(file))

	// connect to the destination port
	fmt.Printf("  ├── Connecting to destination at localhost:%s\n", dest_port)
	conn, err := grpc.Dial("localhost:"+dest_port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		errMsg := fmt.Sprintf("  └── ERROR: Failed to connect to destination: %v", err)
		log.Println(errMsg)
		return &pb.Ack{Success: false, Message: errMsg}, err
	}
	defer conn.Close()

	sourceClient := pb.NewDistributedFileSystemClient(conn)
	fmt.Printf("  ├── Uploading file to destination...\n")

	// Call the UploadFile RPC
	uploadResp, err := sourceClient.UploadFile(context.Background(), &pb.UploadFileRequest{FileName: file_name, FileData: file})
	if err != nil {
		errMsg := fmt.Sprintf("  └── ERROR: Failed to upload file to destination: %v", err)
		log.Println(errMsg)
		return &pb.Ack{Success: false, Message: errMsg}, err
	}

	successMsg := fmt.Sprintf("  └── SUCCESS: Replicated file '%s' to port %s - %s",
		file_name, dest_port, uploadResp.Message)
	log.Println(successMsg)
	return &pb.Ack{Success: true, Message: successMsg}, nil
}

// notifyMasterOfUpload informs the master tracker that the file upload is complete.
func notifyMasterOfUpload(masterIP, masterPort, fileName, dataKeeperID, filePath, uploadToken string) {
	// Connect to the master tracker
	masterAddr := fmt.Sprintf("%s:%s", masterIP, masterPort)
	conn, err := grpc.Dial(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("DataKeeper %s: failed to connect to master tracker: %v", dataKeeperID, err)
		return
	}
	defer conn.Close()

	client := pb.NewDistributedFileSystemClient(conn)
	confirmation := &pb.UploadConfirmation{
		FileName:     fileName,
		DataKeeperId: dataKeeperID,
		FilePath:     filePath,
		UploadToken:  uploadToken,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.ConfirmUpload(ctx, confirmation)
	if err != nil {
		log.Printf("DataKeeper %s: error notifying master tracker: %v", dataKeeperID, err)
		return
	}

	fmt.Printf("DataKeeper %s: Master Tracker responded: %s\n", dataKeeperID, resp.Message)
}

// sendHeartbeats periodically sends heartbeat signals to the master tracker
func (d *dataKeeperServer) sendHeartbeats() {
	masterAddr := fmt.Sprintf("%s:%s", d.masterIP, d.masterPort)

	for {
		conn, err := grpc.Dial(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("DataKeeper %s: Failed to connect to master tracker for heartbeat: %v", d.id, err)
			time.Sleep(1 * time.Second)
			continue
		}

		client := pb.NewDistributedFileSystemClient(conn)

		heartbeatReq := &pb.HeartbeatRequest{
			DataKeeperId:   d.id,
			AvailablePorts: d.ports,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, err = client.SendHeartbeat(ctx, heartbeatReq)
		cancel()

		if err != nil {
			log.Printf("DataKeeper %s: Failed to send heartbeat: %v", d.id, err)
		} else {
			// log.Printf("DataKeeper %s: Heartbeat sent successfully with ports %v", d.id, d.ports)
		}

		conn.Close()
		time.Sleep(1 * time.Second) // Send heartbeat every second
	}
}

// startServer starts a gRPC server on the given port
func startServer(keeper *dataKeeperServer, port string, wg *sync.WaitGroup) {
	defer wg.Done()

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("DataKeeper %s: failed to listen on port %s: %v", keeper.id, port, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterDistributedFileSystemServer(grpcServer, keeper)

	log.Printf("DataKeeper %s: Server is listening on port %s...", keeper.id, port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("DataKeeper %s: failed to serve on port %s: %v", keeper.id, port, err)
	}
}

// DownloadFile handles file download requests from clients
func (d *dataKeeperServer) DownloadFile(ctx context.Context, req *pb.DownloadRequest) (*pb.DownloadResponse, error) {
	fileName := req.FileName
	storagePath := d.storagePath + "/" + d.id + "/"
	filePath := fmt.Sprintf("%s/%s", storagePath, fileName)

	fmt.Printf("DataKeeper %s: Received download request for file '%s'\n", d.id, fileName)

	// Check if the file exists
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		log.Printf("DataKeeper %s: Error reading file for download: %v", d.id, err)
		return nil, fmt.Errorf("file not found or cannot be read: %v", err)
	}

	fmt.Printf("DataKeeper %s: Successfully serving file '%s' (%d bytes)\n",
		d.id, fileName, len(fileData))

	return &pb.DownloadResponse{
		FileData: fileData,
	}, nil
}
func main() {
	// Parse command line arguments
	id := flag.String("name", "DataKeeper-1", "Unique identifier for this data keeper node")
	portsStr := flag.String("ports", "50051", "Space-separated list of ports to listen on")
	masterIP := flag.String("master_ip", "127.0.0.1", "IP address of the master tracker")
	masterPort := flag.String("master_port", "50050", "Port of the master tracker")
	storagePath := flag.String("storage", "./data_keeper", "Path for storage")

	flag.Parse()

	// Split ports string into slice
	ports := strings.Fields(*portsStr)
	if len(ports) == 0 {
		log.Fatal("At least one port must be specified")
	}

	// Create data keeper server
	keeper := &dataKeeperServer{
		id:          *id,
		masterIP:    *masterIP,
		masterPort:  *masterPort,
		ports:       ports,
		storagePath: *storagePath,
	}

	// Start heartbeat goroutine
	go keeper.sendHeartbeats()

	// Create a wait group to keep the main function from returning
	var wg sync.WaitGroup

	// Start a server on each port
	for _, port := range ports {
		wg.Add(1)
		go startServer(keeper, port, &wg)
	}

	// Wait for all servers to finish (which shouldn't happen unless there's an error)
	wg.Wait()
}

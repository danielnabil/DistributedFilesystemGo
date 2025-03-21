package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
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
	storagePath string
}

// UploadFile receives the file data from the client.
func (d *dataKeeperServer) UploadFile(ctx context.Context, req *pb.UploadFileRequest) (*pb.UploadFileResponse, error) {
	fmt.Printf("DataKeeper %s: Received file upload request for '%s' (size: %d bytes).\n",
		d.id, req.FileName, len(req.FileData))

	// Create file storage path if it doesn't exist
	if err := os.MkdirAll(d.storagePath, 0755); err != nil {
		log.Printf("DataKeeper %s: Error creating storage directory: %v", d.id, err)
		return nil, fmt.Errorf("storage error: %v", err)
	}

	// Save the file to disk
	filePath := fmt.Sprintf("%s/%s", d.storagePath, req.FileName)
	if err := os.WriteFile(filePath, req.FileData, 0644); err != nil {
		log.Printf("DataKeeper %s: Error writing file: %v", d.id, err)
		return nil, fmt.Errorf("file write error: %v", err)
	}

	// Notify the master tracker of the upload
	go notifyMasterOfUpload(d.masterIP, d.masterPort, req.FileName, d.id, filePath)

	return &pb.UploadFileResponse{Message: "File successfully received and stored."}, nil
}

// notifyMasterOfUpload informs the master tracker that the file upload is complete.
func notifyMasterOfUpload(masterIP, masterPort, fileName, dataKeeperID, filePath string) {
	// Connect to the master tracker
	masterAddr := fmt.Sprintf("%s:%s", masterIP, masterPort)
	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
		conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("DataKeeper %s: Failed to connect to master tracker for heartbeat: %v", d.id, err)
			time.Sleep(1 * time.Second)
			continue
		}

		client := pb.NewDistributedFileSystemClient(conn)

		// Currently we only use one port, but this could be extended
		availablePorts := []string{"50051"}

		heartbeatReq := &pb.HeartbeatRequest{
			DataKeeperId:   d.id,
			AvailablePorts: availablePorts,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, err = client.SendHeartbeat(ctx, heartbeatReq)
		cancel()

		if err != nil {
			log.Printf("DataKeeper %s: Failed to send heartbeat: %v", d.id, err)
		} else {
			log.Printf("DataKeeper %s: Heartbeat sent successfully", d.id)
		}

		conn.Close()
		time.Sleep(1 * time.Second) // Send heartbeat every second
	}
}

func main() {
	// Data keeper listens on port 50051.
	port := "50051"
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("DataKeeper: failed to listen: %v", err)
	}

	keeperID := "DataKeeper-1"
	masterIP := "127.0.0.1"
	masterPort := "50050"
	storagePath := "./uploading_folder"

	// Create data keeper server
	keeper := &dataKeeperServer{
		id:          keeperID,
		masterIP:    masterIP,
		masterPort:  masterPort,
		storagePath: storagePath,
	}

	// Start heartbeat goroutine
	go keeper.sendHeartbeats()

	// Start gRPC server
	grpcServer := grpc.NewServer()
	pb.RegisterDistributedFileSystemServer(grpcServer, keeper)

	log.Printf("DataKeeper %s: Server is listening on port %s...", keeperID, port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("DataKeeper: failed to serve: %v", err)
	}
}

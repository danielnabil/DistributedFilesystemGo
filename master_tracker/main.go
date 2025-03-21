package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	pb "DistributedFileSystem/dfs" // Import the generated proto package

	"google.golang.org/grpc"
)

// masterTrackerServer implements the DistributedFileSystem gRPC service for the master tracker.
type masterTrackerServer struct {
	pb.UnimplementedDistributedFileSystemServer
	mu          sync.Mutex
	lookupTable map[string]*fileInfo       // Mapping file name -> file information
	dataKeepers map[string]*dataKeeperInfo // Information about all data keepers
}

// fileInfo represents an entry in the lookup table for a file
type fileInfo struct {
	FileName    string
	DataKeepers map[string]string // Map of dataKeeperID -> filePath on that data keeper
}

// dataKeeperInfo tracks information about a data keeper node
type dataKeeperInfo struct {
	ID       string
	IP       string
	Port     string
	IsAlive  bool
	LastSeen int64 // Unix timestamp of last heartbeat
}

// RequestUploadPermission is called by the client to get an available Data Keeper for upload.
func (m *masterTrackerServer) RequestUploadPermission(ctx context.Context, req *pb.UploadPermissionRequest) (*pb.UploadPermissionResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if there are any data keepers registered
	if len(m.dataKeepers) == 0 {
		// If no data keepers registered, use default
		defaultMachine := &pb.MachineInfo{Ip: "127.0.0.1", Port: "50051"}
		token := fmt.Sprintf("%s-%d", req.ClientId, time.Now().Unix())

		fmt.Printf("MasterTracker: No data keepers available. Using default at %s:%s\n",
			defaultMachine.Ip, defaultMachine.Port)

		return &pb.UploadPermissionResponse{
			UploadToken:        token,
			AssignedDataKeeper: defaultMachine,
		}, nil
	}

	// Get a list of all alive data keepers
	var aliveKeepers []*dataKeeperInfo
	for _, keeper := range m.dataKeepers {
		if keeper.IsAlive {
			aliveKeepers = append(aliveKeepers, keeper)
		}
	}

	// If no alive data keepers found, use default
	if len(aliveKeepers) == 0 {
		defaultMachine := &pb.MachineInfo{Ip: "127.0.0.1", Port: "50051"}
		token := fmt.Sprintf("%s-%d", req.ClientId, time.Now().Unix())

		fmt.Printf("MasterTracker: No alive data keepers available. Using default at %s:%s\n",
			defaultMachine.Ip, defaultMachine.Port)

		return &pb.UploadPermissionResponse{
			UploadToken:        token,
			AssignedDataKeeper: defaultMachine,
		}, nil
	}

	// Select a random alive data keeper
	selectedKeeper := aliveKeepers[rand.Intn(len(aliveKeepers))]

	// Create the response
	assigned := &pb.MachineInfo{
		Ip:   selectedKeeper.IP,
		Port: selectedKeeper.Port,
	}

	// Generate upload token (client ID + timestamp)
	token := fmt.Sprintf("%s-%d", req.ClientId, time.Now().Unix())

	fmt.Printf("MasterTracker: Received upload permission request from client %s. Assigned Data Keeper: %s (ID: %s) on port %s\n",
		req.ClientId, selectedKeeper.IP, selectedKeeper.ID, selectedKeeper.Port)

	return &pb.UploadPermissionResponse{
		UploadToken:        token,
		AssignedDataKeeper: assigned,
	}, nil
}

// ConfirmUpload is called by the Data Keeper to confirm that the file was successfully stored.
func (m *masterTrackerServer) ConfirmUpload(ctx context.Context, confirmation *pb.UploadConfirmation) (*pb.Ack, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Update the lookup table
	fileRecord, exists := m.lookupTable[confirmation.FileName]
	if !exists {
		fileRecord = &fileInfo{
			FileName:    confirmation.FileName,
			DataKeepers: make(map[string]string),
		}
		m.lookupTable[confirmation.FileName] = fileRecord
	}

	// Record which data keeper has this file and where
	fileRecord.DataKeepers[confirmation.DataKeeperId] = confirmation.FilePath

	fmt.Printf("MasterTracker: Confirmed upload for file '%s' from DataKeeper '%s'.\n",
		confirmation.FileName, confirmation.DataKeeperId)

	return &pb.Ack{Success: true, Message: "Upload confirmed by Master Tracker."}, nil
}

// SendHeartbeat handles heartbeat messages from data keepers
func (m *masterTrackerServer) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.Ack, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get the data keeper ID
	dataKeeperID := req.DataKeeperId

	// Check if this data keeper is already known
	keeper, exists := m.dataKeepers[dataKeeperID]
	if !exists {
		// If this is a new data keeper, create an entry for it
		// For simplicity, we'll extract IP from context or use a default
		ip := "127.0.0.1"
		port := "50051"

		// If there are available ports, use the first one
		if len(req.AvailablePorts) > 0 {
			port = req.AvailablePorts[0]
		}

		keeper = &dataKeeperInfo{
			ID:       dataKeeperID,
			IP:       ip,
			Port:     port,
			IsAlive:  true,
			LastSeen: time.Now().Unix(),
		}
		m.dataKeepers[dataKeeperID] = keeper

		fmt.Printf("MasterTracker: New Data Keeper registered: %s at %s:%s\n",
			dataKeeperID, ip, port)
	} else {
		// Update existing data keeper
		keeper.IsAlive = true
		keeper.LastSeen = time.Now().Unix()

		fmt.Printf("MasterTracker: Received heartbeat from Data Keeper %s\n", dataKeeperID)
	}

	return &pb.Ack{Success: true, Message: "Heartbeat acknowledged"}, nil
}

// NotifyClientUploadCompletion can be used to notify the client once the upload is registered.
func (m *masterTrackerServer) NotifyClientUploadCompletion(ctx context.Context, notification *pb.ClientNotification) (*pb.Ack, error) {
	fmt.Printf("MasterTracker: Notifying client - %s\n", notification.Message)
	return &pb.Ack{Success: true, Message: "Client notified of upload completion."}, nil
}

// checkDataKeeperStatus periodically checks if data keepers are still alive
func (m *masterTrackerServer) checkDataKeeperStatus() {
	heartbeatTimeout := int64(3) // Consider a data keeper dead after 3 seconds without heartbeat

	for {
		time.Sleep(1 * time.Second)

		m.mu.Lock()
		currentTime := time.Now().Unix()

		for id, keeper := range m.dataKeepers {
			// If we haven't received a heartbeat in the timeout period, mark as not alive
			if currentTime-keeper.LastSeen > heartbeatTimeout {
				if keeper.IsAlive {
					keeper.IsAlive = false
					fmt.Printf("MasterTracker: Data Keeper %s is now considered offline\n", id)
				}
			}
		}

		m.mu.Unlock()
	}
}

// RequestDownloadInfo provides information about available data keepers that have the requested file
func (m *masterTrackerServer) RequestDownloadInfo(ctx context.Context, req *pb.DownloadInfoRequest) (*pb.DownloadInfoResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fileInfo, exists := m.lookupTable[req.FileName]
	if !exists {
		return &pb.DownloadInfoResponse{
			AvailableDataKeepers: []*pb.MachineInfo{},
			FileSize:             0,
		}, nil
	}

	var availableKeepers []*pb.MachineInfo

	// Find all available data keepers that have this file
	for keeperID := range fileInfo.DataKeepers {
		keeper, exists := m.dataKeepers[keeperID]
		if exists && keeper.IsAlive {
			machineInfo := &pb.MachineInfo{
				Ip:   keeper.IP,
				Port: keeper.Port,
			}
			availableKeepers = append(availableKeepers, machineInfo)
		}
	}

	// In a real system, you'd get actual file size
	// For now we'll use a placeholder
	fileSize := 1024.0 // 1KB placeholder

	return &pb.DownloadInfoResponse{
		AvailableDataKeepers: availableKeepers,
		FileSize:             fileSize,
	}, nil
}

func main() {
	// Listen on port 50050 for Master Tracker.
	lis, err := net.Listen("tcp", ":50050")
	if err != nil {
		log.Fatalf("MasterTracker: failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	master := &masterTrackerServer{
		lookupTable: make(map[string]*fileInfo),
		dataKeepers: make(map[string]*dataKeeperInfo),
	}

	// Start the goroutine to check data keeper status
	go master.checkDataKeeperStatus()

	pb.RegisterDistributedFileSystemServer(grpcServer, master)
	log.Println("MasterTracker: Server is listening on port 50050...")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("MasterTracker: failed to serve: %v", err)
	}
}

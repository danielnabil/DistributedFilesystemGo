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
	"google.golang.org/grpc/credentials/insecure"
)

type masterTrackerServer struct {
	pb.UnimplementedDistributedFileSystemServer
	mu          sync.Mutex
	lookupTable map[string]*fileInfo       // Mapping file name -> file information
	dataKeepers map[string]*dataKeeperInfo // Information about all data keepers
	fileOwners  map[string]string          // Maps fileName -> clientId
}

// represents an entry in the lookup table for a file
type fileInfo struct {
	FileName    string
	DataKeepers map[string]string // Map of dataKeeperID -> filePath on that data keeper
}

// tracks information about a data keeper node
type dataKeeperInfo struct {
	ID             string
	IP             string
	Port           string // Primary port
	AvailablePorts []string
	IsAlive        bool
	LastSeen       int64 //timestamp of last heartbeat
}

//////////////////////gRPC methods //////////////////////////

// called by the client to get an available Data Keeper for upload.
func (m *masterTrackerServer) RequestUploadPermission(ctx context.Context, req *pb.UploadPermissionRequest) (*pb.UploadPermissionResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if there are any data keepers registered
	if len(m.dataKeepers) == 0 {

		fmt.Printf("MasterTracker: No data keepers available. \n")

		return &pb.UploadPermissionResponse{}, fmt.Errorf("no data keepers available")
	}

	// Get a list of all alive data keepers
	var aliveKeepers []*dataKeeperInfo
	for _, keeper := range m.dataKeepers {
		if keeper.IsAlive {
			aliveKeepers = append(aliveKeepers, keeper)
		}
	}

	if len(aliveKeepers) == 0 {
		fmt.Printf("MasterTracker: No alive data keepers available.\n")
		return &pb.UploadPermissionResponse{}, fmt.Errorf("no data keepers available")
	}

	// Select a random alive data keeper
	rand.Seed(time.Now().UnixNano())
	selectedKeeper := aliveKeepers[rand.Intn(len(aliveKeepers))]

	// Use port from keeper's available ports (using the first port for simplicity)
	selectedPort := selectedKeeper.Port

	assigned := &pb.MachineInfo{
		Ip:   selectedKeeper.IP,
		Port: selectedPort,
	}

	token := fmt.Sprintf("%s-%d", req.ClientId, time.Now().Unix())

	fmt.Printf("MasterTracker: Received upload permission request from client %s. Assigned Data Keeper: %s (ID: %s) on port %s\n",
		req.ClientId, selectedKeeper.IP, selectedKeeper.ID, selectedPort)

	return &pb.UploadPermissionResponse{
		UploadToken:        token,
		AssignedDataKeeper: assigned,
	}, nil
}

// called by the Data Keeper to confirm that the file was successfully stored.
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
		m.fileOwners[confirmation.FileName] = confirmation.UploadToken
		fmt.Printf("MasterTracker: File '%s' ownership assigned to client '%s'\n",
			confirmation.FileName, confirmation.UploadToken)
	}
	fileRecord.DataKeepers[confirmation.DataKeeperId] = confirmation.FilePath
	// After file ownership is assigned, notify the client of successful upload
	if !exists {
		clientConn, err := grpc.Dial("localhost:"+confirmation.ClientPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("MasterTracker: failed to connect to client for notification: %v", err)
			return &pb.Ack{Success: false, Message: "failed to connect to client for notification"}, err
		}
		defer clientConn.Close()

		// Create client and send notification
		clientService := pb.NewDistributedFileSystemClient(clientConn)
		notification := &pb.ClientNotification{
			Message: fmt.Sprintf("File '%s' has been successfully uploaded and registered", confirmation.FileName),
		}

		_, err = clientService.NotifyClientUploadCompletion(ctx, notification)
		if err != nil {
			log.Printf("MasterTracker: failed to notify client about upload: %v", err)
			return &pb.Ack{Success: false, Message: "failed to notify client about upload"}, err
		}

		fmt.Printf("MasterTracker: Client %s was notified about successful upload of '%s'\n",
			confirmation.UploadToken, confirmation.FileName)
	}

	fmt.Printf("MasterTracker: Confirmed upload for file '%s' from DataKeeper '%s'.\n",
		confirmation.FileName, confirmation.DataKeeperId)

	return &pb.Ack{Success: true, Message: "Upload confirmed by Master Tracker."}, nil
}

// handles heartbeat messages from data keepers
func (m *masterTrackerServer) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.Ack, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	dataKeeperID := req.DataKeeperId

	// Check if this data keeper is already known
	keeper, exists := m.dataKeepers[dataKeeperID]
	if !exists {
		// If this is a new data keeper, create an entry for it
		// Extract IP from peer info
		ip := req.DataKeeperIp
		port := "50051"

		// If there are available ports, use the first one as primary and store all
		availablePorts := []string{port}
		if len(req.AvailablePorts) > 0 {
			port = req.AvailablePorts[0] // Primary port is the first one
			availablePorts = req.AvailablePorts
		}

		keeper = &dataKeeperInfo{
			ID:             dataKeeperID,
			IP:             ip,
			Port:           port,
			AvailablePorts: availablePorts,
			IsAlive:        true,
			LastSeen:       time.Now().Unix(),
		}
		m.dataKeepers[dataKeeperID] = keeper

		fmt.Printf("MasterTracker: New Data Keeper registered: %s at %s with ports %v\n",
			dataKeeperID, ip, availablePorts)
	} else {
		// Update existing data keeper
		keeper.IsAlive = true
		keeper.LastSeen = time.Now().Unix()

		// Update available ports if they changed
		if len(req.AvailablePorts) > 0 {
			keeper.AvailablePorts = req.AvailablePorts
			keeper.Port = req.AvailablePorts[0] // Update primary port as well
		}

		// fmt.Printf("MasterTracker: Received heartbeat from Data Keeper %s with ports %v\n",
		// dataKeeperID, keeper.AvailablePorts)
	}

	return &pb.Ack{Success: true, Message: "Heartbeat acknowledged"}, nil
}

// provides information about available data keepers that have the requested file
func (m *masterTrackerServer) RequestDownloadInfo(ctx context.Context, req *pb.DownloadInfoRequest) (*pb.DownloadInfoResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// First check if the file exists
	fileInfo, exists := m.lookupTable[req.FileName]
	if !exists {
		return &pb.DownloadInfoResponse{
			AvailableDataKeepers: []*pb.MachineInfo{},
			AccessDenied:         false,
			Message:              "File not found.",
		}, nil
	}

	// Check if the client has permission to access this file
	owner, exists := m.fileOwners[req.FileName]
	fmt.Printf("Client %s requested download info for file '%s' whcih has owner '%s'\n", req.ClientId, req.FileName, owner)
	if !exists || owner != req.ClientId {
		return &pb.DownloadInfoResponse{
			AvailableDataKeepers: []*pb.MachineInfo{},
			AccessDenied:         true,
			Message:              "You don't have permission to download this file.",
		}, nil
	}

	var availableKeepers []*pb.MachineInfo

	// Find all available data keepers that have this file
	for keeperID := range fileInfo.DataKeepers {
		keeper, exists := m.dataKeepers[keeperID]
		if exists && keeper.IsAlive {
			// Use a random port from the available ports for load balancing
			port := keeper.Port
			if len(keeper.AvailablePorts) > 0 {
				port = keeper.AvailablePorts[rand.Intn(len(keeper.AvailablePorts))]
			}

			machineInfo := &pb.MachineInfo{
				Ip:   keeper.IP,
				Port: port,
			}
			availableKeepers = append(availableKeepers, machineInfo)
		}
	}

	return &pb.DownloadInfoResponse{
		AvailableDataKeepers: availableKeepers,
		AccessDenied:         false,
		Message:              fmt.Sprintf("Found %d available data keepers for file '%s'", len(availableKeepers), req.FileName),
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50050")
	if err != nil {
		log.Fatalf("MasterTracker: failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	master := &masterTrackerServer{
		lookupTable: make(map[string]*fileInfo),
		dataKeepers: make(map[string]*dataKeeperInfo),
		fileOwners:  make(map[string]string),
	}

	// Start the goroutine to check data keeper status
	go master.checkDataKeeperStatus()

	// Start the goroutine to check and initiate replication
	go master.checkAndInitiateReplication()

	pb.RegisterDistributedFileSystemServer(grpcServer, master)
	log.Println("MasterTracker: Server is listening on port 50050...")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("MasterTracker: failed to serve: %v", err)
	}
}

// //////// Helpers  //////////

// periodically checks and initiates replication for files with less than 3 copies
func (m *masterTrackerServer) checkAndInitiateReplication() {
	replicationInterval := 10 * time.Second // Check for replication every 10 seconds

	for {
		time.Sleep(replicationInterval)

		m.mu.Lock()
		fmt.Println("\n--- REPLICATION CHECK STARTED ---")
		fmt.Printf("System Status: %d active data keepers, %d unique files in storage\n",
			len(m.dataKeepers), len(m.lookupTable))

		// Check each file in the lookup table
		for fileName, fileInfo := range m.lookupTable {
			// Get count of alive instances
			aliveInstances := 0
			var sourceKeeper *dataKeeperInfo
			var sourceFilePath string

			// First find an alive source for this file
			for keeperID, filePath := range fileInfo.DataKeepers {
				keeper, exists := m.dataKeepers[keeperID]
				if exists && keeper.IsAlive {
					sourceKeeper = keeper
					sourceFilePath = filePath
					aliveInstances++
					break
				}
			}

			// If no alive sources, skip this file
			if sourceKeeper == nil {
				fmt.Printf("ALERT: File '%s' has no alive sources - CANNOT REPLICATE\n", fileName)
				continue
			}

			// Count the rest of alive instances
			for keeperID := range fileInfo.DataKeepers {
				keeper, exists := m.dataKeepers[keeperID]
				// Don't count the source keeper again
				if exists && keeper.IsAlive && keeper.ID != sourceKeeper.ID {
					aliveInstances++
				}
			}

			replicaStatus := "NEEDS REPLICATION"
			if aliveInstances >= 3 {
				replicaStatus = "OK"
			}

			fmt.Printf("File: '%s' | Live copies: %d | Status: %s\n",
				fileName, aliveInstances, replicaStatus)

			// Replicate until we have at least 3 instances
			for aliveInstances < 3 {
				// Find a data keeper that doesn't have this file yet
				destinationKeeper := m.selectMachineToCopyTo(fileName)
				if destinationKeeper == nil {
					fmt.Printf("  └── Cannot replicate: No suitable destination nodes available\n")
					break
				}

				// Initiate replication
				fmt.Printf("  └── Attempting replication to '%s' (port %s)...\n",
					destinationKeeper.ID, destinationKeeper.Port)

				success := m.notifyMachineDataTransfer(sourceKeeper, destinationKeeper, fileName, sourceFilePath)
				if success {
					aliveInstances++
					// Success message is handled in notifyMachineDataTransfer
				} else {
					// Error message is handled in notifyMachineDataTransfer
					break
				}
			}
		}

		m.mu.Unlock()
		fmt.Println("--- REPLICATION CHECK COMPLETED ---")
	}
}

// returns a valid data keeper to copy a file to
func (m *masterTrackerServer) selectMachineToCopyTo(fileName string) *dataKeeperInfo {
	fileInfo, exists := m.lookupTable[fileName]
	if !exists {
		return nil
	}

	// Find alive keepers that don't already have this file
	var candidates []*dataKeeperInfo
	for _, keeper := range m.dataKeepers {
		if keeper.IsAlive {
			// Check if this keeper already has the file
			_, hasFile := fileInfo.DataKeepers[keeper.ID]
			if !hasFile {
				candidates = append(candidates, keeper)
			}
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	// Select a random candidate
	return candidates[rand.Intn(len(candidates))]
}

// notifies source and destination machines to transfer a file
func (m *masterTrackerServer) notifyMachineDataTransfer(source, destination *dataKeeperInfo, fileName, sourceFilePath string) bool {
	// Create replication request
	replicationReq := &pb.ReplicationRequest{
		FileName:              fileName,
		SourceDataKeeper:      source.ID,
		DestinationDataKeeper: destination.Port,
	}

	// Connect to the source data keeper to initiate replication
	sourceAddr := fmt.Sprintf("%s:%s", source.IP, source.Port)
	fmt.Printf("    ├── Connecting to source node %s at %s...\n", source.ID, sourceAddr)

	conn, err := grpc.Dial(sourceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("    ├── CONNECTION ERROR: Failed to connect to source node %s at %s: %v\n",
			source.ID, sourceAddr, err)
		return false
	}
	defer conn.Close()

	client := pb.NewDistributedFileSystemClient(conn)

	fmt.Printf("    ├── Requesting replication of '%s' to port %s...\n",
		fileName, destination.Port)

	resp, err := client.InitiateReplication(context.Background(), replicationReq)
	if err != nil {
		fmt.Printf("    └── RPC ERROR: Replication request failed: %v\n", err)
		return false
	}

	if !resp.Success {
		fmt.Printf("    └── REPLICATION ERROR: %s\n", resp.Message)
		return false
	}

	fmt.Printf("    └── SUCCESS: File '%s' replication initiated from %s to %s (port %s)\n",
		fileName, source.ID, destination.ID, destination.Port)

	return true
}

// periodically checks if data keepers are still alive
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

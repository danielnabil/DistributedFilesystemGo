package main

import (
	"context"
	"flag"
	"fmt"
	"io"
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

type dataKeeperServer struct {
	pb.UnimplementedDistributedFileSystemServer
	id          string
	masterIP    string
	masterPort  string
	ports       []string
	storagePath string
	ip          string
}

///////////gRPC methods////////

// receive the file data from the client.
func (d *dataKeeperServer) UploadFile(stream pb.DistributedFileSystem_UploadFileServer) error {
	var fileName string
	var uploadToken string
	var clientPort string
	var clientIp string
	var totalBytes int

	storagePath := d.storagePath + "/" + d.id + "/"

	if err := os.MkdirAll(storagePath, 0755); err != nil {
		log.Printf("DataKeeper %s: Error creating storage directory: %v", d.id, err)
		return fmt.Errorf("storage error: %v", err)
	}

	// Create file path and file handle
	var filePath string
	var file *os.File

	// Process streaming chunks
	for {
		// Receive the next chunk
		req, err := stream.Recv()
		if err == io.EOF {
			// End of file transmission
			break
		}
		if err != nil {
			log.Printf("DataKeeper %s: Error receiving chunk: %v", d.id, err)
			return fmt.Errorf("receive error: %v", err)
		}

		// Extract metadata from first chunk
		if fileName == "" {
			fileName = req.FileName
			uploadToken = req.UploadToken
			clientPort = req.ClientPort
			clientIp = req.ClientIp

			filePath = fmt.Sprintf("%s/%s", storagePath, fileName)

			// Create or truncate the file
			var err error
			file, err = os.Create(filePath)
			if err != nil {
				log.Printf("DataKeeper %s: Error creating file: %v", d.id, err)
				return fmt.Errorf("file creation error: %v", err)
			}
			defer file.Close()

			fmt.Printf("DataKeeper %s: Started receiving file upload for '%s'\n", d.id, fileName)
		}

		// Write chunk to file
		n, err := file.Write(req.FileData)
		if err != nil {
			log.Printf("DataKeeper %s: Error writing chunk to file: %v", d.id, err)
			return fmt.Errorf("file write error: %v", err)
		}

		totalBytes += n
	}

	fmt.Printf("DataKeeper %s: Completed file upload for '%s' (total size: %d bytes)\n",
		d.id, fileName, totalBytes)

	// Notify master of the upload
	go notifyMasterOfUpload(d.masterIP, d.masterPort, fileName, d.id, filePath, uploadToken, clientPort, clientIp)

	// Send response when complete
	return stream.SendAndClose(&pb.UploadFileResponse{
		Message: fmt.Sprintf("File successfully received and stored (%d bytes)", totalBytes),
	})
}

// handles replication requests from the master tracker
func (d *dataKeeperServer) InitiateReplication(ctx context.Context, req *pb.ReplicationRequest) (*pb.Ack, error) {
	fmt.Printf("\n[NODE %s] Replication request received:\n", d.id)
	fmt.Printf("  ├── File: '%s'\n", req.FileName)
	fmt.Printf("  ├── Source: %s\n", req.SourceDataKeeper)
	fmt.Printf("  ├── Destination Port: %s\n", req.DestinationDataKeeper)

	dest_port := req.DestinationDataKeeper
	file_name := req.FileName
	dest_ip := req.DestinationDataKeeperIp
	filePath := fmt.Sprintf("%s/%s/%s", d.storagePath, d.id, file_name)

	// Check if file exists
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		errMsg := fmt.Sprintf("  └── ERROR: Failed to access file: %v", err)
		log.Println(errMsg)
		return &pb.Ack{Success: false, Message: errMsg}, err
	}
	fileSize := fileInfo.Size()
	fmt.Printf("  ├── File size: %d bytes\n", fileSize)

	// Connect to the destination
	fmt.Printf("  ├── Connecting to destination at ip: %s and port:  %s\n", dest_ip, dest_port)
	conn, err := grpc.Dial(dest_ip+":"+dest_port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		errMsg := fmt.Sprintf("  └── ERROR: Failed to connect to destination: %v", err)
		log.Println(errMsg)
		return &pb.Ack{Success: false, Message: errMsg}, err
	}
	defer conn.Close()

	sourceClient := pb.NewDistributedFileSystemClient(conn)
	fmt.Printf("  ├── Uploading file to destination...\n")

	// Open file for streaming
	file, err := os.Open(filePath)
	if err != nil {
		errMsg := fmt.Sprintf("  └── ERROR: Failed to open file for reading: %v", err)
		log.Println(errMsg)
		return &pb.Ack{Success: false, Message: errMsg}, err
	}
	defer file.Close()

	// Create upload stream
	stream, err := sourceClient.UploadFile(context.Background())
	if err != nil {
		errMsg := fmt.Sprintf("  └── ERROR: Failed to create upload stream: %v", err)
		log.Println(errMsg)
		return &pb.Ack{Success: false, Message: errMsg}, err
	}

	// Define chunk size (1MB chunks)
	const chunkSize = 1 * 1024 * 1024
	buffer := make([]byte, chunkSize)
	var totalSent int64

	// Send first chunk with metadata
	bytesRead, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		errMsg := fmt.Sprintf("  └── ERROR: Failed to read first chunk: %v", err)
		log.Println(errMsg)
		return &pb.Ack{Success: false, Message: errMsg}, err
	}

	// Send first chunk with metadata
	err = stream.Send(&pb.UploadFileRequest{
		FileName: file_name,
		FileData: buffer[:bytesRead],
	})
	if err != nil {
		errMsg := fmt.Sprintf("  └── ERROR: Failed to send first chunk: %v", err)
		log.Println(errMsg)
		return &pb.Ack{Success: false, Message: errMsg}, err
	}
	totalSent += int64(bytesRead)

	// Send remaining chunks
	for {
		bytesRead, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			errMsg := fmt.Sprintf("  └── ERROR: Failed to read file chunk: %v", err)
			log.Println(errMsg)
			return &pb.Ack{Success: false, Message: errMsg}, err
		}

		err = stream.Send(&pb.UploadFileRequest{
			FileData: buffer[:bytesRead],
		})
		if err != nil {
			errMsg := fmt.Sprintf("  └── ERROR: Failed to send chunk: %v", err)
			log.Println(errMsg)
			return &pb.Ack{Success: false, Message: errMsg}, err
		}
		totalSent += int64(bytesRead)

		// Print progress
		fmt.Printf("\r  ├── Progress: %.1f%%", float64(totalSent)/float64(fileSize)*100)
	}
	fmt.Println()

	// Close the stream and get response
	uploadResp, err := stream.CloseAndRecv()
	if err != nil {
		errMsg := fmt.Sprintf("  └── ERROR: Failed to complete file upload: %v", err)
		log.Println(errMsg)
		return &pb.Ack{Success: false, Message: errMsg}, err
	}

	successMsg := fmt.Sprintf("  └── SUCCESS: Replicated file '%s' to port %s - %s",
		file_name, dest_port, uploadResp.Message)
	log.Println(successMsg)
	return &pb.Ack{Success: true, Message: successMsg}, nil
}

// handles file download requests from clients
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

	id := flag.String("name", "DataKeeper-1", "Unique identifier for this data keeper node")
	portsStr := flag.String("ports", "50051", "Space-separated list of ports to listen on")
	masterIP := flag.String("master_ip", "127.0.0.1", "IP address of the master tracker")
	masterPort := flag.String("master_port", "50050", "Port of the master tracker")
	storagePath := flag.String("storage", "./data_keeper", "Path for storage")

	flag.Parse()

	ports := strings.Fields(*portsStr)
	if len(ports) == 0 {
		log.Fatal("At least one port must be specified")
	}
	ip, err := getLocalIP()
	if err != nil {
		log.Fatalf("Failed to get local IP address: %v", err)
	}
	if ip == "" {
		log.Fatal("No valid IP address found")
	}

	// Create data keeper server
	keeper := &dataKeeperServer{
		id:          *id,
		masterIP:    *masterIP,
		masterPort:  *masterPort,
		ports:       ports,
		storagePath: *storagePath,
		ip:          ip,
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

	// Wait for all servers to finish starting
	wg.Wait()
}

///////////Helpers methods////////

// informs the master tracker that the file upload is complete.
func notifyMasterOfUpload(masterIP, masterPort, fileName, dataKeeperID, filePath, uploadToken, clientPort, clientIP string) {
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
		ClientPort:   clientPort,
		ClientIp:     clientIP,
	}

	resp, err := client.ConfirmUpload(context.Background(), confirmation)
	if err != nil {
		log.Printf("DataKeeper %s: error notifying master tracker: %v", dataKeeperID, err)
		return
	}

	fmt.Printf("DataKeeper %s: Master Tracker responded: %s\n", dataKeeperID, resp.Message)
}

// periodically sends heartbeat signals to the master tracker every 1 sec
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
			DataKeeperIp:   d.ip,
		}

		_, err = client.SendHeartbeat(context.Background(), heartbeatReq)

		if err != nil {
			log.Printf("DataKeeper %s: Failed to send heartbeat: %v", d.id, err)
		} else {
			// log.Printf("DataKeeper %s: Heartbeat sent successfully with ports %v", d.id, d.ports)
		}

		conn.Close()
		time.Sleep(1 * time.Second) // Send heartbeat every second
	}
}

// starts a gRPC server on the given port
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

func getLocalIP() (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	var fallbackIP string

	for _, iface := range interfaces {
		// Skip down or loopback interfaces
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		// Get interface addresses
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			// Only interested in IPv4
			if ipnet, ok := addr.(*net.IPNet); ok && ipnet.IP.To4() != nil {
				ip := ipnet.IP.String()

				// Prefer wireless interfaces (like wlo1, wlan0)
				if strings.HasPrefix(iface.Name, "wl") {
					return ip, nil
				}

				// Fallback to first non-loopback IP
				if fallbackIP == "" {
					fallbackIP = ip
				}
			}
		}
	}

	if fallbackIP != "" {
		return fallbackIP, nil
	}

	return "", fmt.Errorf("no valid IP address found")
}

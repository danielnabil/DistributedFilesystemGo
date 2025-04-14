package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	pb "DistributedFileSystem/dfs"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultClientPort        = "50057"
	defaultMasterIP   string = "192.168.38.222"
)

type clientServer struct {
	pb.UnimplementedDistributedFileSystemServer
	notificationChan chan string // Channel to communicate notifications to main thread
}

// notify the client once the upload is registered.
func (c *clientServer) NotifyClientUploadCompletion(ctx context.Context, notification *pb.ClientNotification) (*pb.Ack, error) {
	fmt.Printf("\nClient: Upload successful - %s\n", notification.Message)

	// Send notification to main thread if needed
	if c.notificationChan != nil {
		c.notificationChan <- notification.Message
	}

	return &pb.Ack{Success: true, Message: "Client notified of upload completion."}, nil
}

// maintains a record of files uploaded by this client
var uploadedFiles = make(map[string]string) // Maps fileName -> clientId

func main() {
	clientPort := flag.String("port", defaultClientPort, "Port number to listen for notifications")
	masterIP := flag.String("master_ip", defaultMasterIP, "IP for master IP")
	flag.Parse()

	var username string
	for {
		fmt.Print("Enter your username: ")
		fmt.Scanln(&username)
		if username != "" {
			break
		}
		fmt.Println("Username cannot be empty. Please try again.")
	}

	clientId := username
	fmt.Printf("Welcome, %s!\n", clientId)

	// Create notification channel
	notificationChan := make(chan string, 10)

	// Start the client's gRPC server in a goroutine
	go startClientServer(notificationChan, *clientPort)

	// Connect to Master Tracker
	masterConn, err := grpc.Dial(*masterIP+":50050", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Client: failed to connect to Master Tracker: %v", err)
	}
	defer masterConn.Close()
	masterClient := pb.NewDistributedFileSystemClient(masterConn)

	// Load previously uploaded files with user name
	loadUploadedFilesList(clientId)

	for {
		//main menu
		fmt.Println("\n===== Distributed File System Client =====")
		fmt.Println("1. Upload a file")
		fmt.Println("2. Download a file")
		fmt.Println("3. List my files")
		fmt.Println("4. Exit")
		fmt.Print("Enter your choice: ")

		var choice string
		fmt.Scanln(&choice)

		switch choice {
		case "1":
			uploadFile(masterClient, notificationChan, clientId, *clientPort)
		case "2":
			downloadFile(masterClient, clientId)
		case "3":
			listMyFiles()
		case "4", "exit", "quit":
			fmt.Println("Exiting program.")
			return
		default:
			fmt.Println("Invalid choice. Please enter a number from 1-4.")
		}
	}
}

// starts the gRPC server for handling notifications from master
func startClientServer(notificationChan chan string, clientPort string) {
	lis, err := net.Listen("tcp", ":"+clientPort)
	if err != nil {
		log.Fatalf("Client: failed to listen on port %s: %v", clientPort, err)
	}

	grpcServer := grpc.NewServer()
	server := &clientServer{
		notificationChan: notificationChan,
	}

	pb.RegisterDistributedFileSystemServer(grpcServer, server)

	fmt.Printf("Client: Notification server is listening on port %s...\n", clientPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Client: failed to serve: %v", err)
	}
}

func uploadFile(masterClient pb.DistributedFileSystemClient, notificationChan chan string, clientId string, clientPort string) {
	//Request upload permission from the Master Tracker
	uploadPermReq := &pb.UploadPermissionRequest{ClientId: clientId}
	uploadPermResp, err := masterClient.RequestUploadPermission(context.Background(), uploadPermReq)
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "no data keepers available") {
			fmt.Println("\nClient: No data keepers are currently available. Please try again later.")
		} else {
			fmt.Printf("\nClient: Error requesting upload permission: %v\n", err)
		}
		return
	}
	fmt.Printf("Client: Received upload token '%s' and assigned Data Keeper at %s:%s\n",
		uploadPermResp.UploadToken, uploadPermResp.AssignedDataKeeper.Ip, uploadPermResp.AssignedDataKeeper.Port)

	//Get file path from user
	fmt.Print("Enter the path to the file you want to upload: ")
	var path string
	fmt.Scanln(&path)

	//Ask for file name to use in the system
	fileName := filepath.Base(path) // Default to base name from path
	fmt.Printf("Enter file name to use in the system (default: %s): ", fileName)
	var customName string
	fmt.Scanln(&customName)
	if customName != "" {
		fileName = customName
	}

	//Read file data
	fileData, err := os.ReadFile(path)
	if err != nil {
		log.Printf("Client: unable to read file '%s', Error: %v", path, err)
		fmt.Println("File not found. Please try again.")
		return
	}

	//Connect to assigned Data Keeper and upload the file
	dataKeeperAddr := fmt.Sprintf("%s:%s", uploadPermResp.AssignedDataKeeper.Ip, uploadPermResp.AssignedDataKeeper.Port)
	dataKeeperConn, err := grpc.Dial(dataKeeperAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Client: failed to connect to Data Keeper at %s: %v", dataKeeperAddr, err)
		return
	}
	defer dataKeeperConn.Close()

	// Get client's IP address
	clientIP, err := getLocalIP()
	if err != nil {
		log.Printf("Client: failed to get local IP: %v", err)
		clientIP = "127.0.0.1" // Fallback
	}

	dataKeeperClient := pb.NewDistributedFileSystemClient(dataKeeperConn)

	// Streaming implementation for file upload
	stream, err := dataKeeperClient.UploadFile(context.Background())
	if err != nil {
		log.Printf("Client: error creating upload stream: %v", err)
		fmt.Println("Upload failed. Please try again.")
		return
	}

	// Define chunk size (e.g., 1MB chunks)
	const chunkSize = 1 * 1024 * 1024 // 1MB

	// Send file in chunks
	fileSize := len(fileData)
	fmt.Printf("Client: Uploading file '%s' in chunks (%d bytes total)...\n", fileName, fileSize)

	// Send the first chunk with all metadata
	firstChunk := min(chunkSize, fileSize)
	err = stream.Send(&pb.UploadFileRequest{
		FileName:    fileName,
		FileData:    fileData[:firstChunk],
		UploadToken: uploadPermResp.UploadToken,
		ClientPort:  clientPort,
		ClientIp:    clientIP,
	})
	if err != nil {
		log.Printf("Client: error sending first chunk: %v", err)
		return
	}

	// Send remaining chunks if file is larger than chunk size
	for i := firstChunk; i < fileSize; i += chunkSize {
		end := min(i+chunkSize, fileSize)
		err = stream.Send(&pb.UploadFileRequest{
			FileData: fileData[i:end],
		})
		if err != nil {
			log.Printf("Client: error sending chunk: %v", err)
			return
		}
		fmt.Printf("\rProgress: %.1f%%", float64(end)/float64(fileSize)*100)
	}

	// Close the stream and get response
	resp, err := stream.CloseAndRecv()
	if err != nil {
		log.Printf("Client: error completing upload: %v", err)
		fmt.Println("\nUpload failed. Please try again.")
		return
	}

	fmt.Printf("\nClient: Upload response: %s\n", resp.Message)

	fmt.Println("Waiting for upload confirmation...")

	// Set a timeout of 30 seconds for upload confirmation
	select {
	case msg := <-notificationChan:
		fmt.Printf("Upload completed: %s\n", msg)
	case <-time.After(30 * time.Second):
		fmt.Println("Upload confirmation timed out, but file transfer was successful.")
	}

	//Update local record of uploaded files to track ownership
	uploadedFiles[fileName] = uploadPermResp.UploadToken

	//Save the list of uploaded files to disk
	saveUploadedFilesList(clientId)
}

func downloadFile(masterClient pb.DistributedFileSystemClient, clientId string) {
	loadUploadedFilesList(clientId)
	//Show list of files the client has uploaded
	if len(uploadedFiles) == 0 {
		fmt.Println("You haven't uploaded any files yet.")
		return
	}

	fmt.Println("Your uploaded files:")
	var fileNames []string
	for fileName := range uploadedFiles {
		fileNames = append(fileNames, fileName)
		fmt.Printf("- %s\n", fileName)
	}

	fmt.Print("Enter the name of the file to download: ")
	var fileName string
	fmt.Scanln(&fileName)

	//Request download info from master tracker
	downloadInfoReq := &pb.DownloadInfoRequest{
		FileName: fileName,
		ClientId: uploadedFiles[fileName],
	}

	fmt.Printf("Client: Requesting download information for file '%s'...\n", fileName)
	downloadInfoResp, err := masterClient.RequestDownloadInfo(context.Background(), downloadInfoReq)
	if err != nil {
		log.Printf("Client: error requesting download info: %v", err)
		return
	}

	if downloadInfoResp.AccessDenied {
		fmt.Printf("Access denied: %s\n", downloadInfoResp.Message)
		return
	}

	//Check if the file exists and has available keepers
	if len(downloadInfoResp.AvailableDataKeepers) == 0 {
		fmt.Printf("No data keepers available for file '%s'. %s\n", fileName, downloadInfoResp.Message)
		return
	}

	//Select a random data keeper for download
	keeper := downloadInfoResp.AvailableDataKeepers[rand.Intn(len(downloadInfoResp.AvailableDataKeepers))]
	fmt.Printf("Client: Selected data keeper at %s:%s for download\n", keeper.Ip, keeper.Port)

	//Connect to the selected data keeper
	dataKeeperAddr := fmt.Sprintf("%s:%s", keeper.Ip, keeper.Port)
	dataKeeperConn, err := grpc.Dial(dataKeeperAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Client: failed to connect to Data Keeper at %s: %v", err)
		return
	}
	defer dataKeeperConn.Close()

	//Request the file download from datakeeper
	dataKeeperClient := pb.NewDistributedFileSystemClient(dataKeeperConn)
	downloadReq := &pb.DownloadRequest{
		FileName: fileName,
	}

	fmt.Printf("Client: Downloading file '%s'...\n", fileName)
	downloadResp, err := dataKeeperClient.DownloadFile(context.Background(), downloadReq)
	if err != nil {
		log.Printf("Client: error downloading file: %v", err)
		return
	}

	//Create client directory and save the downloaded file
	clientDir := filepath.Join(".", "client", clientId)

	// Ensure the client directory exists or create it
	if err := os.MkdirAll(clientDir, 0755); err != nil {
		log.Printf("Client: error creating client directory: %v", err)
		return
	}

	savePath := filepath.Join(clientDir, fileName)

	fmt.Printf("Saving file to: %s\n", savePath)

	err = os.WriteFile(savePath, downloadResp.FileData, 0644)
	if err != nil {
		log.Printf("Client: error saving downloaded file: %v", err)
		return
	}

	fmt.Printf("Client: File successfully downloaded and saved to '%s' (%d bytes)\n",
		savePath, len(downloadResp.FileData))
}

func listMyFiles() {
	if len(uploadedFiles) == 0 {
		fmt.Println("You haven't uploaded any files yet.")
		return
	}

	fmt.Println("\nYour uploaded files:")
	fmt.Println("--------------------")
	for fileName, token := range uploadedFiles {
		fmt.Printf("File: %s (Token: %s)\n", fileName, token)
	}
}

func saveUploadedFilesList(clientId string) {
	var content strings.Builder
	for fileName, token := range uploadedFiles {
		content.WriteString(fmt.Sprintf("%s\t%s\n", fileName, token))
	}

	filePath := fmt.Sprintf(".%s_uploaded_files", clientId)
	err := os.WriteFile(filePath, []byte(content.String()), 0644)
	if err != nil {
		log.Printf("Client: error saving uploaded files list: %v", err)
	}
}

func loadUploadedFilesList(clientId string) {
	filePath := fmt.Sprintf(".%s_uploaded_files", clientId)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}

		parts := strings.Split(line, "\t")
		if len(parts) == 2 {
			fileName := parts[0]
			token := parts[1]
			uploadedFiles[fileName] = token
		}
	}
}

// Get the client's IP address for remote connections
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

// Helper function to calculate the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"

	pb "DistributedFileSystem/dfs"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	clientId = "Client"
)

// trackUploadedFiles maintains a record of files uploaded by this client
var uploadedFiles = make(map[string]string) // Maps fileName -> uploadToken

func main() {
	// Connect to Master Tracker
	masterConn, err := grpc.Dial("localhost:50050", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Client: failed to connect to Master Tracker: %v", err)
	}
	defer masterConn.Close()
	masterClient := pb.NewDistributedFileSystemClient(masterConn)

	for {
		// Display main menu
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
			uploadFile(masterClient)
		case "2":
			downloadFile(masterClient)
		case "3":
			// listMyFiles()
		case "4", "exit", "quit":
			fmt.Println("Exiting program.")
			return
		default:
			fmt.Println("Invalid choice. Please enter a number from 1-4.")
		}
	}
}

func uploadFile(masterClient pb.DistributedFileSystemClient) {
	// Step 1: Request upload permission from the Master Tracker
	uploadPermReq := &pb.UploadPermissionRequest{ClientId: clientId}
	uploadPermResp, err := masterClient.RequestUploadPermission(context.Background(), uploadPermReq)
	if err != nil {
		log.Printf("Client: error requesting upload permission: %v", err)
		return
	}
	fmt.Printf("Client: Received upload token '%s' and assigned Data Keeper at %s:%s\n",
		uploadPermResp.UploadToken, uploadPermResp.AssignedDataKeeper.Ip, uploadPermResp.AssignedDataKeeper.Port)

	// Step 2: Get file path from user
	fmt.Print("Enter the path to the file you want to upload: ")
	var path string
	fmt.Scanln(&path)

	// Step 3: Ask for file name to use in the system
	fileName := filepath.Base(path) // Default to base name from path
	fmt.Printf("Enter file name to use in the system (default: %s): ", fileName)
	var customName string
	fmt.Scanln(&customName)
	if customName != "" {
		fileName = customName
	}

	// Step 4: Read file data
	fileData, err := os.ReadFile(path)
	if err != nil {
		log.Printf("Client: unable to read file '%s', Error: %v", path, err)
		fmt.Println("File not found. Please try again.")
		return
	}

	// Step 5: Connect to assigned Data Keeper and upload the file
	dataKeeperAddr := fmt.Sprintf("%s:%s", uploadPermResp.AssignedDataKeeper.Ip, uploadPermResp.AssignedDataKeeper.Port)
	dataKeeperConn, err := grpc.Dial(dataKeeperAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Client: failed to connect to Data Keeper at %s: %v", dataKeeperAddr, err)
		return
	}
	defer dataKeeperConn.Close()

	dataKeeperClient := pb.NewDistributedFileSystemClient(dataKeeperConn)
	uploadReq := &pb.UploadFileRequest{
		FileName:    fileName,
		FileData:    fileData,
		UploadToken: uploadPermResp.UploadToken,
	}

	fmt.Printf("Client: Uploading file '%s' (%d bytes)...\n", fileName, len(fileData))
	uploadResp, err := dataKeeperClient.UploadFile(context.Background(), uploadReq)
	if err != nil {
		log.Printf("Client: error uploading file: %v", err)
		fmt.Println("Upload failed. Please try again.")
		return
	}

	// Step 6: Update local record of uploaded files
	uploadedFiles[fileName] = uploadPermResp.UploadToken

	// Step 7: Save the list of uploaded files to disk for persistence
	// saveUploadedFilesList()

	fmt.Printf("Client: Upload successful - %s\n", uploadResp.Message)
}

func downloadFile(masterClient pb.DistributedFileSystemClient) {
	// loadUploadedFilesList()
	// Step 1: Show list of files the client has uploaded
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

	// Step 2: Ask user which file to download
	fmt.Print("Enter the name of the file to download: ")
	var fileName string
	fmt.Scanln(&fileName)

	// Step 3: Request download info from master tracker
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

	// Step 4: Check if access is allowed
	if downloadInfoResp.AccessDenied {
		fmt.Printf("Access denied: %s\n", downloadInfoResp.Message)
		return
	}

	// Step 5: Check if the file exists and has available keepers
	if len(downloadInfoResp.AvailableDataKeepers) == 0 {
		fmt.Printf("No data keepers available for file '%s'. %s\n", fileName, downloadInfoResp.Message)
		return
	}

	// Step 6: Select a random data keeper for load balancing
	keeper := downloadInfoResp.AvailableDataKeepers[rand.Intn(len(downloadInfoResp.AvailableDataKeepers))]
	fmt.Printf("Client: Selected data keeper at %s:%s for download\n", keeper.Ip, keeper.Port)

	// Step 7: Connect to the selected data keeper
	dataKeeperAddr := fmt.Sprintf("%s:%s", keeper.Ip, keeper.Port)
	dataKeeperConn, err := grpc.Dial(dataKeeperAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Client: failed to connect to Data Keeper at %s: %v", dataKeeperAddr, err)
		return
	}
	defer dataKeeperConn.Close()

	// Step 8: Request the file download
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

	// Step 9: Create client directory and save the downloaded file
	clientDir := filepath.Join(".", "client", uploadedFiles[fileName])

	// Ensure the client directory exists
	if err := os.MkdirAll(clientDir, 0755); err != nil {
		log.Printf("Client: error creating client directory: %v", err)
		return
	}

	// Default save path is in the client directory
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

// func listMyFiles() {
// 	loadUploadedFilesList()
// 	if len(uploadedFiles) == 0 {
// 		fmt.Println("You haven't uploaded any files yet.")
// 		return
// 	}

// 	fmt.Println("\nYour uploaded files:")
// 	fmt.Println("--------------------")
// 	for fileName, token := range uploadedFiles {
// 		fmt.Printf("File: %s (Token: %s)\n", fileName, token)
// 	}
// }

// // saveUploadedFilesList saves the list of uploaded files for persistence
// func saveUploadedFilesList() {
// 	var content strings.Builder
// 	for fileName, token := range uploadedFiles {
// 		content.WriteString(fmt.Sprintf("%s\t%s\n", fileName, token))
// 	}

// 	err := os.WriteFile(".uploaded_files", []byte(content.String()), 0644)
// 	if err != nil {
// 		log.Printf("Client: error saving uploaded files list: %v", err)
// 	}
// }

// // loadUploadedFilesList loads the list of previously uploaded files
// func loadUploadedFilesList() {
// 	data, err := os.ReadFile(".uploaded_files")
// 	if err != nil {
// 		// It's okay if the file doesn't exist yet
// 		return
// 	}

// 	lines := strings.Split(string(data), "\n")
// 	for _, line := range lines {
// 		if line == "" {
// 			continue
// 		}

// 		parts := strings.Split(line, "\t")
// 		if len(parts) == 2 {
// 			fileName := parts[0]
// 			token := parts[1]
// 			uploadedFiles[fileName] = token
// 		}
// 	}
// }

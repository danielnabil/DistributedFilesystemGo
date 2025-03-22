package main

import (
	"context"
	"fmt"
	"log"
	"os"

	pb "DistributedFileSystem/dfs"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Connect to Master Tracker
	masterConn, err := grpc.NewClient("localhost:50050", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Client: failed to connect to Master Tracker: %v", err)
	}
	defer masterConn.Close()
	masterClient := pb.NewDistributedFileSystemClient(masterConn)

	for {
		// Ask user if they want to upload a file or exit
		fmt.Println("Enter 'upload' to upload a file or 'exit' to quit:")
		var choice string
		fmt.Scanln(&choice)

		if choice == "exit" {
			fmt.Println("Exiting program.")
			break
		} else if choice == "upload" {
			// Step 1: Request upload permission from the Master Tracker
			uploadPermReq := &pb.UploadPermissionRequest{ClientId: "Client-1"}
			uploadPermResp, err := masterClient.RequestUploadPermission(context.Background(), uploadPermReq)
			if err != nil {
				log.Fatalf("Client: error requesting upload permission: %v", err)
			}
			fmt.Printf("Client: Received upload token '%s' and assigned Data Keeper at %s:%s\n",
				uploadPermResp.UploadToken, uploadPermResp.AssignedDataKeeper.Ip, uploadPermResp.AssignedDataKeeper.Port)

			// Step 2: Upload file to the Data Keeper
			fmt.Println("Client: Please enter the file path:")
			var path string
			fmt.Scanln(&path)

			fmt.Println("Client: Please enter the file name:")
			var name string
			fmt.Scanln(&name)

			fileName := name
			fileData, err := os.ReadFile(path)
			if err != nil {
				log.Printf("Client: unable to read file '%s', Error: %v", fileName, err)
				fmt.Println("File not found. Please try again.")
				continue
			}

			// Connect to assigned Data Keeper
			dataKeeperAddr := fmt.Sprintf("%s:%s", uploadPermResp.AssignedDataKeeper.Ip, uploadPermResp.AssignedDataKeeper.Port)
			dataKeeperConn, err := grpc.NewClient(dataKeeperAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatalf("Client: failed to connect to Data Keeper at %s: %v", dataKeeperAddr, err)
			}

			dataKeeperClient := pb.NewDistributedFileSystemClient(dataKeeperConn)
			uploadReq := &pb.UploadFileRequest{
				FileName: fileName,
				FileData: fileData,
			}
			uploadResp, err := dataKeeperClient.UploadFile(context.Background(), uploadReq)
			if err != nil {
				log.Printf("Client: error uploading file: %v", err)
				fmt.Println("Upload failed. Please try again.")
				dataKeeperConn.Close()
				continue
			}

			fmt.Printf("Client: Upload successful - %s\n", uploadResp.Message)
			dataKeeperConn.Close()
		} else {
			fmt.Println("Invalid choice. Please enter 'upload' or 'exit'.")
		}
	}
}

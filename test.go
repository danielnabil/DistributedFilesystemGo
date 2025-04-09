package main

import (
	"fmt"
	"net"
	"strings"
)

func main() {
	interfaces, err := net.Interfaces()
	if err != nil {
		fmt.Println("Error getting interfaces:", err)
		return
	}

	for _, iface := range interfaces {
		// Skip loopback and down interfaces
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}

		// Try to detect Wi-Fi interface by common naming patterns
		nameLower := strings.ToLower(iface.Name)
		if strings.Contains(nameLower, "w") || strings.Contains(nameLower, "wi-fi") || nameLower == "en0" {
			addrs, err := iface.Addrs()
			if err != nil {
				fmt.Printf("Error getting addresses for interface %s: %v\n", iface.Name, err)
				continue
			}

			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok && ipnet.IP.To4() != nil {
					fmt.Printf("Detected Wi-Fi interface: %s\n", iface.Name)
					fmt.Printf("Local IP: %s\n", ipnet.IP.String())
					return
				}
			}
		}
	}

	fmt.Println("No Wi-Fi interface with IPv4 address found.")
}

package main

import (
	"encoding/json" // SỬ DỤNG THƯ VIỆN JSON TIÊU CHUẨN
	"fmt"
	"io"
	"log"
	"net"
	"os"
	// Bỏ import của msgpack
)

// Cấu trúc dữ liệu không đổi. JSON sẽ tự động xử lý việc
// mã hóa/giải mã `[]byte` thành chuỗi base64.
type TransactionList [][]byte

func handleConnection(conn net.Conn) {
	defer conn.Close()
	log.Println("Executor: New connection accepted.")

	data, err := io.ReadAll(conn)
	if err != nil {
		log.Printf("Executor: Error reading from socket: %v\n", err)
		return
	}

	if len(data) == 0 {
		log.Println("Executor: Received empty data, closing connection.")
		return
	}

	log.Printf("Executor: Received %d bytes from node.\n", len(data))

	var transactions TransactionList

	// THAY ĐỔI Ở ĐÂY: Sử dụng json.Unmarshal
	err = json.Unmarshal(data, &transactions)
	if err != nil {
		log.Printf("Executor: Failed to unmarshal JSON data: %v\n", err)
		return
	}

	log.Printf("Executor: Successfully decoded %d transactions.\n", len(transactions))

	// Logic thực thi giao dịch của bạn ở đây
	for i, tx := range transactions {
		fmt.Printf("  -> Executing Tx %d: %x\n", i+1, tx)
	}
}

func main() {
	// ... (hàm main không thay đổi)
	socketPath := "/tmp/executor.sock"

	if err := os.RemoveAll(socketPath); err != nil {
		log.Fatalf("Failed to remove old socket file: %v", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("Executor: Failed to listen on socket: %v", err)
	}
	defer listener.Close()

	log.Printf("Executor: Listening on Unix socket at %s\n", socketPath)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Executor: Failed to accept connection: %v\n", err)
			continue
		}
		go handleConnection(conn)
	}
}

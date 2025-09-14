// block_listener.go
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
)

// Block định nghĩa cấu trúc dữ liệu của một khối,
// phải khớp chính xác với struct Block của Rust.
type Block struct {
	Author    string   `json:"author"`
	Epoch     uint64   `json:"epoch"`
	Height    uint64   `json:"height"`
	Payload   []string `json:"payload"`
	Signature string   `json:"signature"`
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	fmt.Printf("Accepted new connection from: %s\n", conn.RemoteAddr().String())

	for {
		// 1. Đọc 4 byte đầu tiên để lấy độ dài của tin nhắn JSON
		lenBuf := make([]byte, 4)
		_, err := io.ReadFull(conn, lenBuf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection closed by client.")
				return // Kết thúc hàm khi client đóng kết nối
			}
			fmt.Printf("Error reading length: %v\n", err)
			return
		}
		msgLen := binary.BigEndian.Uint32(lenBuf)

		// 2. Đọc chính xác số byte của tin nhắn JSON
		jsonBuf := make([]byte, msgLen)
		_, err = io.ReadFull(conn, jsonBuf)
		if err != nil {
			fmt.Printf("Error reading message body: %v\n", err)
			return
		}

		// 3. Giải mã JSON thành struct Block
		var block Block
		err = json.Unmarshal(jsonBuf, &block)
		if err != nil {
			fmt.Printf("Error unmarshalling JSON: %v\n", err)
			continue // Bỏ qua tin nhắn này nếu không hợp lệ
		}

		// 4. In khối đã nhận ra console
		fmt.Printf("✅ Received Block | Epoch: %d, Height: %d, Author: %.16s..., Payloads: %d\n",
			block.Epoch, block.Height, block.Author, len(block.Payload))
	}
}

func main() {
	// Lắng nghe kết nối TCP tại cổng 9001
	listener, err := net.Listen("tcp", "127.0.0.1:9001")
	if err != nil {
		panic(fmt.Sprintf("Failed to start server: %v", err))
	}
	defer listener.Close()
	fmt.Println("Go server is listening for committed blocks on port 9001")

	for {
		// Chấp nhận kết nối mới và xử lý trong một goroutine riêng
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Failed to accept connection: %v\n", err)
			continue
		}
		go handleConnection(conn)
	}
}

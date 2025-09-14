// block_listener.go
package main

import (
	"encoding/base64" // Import thư viện base64
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
)

// Block định nghĩa cấu trúc dữ liệu của một khối.
type Block struct {
	Author    string   `json:"author"`
	Epoch     uint64   `json:"epoch"`
	Height    uint64   `json:"height"`
	Payload   [][]byte `json:"payload"` // Sửa thành mảng của các mảng byte
	Signature string   `json:"signature"`
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	fmt.Printf("Accepted new connection from: %s\n", conn.RemoteAddr().String())

	for {
		// 1. Đọc độ dài tin nhắn (4 byte)
		lenBuf := make([]byte, 4)
		_, err := io.ReadFull(conn, lenBuf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection closed by client.")
				return
			}
			fmt.Printf("Error reading length: %v\n", err)
			return
		}
		msgLen := binary.BigEndian.Uint32(lenBuf)

		// 2. Đọc nội dung tin nhắn JSON
		jsonBuf := make([]byte, msgLen)
		_, err = io.ReadFull(conn, jsonBuf)
		if err != nil {
			fmt.Printf("Error reading message body: %v\n", err)
			return
		}

		// 3. Giải mã JSON
		var block Block
		err = json.Unmarshal(jsonBuf, &block)
		if err != nil {
			// Lỗi sẽ không còn xảy ra ở đây nữa
			fmt.Printf("Error unmarshalling JSON: %v\n", err)
			continue
		}

		// 4. In thông tin khối ra console
		fmt.Printf("✅ Received Block | Epoch: %d, Height: %d, Payloads: %d\n",
			block.Epoch, block.Height, len(block.Payload))

		// (Tùy chọn) In ra các digest dưới dạng Base64 để dễ đọc
		for i, digestBytes := range block.Payload {
			digestBase64 := base64.StdEncoding.EncodeToString(digestBytes)
			fmt.Printf("  - Digest %d: %s\n", i+1, digestBase64)
		}
	}
}

func main() {
	listener, err := net.Listen("tcp", "127.0.0.1:9001")
	if err != nil {
		panic(fmt.Sprintf("Failed to start server: %v", err))
	}
	defer listener.Close()
	fmt.Println("Go server is listening for committed blocks on port 9001")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Failed to accept connection: %v\n", err)
			continue
		}
		go handleConnection(conn)
	}
}

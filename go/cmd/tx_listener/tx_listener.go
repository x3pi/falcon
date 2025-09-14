// tx_listener.go
package main

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
)

// CommittedTransactions khớp với struct trong Rust
type CommittedTransactions struct {
	Epoch        uint64   `json:"epoch"`
	Height       uint64   `json:"height"`
	Transactions [][]byte `json:"transactions"`
}

func handleTxConnection(conn net.Conn) {
	defer conn.Close()
	fmt.Printf("Accepted new mempool connection from: %s\n", conn.RemoteAddr().String())

	for {
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			if err == io.EOF {
				fmt.Println("Mempool connection closed.")
			} else {
				fmt.Printf("Error reading length from mempool: %v\n", err)
			}
			return
		}
		msgLen := binary.BigEndian.Uint32(lenBuf)

		jsonBuf := make([]byte, msgLen)
		if _, err := io.ReadFull(conn, jsonBuf); err != nil {
			fmt.Printf("Error reading tx data from mempool: %v\n", err)
			return
		}

		var data CommittedTransactions
		if err := json.Unmarshal(jsonBuf, &data); err != nil {
			fmt.Printf("Error unmarshalling tx data: %v\n", err)
			continue
		}

		// ---- BẮT ĐẦU THAY ĐỔI ----
		// Kiểm tra xem block có rỗng không và in ra thông báo tương ứng
		if len(data.Transactions) == 0 {
			fmt.Printf("⚪ Received Empty Block (Epoch: %d, Height: %d)\n",
				data.Epoch, data.Height)
		} else {
			fmt.Printf("🚚 Received %d transactions from Block (Epoch: %d, Height: %d)\n",
				len(data.Transactions), data.Epoch, data.Height)

			// (Tùy chọn) In ra một vài giao dịch để kiểm tra
			for i, tx := range data.Transactions {
				if i < 2 { // Chỉ in 2 giao dịch đầu tiên để tránh spam console
					fmt.Printf("  - TX %d: %s\n", i+1, base64.StdEncoding.EncodeToString(tx))
				}
			}
		}
		// ---- KẾT THÚC THAY ĐỔI ----
	}
}

func main() {
	listener, err := net.Listen("tcp", "127.0.0.1:9002")
	if err != nil {
		panic(fmt.Sprintf("Failed to start TX server: %v", err))
	}
	defer listener.Close()
	fmt.Println("Go server is listening for committed transactions on port 9002")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Failed to accept tx connection: %v\n", err)
			continue
		}
		go handleTxConnection(conn)
	}
}

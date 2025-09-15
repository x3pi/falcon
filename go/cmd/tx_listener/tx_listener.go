// tx_listener.go

package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/meta-node-blockchain/meta-node/pkg/bls"
)

// Khai báo đường dẫn socket ở một nơi để dễ thay đổi
const socketPath = "/tmp/consensus.sock"

// Khai báo địa chỉ và port của node nhận giao dịch
const receiverNodeAddress = "0.0.0.0:4201"

// CommittedTransactions khớp với struct trong Rust
type CommittedTransactions struct {
	Epoch        uint64   `json:"epoch"`
	Height       uint64   `json:"height"`
	Transactions [][]byte `json:"transactions"`
}

// Khai báo các biến toàn cục cho network module
var txCounter uint64
var totalTxCounter uint64

// Khởi tạo các đối tượng global để tái sử dụng
// var messageSender t_network.MessageSender
// var nodeConnection t_network.Connection

// func init() {

// 	// Khởi tạo một đối tượng MessageSender.
// 	messageSender = network.NewMessageSender("v1.0.0")
// 	// Khởi tạo một đối tượng Connection.
// 	nodeConnection = network.NewConnection(
// 		common.Address{}, // Địa chỉ có thể là 0 nếu không quan trọng.
// 		"TX_SENDER",
// 		network.DefaultConfig(),
// 	)
// 	// Thiết lập địa chỉ đích.
// 	nodeConnection.SetRealConnAddr(receiverNodeAddress)
// }

// handleTxConnection xử lý kết nối từ mempool và gửi giao dịch đi
func handleTxConnection(conn net.Conn) {
	defer conn.Close()
	fmt.Printf("Accepted new mempool connection from: %s\n", conn.RemoteAddr().String())

	// // Đảm bảo kết nối tới node đích đã sẵn sàng
	// if !nodeConnection.IsConnect() {
	// 	fmt.Printf("Kết nối tới node đích %s chưa sẵn sàng. Đang thử kết nối...\n", receiverNodeAddress)
	// 	err := nodeConnection.Connect()
	// 	if err != nil {
	// 		fmt.Printf("Lỗi khi kết nối tới node đích: %v\n", err)
	// 		return
	// 	}
	// }

	// // Khởi chạy goroutine đọc request
	// go nodeConnection.ReadRequest()

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

		numTxs := len(data.Transactions)
		atomic.AddUint64(&txCounter, uint64(numTxs))
		atomic.AddUint64(&totalTxCounter, uint64(numTxs))

		if numTxs == 0 {
			// fmt.Printf("⚪ Received Empty Block (Epoch: %d, Height: %d)\n",
			// 	data.Epoch, data.Height)
		} else {
			fmt.Printf("🚚 Received %d transactions from Block (Epoch: %d, Height: %d , Len tx0: %d)\n",
				numTxs, data.Epoch, data.Height, len(data.Transactions[0]))

			// Gửi từng giao dịch tới node đích
			// for i, tx := range data.Transactions {
			// 	sendErr := messageSender.SendBytes(nodeConnection, p_common.TransactionsFromSubTopic, tx)
			// 	if sendErr != nil {
			// 		fmt.Printf("Lỗi khi gửi giao dịch: %v\n", sendErr)
			// 	}
			// 	if i < 2 {
			// 		fmt.Printf("  - TX %d: %s\n", i+1, base64.StdEncoding.EncodeToString(tx))
			// 	}
			// }
		}
	}
}

// tpsCalculator không thay đổi
func tpsCalculator() {
	const intervalSeconds = 20
	ticker := time.NewTicker(intervalSeconds * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		currentTxCount := atomic.LoadUint64(&txCounter)
		atomic.StoreUint64(&txCounter, 0)
		totalTxs := atomic.LoadUint64(&totalTxCounter)
		tps := float64(currentTxCount) / float64(intervalSeconds)

		fmt.Printf("\n========================================\n")
		fmt.Printf("📈 TPS over the last %d seconds: %.2f tx/s\n", intervalSeconds, tps)
		fmt.Printf("(Transactions in this interval: %d)\n", currentTxCount)
		fmt.Printf("📊 Cumulative total transactions: %d\n", totalTxs)
		fmt.Printf("========================================\n\n")
	}
}

func main() {
	bls.Init()

	// 1. Xóa tệp socket cũ nếu nó tồn tại
	if _, err := os.Stat(socketPath); err == nil {
		if err := os.Remove(socketPath); err != nil {
			panic(fmt.Sprintf("Failed to remove existing socket file: %v", err))
		}
	}

	// 2. Lắng nghe trên Unix Domain Socket
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to start TX server: %v", err))
	}

	defer listener.Close()
	fmt.Printf("Go server is listening for committed transactions on socket: %s\n", socketPath)

	go tpsCalculator()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Failed to accept tx connection: %v\n", err)
			continue
		}
		go handleTxConnection(conn)
	}
}

package main

import (
	"crypto/rand"
	"encoding/binary"
	"log"

	"github.com/meta-node-blockchain/meta-node/pkg/txsender"
)

// createSampleTransaction tạo ra một payload giao dịch mẫu với dữ liệu ngẫu nhiên.
func createSampleTransaction(id uint64, size int) []byte {
	payload := make([]byte, size)
	payload[0] = 16 // Loại giao dịch chuẩn
	binary.BigEndian.PutUint64(payload[1:9], id)

	// Sinh ngẫu nhiên phần còn lại của payload
	if _, err := rand.Read(payload[9:]); err != nil {
		log.Fatalf("Không thể sinh dữ liệu ngẫu nhiên: %v", err)
	}
	return payload
}

func main() {
	nodeAddress := "127.0.0.1:6005"
	transactionSize := 128

	// 1. Khởi tạo client.
	client := txsender.NewClient(nodeAddress)

	// --- Gửi giao dịch 1 ---
	log.Println("Đang gửi giao dịch #1...")
	txData1 := createSampleTransaction(101, transactionSize)
	if err := client.SendTransaction(txData1); err != nil {
		log.Fatalf("Gửi giao dịch #1 thất bại: %v", err)
	}
	log.Println("Giao dịch #1 đã gửi thành công.")

	// --- Gửi giao dịch 2 ---
	// log.Println("Đang gửi giao dịch #2...")
	// txData2 := createSampleTransaction(102, transactionSize)
	// if err := client.SendTransaction(txData2); err != nil {
	// 	log.Fatalf("Gửi giao dịch #2 thất bại: %v", err)
	// }
	// log.Println("Giao dịch #2 đã gửi thành công.")

	// 4. Đóng kết nối khi hoàn tất.
	log.Println("Đóng kết nối.")
	client.Close()
}

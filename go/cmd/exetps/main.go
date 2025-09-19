package main

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

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
	nodeAddress := "127.0.0.1:6006"
	transactionSize := 128
	var transactionCounter uint64 // Sử dụng atomic để đếm an toàn trong multithreading
	var wg sync.WaitGroup

	// 1. Khởi tạo client.
	client := txsender.NewClient(nodeAddress)

	// 2. Goroutine gửi giao dịch liên tục
	wg.Add(1)
	go func() {
		defer wg.Done()
		var idCounter uint64 = 100 // ID bắt đầu từ 100
		for {
			txData := createSampleTransaction(idCounter, transactionSize)
			if err := client.SendTransaction(txData); err != nil {
				log.Printf("Gửi giao dịch #%d thất bại: %v", idCounter, err)
			} else {
				// Tăng bộ đếm an toàn
				atomic.AddUint64(&transactionCounter, 1)
			}
			idCounter++
		}
	}()

	// 3. Goroutine báo cáo mỗi phút
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			// Đọc giá trị bộ đếm và reset nó
			count := atomic.SwapUint64(&transactionCounter, 0)
			tps := float64(count) / 60.0
			fmt.Printf("✅ Báo cáo sau 1 phút:\n")
			fmt.Printf("  - Tổng số giao dịch đã gửi: %d\n", count)
			fmt.Printf("  - TPS trung bình (Transaction per second): %.2f\n", tps)
			fmt.Println("------------------------------------")
		}
	}()

	// Đợi các goroutine chạy, chương trình sẽ không dừng lại trừ khi bị ngắt bởi người dùng (Ctrl+C)
	log.Println("Bắt đầu gửi giao dịch và tính TPS. Nhấn Ctrl+C để thoát.")
	wg.Wait()
}

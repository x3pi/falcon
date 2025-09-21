package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	pb "github.com/meta-node-blockchain/meta-node/pkg/proto"

	"google.golang.org/protobuf/proto"
)

const socketPath = "/tmp/executor.sock"
const statsInterval = 30 * time.Second

// TransactionCounter không thay đổi
type TransactionCounter struct {
	mu       sync.Mutex
	totalTxs int64
}

func (tc *TransactionCounter) addTransactions(count int) {
	tc.mu.Lock()
	tc.totalTxs += int64(count)
	tc.mu.Unlock()
}

func (tc *TransactionCounter) reset() {
	tc.mu.Lock()
	tc.totalTxs = 0
	tc.mu.Unlock()
}

func main() {
	if err := os.RemoveAll(socketPath); err != nil {
		log.Fatalf("Không thể xóa file socket cũ: %v", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("Lỗi khi lắng nghe trên socket: %v", err)
	}
	defer listener.Close()

	log.Printf("Máy chủ đang lắng nghe trên socket: %s", socketPath)

	counter := &TransactionCounter{}
	txChan := make(chan int)

	go processStats(txChan, counter)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Lỗi khi chấp nhận kết nối: %v", err)
			continue
		}
		go handleConnection(conn, txChan)
	}
}

// THAY ĐỔI: handleConnection được viết lại hoàn toàn để đọc Protobuf
func handleConnection(conn net.Conn, txChan chan<- int) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		// Đọc độ dài của message (được mã hóa theo Uvarint)
		msgLen, err := binary.ReadUvarint(reader)
		if err != nil {
			if err != io.EOF {
				log.Printf("Lỗi khi đọc độ dài message: %v", err)
			}
			break // Kết thúc nếu gặp lỗi hoặc client đóng kết nối
		}

		// Đọc đúng số byte của message vào buffer
		buf := make([]byte, msgLen)
		if _, err := io.ReadFull(reader, buf); err != nil {
			log.Printf("Lỗi khi đọc message vào buffer: %v", err)
			break
		}

		// Giải mã buffer bằng Protobuf
		var epochData pb.CommittedEpochData
		if err := proto.Unmarshal(buf, &epochData); err != nil {
			log.Printf("Lỗi khi giải mã Protobuf: %v", err)
			continue
		}

		// Logic đếm giao dịch không thay đổi
		totalTxsInEpoch := 0
		for _, block := range epochData.Blocks {
			totalTxsInEpoch += len(block.Transactions)
			if len(block.Transactions) > 0 {
				fmt.Println("=========================================================")
				fmt.Printf("ĐÃ NHẬN DỮ LIỆU EPOCH: %d\n", block.Epoch)
				fmt.Printf("ĐÃ NHẬN DỮ LIỆU block Height: %d\n", block.Height)
				fmt.Printf("Tổng số giao dịch trong block này: %d\n", len(block.Transactions))
				fmt.Println("=========================================================")
			}
		}
		txChan <- totalTxsInEpoch
	}
}

// processStats không thay đổi
func processStats(txChan <-chan int, counter *TransactionCounter) {
	ticker := time.NewTicker(statsInterval)
	defer ticker.Stop()

	for {
		select {
		case count := <-txChan:
			counter.addTransactions(count)
		case <-ticker.C:
			total := counter.totalTxs
			tps := float64(total) / statsInterval.Seconds()

			fmt.Println("\n--- THỐNG KÊ TPS ---")
			fmt.Printf("Tổng số giao dịch đã nhận (trong %s): %d\n", statsInterval, total)
			fmt.Printf("TPS trung bình (trong %s): %.2f giao dịch/giây\n", statsInterval, tps)
			fmt.Println("--------------------")

			counter.reset()
		}
	}
}

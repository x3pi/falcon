package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

// Đường dẫn đến file Unix Domain Socket, phải khớp với client Rust.
const socketPath = "/tmp/executor.sock"

// Khoảng thời gian để in ra thống kê (30 giây).
const statsInterval = 30 * time.Second

// FullBlock định nghĩa cấu trúc của một block đầy đủ thông tin, khớp với struct Rust.
type FullBlock struct {
	Author       string   `json:"author"`
	Epoch        uint64   `json:"epoch"`
	Height       uint64   `json:"height"`
	Transactions [][]byte `json:"transactions"`
}

// CommittedEpochData định nghĩa cấu trúc dữ liệu cấp cao nhất cho một epoch đã commit.
type CommittedEpochData struct {
	Epoch  uint64      `json:"epoch"`
	Blocks []FullBlock `json:"blocks"`
}

// TransactionCounter quản lý tổng số giao dịch đã nhận và thời gian bắt đầu.
type TransactionCounter struct {
	mu        sync.Mutex
	totalTxs  int64
	startTime time.Time
}

func (tc *TransactionCounter) addTransactions(count int) {
	tc.mu.Lock()
	tc.totalTxs += int64(count)
	tc.mu.Unlock()
}

func (tc *TransactionCounter) getStats() (int64, float64) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	elapsed := time.Since(tc.startTime).Seconds()
	if elapsed == 0 {
		return tc.totalTxs, 0
	}
	tps := float64(tc.totalTxs) / elapsed
	return tc.totalTxs, tps
}

func main() {
	// Xóa file socket cũ nếu nó tồn tại để tránh lỗi "address already in use".
	if err := os.RemoveAll(socketPath); err != nil {
		log.Fatalf("Không thể xóa file socket cũ: %v", err)
	}

	// Lắng nghe trên Unix Domain Socket.
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("Lỗi khi lắng nghe trên socket: %v", err)
	}
	// Đảm bảo listener sẽ được đóng khi hàm main kết thúc.
	defer listener.Close()

	log.Printf("Máy chủ đang lắng nghe trên socket: %s", socketPath)

	// Khởi tạo bộ đếm giao dịch.
	counter := &TransactionCounter{
		startTime: time.Now(),
	}
	// Kênh để gửi số lượng giao dịch từ các goroutine xử lý kết nối.
	txChan := make(chan int)

	// Goroutine để xử lý việc đếm giao dịch và in thống kê.
	go processStats(txChan, counter)

	// Vòng lặp vô hạn để chấp nhận các kết nối mới.
	for {
		// Chấp nhận một kết nối đến.
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Lỗi khi chấp nhận kết nối: %v", err)
			continue // Bỏ qua lỗi và tiếp tục chờ kết nối khác.
		}

		// Xử lý mỗi kết nối trong một goroutine riêng để không làm chặn vòng lặp chính.
		go handleConnection(conn, txChan)
	}
}

// handleConnection xử lý việc đọc và giải mã dữ liệu từ một kết nối.
func handleConnection(conn net.Conn, txChan chan<- int) {
	// Đảm bảo kết nối sẽ được đóng sau khi xử lý xong.
	defer conn.Close()
	log.Printf("Đã nhận kết nối từ: %s", conn.RemoteAddr().String())

	// Tạo một bộ giải mã JSON để đọc trực tiếp từ luồng kết nối.
	decoder := json.NewDecoder(conn)

	var epochData CommittedEpochData

	// Giải mã dữ liệu JSON nhận được vào struct CommittedEpochData.
	if err := decoder.Decode(&epochData); err != nil {
		// Nếu có lỗi (ví dụ: client đóng kết nối, dữ liệu không hợp lệ), ghi lại log.
		if err.Error() != "EOF" {
			log.Printf("Lỗi khi giải mã JSON: %v", err)
		}
		return
	}

	// Đếm tổng số giao dịch trong epoch.
	totalTxsInEpoch := 0
	for _, block := range epochData.Blocks {
		totalTxsInEpoch += len(block.Transactions)
	}

	// Gửi tổng số giao dịch đã nhận được vào channel.
	txChan <- totalTxsInEpoch

	fmt.Println("=========================================================")
	fmt.Printf("ĐÃ NHẬN DỮ LIỆU EPOCH: %d\n", epochData.Epoch)
	fmt.Printf("Tổng số giao dịch trong epoch này: %d\n", totalTxsInEpoch)
	fmt.Println("=========================================================")
}

// processStats xử lý việc đếm và in ra thống kê định kỳ.
func processStats(txChan <-chan int, counter *TransactionCounter) {
	ticker := time.NewTicker(statsInterval)
	defer ticker.Stop()

	// Vòng lặp vô hạn để xử lý dữ liệu và in thống kê.
	for {
		select {
		case count := <-txChan:
			// Nhận số lượng giao dịch từ các kết nối và cập nhật bộ đếm.
			counter.addTransactions(count)
		case <-ticker.C:
			// In thống kê sau mỗi 30 giây.
			total, tps := counter.getStats()
			fmt.Println("\n--- THỐNG KÊ TPS ---")
			fmt.Printf("Tổng số giao dịch đã nhận: %d\n", total)
			fmt.Printf("TPS trung bình (trong %s): %.2f giao dịch/giây\n", statsInterval, tps)
			fmt.Println("--------------------")
		}
	}
}

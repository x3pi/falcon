package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// Client struct chứa thông tin cấu hình cho client.
type Client struct {
	target  string
	size    int
	rate    uint64
	timeout time.Duration
	nodes   []string
}

func main() {
	// Định nghĩa các cờ (flags) cho dòng lệnh.
	addr := flag.String("addr", "", "The network address of the node where to send txs")
	timeout := flag.Duration("timeout", 0, "The nodes timeout value")
	size := flag.Int("size", 0, "The size of each transaction in bytes")
	rate := flag.Uint64("rate", 0, "The rate (txs/s) at which to send the transactions")
	nodesStr := flag.String("nodes", "", "Network addresses that must be reachable before starting the benchmark (comma-separated)")

	flag.Parse()

	if *addr == "" || *size == 0 || *rate == 0 || *timeout == 0 {
		fmt.Println("Usage:")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Tách chuỗi địa chỉ các node thành một slice.
	var nodes []string
	if *nodesStr != "" {
		nodes = strings.Split(*nodesStr, ",")
	}

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	log.Printf("Node address: %s", *addr)
	log.Printf("Transactions size: %d B", *size)
	log.Printf("Transactions rate: %d tx/s", *rate)

	client := Client{
		target:  *addr,
		size:    *size,
		rate:    *rate,
		timeout: *timeout,
		nodes:   nodes,
	}

	// Đợi tất cả các node sẵn sàng.
	client.wait()

	// Bắt đầu gửi giao dịch.
	if err := client.send(); err != nil {
		log.Fatalf("Failed to submit transactions: %v", err)
	}
}

// send kết nối đến node mục tiêu và gửi giao dịch.
func (c *Client) send() error {
	const precision uint64 = 20
	const burstDuration = 1000 / precision

	if c.size < 9 {
		return fmt.Errorf("Transaction size must be at least 9 bytes")
	}

	// Kết nối đến mempool.
	conn, err := net.Dial("tcp", c.target)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", c.target, err)
	}
	defer conn.Close()

	burst := c.rate / precision
	tx := make([]byte, c.size)
	var counter uint64 = 0
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	rCounter := r.Uint64()

	ticker := time.NewTicker(time.Duration(burstDuration) * time.Millisecond)
	defer ticker.Stop()

	log.Println("Start sending transactions")

	for range ticker.C {
		startTime := time.Now()
		for x := uint64(0); x < burst; x++ {
			var payload []byte

			if x == counter%burst {
				log.Printf("Sending sample transaction %d", counter)
				tx[0] = 0 // Giao dịch mẫu bắt đầu bằng 0.
				binary.BigEndian.PutUint64(tx[1:9], counter)
				payload = tx
			} else {
				rCounter++
				tx[0] = 1 // Giao dịch chuẩn bắt đầu bằng 1.
				binary.BigEndian.PutUint64(tx[1:9], rCounter)
				payload = tx
			}

			// Gửi độ dài của message trước (length-prefixed).
			lenBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))
			if _, err := conn.Write(lenBuf); err != nil {
				log.Printf("Failed to send transaction length: %v", err)
				return err
			}

			// Gửi message.
			if _, err := conn.Write(payload); err != nil {
				log.Printf("Failed to send transaction payload: %v", err)
				return err
			}
		}

		if time.Since(startTime).Milliseconds() > int64(burstDuration) {
			log.Println("Transaction rate too high for this client")
		}
		counter++
	}
	return nil
}

// wait chờ cho đến khi tất cả các node được chỉ định trực tuyến và đồng bộ.
func (c *Client) wait() {
	if len(c.nodes) == 0 {
		return
	}
	log.Println("Waiting for all nodes to be online...")
	var wg sync.WaitGroup
	for _, address := range c.nodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			for {
				conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
				if err == nil {
					conn.Close()
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(address)
	}
	wg.Wait()

	log.Println("Waiting for all nodes to be synchronized...")
	time.Sleep(c.timeout)
}

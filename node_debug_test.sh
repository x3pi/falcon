#!/bin/bash

# ==============================================================================
# SCRIPT CẤU HÌNH (không thay đổi)
# ==============================================================================
# Dừng script ngay lập tức nếu có bất kỳ lệnh nào thất bại
set -e

# Cấu hình Benchmark (giống hệt trong fabfile.py)
NODES=4
RATE=100000
TX_SIZE=512
DURATION=30

# Cấu hình Node đầy đủ (giống hệt trong fabfile.py)
FAULT=1
SYNC_TIMEOUT=5000
TIMEOUT_DELAY=10000
SYNC_RETRY_DELAY_CONSENSUS=1000
MAX_PAYLOAD_SIZE_CONSENSUS=5000
MIN_BLOCK_DELAY_CONSENSUS=0
NETWORK_DELAY=200
DDOS=false
RANDOM_DDOS=false
RANDOM_CHANCE=10
EXP=0
FALLBACK=0
QUEUE_CAPACITY=50000000
SYNC_RETRY_DELAY_MEMPOOL=10000
MAX_PAYLOAD_SIZE_MEMPOOL=500000
MIN_BLOCK_DELAY_MEMPOOL=0
PROTOCOL=0

# --- Đường dẫn ---
BASE_PORT=6000
BENCHMARK_DIR="benchmark"
NODE_BINARY="./target/release/node"
CLIENT_BINARY="./target/release/client"

# --- Đường dẫn file cấu hình ---
LOG_DIR="$BENCHMARK_DIR/logs"
FABFILE_PATH="fabfile.py"
COMMITTEE_FILE="$BENCHMARK_DIR/.committee.json"
PARAMETERS_FILE="$BENCHMARK_DIR/.parameters.json"

# ==============================================================================
# THỰC THI SCRIPT
# ==============================================================================

# --- Giai đoạn 1: Chuẩn bị (sử dụng cấu hình hiện có) ---
echo "--- Stage 1: Preparation (Using Existing Configuration) ---"
echo "INFO: Forcefully killing any lingering node or client processes..."
pkill -f "$NODE_BINARY" || true
pkill -f "$CLIENT_BINARY" || true
sleep 1

echo "INFO: Ensuring log directory exists..."
mkdir -p "$LOG_DIR"

if ! command -v jq &> /dev/null; then
    echo "LỖI: Lệnh 'jq' không tồn tại. Vui lòng cài đặt: sudo apt-get install jq"
    exit 1
fi
for bin in "$NODE_BINARY" "$CLIENT_BINARY"; do
    if [ ! -f "$bin" ]; then
        echo "LỖI: Không tìm thấy file thực thi tại '$bin'. Bạn đã biên dịch code (cargo build --release) chưa?"
        exit 1
    fi
    if [ ! -x "$bin" ]; then
        echo "LỖI: File '$bin' không có quyền thực thi. Hãy chạy: chmod +x $bin"
        exit 1
    fi
done

# --- Giai đoạn 2: Bỏ qua tạo cấu hình vì chúng ta đang sử dụng các file cũ ---
echo ""
echo "--- Stage 2: Skipping Configuration Generation ---"
echo "INFO: Using existing configuration files."
if [ ! -f "$COMMITTEE_FILE" ] || [ ! -f "$PARAMETERS_FILE" ]; then
    echo "LỖI: Không tìm thấy các file cấu hình cần thiết. Hãy chạy script gốc ít nhất một lần để tạo chúng."
    exit 1
fi

# --- Giai đoạn 3: Khởi chạy Nodes và Clients ---
echo ""
echo "--- Stage 3: Launching Nodes and Clients ---"
rate_share=$((RATE / NODES))
echo "INFO: Launching $NODES clients..."
for i in $(seq 0 $((NODES-1))); do
    port=$((BASE_PORT + NODES + i)); addr="127.0.0.1:$port"; log_file="$LOG_DIR/client-$i.log"
    full_cmd="$CLIENT_BINARY $addr --size $TX_SIZE --rate $rate_share --timeout $SYNC_TIMEOUT"
    tmux new -d -s "client-$i" "sh -c '$full_cmd 2> $log_file || echo \"[FATAL] Client process exited.\" >> $log_file'"
done

echo "INFO: Launching $NODES nodes..."
for i in $(seq 0 $((NODES-1))); do
    key_file="$BENCHMARK_DIR/.node-$i.json"; threshold_key_file="$BENCHMARK_DIR/.node-tss-$i.json"
    db_path="$BENCHMARK_DIR/db_$i"; log_file="$LOG_DIR/node-$i.log"
    cmd="$NODE_BINARY run --keys $key_file --threshold_keys $threshold_key_file --committee $COMMITTEE_FILE --store $db_path --parameters $PARAMETERS_FILE"
    
    full_cmd_with_log="RUST_LOG=info $cmd"
    tmux new -d -s "node-$i" "sh -c '$full_cmd_with_log 2> $log_file || echo \"[FATAL] Node process exited.\" >> $log_file'"
done

# --- Giai đoạn 4: Thực thi và Hiển thị Kết quả ---
echo ""
echo "--- Stage 4: Execution, Termination, and Results ---"
echo "INFO: Waiting for nodes to synchronize..."
sleep $(echo "2 * $SYNC_TIMEOUT / 1000" | bc -l)
echo "INFO: Benchmark running for $DURATION seconds..."
sleep $DURATION
echo "INFO: Stopping all clients and nodes."
tmux kill-server > /dev/null 2>&1 || true

echo ""
echo "========================================================"
echo "          📊 BENCHMARK RESULTS 📊"
echo "========================================================"
(cd "$BENCHMARK_DIR" && ./venv/bin/fab logs)

echo ""
echo "✅ COMPLETE!"

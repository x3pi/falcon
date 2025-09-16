#!/bin/bash

# ==============================================================================
# SCRIPT CONFIGURATION
# ==============================================================================
# Dừng script nếu có bất kỳ lỗi nào xảy ra
set -e

# Cấu hình Benchmark (giống hệt trong run_benchmark.sh)
NODES=4
RATE=100000
TX_SIZE=600
SYNC_TIMEOUT=2000

# --- Đường dẫn ---
BASE_PORT=6000
BENCHMARK_DIR="benchmark"
CLIENT_BINARY="./target/release/client"

# --- Đường dẫn file cấu hình ---
LOG_DIR="$BENCHMARK_DIR/logs"

# ==============================================================================
# SCRIPT EXECUTION
# ==============================================================================

# Kiểm tra sự tồn tại của file thực thi client
if [ ! -f "$CLIENT_BINARY" ]; then
    echo "LỖI: Không tìm thấy file thực thi client tại '$CLIENT_BINARY'."
    exit 1
fi
if [ ! -x "$CLIENT_BINARY" ]; then
    echo "LỖI: File '$CLIENT_BINARY' không có quyền thực thi. Hãy chạy: chmod +x $CLIENT_BINARY"
    exit 1
fi

# Khởi chạy các client
echo "INFO: Launching $NODES clients..."
rate_share=$((RATE / NODES))
for i in $(seq 0 $((NODES-1))); do
    port=$((BASE_PORT + NODES + i)); addr="127.0.0.1:$port"; log_file="$LOG_DIR/client-$i.log"
    full_cmd="$CLIENT_BINARY $addr --size $TX_SIZE --rate $rate_share --timeout $SYNC_TIMEOUT"
    
    # Cưỡng bức ghi log và thêm cơ chế chẩn đoán lỗi
    tmux new -d -s "client-$i" "sh -c '$full_cmd 2> $log_file || echo \"[FATAL] Client process exited.\" >> $log_file'"
done
echo "INFO: Clients launched successfully in tmux sessions."

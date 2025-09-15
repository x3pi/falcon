#!/bin/bash

# ==============================================================================
# SCRIPT CONFIGURATION
# ==============================================================================
set -e  # Dừng script ngay lập tức nếu có bất kỳ lệnh nào thất bại

# Cấu hình Benchmark
NODES=4
RATE=100000
TX_SIZE=512
DURATION=30

# Cấu hình Node
FAULT=0
SYNC_TIMEOUT=5000
TIMEOUT_DELAY=5000
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

# --- File cấu hình ---
LOG_DIR="$BENCHMARK_DIR/logs"
FABFILE_PATH="fabfile.py"
COMMITTEE_FILE="$BENCHMARK_DIR/.committee.json"
PARAMETERS_FILE="$BENCHMARK_DIR/.parameters.json"

# ==============================================================================
# SCRIPT EXECUTION
# ==============================================================================

# --- Stage 0: Build ---
cargo clean
cargo build --release --features benchmark

# # --- Stage 1: Dọn dẹp (có thể bật lại khi cần) ---
# echo "--- Stage 1: Cleanup and Preparation ---"
# tmux kill-server > /dev/null 2>&1 || true
# pkill -f "$NODE_BINARY" || true
# pkill -f "$CLIENT_BINARY" || true
# sleep 1
# rm -rf "$LOG_DIR" "$BENCHMARK_DIR"/db_* "$BENCHMARK_DIR"/.node* "$COMMITTEE_FILE" "$PARAMETERS_FILE"
# mkdir -p "$LOG_DIR"

# # --- Stage 2: Tạo cấu hình (có thể comment nếu đã có sẵn) ---
# echo "--- Stage 2: Configuration File Generation ---"
# key_files=()
# threshold_key_files=()
# tss_cmd="$NODE_BINARY threshold_keys"
# for i in $(seq 0 $((NODES-1))); do
#     key_file="$BENCHMARK_DIR/.node-$i.json"
#     threshold_key_file="$BENCHMARK_DIR/.node-tss-$i.json"
#     $NODE_BINARY keys --filename "$key_file"
#     tss_cmd+=" --filename $threshold_key_file"
# done
# $tss_cmd
# jq -n \
#   --argjson fault "$FAULT" --argjson sync_timeout "$SYNC_TIMEOUT" --argjson timeout_delay "$TIMEOUT_DELAY" \
#   --argjson sync_retry_delay_consensus "$SYNC_RETRY_DELAY_CONSENSUS" --argjson max_payload_size_consensus "$MAX_PAYLOAD_SIZE_CONSENSUS" \
#   --argjson min_block_delay_consensus "$MIN_BLOCK_DELAY_CONSENSUS" --argjson network_delay "$NETWORK_DELAY" \
#   --argjson ddos "$DDOS" --argjson random_ddos "$RANDOM_DDOS" --argjson random_chance "$RANDOM_CHANCE" \
#   --argjson exp "$EXP" --argjson fallback "$FALLBACK" \
#   --argjson queue_capacity "$QUEUE_CAPACITY" --argjson sync_retry_delay_mempool "$SYNC_RETRY_DELAY_MEMPOOL" \
#   --argjson max_payload_size_mempool "$MAX_PAYLOAD_SIZE_MEMPOOL" --argjson min_block_delay_mempool "$MIN_BLOCK_DELAY_MEMPOOL" \
#   --argjson protocol "$PROTOCOL" \
#   '{
#     "consensus": { "fault": $fault, "sync_timeout": $sync_timeout, "timeout_delay": $timeout_delay, "sync_retry_delay": $sync_retry_delay_consensus, "max_payload_size": $max_payload_size_consensus, "min_block_delay": $min_block_delay_consensus, "network_delay": $network_delay, "ddos": $ddos, "random_ddos": $random_ddos, "random_chance": $random_chance, "exp": $exp, "fallback": $fallback },
#     "mempool": { "queue_capacity": $queue_capacity, "sync_retry_delay": $sync_retry_delay_mempool, "max_payload_size": $max_payload_size_mempool, "min_block_delay": $min_block_delay_mempool }, "protocol": $protocol
#   }' > "$PARAMETERS_FILE"

# --- Stage 3: Khởi chạy Nodes và Clients ---
echo ""
echo "--- Stage 3: Launching Nodes and Clients ---"
mkdir -p "$LOG_DIR"

rate_share=$((RATE / NODES))
echo "INFO: Launching $NODES clients..."
for i in $(seq 0 $((NODES-1))); do
    port=$((BASE_PORT + NODES + i))
    addr="127.0.0.1:$port"
    log_file="$LOG_DIR/client-$i.log"
    full_cmd="$CLIENT_BINARY $addr --size $TX_SIZE --rate $rate_share --timeout $SYNC_TIMEOUT"
    tmux new -d -s "client-$i" "sh -c '$full_cmd 2> $log_file || echo \"[FATAL] Client exited.\" >> $log_file'"
done

echo "INFO: Launching $NODES nodes..."
for i in $(seq 0 $((NODES-1))); do
    key_file="$BENCHMARK_DIR/.node-$i.json"
    threshold_key_file="$BENCHMARK_DIR/.node-tss-$i.json"
    db_path="$BENCHMARK_DIR/db_$i"
    log_file="$LOG_DIR/node-$i.log"
    cmd="$NODE_BINARY run --keys $key_file --threshold_keys $threshold_key_file --committee $COMMITTEE_FILE --store $db_path --parameters $PARAMETERS_FILE"
    full_cmd_with_log="RUST_LOG=info $cmd"
    tmux new -d -s "node-$i" "sh -c '$full_cmd_with_log 2> $log_file || echo \"[FATAL] Node exited.\" >> $log_file'"
done

# --- Stage 4: Thực thi và Hiển thị Kết quả ---
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

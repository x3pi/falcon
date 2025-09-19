#!/bin/bash

# ==============================================================================
# SCRIPT CONFIGURATION
# ==============================================================================
# Dừng script ngay lập tức nếu có bất kỳ lệnh nào thất bại
set -e

# Cấu hình Benchmark (giống hệt trong fabfile.py)
NODES=5
RATE=100000
TX_SIZE=600
DURATION=30

# Cấu hình Node đầy đủ (giống hệt trong fabfile.py)
FAULT=0
SYNC_TIMEOUT=2000
TIMEOUT_DELAY=2000
SYNC_RETRY_DELAY_CONSENSUS=10000
MAX_PAYLOAD_SIZE_CONSENSUS=6000
MIN_BLOCK_DELAY_CONSENSUS=0
NETWORK_DELAY=0
DDOS=false
RANDOM_DDOS=false
RANDOM_CHANCE=10
EXP=0
FALLBACK=0
QUEUE_CAPACITY=100000000
SYNC_RETRY_DELAY_MEMPOOL=100000
MAX_PAYLOAD_SIZE_MEMPOOL=18000
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
# SCRIPT EXECUTION
# ==============================================================================
# --- Stage 0: Build ---
cargo clean
cargo build --release --features benchmark

# --- Giai đoạn 1: Dọn dẹp và Kiểm tra ---
echo "--- Stage 1: Cleanup and Preparation ---"
echo "INFO: Stopping tmux server..."
tmux kill-server > /dev/null 2>&1 || true
echo "INFO: Forcefully killing any lingering node or client processes..."
pkill -f "$NODE_BINARY" || true
pkill -f "$CLIENT_BINARY" || true
sleep 1


echo "INFO: Cleaning up old files..."
rm -rf aba_deadlocks.log
rm -rf "$LOG_DIR" "$BENCHMARK_DIR"/db_* "$BENCHMARK_DIR"/.node* "$COMMITTEE_FILE" "$PARAMETERS_FILE"
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


# --- Giai đoạn 2: Tạo Cấu hình ---
echo ""
echo "--- Stage 2: Configuration File Generation ---"
echo "INFO: Generating key files..."
key_files=()
for i in $(seq 0 $((NODES-1))); do
    key_file="$BENCHMARK_DIR/.node-$i.json"
    $NODE_BINARY keys --filename "$key_file"
    key_files+=("$key_file")
done

echo "INFO: Creating parameters file ($PARAMETERS_FILE)..."
jq -n \
  --argjson fault "$FAULT" --argjson sync_timeout "$SYNC_TIMEOUT" --argjson timeout_delay "$TIMEOUT_DELAY" \
  --argjson sync_retry_delay_consensus "$SYNC_RETRY_DELAY_CONSENSUS" --argjson max_payload_size_consensus "$MAX_PAYLOAD_SIZE_CONSENSUS" \
  --argjson min_block_delay_consensus "$MIN_BLOCK_DELAY_CONSENSUS" --argjson network_delay "$NETWORK_DELAY" \
  --argjson ddos "$DDOS" --argjson random_ddos "$RANDOM_DDOS" --argjson random_chance "$RANDOM_CHANCE" \
  --argjson exp "$EXP" --argjson fallback "$FALLBACK" \
  --argjson queue_capacity "$QUEUE_CAPACITY" --argjson sync_retry_delay_mempool "$SYNC_RETRY_DELAY_MEMPOOL" \
  --argjson max_payload_size_mempool "$MAX_PAYLOAD_SIZE_MEMPOOL" --argjson min_block_delay_mempool "$MIN_BLOCK_DELAY_MEMPOOL" \
  --argjson protocol "$PROTOCOL" \
  '{
    "consensus": { "fault": $fault, "sync_timeout": $sync_timeout, "timeout_delay": $timeout_delay, "sync_retry_delay": $sync_retry_delay_consensus, "max_payload_size": $max_payload_size_consensus, "min_block_delay": $min_block_delay_consensus, "network_delay": $network_delay, "ddos": $ddos, "random_ddos": $random_ddos, "random_chance": $random_chance, "exp": $exp, "fallback": $fallback },
    "mempool": { "queue_capacity": $queue_capacity, "sync_retry_delay": $sync_retry_delay_mempool, "max_payload_size": $max_payload_size_mempool, "min_block_delay": $min_block_delay_mempool }, "protocol": $protocol
  }' > "$PARAMETERS_FILE"

echo "INFO: Creating committee file ($COMMITTEE_FILE)..."
json_template='{ "consensus": { "authorities": {}, "epoch": 1 }, "mempool": { "authorities": {}, "epoch": 1 } }'
committee_json="$json_template"
for i in $(seq 0 $((NODES-1))); do
    key_file="${key_files[$i]}";
    name=$(jq -r '.name' "$key_file");
    consensus_addr="127.0.0.1:$((BASE_PORT + i))"; front_addr="127.0.0.1:$((BASE_PORT + NODES + i))"; mempool_addr="127.0.0.1:$((BASE_PORT + 2*NODES + i))"
    committee_json=$(echo "$committee_json" | jq --arg name "$name" --arg addr "$consensus_addr" --argjson id "$i" '.consensus.authorities[$name] = { "address": $addr, "id": $id, "name": $name, "stake": 1 }')
    committee_json=$(echo "$committee_json" | jq --arg name "$name" --arg front "$front_addr" --arg mempool "$mempool_addr" '.mempool.authorities[$name] = { "name": $name, "front_address": $front, "mempool_address": $mempool }')
done
echo "$committee_json" | jq . > "$COMMITTEE_FILE"
echo "INFO: Configuration files generated successfully."

# --- Giai đoạn 3: Khởi chạy Nodes ---
echo ""
echo "--- Stage 3: Launching Nodes ---"
echo "INFO: Launching $NODES nodes..."
for i in $(seq 0 $((NODES-1))); do
    key_file="${key_files[$i]}";
    db_path="$BENCHMARK_DIR/db_$i"; log_file="$LOG_DIR/node-$i.log"
    cmd="$NODE_BINARY run --keys $key_file --committee $COMMITTEE_FILE --store $db_path --parameters $PARAMETERS_FILE"
    if [ "$i" -eq 0 ]; then
       cmd="$cmd --executor-socket /tmp/executor.sock"
    fi
    full_cmd_with_log="RUST_LOG=info $cmd"
    tmux new -d -s "node-$i" "sh -c '$full_cmd_with_log 2> $log_file || echo \"[FATAL] Node process exited.\" >> $log_file'"
done

echo ""
echo "✅ Nodes are now running. Please use 'tmux ls' to view sessions and 'tmux attach -t node-0' to inspect a node's output. To stop all nodes, run 'tmux kill-server'."
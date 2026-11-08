package data

import (
	"strings"
	"sync"
	"time"
)

// RpcRuntimeStatsSnapshot is an in-process snapshot of RPC activity for one
// chain. It is intentionally not persisted; restarting the process resets it.
type RpcRuntimeStatsSnapshot struct {
	Network             string
	SuccessCount        int64
	FailureCount        int64
	LatestBlockHeight   int64
	LastSyncAt          time.Time
	LastProcessedBlock  int64
	ProcessedBlockCount int64
	LastProcessedAt     time.Time
}

type rpcRuntimeStatsCounter struct {
	successCount        int64
	failureCount        int64
	latestBlockHeight   int64
	lastSyncAt          time.Time
	lastProcessedBlock  int64
	processedBlockCount int64
	lastProcessedAt     time.Time
}

var gRpcRuntimeStats = struct {
	sync.RWMutex
	byNetwork map[string]*rpcRuntimeStatsCounter
}{
	byNetwork: make(map[string]*rpcRuntimeStatsCounter),
}

func normalizeRpcStatsNetwork(network string) string {
	return strings.ToLower(strings.TrimSpace(network))
}

func rpcStatsCounterLocked(network string) *rpcRuntimeStatsCounter {
	counter := gRpcRuntimeStats.byNetwork[network]
	if counter == nil {
		counter = &rpcRuntimeStatsCounter{}
		gRpcRuntimeStats.byNetwork[network] = counter
	}
	return counter
}

// RecordRpcSuccess records one successful application-level RPC operation.
func RecordRpcSuccess(network string) {
	network = normalizeRpcStatsNetwork(network)
	if network == "" {
		return
	}
	gRpcRuntimeStats.Lock()
	defer gRpcRuntimeStats.Unlock()
	counter := rpcStatsCounterLocked(network)
	counter.successCount++
	counter.lastSyncAt = time.Now()
}

// RecordRpcFailure records one failed application-level RPC operation.
func RecordRpcFailure(network string) {
	network = normalizeRpcStatsNetwork(network)
	if network == "" {
		return
	}
	gRpcRuntimeStats.Lock()
	defer gRpcRuntimeStats.Unlock()
	counter := rpcStatsCounterLocked(network)
	counter.failureCount++
}

// RecordProcessedBlock records one block that a scanner finished processing.
// It does not update chain-tip health fields.
func RecordProcessedBlock(network string, blockNumber int64) {
	network = normalizeRpcStatsNetwork(network)
	if network == "" || blockNumber <= 0 {
		return
	}
	gRpcRuntimeStats.Lock()
	defer gRpcRuntimeStats.Unlock()
	counter := rpcStatsCounterLocked(network)
	counter.lastProcessedBlock = blockNumber
	counter.processedBlockCount++
	counter.lastProcessedAt = time.Now()
}

// RecordRpcBlockHeight records the latest observed chain height.
func RecordRpcBlockHeight(network string, height int64) {
	network = normalizeRpcStatsNetwork(network)
	if network == "" || height <= 0 {
		return
	}
	gRpcRuntimeStats.Lock()
	defer gRpcRuntimeStats.Unlock()
	counter := rpcStatsCounterLocked(network)
	counter.lastSyncAt = time.Now()
	if height > counter.latestBlockHeight {
		counter.latestBlockHeight = height
	}
}

// SnapshotRpcRuntimeStats returns a copy of the current per-chain counters.
func SnapshotRpcRuntimeStats() map[string]RpcRuntimeStatsSnapshot {
	gRpcRuntimeStats.RLock()
	defer gRpcRuntimeStats.RUnlock()
	out := make(map[string]RpcRuntimeStatsSnapshot, len(gRpcRuntimeStats.byNetwork))
	for network, counter := range gRpcRuntimeStats.byNetwork {
		out[network] = RpcRuntimeStatsSnapshot{
			Network:             network,
			SuccessCount:        counter.successCount,
			FailureCount:        counter.failureCount,
			LatestBlockHeight:   counter.latestBlockHeight,
			LastSyncAt:          counter.lastSyncAt,
			LastProcessedBlock:  counter.lastProcessedBlock,
			ProcessedBlockCount: counter.processedBlockCount,
			LastProcessedAt:     counter.lastProcessedAt,
		}
	}
	return out
}

// ResetRpcRuntimeStatsForTest clears process-local stats for isolated tests.
func ResetRpcRuntimeStatsForTest() {
	gRpcRuntimeStats.Lock()
	defer gRpcRuntimeStats.Unlock()
	gRpcRuntimeStats.byNetwork = make(map[string]*rpcRuntimeStatsCounter)
}

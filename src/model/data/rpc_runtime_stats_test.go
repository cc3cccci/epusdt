package data

import (
	"testing"
	"time"
)

func TestRpcRuntimeStatsRecordsCountsAndLatestHeight(t *testing.T) {
	ResetRpcRuntimeStatsForTest()
	t.Cleanup(ResetRpcRuntimeStatsForTest)

	start := time.Now()
	RecordRpcSuccess(" TRON ")
	RecordRpcSuccess("tron")
	RecordRpcFailure("TRON")
	RecordRpcBlockHeight("Tron", 100)
	RecordRpcBlockHeight("tron", 90)

	stats := SnapshotRpcRuntimeStats()
	got, ok := stats["tron"]
	if !ok {
		t.Fatalf("missing tron stats: %#v", stats)
	}
	if got.Network != "tron" {
		t.Fatalf("network = %q, want tron", got.Network)
	}
	if got.SuccessCount != 2 {
		t.Fatalf("success count = %d, want 2", got.SuccessCount)
	}
	if got.FailureCount != 1 {
		t.Fatalf("failure count = %d, want 1", got.FailureCount)
	}
	if got.LatestBlockHeight != 100 {
		t.Fatalf("latest block height = %d, want 100", got.LatestBlockHeight)
	}
	if got.LastSyncAt.Before(start) {
		t.Fatalf("last sync at = %s, want after %s", got.LastSyncAt, start)
	}
}

func TestRpcRuntimeStatsIgnoresEmptyNetworkAndInvalidHeight(t *testing.T) {
	ResetRpcRuntimeStatsForTest()
	t.Cleanup(ResetRpcRuntimeStatsForTest)

	RecordRpcSuccess("")
	RecordRpcFailure(" ")
	RecordRpcBlockHeight("tron", 0)

	stats := SnapshotRpcRuntimeStats()
	if len(stats) != 0 {
		t.Fatalf("stats = %#v, want empty", stats)
	}
}

func TestRpcRuntimeStatsBlockHeightUpdatesLastSyncAt(t *testing.T) {
	ResetRpcRuntimeStatsForTest()
	t.Cleanup(ResetRpcRuntimeStatsForTest)

	start := time.Now()
	RecordRpcBlockHeight("ethereum", 123)

	stats := SnapshotRpcRuntimeStats()
	got := stats["ethereum"]
	if got.SuccessCount != 0 {
		t.Fatalf("success count = %d, want 0", got.SuccessCount)
	}
	if got.LastSyncAt.Before(start) {
		t.Fatalf("last sync at = %s, want after %s", got.LastSyncAt, start)
	}
}

func TestProcessedBlockDoesNotUpdateChainTipHealth(t *testing.T) {
	ResetRpcRuntimeStatsForTest()
	t.Cleanup(ResetRpcRuntimeStatsForTest)

	start := time.Now()
	RecordRpcBlockHeight("tron", 500)
	RecordRpcSuccess("tron")

	before := SnapshotRpcRuntimeStats()["tron"]
	if before.LastProcessedBlock != 0 || before.ProcessedBlockCount != 0 || !before.LastProcessedAt.IsZero() {
		t.Fatalf("chain tip recorded as processed: %+v", before)
	}

	RecordProcessedBlock(" TRON ", 480)
	RecordProcessedBlock("tron", 481)

	got := SnapshotRpcRuntimeStats()["tron"]
	if got.LastProcessedBlock != 481 {
		t.Fatalf("last processed block = %d, want 481", got.LastProcessedBlock)
	}
	if got.ProcessedBlockCount != 2 {
		t.Fatalf("processed block count = %d, want 2", got.ProcessedBlockCount)
	}
	if got.LastProcessedAt.Before(start) {
		t.Fatalf("last processed at = %s, want after %s", got.LastProcessedAt, start)
	}
	if got.LatestBlockHeight != 500 {
		t.Fatalf("latest block height = %d, want 500", got.LatestBlockHeight)
	}
	if got.SuccessCount != 1 {
		t.Fatalf("success count = %d, want 1", got.SuccessCount)
	}
	if !got.LastSyncAt.Equal(before.LastSyncAt) {
		t.Fatalf("last sync at changed from %s to %s", before.LastSyncAt, got.LastSyncAt)
	}
}

func TestRecordProcessedBlockIgnoresEmptyNetworkAndNonPositiveBlock(t *testing.T) {
	ResetRpcRuntimeStatsForTest()
	t.Cleanup(ResetRpcRuntimeStatsForTest)

	RecordProcessedBlock("", 10)
	RecordProcessedBlock("tron", 0)
	RecordProcessedBlock("tron", -1)

	if stats := SnapshotRpcRuntimeStats(); len(stats) != 0 {
		t.Fatalf("stats = %#v, want empty", stats)
	}
}

func TestResetRpcRuntimeStatsForTest(t *testing.T) {
	ResetRpcRuntimeStatsForTest()
	RecordRpcSuccess("tron")
	ResetRpcRuntimeStatsForTest()

	stats := SnapshotRpcRuntimeStats()
	if len(stats) != 0 {
		t.Fatalf("stats = %#v, want empty after reset", stats)
	}
}

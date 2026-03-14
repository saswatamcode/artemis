package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/span"
)

// TestIsolationCoordinator_InFlightTransactions tests that in-flight transactions
// are not visible to queries, preventing the visibility race condition.
func TestIsolationCoordinator_InFlightTransactions(t *testing.T) {
	ic := NewIsolationCoordinator()

	// First, commit a transaction normally to establish a baseline
	ic.AcquireAppendLock()
	txnID0 := uint64(99)
	commitSeq0 := ic.AssignCommitSequence(txnID0)
	ic.MarkInFlight(txnID0, commitSeq0)
	ic.ReleaseAppendLock()
	ic.RegisterCommit(txnID0, commitSeq0, []string{"span0"})

	// Now test the in-flight behavior
	// 1. Assign commit sequence and mark in-flight
	ic.AcquireAppendLock()
	txnID := uint64(100)
	commitSeq := ic.AssignCommitSequence(txnID)
	ic.MarkInFlight(txnID, commitSeq)
	ic.ReleaseAppendLock()

	// 2. Manually add span to spanIDToTxn to simulate the state where:
	//    - Span IDs are being collected for MVCC registration
	//    - But RegisterCommit hasn't been called yet
	ic.mvccMu.Lock()
	ic.spanIDToTxn["span1"] = txnID
	ic.spanIDToTxn["span2"] = txnID
	ic.mvccMu.Unlock()

	// 3. Capture a snapshot AFTER the in-flight transaction started
	snapshotSeq := ic.BeginQuery()

	// 4. Verify that in-flight spans are NOT visible
	if ic.IsVisible("span1", snapshotSeq) {
		t.Error("span1 should NOT be visible while transaction is in-flight")
	}
	if ic.IsVisible("span2", snapshotSeq) {
		t.Error("span2 should NOT be visible while transaction is in-flight")
	}

	// 5. Complete the commit by calling RegisterCommit
	ic.RegisterCommit(txnID, commitSeq, []string{"span1", "span2"})

	// 6. Verify that spans are now visible at a NEW snapshot
	newSnapshotSeq := ic.BeginQuery()
	if !ic.IsVisible("span1", newSnapshotSeq) {
		t.Error("span1 should be visible after transaction completes")
	}
	if !ic.IsVisible("span2", newSnapshotSeq) {
		t.Error("span2 should be visible after transaction completes")
	}

	// 7. Old snapshot should still NOT see the spans (they committed after it)
	if ic.IsVisible("span1", snapshotSeq) {
		t.Error("span1 should NOT be visible at old snapshot (committed after snapshot)")
	}
}

// TestIsolationCoordinator_VisibilityRacePrevention tests that the visibility race
// is prevented by the two-phase commit with in-flight tracking.
func TestIsolationCoordinator_VisibilityRacePrevention(t *testing.T) {
	ic := NewIsolationCoordinator()
	storage := NewArrowStorage()
	defer storage.Release()
	linkStorage := NewArrowLinkStorage()
	defer linkStorage.Release()
	wal := newMockWAL()
	storage.SetTransactionDependencies(ic, linkStorage, wal)

	// Track whether a race occurred
	var raceDetected bool
	var mu sync.Mutex

	// Start a goroutine that continuously queries for a limited time
	done := make(chan bool)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	go func() {
		defer func() {
			done <- true
		}()

		// Run checks for a limited time (until ticker stops or test completes)
		timeout := time.After(2 * time.Second)
		for {
			select {
			case <-timeout:
				return
			case <-ticker.C:
				// Capture snapshot
				snapshotSeq := ic.BeginQuery()

				// Check all spans in the MVCC map
				ic.mvccMu.RLock()
				spanIDsCopy := make(map[string]uint64)
				for k, v := range ic.spanIDToTxn {
					spanIDsCopy[k] = v
				}
				ic.mvccMu.RUnlock()

				// Check visibility outside the lock
				for spanID, txnID := range spanIDsCopy {
					visible := ic.IsVisible(spanID, snapshotSeq)

					// If the span is visible, its transaction must be committed
					// and must have commitSeq <= snapshotSeq
					if visible {
						ic.mvccMu.RLock()
						commitSeq, ok := ic.commitSeqMap[txnID]
						_, inFlight := ic.inFlightTxns[txnID]
						ic.mvccMu.RUnlock()

						if inFlight {
							mu.Lock()
							raceDetected = true
							mu.Unlock()
							t.Errorf("Race detected: span %s (txn %d) is visible but transaction is in-flight", spanID, txnID)
						} else if !ok {
							mu.Lock()
							raceDetected = true
							mu.Unlock()
							t.Errorf("Race detected: span %s is visible but transaction %d is not committed", spanID, txnID)
						} else if commitSeq > snapshotSeq {
							mu.Lock()
							raceDetected = true
							mu.Unlock()
							t.Errorf("Race detected: span %s is visible but committed after snapshot (commitSeq=%d > snapshotSeq=%d)", spanID, commitSeq, snapshotSeq)
						}
					}
				}
			}
		}
	}()

	// Commit multiple transactions concurrently
	const numTransactions = 50
	var wg sync.WaitGroup
	now := time.Now()

	for i := range numTransactions {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			appender := storage.BeginTransaction()
			testSpan := &span.Span{
				TraceID:     "00000000000000010000000000000000",
				SpanID:      "span-" + string(rune(idx)),
				Name:        "test",
				StartTime:   now,
				EndTime:     now.Add(time.Millisecond),
				Duration:    1_000_000,
				ServiceName: "svc",
			}
			appender.AddSpan(testSpan)
			if err := appender.Commit(); err != nil {
				t.Errorf("Transaction %d failed to commit: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()

	// Wait a bit more for final checks
	time.Sleep(100 * time.Millisecond)

	// Stop the checker
	ticker.Stop()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if raceDetected {
		t.Error("Visibility race was detected during concurrent commits")
	}
}

// TestIsolationCoordinator_RollbackInFlight tests the rollback of in-flight transactions
func TestIsolationCoordinator_RollbackInFlight(t *testing.T) {
	ic := NewIsolationCoordinator()

	// Start a transaction
	ic.AcquireAppendLock()
	txnID := uint64(100)
	commitSeq := ic.AssignCommitSequence(txnID)
	ic.MarkInFlight(txnID, commitSeq)
	ic.ReleaseAppendLock()

	// Verify it's in-flight
	ic.mvccMu.RLock()
	_, inFlight := ic.inFlightTxns[txnID]
	ic.mvccMu.RUnlock()
	if !inFlight {
		t.Error("Transaction should be marked as in-flight")
	}

	// Rollback the transaction (simulate a failed commit)
	ic.RegisterRollback(txnID)

	// Verify it's no longer in-flight
	ic.mvccMu.RLock()
	_, inFlight = ic.inFlightTxns[txnID]
	ic.mvccMu.RUnlock()
	if inFlight {
		t.Error("Transaction should not be marked as in-flight after unregister")
	}

	// Verify it's not in the commit sequence map
	ic.mvccMu.RLock()
	_, hasCommitSeq := ic.commitSeqMap[txnID]
	ic.mvccMu.RUnlock()
	if hasCommitSeq {
		t.Error("Transaction should not have commit sequence after rollback")
	}

	// Verify rollback counter was incremented
	stats := ic.Stats()
	if stats.TotalRollbacks != 1 {
		t.Errorf("Expected 1 rollback, got %d", stats.TotalRollbacks)
	}
}

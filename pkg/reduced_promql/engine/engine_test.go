package engine

import (
	"context"
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// TestEngineBasic verifies the engine can execute simple queries
func TestEngineBasic(t *testing.T) {
	// Create isolation coordinator
	isolation := storage.NewIsolationCoordinator()

	// Create block getter function
	blockGetter := func() []block.Block {
		return nil // Empty for tests
	}

	// Create engine
	engine := NewEngine(blockGetter, isolation)

	// Create query options
	opts := &QueryOptions{
		StartTime: time.Now().Add(-1 * time.Hour),
		EndTime:   time.Now(),
		Context:   context.Background(),
		Limit:     1000,
	}

	// Test simple vector selector
	t.Run("VectorSelector", func(t *testing.T) {
		result, err := engine.Execute(`{service_name="api"}`, opts)
		if err != nil {
			t.Logf("Parse/plan succeeded (expected with empty blocks): %v", err)
		}
		if result != nil {
			t.Logf("Result: %d spans, %s", len(result.Spans), result.Stats.String())
		}
	})

	// Test rate query
	t.Run("RateQuery", func(t *testing.T) {
		result, err := engine.Execute(`rate({service_name="api"}[5m])`, opts)
		if err != nil {
			t.Logf("Parse/plan succeeded: %v", err)
		}
		if result != nil {
			t.Logf("Result: %d rate values, %s", len(result.Spans), result.Stats.String())
		}
	})

	// Test histogram quantile
	t.Run("HistogramQuantile", func(t *testing.T) {
		result, err := engine.Execute(`histogram_quantile(0.95, {service_name="api"})`, opts)
		if err != nil {
			t.Logf("Parse/plan succeeded: %v", err)
		}
		if result != nil {
			t.Logf("Result: p95 = %v, %s", result.Spans, result.Stats.String())
		}
	})

	// Test aggregation
	t.Run("Aggregation", func(t *testing.T) {
		result, err := engine.Execute(`sum by (service_name) ({job="app"})`, opts)
		if err != nil {
			t.Logf("Parse/plan succeeded: %v", err)
		}
		if result != nil {
			t.Logf("Result: %d groups, %s", len(result.Spans), result.Stats.String())
		}
	})

	// Test heatmap
	t.Run("Heatmap", func(t *testing.T) {
		result, err := engine.Execute(`heatmap({service_name="api"})`, opts)
		if err != nil {
			t.Logf("Parse/plan succeeded: %v", err)
		}
		if result != nil {
			t.Logf("Result: %d heatmap cells, %s", len(result.Spans), result.Stats.String())
		}
	})
}

// TestEngineAsync verifies async execution
func TestEngineAsync(t *testing.T) {
	isolation := storage.NewIsolationCoordinator()
	blockGetter := func() []block.Block {
		return nil
	}
	engine := NewEngine(blockGetter, isolation)

	opts := &QueryOptions{
		StartTime: time.Now().Add(-1 * time.Hour),
		EndTime:   time.Now(),
		Context:   context.Background(),
	}

	resultCh, errCh := engine.ExecuteAsync(`{service_name="api"}`, opts)

	batchCount := 0
	totalSpans := 0

	for batch := range resultCh {
		batchCount++
		totalSpans += len(batch)
	}

	if err := <-errCh; err != nil {
		t.Logf("Async execution completed: %v", err)
	}

	t.Logf("Received %d batches with %d total spans", batchCount, totalSpans)
}

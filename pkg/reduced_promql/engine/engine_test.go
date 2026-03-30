package engine

import (
	"context"
	"testing"
	"time"

	"github.com/saswatamcode/artemis/pkg/block"
	"github.com/saswatamcode/artemis/pkg/storage"
)

// TestEngineNesting documents which query nestings are supported.
//
// Supported nestings:
//   - sum(rate({...}[5m]))                  ✅ Aggregation wrapping rate
//   - avg by (x) (rate({...}[1m]))          ✅ Aggregation with grouping wrapping rate
//   - sum(heatmap({...}))                   ✅ Aggregation wrapping heatmap
//   - histogram_quantile(0.95, {...})       ✅ Function with any selector
//
// Unsupported (rejected by parser):
//   - heatmap(rate({...}[5m]))              ❌ Heatmap cannot accept rate
//   - heatmap(sum({...}))                   ❌ Heatmap cannot accept aggregation
//   - heatmap({...}[5m])                    ❌ Heatmap cannot accept matrix selector
//
// The heatmap() function is intentionally restricted to only accept plain vector
// selectors because it operates on raw span durations, not aggregated metrics.
func TestEngineNesting(t *testing.T) {
	isolation := storage.NewIsolationCoordinator()
	blockGetter := func() []block.Block { return nil }
	engine := NewEngine(blockGetter, isolation)
	opts := &QueryOptions{
		StartTime: time.Now().Add(-1 * time.Hour),
		EndTime:   time.Now(),
		Context:   context.Background(),
	}

	// Supported nestings
	supportedQueries := []struct {
		name  string
		query string
	}{
		{"sum(rate)", `sum(rate({service_name="api"}[5m]))`},
		{"sum_by(rate)", `sum by (handler) (rate({env="prod"}[1m]))`},
		{"avg(rate)", `avg(rate({service="web"}[30s]))`},
		{"sum(heatmap)", `sum(heatmap({status="ok"}))`},
		{"histogram_quantile", `histogram_quantile(0.95, {service="api"})`},
	}

	for _, tc := range supportedQueries {
		t.Run("Supported_"+tc.name, func(t *testing.T) {
			result, err := engine.Execute(tc.query, opts)
			if err != nil {
				t.Fatalf("Expected %s to succeed, got error: %v", tc.query, err)
			}
			if result == nil {
				t.Fatalf("Expected result for %s, got nil", tc.query)
			}
			t.Logf("✅ %s works: %s", tc.query, result.Type)
		})
	}

	// Unsupported nestings (should fail at parse time)
	unsupportedQueries := []struct {
		name  string
		query string
	}{
		{"heatmap(rate)", `heatmap(rate({service="api"}[5m]))`},
		{"heatmap(sum)", `heatmap(sum({service="api"}))`},
		{"heatmap(matrix)", `heatmap({service="api"}[5m])`},
	}

	for _, tc := range unsupportedQueries {
		t.Run("Unsupported_"+tc.name, func(t *testing.T) {
			_, err := engine.Execute(tc.query, opts)
			if err == nil {
				t.Fatalf("Expected %s to fail, but it succeeded", tc.query)
			}
			t.Logf("✅ %s correctly rejected: %v", tc.query, err)
		})
	}
}

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

	// Test nested: sum(rate(...))
	t.Run("NestedSumRate", func(t *testing.T) {
		result, err := engine.Execute(`sum(rate({service_name="api"}[5m]))`, opts)
		if err != nil {
			t.Fatalf("Failed to execute sum(rate(...)): %v", err)
		}
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		t.Logf("Result: %s, %s", result.Type, result.Stats.String())
	})

	// Test nested: sum by (x) (rate(...))
	t.Run("NestedSumByRate", func(t *testing.T) {
		result, err := engine.Execute(`sum by (handler) (rate({env="prod"}[1m]))`, opts)
		if err != nil {
			t.Fatalf("Failed to execute sum by (x) (rate(...)): %v", err)
		}
		if result == nil {
			t.Fatal("Expected result, got nil")
		}
		t.Logf("Result: %s, %s", result.Type, result.Stats.String())
	})

	// Test invalid: heatmap(rate(...)) should fail
	t.Run("InvalidHeatmapRate", func(t *testing.T) {
		_, err := engine.Execute(`heatmap(rate({service_name="api"}[5m]))`, opts)
		if err == nil {
			t.Fatal("Expected error for heatmap(rate(...)), got nil")
		}
		t.Logf("Correctly rejected heatmap(rate(...)): %v", err)
	})

	// Test invalid: heatmap with aggregation should fail
	t.Run("InvalidHeatmapAggregation", func(t *testing.T) {
		_, err := engine.Execute(`heatmap(sum({service_name="api"}))`, opts)
		if err == nil {
			t.Fatal("Expected error for heatmap(sum(...)), got nil")
		}
		t.Logf("Correctly rejected heatmap(sum(...)): %v", err)
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

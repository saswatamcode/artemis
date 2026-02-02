package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:8815", "Flight SQL server address")
	query := flag.String("query", "", "SQL query to execute (required)")
	limit := flag.Int("limit", 100, "Maximum rows to display")
	flag.Parse()

	if *query == "" {
		fmt.Println("Usage: artemis-query -query \"SELECT * FROM spans LIMIT 10\"")
		fmt.Println("\nExamples:")
		fmt.Println("  # Count all spans")
		fmt.Println("  artemis-query -query \"SELECT * FROM spans\"")
		fmt.Println()
		fmt.Println("  # Find spans from specific service")
		fmt.Println("  artemis-query -query \"SELECT * FROM spans WHERE service_name = 'my-test-service' LIMIT 10\"")
		fmt.Println()
		fmt.Println("  # Find slowest operations")
		fmt.Println("  artemis-query -query \"SELECT name, duration FROM spans ORDER BY duration DESC LIMIT 10\"")
		os.Exit(1)
	}

	// Connect to Flight SQL with insecure credentials
	client, err := flightsql.NewClient(*addr, nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to %s: %v", *addr, err)
	}
	defer client.Close()

	fmt.Printf("Connected to %s\n", *addr)
	fmt.Printf("Executing: %s\n\n", *query)

	ctx := context.Background()

	// Execute query
	info, err := client.Execute(ctx, *query)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}

	if len(info.Endpoint) == 0 {
		fmt.Println("No results returned")
		return
	}

	// Read results
	reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		log.Fatalf("Failed to get results: %v", err)
	}
	defer reader.Release()

	totalRows := 0
	rowsDisplayed := 0

	// Process records
	for reader.Next() {
		record := reader.Record()
		numRows := int(record.NumRows())
		totalRows += numRows

		// Print header on first record
		if rowsDisplayed == 0 {
			printHeader(record.Schema())
		}

		// Print rows
		for i := 0; i < numRows && rowsDisplayed < *limit; i++ {
			printRow(record, i)
			rowsDisplayed++
		}

		if rowsDisplayed >= *limit {
			break
		}
	}

	if err := reader.Err(); err != nil {
		log.Fatalf("Reader error: %v", err)
	}

	fmt.Printf("\n---\n")
	fmt.Printf("Total rows: %d", totalRows)
	if rowsDisplayed < totalRows {
		fmt.Printf(" (showing first %d)", rowsDisplayed)
	}
	fmt.Println()
}

func printHeader(schema *arrow.Schema) {
	fmt.Print("| ")
	for _, field := range schema.Fields() {
		fmt.Printf("%-20s | ", field.Name)
	}
	fmt.Println()

	fmt.Print("|-")
	for range schema.Fields() {
		fmt.Print("---------------------|-")
	}
	fmt.Println()
}

func printRow(record arrow.Record, rowIdx int) {
	fmt.Print("| ")
	for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
		col := record.Column(colIdx)
		value := formatValue(col, rowIdx)
		fmt.Printf("%-20s | ", truncate(value, 20))
	}
	fmt.Println()
}

func formatValue(arr arrow.Array, idx int) string {
	if arr.IsNull(idx) {
		return "<null>"
	}

	switch arr := arr.(type) {
	case *array.String:
		return arr.Value(idx)
	case *array.Int64:
		val := arr.Value(idx)
		// Convert duration to milliseconds for readability
		if val > 1000000 {
			return fmt.Sprintf("%d (%.2fms)", val, float64(val)/1e6)
		}
		return fmt.Sprintf("%d", val)
	case *array.Map:
		// For map columns (tags), just indicate presence
		if arr.IsNull(idx) {
			return "<no tags>"
		}
		return "<tags>"
	default:
		return fmt.Sprintf("%v", arr)
	}
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

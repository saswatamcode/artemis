package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

var (
	// Global flags
	artemisURL string
	tempoPort  int
	jaegerPort int
	sqlPort    int

	// Root command
	rootCmd = &cobra.Command{
		Use:   "artemistool",
		Short: "CLI tool for querying Artemis trace server",
		Long:  `artemistool is a command-line interface for querying an Artemis trace server via its various APIs.`,
	}

	// Tempo command
	tempoCmd = &cobra.Command{
		Use:   "tempo",
		Short: "Query Artemis using Tempo API",
		Long:  `Query the Artemis server using the Tempo-compatible API.`,
	}

	tempoSearchCmd = &cobra.Command{
		Use:   "search",
		Short: "Search for traces using Tempo API",
		RunE:  runTempoSearch,
	}

	// Jaeger command
	jaegerCmd = &cobra.Command{
		Use:   "jaeger",
		Short: "Query Artemis using Jaeger API",
		Long:  `Query the Artemis server using the Jaeger-compatible API.`,
	}

	jaegerSearchCmd = &cobra.Command{
		Use:   "search",
		Short: "Search for traces using Jaeger API",
		RunE:  runJaegerSearch,
	}

	jaegerGetCmd = &cobra.Command{
		Use:   "get [traceID]",
		Short: "Get a specific trace by ID",
		Args:  cobra.ExactArgs(1),
		RunE:  runJaegerGet,
	}

	// SQL command
	sqlCmd = &cobra.Command{
		Use:   "sql",
		Short: "Query Artemis using SQL API",
		RunE:  runSQL,
	}

	// Tempo search flags
	tempoQuery string
	tempoStart string
	tempoEnd   string
	tempoLimit int

	// Jaeger search flags
	jaegerService   string
	jaegerOperation string
	jaegerStart     string
	jaegerEnd       string
	jaegerLimit     int
	jaegerTags      string

	// SQL flags
	sqlQuery string
)

func init() {
	// Global flags
	rootCmd.PersistentFlags().StringVar(&artemisURL, "query-url", "http://localhost", "Artemis server query endpoint URL")
	rootCmd.PersistentFlags().IntVar(&tempoPort, "tempo-port", 3200, "Tempo API port")
	rootCmd.PersistentFlags().IntVar(&jaegerPort, "jaeger-port", 16686, "Jaeger API port")
	rootCmd.PersistentFlags().IntVar(&sqlPort, "sql-port", 5433, "SQL API port")

	// Tempo search flags
	tempoSearchCmd.Flags().StringVarP(&tempoQuery, "query", "q", "", "TraceQL query (e.g., '{service.name=\"foo\"}')")
	tempoSearchCmd.Flags().StringVar(&tempoStart, "start", "", "Start time (Unix timestamp or RFC3339)")
	tempoSearchCmd.Flags().StringVar(&tempoEnd, "end", "", "End time (Unix timestamp or RFC3339)")
	tempoSearchCmd.Flags().IntVarP(&tempoLimit, "limit", "l", 20, "Maximum number of results")

	// Jaeger search flags
	jaegerSearchCmd.Flags().StringVarP(&jaegerService, "service", "s", "", "Service name")
	jaegerSearchCmd.Flags().StringVarP(&jaegerOperation, "operation", "o", "", "Operation name")
	jaegerSearchCmd.Flags().StringVar(&jaegerStart, "start", "", "Start time (Unix timestamp or RFC3339)")
	jaegerSearchCmd.Flags().StringVar(&jaegerEnd, "end", "", "End time (Unix timestamp or RFC3339)")
	jaegerSearchCmd.Flags().IntVarP(&jaegerLimit, "limit", "l", 20, "Maximum number of results")
	jaegerSearchCmd.Flags().StringVarP(&jaegerTags, "tags", "t", "", "Tags in key=value,key2=value2 format")

	// SQL flags
	sqlCmd.Flags().StringVarP(&sqlQuery, "query", "q", "", "SQL query to execute")
	sqlCmd.MarkFlagRequired("query")

	// Add subcommands
	tempoCmd.AddCommand(tempoSearchCmd)
	jaegerCmd.AddCommand(jaegerSearchCmd, jaegerGetCmd)
	rootCmd.AddCommand(tempoCmd, jaegerCmd, sqlCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// runTempoSearch executes a Tempo search query
func runTempoSearch(cmd *cobra.Command, args []string) error {
	url := fmt.Sprintf("%s:%d/api/search", artemisURL, tempoPort)

	// Build query parameters
	params := make([]string, 0)
	if tempoQuery != "" {
		params = append(params, fmt.Sprintf("q=%s", tempoQuery))
	}
	if tempoStart != "" {
		ts, err := parseTime(tempoStart)
		if err != nil {
			return fmt.Errorf("invalid start time: %w", err)
		}
		params = append(params, fmt.Sprintf("start=%d", ts.Unix()))
	}
	if tempoEnd != "" {
		ts, err := parseTime(tempoEnd)
		if err != nil {
			return fmt.Errorf("invalid end time: %w", err)
		}
		params = append(params, fmt.Sprintf("end=%d", ts.Unix()))
	}
	if tempoLimit > 0 {
		params = append(params, fmt.Sprintf("limit=%d", tempoLimit))
	}

	if len(params) > 0 {
		url = url + "?" + strings.Join(params, "&")
	}

	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to query Tempo API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("tempo API returned status %d: %s", resp.StatusCode, string(body))
	}

	var result TempoSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	printTempoSearchResults(result)
	return nil
}

// runJaegerSearch executes a Jaeger search query
func runJaegerSearch(cmd *cobra.Command, args []string) error {
	url := fmt.Sprintf("%s:%d/api/traces", artemisURL, jaegerPort)

	// Build query parameters
	params := make([]string, 0)
	if jaegerService != "" {
		params = append(params, fmt.Sprintf("service=%s", jaegerService))
	}
	if jaegerOperation != "" {
		params = append(params, fmt.Sprintf("operation=%s", jaegerOperation))
	}
	if jaegerStart != "" {
		ts, err := parseTime(jaegerStart)
		if err != nil {
			return fmt.Errorf("invalid start time: %w", err)
		}
		params = append(params, fmt.Sprintf("start=%d", ts.Unix()))
	}
	if jaegerEnd != "" {
		ts, err := parseTime(jaegerEnd)
		if err != nil {
			return fmt.Errorf("invalid end time: %w", err)
		}
		params = append(params, fmt.Sprintf("end=%d", ts.Unix()))
	}
	if jaegerLimit > 0 {
		params = append(params, fmt.Sprintf("limit=%d", jaegerLimit))
	}
	if jaegerTags != "" {
		tagPairs := strings.Split(jaegerTags, ",")
		for _, pair := range tagPairs {
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) == 2 {
				params = append(params, fmt.Sprintf("tag.%s=%s", parts[0], parts[1]))
			}
		}
	}

	if len(params) > 0 {
		url = url + "?" + strings.Join(params, "&")
	}

	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to query Jaeger API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("jaeger API returned status %d: %s", resp.StatusCode, string(body))
	}

	var result JaegerSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	printJaegerSearchResults(result)
	return nil
}

// runJaegerGet retrieves a specific trace by ID
func runJaegerGet(cmd *cobra.Command, args []string) error {
	traceID := args[0]
	url := fmt.Sprintf("%s:%d/api/traces/%s", artemisURL, jaegerPort, traceID)

	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to query Jaeger API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("jaeger API returned status %d: %s", resp.StatusCode, string(body))
	}

	var result JaegerSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	printJaegerSearchResults(result)
	return nil
}

// runSQL executes a SQL query
func runSQL(cmd *cobra.Command, args []string) error {
	url := fmt.Sprintf("%s:%d/api/query", artemisURL, sqlPort)

	reqBody := SQLQueryRequest{
		Query: sqlQuery,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to query SQL API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("sql API returned status %d: %s", resp.StatusCode, string(body))
	}

	var result SQLQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if !result.Success {
		return fmt.Errorf("query failed: %s", result.Error)
	}

	printSQLResults(result)
	return nil
}

// printTempoSearchResults prints Tempo search results in tabular format
func printTempoSearchResults(result TempoSearchResponse) {
	if len(result.Traces) == 0 {
		fmt.Println("No traces found")
		return
	}

	fmt.Printf("Found %d traces (inspected %d traces, %d blocks)\n\n",
		len(result.Traces), result.Metrics.InspectedTraces, result.Metrics.InspectedBlocks)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TRACE ID\tROOT SERVICE\tROOT NAME\tSTART TIME\tDURATION\tSPANS")
	fmt.Fprintln(w, "--------\t------------\t---------\t----------\t--------\t-----")

	for _, trace := range result.Traces {
		startTime := time.Unix(0, int64(trace.StartTimeUnixNano))
		duration := time.Duration(trace.DurationMs) * time.Millisecond

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\n",
			trace.TraceID,
			trace.RootServiceName,
			trace.RootTraceName,
			startTime.Format("15:04:05"),
			duration,
			trace.SpanSets[0].Spans,
		)
	}
	w.Flush()
}

// printJaegerSearchResults prints Jaeger search results in tabular format
func printJaegerSearchResults(result JaegerSearchResponse) {
	if len(result.Data) == 0 {
		fmt.Println("No traces found")
		return
	}

	fmt.Printf("Found %d traces\n\n", result.Total)

	for _, trace := range result.Data {
		fmt.Printf("Trace ID: %s (%d spans)\n", trace.TraceID, len(trace.Spans))

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "SPAN ID\tSERVICE\tOPERATION\tSTART TIME\tDURATION")
		fmt.Fprintln(w, "-------\t-------\t---------\t----------\t--------")

		for _, span := range trace.Spans {
			startTime := time.Unix(0, span.StartTime*1000)
			duration := time.Duration(span.Duration) * time.Microsecond

			serviceName := "unknown"
			if proc, ok := trace.Processes[span.ProcessID]; ok {
				serviceName = proc.ServiceName
			}

			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				span.SpanID,
				serviceName,
				span.OperationName,
				startTime.Format("15:04:05.000"),
				duration,
			)
		}
		w.Flush()
		fmt.Println()
	}
}

// printSQLResults prints SQL query results in tabular format
func printSQLResults(result SQLQueryResponse) {
	if result.RowCount == 0 {
		fmt.Println("No results found")
		return
	}

	fmt.Printf("Query returned %d rows\n\n", result.RowCount)

	if len(result.Rows) == 0 {
		fmt.Println("No rows to display")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	// Print header
	fmt.Fprintln(w, strings.Join(result.Columns, "\t"))
	fmt.Fprintln(w, strings.Repeat("-", len(result.Columns)*8))

	// Print rows
	for _, row := range result.Rows {
		values := make([]string, len(result.Columns))
		for i, col := range result.Columns {
			if val, ok := row[col]; ok && val != nil {
				values[i] = fmt.Sprintf("%v", val)
			} else {
				values[i] = "NULL"
			}
		}
		fmt.Fprintln(w, strings.Join(values, "\t"))
	}

	w.Flush()
}

// parseTime parses a time string in various formats
func parseTime(s string) (time.Time, error) {
	// Try Unix timestamp first
	var ts int64
	if _, err := fmt.Sscanf(s, "%d", &ts); err == nil {
		return time.Unix(ts, 0), nil
	}

	// Try RFC3339
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}

	// Try common formats
	formats := []string{
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse time: %s", s)
}

// API response models

type TempoSearchResponse struct {
	Traces  []TempoTrace `json:"traces"`
	Metrics TempoMetrics `json:"metrics"`
}

type TempoTrace struct {
	TraceID           string         `json:"traceID"`
	RootServiceName   string         `json:"rootServiceName"`
	RootTraceName     string         `json:"rootTraceName"`
	StartTimeUnixNano uint64         `json:"startTimeUnixNano"`
	DurationMs        uint64         `json:"durationMs"`
	SpanSets          []TempoSpanSet `json:"spanSets"`
}

type TempoSpanSet struct {
	Matched int `json:"matched"`
	Spans   int `json:"spans"`
}

type TempoMetrics struct {
	InspectedTraces int `json:"inspectedTraces"`
	InspectedBlocks int `json:"inspectedBlocks"`
	TotalBlocks     int `json:"totalBlocks"`
}

type JaegerSearchResponse struct {
	Data   []JaegerTrace `json:"data"`
	Total  int           `json:"total"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
	Errors []any         `json:"errors"`
}

type JaegerTrace struct {
	TraceID   string                   `json:"traceID"`
	Spans     []JaegerSpan             `json:"spans"`
	Processes map[string]JaegerProcess `json:"processes"`
}

type JaegerSpan struct {
	TraceID       string            `json:"traceID"`
	SpanID        string            `json:"spanID"`
	OperationName string            `json:"operationName"`
	References    []JaegerReference `json:"references"`
	StartTime     int64             `json:"startTime"`
	Duration      int64             `json:"duration"`
	Tags          []JaegerTag       `json:"tags"`
	Logs          []any             `json:"logs"`
	ProcessID     string            `json:"processID"`
}

type JaegerReference struct {
	RefType string `json:"refType"`
	TraceID string `json:"traceID"`
	SpanID  string `json:"spanID"`
}

type JaegerTag struct {
	Key   string `json:"key"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}

type JaegerProcess struct {
	ServiceName string      `json:"serviceName"`
	Tags        []JaegerTag `json:"tags"`
}

type SQLQueryRequest struct {
	Query string `json:"query"`
}

type SQLQueryResponse struct {
	Success  bool                     `json:"success"`
	Columns  []string                 `json:"columns"`
	RowCount int                      `json:"row_count"`
	Rows     []map[string]interface{} `json:"rows"`
	Error    string                   `json:"error"`
}

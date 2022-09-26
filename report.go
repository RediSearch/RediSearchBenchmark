package main

import (
	"fmt"
	"github.com/HdrHistogram/hdrhistogram-go"
	"sync/atomic"
	"text/tabwriter"
	"time"
)

const (
	CurrentResultFormatVersion = "0.1"
)

// globals
// the total time it took to run the functions, to measure average latency, in nanoseconds
var totalTime uint64
var totalOps uint64
var totalHistogram *hdrhistogram.Histogram

func GetOverallRatesMap(took time.Duration) map[string]interface{} {
	/////////
	// Overall Rates
	/////////
	configs := map[string]interface{}{}
	overallOpsRate := calculateRateMetrics(totalOps, 0, took)
	configs["overallOpsRate"] = overallOpsRate
	return configs
}

func generateQuantileMap(hist *hdrhistogram.Histogram) (int64, map[string]float64) {
	ops := hist.TotalCount()
	q0 := 0.0
	q50 := 0.0
	q95 := 0.0
	q99 := 0.0
	q999 := 0.0
	q100 := 0.0
	if ops > 0 {
		q0 = float64(hist.ValueAtQuantile(0.0)) / 10e2
		q50 = float64(hist.ValueAtQuantile(50.0)) / 10e2
		q95 = float64(hist.ValueAtQuantile(95.0)) / 10e2
		q99 = float64(hist.ValueAtQuantile(99.0)) / 10e2
		q999 = float64(hist.ValueAtQuantile(99.90)) / 10e2
		q100 = float64(hist.ValueAtQuantile(100.0)) / 10e2
	}

	mp := map[string]float64{"q0": q0, "q50": q50, "q95": q95, "q99": q99, "q999": q999, "q100": q100}
	return ops, mp
}

func GetOverallQuantiles() map[string]interface{} {
	configs := map[string]interface{}{}
	_, all := generateQuantileMap(totalHistogram)
	configs["allCommands"] = all
	return configs
}

func calculateRateMetrics(current, prev uint64, took time.Duration) (rate float64) {
	rate = float64(current-prev) / float64(took.Seconds())
	return
}

// report handles periodic reporting of loading stats
func report(period time.Duration, start, end time.Time, w *tabwriter.Writer) {
	prevTime := start
	prevTotalOps := uint64(0)
	totalDuration := end.Sub(start)
	totalDurationMs := float64(totalDuration.Milliseconds())

	fmt.Printf("%26s %7s %25s %25s %25s\n", "Test time", " ", "Command Rate", "Client p50 with RTT(ms)", "Total Commands")
	for now := range time.NewTicker(period).C {

		took := now.Sub(prevTime)
		tookTotal := end.Sub(now)
		currentCount := atomic.LoadUint64(&totalOps)
		completionPercent := (totalDurationMs - float64(tookTotal.Milliseconds())) / totalDurationMs * 100.0
		completionPercentStr := fmt.Sprintf("[%3.1f%%]", completionPercent)

		opsRate := calculateRateMetrics((currentCount), prevTotalOps, took)
		histogramMutex.Lock()
		instantP50 := float64(totalHistogram.ValueAtQuantile(50.0)) / 10e2
		histogramMutex.Unlock()
		fmt.Printf("%25.0fs %7s %25.2f %25.3f %25d", time.Since(start).Seconds(), completionPercentStr, opsRate, instantP50, currentCount)
		fmt.Printf("\r")
		prevTotalOps = (currentCount)
		prevTime = now
	}
}

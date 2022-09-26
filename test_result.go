package main

type DataPoint struct {
	Timestamp   int64              `json:"Timestamp"`
	MultiValues map[string]float64 `json:"MultiValues"`
}

func (p DataPoint) AddValue(s string, value float64) {
	p.MultiValues[s] = value
}

func NewDataPoint(timestamp int64) *DataPoint {
	mp := map[string]float64{}
	return &DataPoint{Timestamp: timestamp, MultiValues: mp}
}

// ByTimestamp implements sort.Interface based on the Timestamp field of the DataPoint.
type ByTimestamp []DataPoint

func (a ByTimestamp) Len() int           { return len(a) }
func (a ByTimestamp) Less(i, j int) bool { return a[i].Timestamp < a[j].Timestamp }
func (a ByTimestamp) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type TestResult struct {

	// Test Configs
	Metadata            string `json:"Metadata"`
	ResultFormatVersion string `json:"ResultFormatVersion"`
	Limit               uint64 `json:"Limit"`
	Workers             uint   `json:"Workers"`
	MaxRps              int64  `json:"MaxRps"`

	// DB Spefic Configs
	DBSpecificConfigs map[string]interface{} `json:"DBSpecificConfigs"`

	StartTime      int64 `json:"StartTime"`
	EndTime        int64 `json:"EndTime"`
	DurationMillis int64 `json:"DurationMillis"`

	// Totals
	Totals map[string]interface{} `json:"Totals"`

	// Overall Rates
	OverallRates map[string]interface{} `json:"OverallRates"`

	// Overall Quantiles
	OverallQuantiles map[string]interface{} `json:"OverallQuantiles"`

	// Time-Series
	TimeSeries map[string]interface{} `json:"TimeSeries"`
}

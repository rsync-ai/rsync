package sentinel

import (
	"context"
	"math"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// AnomalyPredictor uses statistical methods to predict issues before they become critical
type AnomalyPredictor struct {
	config *SentinelConfig
	logger *AuditLogger

	// Metric tracking
	metricWindows map[string]*MetricWindow
	mu            sync.RWMutex

	// Control
	ctx    context.Context
	cancel context.CancelFunc
}

// MetricWindow tracks a time-series metric for anomaly detection
type MetricWindow struct {
	ComponentID string
	MetricName  string
	Values      []MetricPoint
	EMA         float64 // Exponential Moving Average
	Variance    float64 // Variance for standard deviation
	StdDev      float64 // Standard Deviation
	LastUpdated time.Time
}

// MetricPoint represents a single metric data point
type MetricPoint struct {
	Value     float64
	Timestamp time.Time
}

// NewAnomalyPredictor creates a new anomaly predictor
func NewAnomalyPredictor(config *SentinelConfig, logger *AuditLogger) *AnomalyPredictor {
	return &AnomalyPredictor{
		config:        config,
		logger:        logger,
		metricWindows: make(map[string]*MetricWindow),
	}
}

// Start starts the anomaly predictor
func (p *AnomalyPredictor) Start(ctx context.Context) error {
	p.ctx, p.cancel = context.WithCancel(ctx)

	// Start cleanup goroutine
	go p.cleanupOldMetrics()

	return nil
}

// Stop stops the anomaly predictor
func (p *AnomalyPredictor) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
}

// RecordMetric records a metric value for anomaly detection
func (p *AnomalyPredictor) RecordMetric(componentID, metricName string, value float64, timestamp time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()

	windowKey := p.getWindowKey(componentID, metricName)
	window, exists := p.metricWindows[windowKey]

	if !exists {
		window = &MetricWindow{
			ComponentID: componentID,
			MetricName:  metricName,
			Values:      make([]MetricPoint, 0, p.config.AnomalyWindowSize),
			EMA:         value, // Initialize EMA with first value
		}
		p.metricWindows[windowKey] = window
	}

	// Add new data point
	point := MetricPoint{
		Value:     value,
		Timestamp: timestamp,
	}
	window.Values = append(window.Values, point)

	// Maintain window size
	if len(window.Values) > p.config.AnomalyWindowSize {
		window.Values = window.Values[1:]
	}

	// Update statistics
	p.updateStatistics(window, value)
	window.LastUpdated = timestamp

	// Check for anomaly
	if p.isAnomaly(window, value) {
		p.handleAnomaly(componentID, metricName, value, window)
	}
}

// updateStatistics updates EMA and standard deviation
func (p *AnomalyPredictor) updateStatistics(window *MetricWindow, newValue float64) {
	// Calculate EMA (Exponential Moving Average)
	// EMA = α * newValue + (1 - α) * previousEMA
	// where α = 2 / (N + 1), N = window size
	alpha := 2.0 / float64(len(window.Values)+1)
	window.EMA = alpha*newValue + (1-alpha)*window.EMA

	// Calculate variance and standard deviation
	if len(window.Values) < 2 {
		window.Variance = 0
		window.StdDev = 0
		return
	}

	// Calculate mean
	var sum float64
	for _, point := range window.Values {
		sum += point.Value
	}
	mean := sum / float64(len(window.Values))

	// Calculate variance
	var varianceSum float64
	for _, point := range window.Values {
		diff := point.Value - mean
		varianceSum += diff * diff
	}
	window.Variance = varianceSum / float64(len(window.Values))
	window.StdDev = math.Sqrt(window.Variance)
}

// isAnomaly checks if a value is anomalous based on standard deviation
func (p *AnomalyPredictor) isAnomaly(window *MetricWindow, value float64) bool {
	// Need at least 10 data points for meaningful anomaly detection
	if len(window.Values) < 10 {
		return false
	}

	// Check if value is more than threshold standard deviations from EMA
	deviation := math.Abs(value - window.EMA)
	threshold := p.config.AnomalyStdDevThreshold * window.StdDev

	return deviation > threshold
}

// handleAnomaly handles a detected anomaly
func (p *AnomalyPredictor) handleAnomaly(componentID, metricName string, value float64, window *MetricWindow) {
	log.WithFields(log.Fields{
		"component_id": componentID,
		"metric":       metricName,
		"value":        value,
		"ema":          window.EMA,
		"std_dev":      window.StdDev,
		"deviation":    math.Abs(value - window.EMA),
	}).Warn("📈 Anomaly detected in metric")

	// TODO: Create an issue for the anomaly
	// This would integrate back to the issue detector
}

// cleanupOldMetrics removes old metric windows that haven't been updated
func (p *AnomalyPredictor) cleanupOldMetrics() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.mu.Lock()
			now := time.Now()
			for key, window := range p.metricWindows {
				if now.Sub(window.LastUpdated) > 10*time.Minute {
					delete(p.metricWindows, key)
				}
			}
			p.mu.Unlock()
		}
	}
}

// getWindowKey generates a unique key for a metric window
func (p *AnomalyPredictor) getWindowKey(componentID, metricName string) string {
	return componentID + ":" + metricName
}

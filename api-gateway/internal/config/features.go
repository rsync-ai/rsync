package config

import (
	"os"
	"strconv"
	"sync"

	log "github.com/sirupsen/logrus"
)

// FeatureFlags holds all feature flags for the API Gateway
type FeatureFlags struct {
	// Monitoring feature flags
	MonitoringOverview  bool
	MonitoringInfra     bool
	MonitoringTraces    bool
	SigNozDeepLinks     bool
	
	// SigNoz configuration
	SigNozUIURL         string
	SigNozAPIURL        string
}

var (
	features     *FeatureFlags
	featuresOnce sync.Once
)

// LoadFeatures loads feature flags from environment variables
// This should be called once during application startup
func LoadFeatures() *FeatureFlags {
	featuresOnce.Do(func() {
		features = &FeatureFlags{
			// Monitoring Overview - defaults to OFF for safety
			MonitoringOverview: getBoolEnv("FEATURE_MONITORING_OVERVIEW", false),
			
			// Monitoring Infrastructure tab - defaults to OFF for safety
			MonitoringInfra: getBoolEnv("FEATURE_MONITORING_INFRA", false),
			
			// Monitoring Traces tab - defaults to OFF in production, ON in dev
			MonitoringTraces: getBoolEnv("FEATURE_MONITORING_TRACES", isDevelopment()),
			
			// SigNoz deep links - defaults to ON
			SigNozDeepLinks: getBoolEnv("FEATURE_SIGNOZ_DEEPLINKS", true),
			
			// SigNoz URLs
			SigNozUIURL:  getEnv("SIGNOZ_UI_URL", "http://localhost:3301"),
			SigNozAPIURL: getEnv("SIGNOZ_API_URL", "http://localhost:3301/api"),
		}

		log.WithFields(log.Fields{
			"monitoring_overview":  features.MonitoringOverview,
			"monitoring_infra":     features.MonitoringInfra,
			"monitoring_traces":    features.MonitoringTraces,
			"signoz_deeplinks":     features.SigNozDeepLinks,
			"signoz_ui_url":        features.SigNozUIURL,
		}).Info("Feature flags loaded")
	})

	return features
}

// GetFeatures returns the loaded feature flags
// Returns nil if features haven't been loaded yet
func GetFeatures() *FeatureFlags {
	return features
}

// Helper functions

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getBoolEnv(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		boolVal, err := strconv.ParseBool(value)
		if err != nil {
			log.Warnf("Invalid boolean value for %s: %s, using default: %v", key, value, defaultValue)
			return defaultValue
		}
		return boolVal
	}
	return defaultValue
}

func isDevelopment() bool {
	env := os.Getenv("ENV")
	if env == "" {
		env = os.Getenv("ENVIRONMENT")
	}
	return env == "development" || env == "dev" || env == ""
}

// GetSigNozTraceURL generates a SigNoz trace URL from a trace ID
func (f *FeatureFlags) GetSigNozTraceURL(traceID string) string {
	if traceID == "" || !f.SigNozDeepLinks {
		return ""
	}
	return f.SigNozUIURL + "/trace/" + traceID
}

// GetSigNozMetricsURL generates a SigNoz metrics dashboard URL
func (f *FeatureFlags) GetSigNozMetricsURL(path string) string {
	if !f.SigNozDeepLinks {
		return ""
	}
	if path == "" {
		path = "/dashboard"
	}
	return f.SigNozUIURL + path
}

// ToAPIResponse returns feature flags in a format safe for API responses
// Only returns flags that control UI behavior, not sensitive configuration
func (f *FeatureFlags) ToAPIResponse() map[string]interface{} {
	return map[string]interface{}{
		"monitoring_overview": f.MonitoringOverview,
		"monitoring_infra":    f.MonitoringInfra,
		"monitoring_traces":   f.MonitoringTraces,
		"signoz_deeplinks":    f.SigNozDeepLinks,
		"signoz_available":    f.SigNozUIURL != "",
	}
}


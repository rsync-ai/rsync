package config

import (
	"os"
	"strconv"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
)

// FeatureFlags holds all feature flags for the API Gateway
type FeatureFlags struct {
	// Monitoring feature flags
	MonitoringOverview  bool
	MonitoringInfra     bool
	MonitoringTraces    bool

	// Billing surfaces. UsagePanel gates the plan/quota UI -- the workspace
	// and admin Usage pages plus their nav entries. See LoadFeatures for why
	// its default is derived from BillingEnforced rather than hard-coded.
	UsagePanel bool
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

			// Usage panel - a BILLING surface: it renders the plan, the pipeline
			// and query limits, trial expiry and metered transfer GB. Defaults to
			// the CLOUD behaviour (shown) and then follows plan enforcement, so the
			// self-host artifacts that already opt out of billing
			// (docker-compose.quickstart.yml, the chart's apiGateway.billingEnforced)
			// hide it without a second flag to keep in sync. That default is the
			// right one because with enforcement off resolvePlanQuota short-circuits
			// to unlimitedQuota, so every number the panel shows is fiction.
			// FEATURE_USAGE_PANEL overrides in either direction.
			UsagePanel: resolveUsagePanel(),
		}

		log.WithFields(log.Fields{
			"monitoring_overview":  features.MonitoringOverview,
			"monitoring_infra":     features.MonitoringInfra,
			"monitoring_traces":    features.MonitoringTraces,
			"usage_panel":          features.UsagePanel,
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

// resolveUsagePanel resolves the UsagePanel flag from the environment. Split out
// of LoadFeatures because LoadFeatures is a sync.Once: a test can only ever
// observe one environment through it, and the interesting behaviour here is the
// derived default, which needs several.
func resolveUsagePanel() bool {
	return getBoolEnv("FEATURE_USAGE_PANEL", BillingEnforced())
}

// BillingEnforced reports whether plan/quota entitlements should be enforced.
//
// It defaults to ENFORCED so cloud (app.rsync.ai) - which never sets the var -
// keeps its trial/plan gating unchanged. Self-host / OSS installs have no Stripe
// and no cloud plans, so a fresh install must NOT be trial-gated: the OSS compose
// (docker-compose.quickstart.yml) and the Helm chart both set
// RSYNC_BILLING_ENFORCED=false to disable it.
//
// Parsing fails toward enforcement: an unset or unparseable value => true, so only
// an explicit false ("false"/"0"/"f") ever turns billing off. This is a billing
// feature-flag, not a product-edition gate - there is deliberately no runtime
// edition switch elsewhere (the OSS/cloud split is enforced by which artifact ships).
//
// It lives here, beside the other flags, because two callers need it: the quota
// resolver (handlers.billingEnforced delegates to this) and the UsagePanel default
// above. handlers imports config, so config must never import handlers. It reads
// the environment on every call rather than caching, because the quota resolver's
// tests set the variable per case.
func BillingEnforced() bool {
	v := strings.TrimSpace(os.Getenv("RSYNC_BILLING_ENFORCED"))
	if v == "" {
		return true
	}
	enforced, err := strconv.ParseBool(v)
	if err != nil {
		return true
	}
	return enforced
}

// ToAPIResponse returns feature flags in a format safe for API responses
// Only returns flags that control UI behavior, not sensitive configuration
func (f *FeatureFlags) ToAPIResponse() map[string]interface{} {
	return map[string]interface{}{
		"monitoring_overview": f.MonitoringOverview,
		"monitoring_infra":    f.MonitoringInfra,
		"monitoring_traces":   f.MonitoringTraces,
		"usage_panel":         f.UsagePanel,
	}
}


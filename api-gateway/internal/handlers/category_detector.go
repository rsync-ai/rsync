package handlers

import (
	_ "embed"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// CategoryDetectorRequest represents a request to detect API category
type CategoryDetectorRequest struct {
	APIName string `json:"api_name" binding:"required"`
}

// CategoryDetectorResponse represents the response from category detection
type CategoryDetectorResponse struct {
	CategoryHint       string  `json:"category_hint"`
	Confidence         float64 `json:"confidence"`
	Source             string  `json:"source"` // "oauth_registry", "known_apis", "partial_match", "keyword", "default"
	SuggestedName      string  `json:"suggested_name,omitempty"`
	AvailableCategories []string `json:"available_categories"`
}

// KnownAPIsConfig represents the structure of known_apis.yaml
type KnownAPIsConfig map[string][]string

var knownAPIsConfig KnownAPIsConfig
var availableCategories = []string{
	"crm",
	"ecommerce",
	"payments",
	"marketing",
	"support",
	"devtools",
	"analytics",
	"storage",
	"project_management",
	"communication",
	"accounting",
	"hr",
	"api_saas", // Default/generic
}

//go:embed known_apis.yaml
var knownAPIsYAML []byte

func init() {
	// Load embedded known_apis.yaml (no runtime FS dependency)
	if err := yaml.Unmarshal(knownAPIsYAML, &knownAPIsConfig); err != nil {
		log.Errorf("Failed to parse known_apis.yaml: %v", err)
		knownAPIsConfig = make(KnownAPIsConfig)
		return
	}

	log.Infof("Loaded known_apis.yaml with %d categories", len(knownAPIsConfig))
}

// DetectCategory auto-detects the API category hint
func DetectCategory(c *gin.Context) {
	var req CategoryDetectorRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	apiName := strings.ToLower(strings.TrimSpace(req.APIName))
	apiName = strings.ReplaceAll(apiName, " ", "-")
	apiName = strings.ReplaceAll(apiName, "_", "-")

	log.Infof("🔍 Detecting category for API: %s", apiName)

	// Priority 1: OAuth registry match (95% confidence)
	if category, found := checkOAuthRegistry(apiName); found {
		log.Infof("✅ OAuth registry match: %s → %s (95%%)", apiName, category)
		c.JSON(http.StatusOK, CategoryDetectorResponse{
			CategoryHint:        category,
			Confidence:          0.95,
			Source:              "oauth_registry",
			AvailableCategories: availableCategories,
		})
		return
	}

	// Priority 2: Known APIs list (85% confidence)
	if category, found := checkKnownAPIs(apiName); found {
		log.Infof("✅ Known APIs match: %s → %s (85%%)", apiName, category)
		c.JSON(http.StatusOK, CategoryDetectorResponse{
			CategoryHint:        category,
			Confidence:          0.85,
			Source:              "known_apis",
			AvailableCategories: availableCategories,
		})
		return
	}

	// Priority 3: Partial name match (65% confidence)
	if category, matchedName, found := checkPartialMatch(apiName); found {
		log.Infof("✅ Partial match: %s → %s (matched: %s, 65%%)", apiName, category, matchedName)
		c.JSON(http.StatusOK, CategoryDetectorResponse{
			CategoryHint:        category,
			Confidence:          0.65,
			Source:              "partial_match",
			SuggestedName:       matchedName,
			AvailableCategories: availableCategories,
		})
		return
	}

	// Priority 4: Keyword heuristic (45% confidence)
	if category, found := checkKeywords(apiName); found {
		log.Infof("✅ Keyword match: %s → %s (45%%)", apiName, category)
		c.JSON(http.StatusOK, CategoryDetectorResponse{
			CategoryHint:        category,
			Confidence:          0.45,
			Source:              "keyword",
			AvailableCategories: availableCategories,
		})
		return
	}

	// Priority 5: No match.
	// IMPORTANT: Do not default to "api_saas" here. This endpoint is used to *suggest* an optional
	// api_category_hint (crm/payments/etc). For non-API names like "mysql", returning "api_saas"
	// causes the UI to look "auto-selected" even though we have no signal.
	//
	// Callers can still choose "General API/SaaS" manually from available_categories.
	log.Infof("⚠️  No match found for %s, returning empty category_hint (30%%)", apiName)
	c.JSON(http.StatusOK, CategoryDetectorResponse{
		CategoryHint:        "",
		Confidence:          0.30,
		Source:              "default",
		AvailableCategories: availableCategories,
	})
}

// oauthCategoryCache is populated lazily from providers.json; fallback is the built-in map.
var (
	oauthCategoryOnce  sync.Once
	oauthCategoryCache map[string]string
)

// builtinOAuthCategories is the fallback used when providers.json is unavailable.
var builtinOAuthCategories = map[string]string{
	"pipedrive":  "crm",
	"hubspot":    "crm",
	"salesforce": "crm",
	"zoho-crm":   "crm",
	"shopify":    "ecommerce",
	"stripe":     "payments",
	"github":     "devtools",
	"gitlab":     "devtools",
	"slack":      "communication",
	"google":     "analytics",
	"notion":     "productivity",
	"jira":       "project_management",
	"zendesk":    "support",
	"freshdesk":  "support",
	"intercom":   "support",
	"dropbox":    "storage",
	"mailchimp":  "marketing",
}

// loadOAuthCategoryMap reads provider → category from providers.json and merges
// it with the built-in fallback map. Called exactly once (sync.Once).
func loadOAuthCategoryMap() map[string]string {
	merged := make(map[string]string, len(builtinOAuthCategories))
	for k, v := range builtinOAuthCategories {
		merged[k] = v
	}

	registryPath := GetMCPConnectorsPath() + "/oauth/providers.json"
	data, err := os.ReadFile(registryPath)
	if err != nil {
		log.Debugf("oauth/providers.json not found at %s, using built-in category map", registryPath)
		return merged
	}

	var registry map[string]struct {
		Category string `json:"category"`
	}
	if err := json.Unmarshal(data, &registry); err != nil {
		log.Warnf("Failed to parse %s: %v — using built-in category map", registryPath, err)
		return merged
	}

	for providerID, entry := range registry {
		if entry.Category != "" {
			merged[providerID] = entry.Category
		}
	}

	log.Infof("Loaded %d OAuth provider categories from %s", len(registry), registryPath)
	return merged
}

// checkOAuthRegistry checks if the API name matches a known OAuth provider and returns its category.
func checkOAuthRegistry(apiName string) (string, bool) {
	oauthCategoryOnce.Do(func() {
		oauthCategoryCache = loadOAuthCategoryMap()
	})

	if category, found := oauthCategoryCache[apiName]; found {
		return category, true
	}
	return "", false
}

// checkKnownAPIs checks if the API is in the known_apis.yaml config
func checkKnownAPIs(apiName string) (string, bool) {
	for category, apis := range knownAPIsConfig {
		for _, knownAPI := range apis {
			if strings.EqualFold(knownAPI, apiName) {
				return category, true
			}
		}
	}
	return "", false
}

// checkPartialMatch checks for fuzzy/partial matches
func checkPartialMatch(apiName string) (string, string, bool) {
	// Try removing common suffixes/prefixes
	variations := []string{
		apiName,
		strings.TrimSuffix(apiName, "-api"),
		strings.TrimSuffix(apiName, "api"),
		strings.TrimPrefix(apiName, "api-"),
		strings.TrimSuffix(apiName, "-crm"),
	}

	for _, variant := range variations {
		if variant == apiName {
			continue // Skip if same as original
		}

		// Check in known APIs
		if category, found := checkKnownAPIs(variant); found {
			return category, variant, true
		}
	}

	// Try substring matching (e.g., "zoho-crm" contains "zoho")
	for category, apis := range knownAPIsConfig {
		for _, knownAPI := range apis {
			if strings.Contains(apiName, knownAPI) || strings.Contains(knownAPI, apiName) {
				return category, knownAPI, true
			}
		}
	}

	return "", "", false
}

// checkKeywords checks for category keywords in the API name
func checkKeywords(apiName string) (string, bool) {
	keywords := map[string][]string{
		"crm": {"crm", "sales", "lead", "deal", "contact"},
		"ecommerce": {"shop", "store", "commerce", "cart", "product"},
		"payments": {"pay", "stripe", "transaction", "invoice", "billing"},
		"marketing": {"mail", "campaign", "newsletter", "email", "sms"},
		"support": {"support", "ticket", "help", "desk", "chat"},
		"devtools": {"git", "repo", "code", "ci", "deploy"},
		"analytics": {"analytics", "track", "metric", "event"},
		"storage": {"storage", "file", "drive", "bucket", "cloud"},
		"project_management": {"project", "task", "board", "issue"},
		"communication": {"slack", "chat", "message", "call"},
	}

	for category, kws := range keywords {
		for _, kw := range kws {
			if strings.Contains(apiName, kw) {
				return category, true
			}
		}
	}

	return "", false
}


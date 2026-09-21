package main

import (
	"strings"

	log "github.com/sirupsen/logrus"
)

// startupCheckLogPrefix starts every startup-check line. docs/deployment/env-vars.md
// tells operators to grep for it, so it is part of the contract.
const startupCheckLogPrefix = "Startup check: "

// startupSettingProblems returns one message per setting the gateway needs but was
// not given, each naming the setting, exactly what will not work without it and
// what to set. Messages never contain a value. getenv is os.Getenv in main and a
// map in tests.
//
// These are ERROR lines, not a refusal to start: the start-path census for issue
// #24 found the secret empty or absent on the dev compose, the CI gates, a bare
// quickstart compose and Helm (optional secretKeyRef), so a fatal here would take
// down stacks that serve everything except the internal endpoints.
func startupSettingProblems(getenv func(string) string) []string {
	var problems []string
	if strings.TrimSpace(getenv("INTERNAL_SERVICE_SECRET")) == "" {
		problems = append(problems, "INTERNAL_SERVICE_SECRET is not set, empty or only spaces. "+
			"Every internal endpoint (/api/v1/internal/...) will refuse calls (503 when it is "+
			"empty, 401 when it is only spaces), so scheduled saved-query runs, the model "+
			"freshness sweep, pipeline re-runs started by the self-healer, pipeline namespace "+
			"locking and OAuth token refresh requested by the orchestrator will not work. "+
			"Generate one (for example openssl rand -hex 32) and give the same value to "+
			"api-gateway, orchestrator, temporal-adapter and frontend.")
	}
	return problems
}

// reportStartupSettingProblems runs the startup check against getenv and writes each
// problem to logger as one ERROR line starting with startupCheckLogPrefix. main passes
// log.StandardLogger() and os.Getenv; tests pass a hooked logger and a map.
func reportStartupSettingProblems(logger *log.Logger, getenv func(string) string) {
	for _, p := range startupSettingProblems(getenv) {
		logger.Error(startupCheckLogPrefix + p)
	}
}

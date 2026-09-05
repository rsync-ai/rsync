package retention

import (
	"context"
	"fmt"
	"time"
)

// SafetyChecker performs safety checks before cleanup operations
type SafetyChecker struct {
	config        *Config
	offsetTracker *OffsetTracker
}

// NewSafetyChecker creates a new safety checker
func NewSafetyChecker(config *Config, offsetTracker *OffsetTracker) *SafetyChecker {
	return &SafetyChecker{
		config:        config,
		offsetTracker: offsetTracker,
	}
}

// CanCleanupSafely determines if it's safe to clean up a bulk load
func (s *SafetyChecker) CanCleanupSafely(
	ctx context.Context,
	bulkLoad *BulkLoadInfo,
) (*SafetyCheckResult, error) {
	result := &SafetyCheckResult{
		Safe:            true,
		ChecksPerformed: make([]string, 0),
		FailedChecks:    make([]string, 0),
		Warnings:        make([]string, 0),
	}

	// Check 1: Minimum wait time after load
	if err := s.checkMinWaitTime(bulkLoad, result); err != nil {
		return result, err
	}

	// Check 2: All consumers caught up
	if err := s.checkAllConsumersCaughtUp(ctx, bulkLoad, result); err != nil {
		return result, err
	}

	// Check 3: No consumers have high lag
	if err := s.checkNoHighLag(ctx, bulkLoad, result); err != nil {
		return result, err
	}

	// Check 4: Grace period elapsed since all caught up
	if err := s.checkGracePeriod(bulkLoad, result); err != nil {
		return result, err
	}

	// Check 5: Minimum consumers caught up
	if err := s.checkMinConsumersCaughtUp(bulkLoad, result); err != nil {
		return result, err
	}

	// Determine final result
	if len(result.FailedChecks) > 0 {
		result.Safe = false
		result.Reason = fmt.Sprintf("Failed checks: %v", result.FailedChecks)
	} else {
		result.Reason = "All safety checks passed"
	}

	return result, nil
}

// checkMinWaitTime checks if minimum wait time has elapsed
func (s *SafetyChecker) checkMinWaitTime(bulkLoad *BulkLoadInfo, result *SafetyCheckResult) error {
	checkName := "min_wait_time"
	result.ChecksPerformed = append(result.ChecksPerformed, checkName)

	minWait := time.Duration(s.config.Safety.MinWaitAfterLoadMins) * time.Minute
	elapsed := time.Since(bulkLoad.RegisteredAt)

	if elapsed < minWait {
		remaining := minWait - elapsed
		result.FailedChecks = append(result.FailedChecks, checkName)
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("Minimum wait time not elapsed (%.0f mins remaining)", remaining.Minutes()))
		return nil
	}

	return nil
}

// checkAllConsumersCaughtUp checks if all expected consumers have caught up
func (s *SafetyChecker) checkAllConsumersCaughtUp(
	ctx context.Context,
	bulkLoad *BulkLoadInfo,
	result *SafetyCheckResult,
) error {
	if !s.config.Safety.RequireAllConsumersCaughtUp {
		return nil
	}

	checkName := "all_consumers_caught_up"
	result.ChecksPerformed = append(result.ChecksPerformed, checkName)

	allCaughtUp, progressMap := s.offsetTracker.CheckAllConsumersCaughtUp(
		ctx,
		bulkLoad.Topic,
		bulkLoad.EndOffset,
		bulkLoad.ExpectedConsumers,
	)

	if !allCaughtUp {
		result.FailedChecks = append(result.FailedChecks, checkName)

		// Build warning message
		for consumer, progress := range progressMap {
			if !progress.CaughtUp {
				result.Warnings = append(result.Warnings,
					fmt.Sprintf("Consumer %s still processing (%.1f%% complete, lag: %d)",
						consumer, progress.Progress, progress.Lag))
			}
		}
		return nil
	}

	return nil
}

// checkNoHighLag checks that no consumers have high lag
func (s *SafetyChecker) checkNoHighLag(
	ctx context.Context,
	bulkLoad *BulkLoadInfo,
	result *SafetyCheckResult,
) error {
	checkName := "no_high_lag"
	result.ChecksPerformed = append(result.ChecksPerformed, checkName)

	_, progressMap := s.offsetTracker.CheckAllConsumersCaughtUp(
		ctx,
		bulkLoad.Topic,
		bulkLoad.EndOffset,
		bulkLoad.ExpectedConsumers,
	)

	for consumer, progress := range progressMap {
		if progress.Lag > s.config.Safety.MaxLagBeforeCleanup {
			result.FailedChecks = append(result.FailedChecks, checkName)
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("Consumer %s has high lag: %d (max: %d)",
					consumer, progress.Lag, s.config.Safety.MaxLagBeforeCleanup))
			return nil
		}
	}

	return nil
}

// checkGracePeriod checks if grace period has elapsed since all caught up
func (s *SafetyChecker) checkGracePeriod(bulkLoad *BulkLoadInfo, result *SafetyCheckResult) error {
	checkName := "grace_period"
	result.ChecksPerformed = append(result.ChecksPerformed, checkName)

	// Skip if not all caught up yet
	if bulkLoad.AllCaughtUpAt.IsZero() {
		result.FailedChecks = append(result.FailedChecks, checkName)
		result.Warnings = append(result.Warnings, "Consumers have not all caught up yet")
		return nil
	}

	gracePeriod := time.Duration(s.config.Safety.GracePeriodMins) * time.Minute
	elapsed := time.Since(bulkLoad.AllCaughtUpAt)

	if elapsed < gracePeriod {
		remaining := gracePeriod - elapsed
		result.FailedChecks = append(result.FailedChecks, checkName)
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("Grace period for late-joining consumers (%.0f mins remaining)", remaining.Minutes()))
		return nil
	}

	return nil
}

// checkMinConsumersCaughtUp checks if minimum number of consumers are caught up
func (s *SafetyChecker) checkMinConsumersCaughtUp(bulkLoad *BulkLoadInfo, result *SafetyCheckResult) error {
	checkName := "min_consumers_caught_up"
	result.ChecksPerformed = append(result.ChecksPerformed, checkName)

	caughtUpCount := len(bulkLoad.CaughtUpConsumers)
	minRequired := s.config.Safety.MinConsumersCaughtUp

	if caughtUpCount < minRequired {
		result.FailedChecks = append(result.FailedChecks, checkName)
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("Only %d consumers caught up (minimum: %d)", caughtUpCount, minRequired))
		return nil
	}

	return nil
}

// PerformQuickCheck performs a quick safety check (fewer checks)
func (s *SafetyChecker) PerformQuickCheck(ctx context.Context, bulkLoad *BulkLoadInfo) (bool, string) {
	// Check 1: Is bulk load in correct status?
	if bulkLoad.Status != StatusMonitoring && bulkLoad.Status != StatusCompleted {
		return false, fmt.Sprintf("Bulk load status is %s, expected monitoring or completed", bulkLoad.Status)
	}

	// Check 2: Is all caught up time set?
	if bulkLoad.AllCaughtUpAt.IsZero() {
		return false, "All consumers have not caught up yet"
	}

	// Check 3: Grace period
	gracePeriod := time.Duration(s.config.Safety.GracePeriodMins) * time.Minute
	if time.Since(bulkLoad.AllCaughtUpAt) < gracePeriod {
		return false, "Grace period has not elapsed"
	}

	return true, "Quick check passed"
}

// ValidateCleanupAction validates a cleanup action before execution
func (s *SafetyChecker) ValidateCleanupAction(action CleanupAction, bulkLoad *BulkLoadInfo) error {
	switch action {
	case ActionReduceRetention:
		// Always allowed if safety checks pass
		return nil

	case ActionDeleteRecords:
		// Only allowed for completed bulk loads
		if bulkLoad.Status != StatusCompleted && bulkLoad.Status != StatusMonitoring {
			return fmt.Errorf("cannot delete records: bulk load status is %s", bulkLoad.Status)
		}
		return nil

	case ActionArchiveToS3:
		// Requires archive to be enabled
		if !s.config.Archive.Enabled {
			return fmt.Errorf("S3 archival is not enabled")
		}
		return nil

	case ActionCompact:
		// Only for compacted topics
		return nil

	default:
		return fmt.Errorf("unknown cleanup action: %s", action)
	}
}

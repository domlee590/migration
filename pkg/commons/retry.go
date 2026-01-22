package commons

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/pterm/pterm"
)

// RetryConfig holds configuration for retry behavior
type RetryConfig struct {
	MaxAttempts     int
	InitialDelay    time.Duration
	MaxDelay        time.Duration
	BackoffFactor   float64
	JitterFraction  float64
	OperationName   string
}

// DefaultRetryConfig returns a retry config with sensible defaults
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:    10,
		InitialDelay:   1 * time.Second,
		MaxDelay:       30 * time.Second,
		BackoffFactor:  2.0,
		JitterFraction: 0.1,
		OperationName:  "operation",
	}
}

// RetryWithExponentialBackoff retries an operation with exponential backoff and jitter
func RetryWithExponentialBackoff(config RetryConfig, operation func() error) error {
	var lastErr error
	
	for attempt := 1; attempt <= config.MaxAttempts; attempt++ {
		err := operation()
		if err == nil {
			// Success!
			if attempt > 1 {
				pterm.Success.Printfln("✓ %s succeeded after %d attempts", config.OperationName, attempt)
			}
			return nil
		}
		
		lastErr = err
		
		// If this was the last attempt, don't wait
		if attempt == config.MaxAttempts {
			pterm.Error.Printfln("✗ %s failed after %d attempts. Final error: %v", config.OperationName, config.MaxAttempts, err)
			break
		}
		
		// Calculate backoff with exponential increase and jitter
		delay := calculateBackoff(attempt, config)
		
		pterm.Warning.Printfln("⚠ %s attempt %d/%d failed: %v. Retrying in %v...", 
			config.OperationName, attempt, config.MaxAttempts, err, delay)
		
		time.Sleep(delay)
	}
	
	return fmt.Errorf("operation failed after %d attempts: %w", config.MaxAttempts, lastErr)
}

// calculateBackoff calculates the delay for a given attempt with exponential backoff and jitter
func calculateBackoff(attempt int, config RetryConfig) time.Duration {
	// Calculate exponential backoff
	backoff := float64(config.InitialDelay) * math.Pow(config.BackoffFactor, float64(attempt-1))
	
	// Cap at max delay
	if backoff > float64(config.MaxDelay) {
		backoff = float64(config.MaxDelay)
	}
	
	// Add jitter (random variation) to prevent thundering herd
	jitter := backoff * config.JitterFraction * (rand.Float64()*2 - 1) // Random value between -jitter and +jitter
	backoff += jitter
	
	// Ensure we don't go below initial delay
	if backoff < float64(config.InitialDelay) {
		backoff = float64(config.InitialDelay)
	}
	
	return time.Duration(backoff)
}

// RetryWeaviateOperation is a convenience wrapper for Weaviate operations with default retry config
func RetryWeaviateOperation(operationName string, operation func() error) error {
	config := DefaultRetryConfig()
	config.OperationName = operationName
	return RetryWithExponentialBackoff(config, operation)
}

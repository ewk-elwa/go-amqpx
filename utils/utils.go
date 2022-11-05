package utils

import (
	"os"
	"time"
)

// GetEnv returns env value or fallback value
func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// GetMillisecondsInt64 returns the duration as a Int64 millisecond
func GetMillisecondsInt64(duration time.Duration) int64 {
	return duration.Nanoseconds() / 1e6
}

// GetMillisecondsFloat64 returns the duration as a Int64 millisecond
func GetMillisecondsFloat64(duration time.Duration) float64 {
	return duration.Seconds() / 1e3
}

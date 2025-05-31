package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/surrealdb/surrealdb.go/pkg/models"
)

// SurrealValue represents a value that can be either a regular Go value or a SurrealDB-specific type
type SurrealValue struct {
	Value interface{}
}

// UnmarshalYAML implements the yaml.Unmarshaler interface
func (sv *SurrealValue) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var raw interface{}
	if err := unmarshal(&raw); err != nil {
		return err
	}

	// Handle map[string]interface{} which represents special SurrealDB types
	if m, ok := raw.(map[string]interface{}); ok {
		// Check for RecordID type
		if table, ok := m["table"].(string); ok {
			switch id := m["id"].(type) {
			case string:
				sv.Value = models.RecordID{
					Table: table,
					ID:    id,
				}
				return nil
			case []any:
				sv.Value = models.RecordID{
					Table: table,
					ID:    id,
				}
				return nil
			default:
				return fmt.Errorf("invalid id type: %T", id)
			}
		}

		if intlike, ok := m["uint64"]; ok {
			switch v := intlike.(type) {
			case int:
				casted := (uint64)(v)
				sv.Value = casted
				return nil
			default:
				return fmt.Errorf("invalid intlike type: %T", intlike)
			}
		}

		// "Uses BigDecimal for storing any real number with arbitrary precision."
		// Check for DecimalString type
		if decimal, ok := m["decimal"].(string); ok {
			sv.Value = models.DecimalString(decimal)
			return nil
		}

		// "An ISO 8601 compliant data type that stores a date with time and time zone."
		if tm, ok := m["datetime"].(string); ok {
			t, err := time.Parse(time.RFC3339, tm)
			if err != nil {
				return fmt.Errorf("invalid time format: %s", tm)
			}
			// Use UTC time
			t = t.UTC()
			sv.Value = models.CustomDateTime{Time: t}
			return nil
		}

		// "Store formatted objects containing values of any supported type with no limit to object depth or nesting."
		if obj, ok := m["object"].(map[string]interface{}); ok {
			sv.Value = obj
			return nil
		}

		// Check for recent-enough time matcher
		if _, ok := m["recent-enough"]; ok {
			var ret RecentEnoughTime
			if err := unmarshal(&ret); err != nil {
				return err
			}
			sv.Value = ret
			return nil
		}

		// Add more special type handling here as needed
	}

	// For all other cases, use the raw value as is
	sv.Value = raw
	return nil
}

// ExpectedDBState represents the expected state of the database
type ExpectedDBState struct {
	Tables map[string][]map[string]SurrealValue `yaml:"tables"`
}

// ConvertToMap converts the SurrealValue map to a regular map[string]interface{}
func ConvertToMap(svMap map[string]SurrealValue) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range svMap {
		result[k] = v.Value
	}
	return result
}

// ParseRecordID parses a string in the format "table:id" into a RecordID
func ParseRecordID(s string) (models.RecordID, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return models.RecordID{}, fmt.Errorf("invalid record ID format: %s", s)
	}
	return models.RecordID{
		Table: parts[0],
		ID:    parts[1],
	}, nil
}

// RecentEnoughTime represents a time that should be within a certain duration from now
type RecentEnoughTime struct {
	Within time.Duration
}

// UnmarshalYAML implements the yaml.Unmarshaler interface for RecentEnoughTime
func (ret *RecentEnoughTime) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var raw interface{}
	if err := unmarshal(&raw); err != nil {
		return err
	}

	var recentEnough map[string]interface{}

	if m, ok := raw.(map[string]interface{}); ok {
		if r, ok := m["recent-enough"]; ok {
			recentEnough = r.(map[string]interface{})
		}
	}

	if recentEnough == nil {
		return fmt.Errorf("invalid recent-enough duration format: missing recent-enough key: %T: %v", raw, raw)
	}

	if within, ok := recentEnough["within"].(string); ok {
		duration, err := time.ParseDuration(within)
		if err != nil {
			return fmt.Errorf("invalid duration format: %s", within)
		}
		ret.Within = duration
		return nil
	}

	return fmt.Errorf("invalid recent-enough duration format: missing within key: %T: %v", raw, raw)
}

// IsRecentEnough checks if the given time is within the specified duration from now
func (ret *RecentEnoughTime) IsRecentEnough(t time.Time) bool {
	return time.Since(t) <= ret.Within
}

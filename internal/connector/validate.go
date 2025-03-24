package connector

import (
	"fmt"
	"unicode"
)

// The only allowed characters are alphanumeric and underscores.
func validateColumnName(name string) error {
	if name == "" {
		return fmt.Errorf("column name is required")
	}
	for _, c := range name {
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != '_' {
			return fmt.Errorf("column name contains invalid characters")
		}
	}
	return nil
}

// The only allowed characters are alphanumeric and underscores.
func validateTableName(name string) error {
	if name == "" {
		return fmt.Errorf("table name is required")
	}
	for _, c := range name {
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != '_' {
			return fmt.Errorf("table name contains invalid characters")
		}
	}
	return nil
}

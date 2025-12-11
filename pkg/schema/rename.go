package schema

import (
	"maps"
	"sort"
)

// RenamePair represents a rename operation from OldName to NewName
type RenamePair struct {
	OldName string
	NewName string
}

// DetectRenames identifies potential rename operations between current and target states.
//
// The algorithm finds objects that:
// 1. Exist in current but not in target (by name)
// 2. Have a matching object in target (by properties, excluding name)
// 3. That target object doesn't exist in current (by name)
//
// When all three conditions are met, it's detected as a rename operation.
//
// Returns:
//   - renames: Slice of RenamePair indicating detected rename operations
//   - remainingCurrent: Map of current objects that weren't renamed (for drop detection)
//   - remainingTarget: Map of target objects that weren't renamed (for create detection)
//
// Type parameter T must implement SchemaObject interface.
func DetectRenames[T SchemaObject](current, target map[string]T) (
	renames []RenamePair,
	remainingCurrent map[string]T,
	remainingTarget map[string]T,
) {
	remainingCurrent = make(map[string]T)
	remainingTarget = make(map[string]T)

	// Copy all objects to remaining maps initially
	maps.Copy(remainingCurrent, current)
	maps.Copy(remainingTarget, target)

	// Track which objects have been matched as renames
	matchedCurrent := make(map[string]bool)
	matchedTarget := make(map[string]bool)

	// Sort current names for deterministic ordering
	currentNames := make([]string, 0, len(current))
	for name := range current {
		currentNames = append(currentNames, name)
	}
	sort.Strings(currentNames)

	// Sort target names for deterministic ordering (done once, not per iteration)
	targetNames := make([]string, 0, len(target))
	for name := range target {
		targetNames = append(targetNames, name)
	}
	sort.Strings(targetNames)

	// Look for potential renames: objects that don't exist by name but have identical properties
	for _, currentName := range currentNames {
		if matchedCurrent[currentName] {
			continue
		}
		currentObj := current[currentName]

		if _, exists := target[currentName]; exists {
			continue // Object exists in both, not a rename
		}

		// Look for an object in target with identical properties but different name
		for _, targetName := range targetNames {
			if matchedTarget[targetName] {
				continue
			}
			targetObj := target[targetName]

			if _, exists := current[targetName]; exists {
				continue // Target object exists in current, not a rename target
			}

			// Check if properties match (everything except name)
			if currentObj.PropertiesMatch(targetObj) {
				// This is a rename operation
				renames = append(renames, RenamePair{
					OldName: currentName,
					NewName: targetName,
				})

				// Mark as matched
				matchedCurrent[currentName] = true
				matchedTarget[targetName] = true

				// Remove from remaining maps so they're not treated as drop+create
				delete(remainingCurrent, currentName)
				delete(remainingTarget, targetName)
				break // Found the rename target, move to next current object
			}
		}
	}

	return renames, remainingCurrent, remainingTarget
}

// SortedKeys returns sorted keys from a SchemaObject map.
// This provides deterministic iteration order for comparison operations.
func SortedKeys[T SchemaObject](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// FilterExcluding returns sorted keys from 'from' that don't exist in 'exclude'.
// Useful for finding objects to drop (exist in current but not in target).
func FilterExcluding[T SchemaObject](from, exclude map[string]T) []string {
	result := make([]string, 0)
	for k := range from {
		if _, exists := exclude[k]; !exists {
			result = append(result, k)
		}
	}

	sort.Strings(result)
	return result
}

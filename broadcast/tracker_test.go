package main

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTracker(t *testing.T) {
	tracker := NewTracker()
	tracker.AddValue(1)

	require.Equal(t, []int{1}, tracker.CurValues())
	tracker.AddValue(2)
	require.Equal(t, []int{1, 2}, tracker.CurValues())
}

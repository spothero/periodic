package testhelpers

import (
	"github.com/spothero/periodic"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// NewFloatingPeriodForTest constructs a new FloatingPeriod but will fail the test if there is an error constructing
// the FloatingPeriod.
func NewFloatingPeriodForTest(t *testing.T, s, e time.Duration, ad periodic.ApplicableDays, l *time.Location) periodic.FloatingPeriod {
	t.Helper()
	fp, err := periodic.NewFloatingPeriod(s, e, ad, l)
	require.NoError(t, err)
	return fp
}

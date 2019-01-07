// Copyright 2018 SpotHero
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package periodic

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeriod_Intersects(t *testing.T) {
	p := Period{
		Start: time.Date(2018, 5, 25, 13, 14, 15, 0, time.UTC),
		End:   time.Date(2018, 5, 26, 13, 14, 15, 0, time.UTC),
	}
	tests := []struct {
		name           string
		expectedResult bool
		p              Period
		o              Period
	}{
		{
			"True when start intersects",
			true,
			p,
			NewPeriod(p.Start, p.End.Add(time.Duration(1)*time.Minute)),
		}, {
			"True when end intersects",
			true,
			p,
			NewPeriod(p.Start.Add(-time.Duration(1)*time.Minute), p.End.Add(-time.Duration(1)*time.Minute)),
		}, {
			"True when start and end intersects through containment",
			true,
			p,
			NewPeriod(p.Start, p.End.Add(-time.Duration(1)*time.Minute)),
		}, {
			"true when start and end contain the period",
			true,
			p,
			NewPeriod(p.Start.Add(-time.Duration(1)*time.Minute), p.End.Add(time.Duration(1)*time.Minute)),
		}, {
			"False when start and end are before",
			false,
			p,
			NewPeriod(p.Start.Add(-time.Duration(2)*time.Minute), p.Start.Add(-time.Duration(1)*time.Minute)),
		}, {
			"False when start and end are after",
			false,
			p,
			NewPeriod(p.End.Add(time.Duration(1)*time.Minute), p.End.Add(time.Duration(2)*time.Minute)),
		}, {
			"True when start intersects and other end is unbounded",
			true,
			p,
			NewPeriod(p.Start, time.Time{}),
		}, {
			"True when start intersects and end is unbounded",
			true,
			NewPeriod(p.Start, time.Time{}),
			p,
		}, {
			"False when end is unbounded and start comes after other end",
			false,
			NewPeriod(p.Start, time.Time{}),
			NewPeriod(time.Unix(0, 0), p.Start.Add(-time.Hour)),
		}, {
			"False when other end is unbounded and other start comes after end",
			false,
			p,
			NewPeriod(p.End.Add(time.Second), time.Time{}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.p.Intersects(test.o))
		})
	}
}

func TestPeriod_Contains(t *testing.T) {
	testTime1, err := time.Parse(time.RFC3339, "2018-05-25T13:14:15Z")
	require.NoError(t, err)
	testTime2, err := time.Parse(time.RFC3339, "2018-05-26T13:14:15Z")
	require.NoError(t, err)
	p := Period{Start: testTime1, End: testTime2}
	tests := []struct {
		name           string
		expectedResult bool
		p              Period
		o              Period
	}{
		{
			"False when only start intersects",
			false,
			p,
			NewPeriod(p.Start, p.End.Add(time.Duration(1)*time.Minute)),
		}, {
			"False when only end intersects",
			false,
			p,
			NewPeriod(p.Start.Add(-time.Duration(1)*time.Minute), p.End.Add(-time.Duration(1)*time.Minute)),
		}, {
			"True when start and end intersects through containment",
			true,
			p,
			NewPeriod(p.Start, p.End.Add(-time.Duration(1)*time.Minute)),
		}, {
			"False when start and end contain the period",
			false,
			p,
			NewPeriod(p.Start.Add(-time.Duration(1)*time.Minute), p.End.Add(time.Duration(1)*time.Minute)),
		}, {
			"False when start and end are before",
			false,
			p,
			NewPeriod(p.Start.Add(-time.Duration(2)*time.Minute), p.Start.Add(-time.Duration(1)*time.Minute)),
		}, {
			"False when start and end are after",
			false,
			p,
			NewPeriod(p.End.Add(time.Duration(1)*time.Minute), p.End.Add(time.Duration(2)*time.Minute)),
		}, {
			"False when start is before and no end",
			false,
			NewPeriod(testTime1, time.Time{}),
			NewPeriod(testTime1.Add(-time.Minute), testTime2),
		}, {
			"True when start is after and no end",
			true,
			NewPeriod(testTime1, time.Time{}),
			NewPeriod(testTime1.Add(time.Minute), testTime2),
		}, {
			"False when end is after and no start",
			false,
			NewPeriod(time.Time{}, testTime2),
			NewPeriod(testTime1, testTime2.Add(time.Minute)),
		}, {
			"True when end is before and no start",
			true,
			NewPeriod(time.Time{}, testTime2),
			NewPeriod(testTime1, testTime2.Add(-time.Minute)),
		}, {
			"True when period has no start and no end",
			true,
			NewPeriod(time.Time{}, time.Time{}),
			NewPeriod(testTime1, testTime2),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.p.Contains(test.o))
		})
	}
}

func TestPeriod_ContainsAny(t *testing.T) {
	start, err := time.Parse(time.RFC3339, "2018-05-24T07:14:16-06:00")
	require.NoError(t, err)
	end, err := time.Parse(time.RFC3339, "2018-05-25T07:14:14-06:00")
	require.NoError(t, err)
	p := Period{
		Start: start,
		End:   end,
	}
	pos := Period{
		Start: time.Time{},
		End:   end,
	}
	poe := Period{
		Start: start,
		End:   time.Time{},
	}
	tests := []struct {
		name           string
		expectedResult bool
		p              Period
		o              Period
	}{
		{
			"Identical time periods are contained (start)",
			true,
			p,
			p,
		}, {
			"True when start is contained",
			true,
			p,
			NewPeriod(p.Start, p.End.Add(time.Duration(1)*time.Minute)),
		}, {
			"True when end is contained",
			true,
			p,
			NewPeriod(p.Start.Add(-time.Duration(1)*time.Minute), p.End.Add(-time.Duration(1)*time.Minute)),
		}, {
			"True when period is fully contained",
			true,
			p,
			NewPeriod(p.Start.Add(time.Duration(1)*time.Minute), p.End.Add(-time.Duration(1)*time.Minute)),
		}, {
			"False when period is fully before",
			false,
			p,
			NewPeriod(p.Start.Add(-time.Duration(2)*time.Minute), p.Start.Add(-time.Duration(1)*time.Minute)),
		}, {
			"False when period is fully after",
			false,
			p,
			NewPeriod(p.End.Add(time.Duration(1)*time.Minute), p.End.Add(time.Duration(2)*time.Minute)),
		}, {
			"True when open starts period start time is before requested time",
			true,
			pos,
			NewPeriod(pos.Start.Add(-time.Duration(1)*time.Minute), pos.Start.AddDate(1, 0, 0)),
		}, {
			"False when open starts period start time is after requested time",
			false,
			pos,
			NewPeriod(pos.End, pos.End.Add(time.Duration(1)*time.Minute)),
		}, {
			"True when open ends period end time is after requested time",
			true,
			poe,
			NewPeriod(poe.Start.Add(time.Duration(1)*time.Minute), poe.Start.AddDate(2, 0, 0)),
		}, {
			"False when open ends period end time is before requested time",
			false,
			poe,
			NewPeriod(poe.Start.AddDate(-1, 0, 0), poe.Start.Add(-time.Duration(1)*time.Minute)),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.p.ContainsAny(test.o))
		})
	}
}

func TestPeriod_Less(t *testing.T) {
	tests := []struct {
		name           string
		expectedResult bool
		p              Period
		d              time.Duration
	}{
		{
			"01/01/2018 05:00 - 01/01/2018 21:00 is less than 24 hours",
			true,
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
			time.Duration(24) * time.Hour,
		}, {
			"01/01/2018 05:00 - 01/01/2018 21:00 is not less than 16 hours",
			false,
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
			time.Duration(16) * time.Hour,
		}, {
			"01/01/2018 05:00 - 01/01/2018 21:00 is not less than 1 hour",
			false,
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
			time.Duration(1) * time.Hour,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.p.Less(test.d))
		})
	}
}

func TestPeriod_ContainsTime(t *testing.T) {
	tests := []struct {
		name           string
		expectedResult bool
		p              Period
		t              time.Time
	}{
		{
			"Period 01/01/2018 05:00-21:00, request for 05:00 is contained",
			true,
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
			time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC),
		}, {
			"Period 01/01/2018 05:00-21:00, request for 04:59 is not contained",
			false,
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
			time.Date(2018, 1, 1, 4, 59, 0, 0, time.UTC),
		}, {
			"01/01/2018 Period 21:00 - 01/02/2018 05:00, request for 21:00 is contained",
			true,
			NewPeriod(time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 5, 0, 0, 0, time.UTC)),
			time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC),
		}, {
			"01/01/2018 Period 21:00 - 01/02/2018 05:00, request for 20:59 is not contained",
			false,
			NewPeriod(time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 5, 0, 0, 0, time.UTC)),
			time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC),
		}, {
			"Period 0 - 0 contains any time",
			true,
			Period{},
			time.Date(2018, 1, 1, 1, 1, 1, 1, time.UTC),
		}, {
			"Period with only start time contains anything after start",
			true,
			NewPeriod(time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC), time.Time{}),
			time.Date(2018, 1, 1, 1, 1, 1, 1, time.UTC),
		}, {
			"Period with only start time does not contain anything before start",
			false,
			NewPeriod(time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC), time.Time{}),
			time.Date(2017, 12, 31, 23, 59, 0, 0, time.UTC),
		}, {
			"Period with only end time contains anything before the end",
			true,
			NewPeriod(time.Time{}, time.Date(2018, 1, 1, 1, 1, 1, 1, time.UTC)),
			time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC),
		}, {
			"Period with only end time does not contain anything after the end",
			false,
			NewPeriod(time.Time{}, time.Date(2018, 1, 1, 1, 1, 1, 1, time.UTC)),
			time.Date(2018, 1, 2, 0, 0, 0, 0, time.UTC),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.p.ContainsTime(test.t))
		})
	}
}

func TestMaxTime(t *testing.T) {
	t1, err := time.Parse(time.RFC3339, "2018-05-24T07:14:16-06:00")
	require.NoError(t, err)
	t2, err := time.Parse(time.RFC3339, "2018-05-24T07:14:16-06:00")
	require.NoError(t, err)
	tests := []struct {
		name           string
		expectedResult time.Time
		t1             time.Time
		t2             time.Time
	}{
		{
			"T1 is returned when T1 and T2 are identical",
			t1,
			t1,
			t2,
		}, {
			"T1 is returned when T1 is greater than T2",
			t1,
			t1,
			t2.Add(-time.Duration(1) * time.Minute),
		}, {
			"T2 is returned when T2 is greater than T1",
			t2,
			t1.Add(-time.Duration(1) * time.Minute),
			t2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, MaxTime(t1, t2))
		})
	}
}

func TestMinTime(t *testing.T) {
	t1, err := time.Parse(time.RFC3339, "2018-05-24T07:14:16-06:00")
	require.NoError(t, err)
	t2, err := time.Parse(time.RFC3339, "2018-05-24T07:14:16-06:00")
	require.NoError(t, err)
	tests := []struct {
		name           string
		expectedResult time.Time
		t1             time.Time
		t2             time.Time
	}{
		{
			"T1 is returned when T1 and T2 are identical",
			t1,
			t1,
			t2,
		}, {
			"T2 is returned when T1 is greater than T2",
			t1,
			t1.Add(time.Duration(1) * time.Minute),
			t2,
		}, {
			"T1 is returned when T2 is greater than T1",
			t2,
			t1,
			t2.Add(time.Duration(1) * time.Minute),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, MinTime(t1, t2))
		})
	}
}

func TestNewApplicableDaysMonStart(t *testing.T) {
	continuousDayTests := []struct {
		startDay       int
		endDay         int
		expectedResult *ApplicableDays
	}{
		{0, 0, &ApplicableDays{true, false, false, false, false, false, false}},
		{0, 4, &ApplicableDays{true, true, true, true, true, false, false}},
		{5, 1, &ApplicableDays{true, true, false, false, false, true, true}},
	}
	for _, test := range continuousDayTests {
		t.Run(fmt.Sprintf("start: %d, end: %d", test.startDay, test.endDay), func(t *testing.T) {
			applicableDays := NewApplicableDaysMonStart(test.startDay, test.endDay)
			assert.Equal(t, applicableDays.Monday, test.expectedResult.Monday)
			assert.Equal(t, applicableDays.Tuesday, test.expectedResult.Tuesday)
			assert.Equal(t, applicableDays.Wednesday, test.expectedResult.Wednesday)
			assert.Equal(t, applicableDays.Thursday, test.expectedResult.Thursday)
			assert.Equal(t, applicableDays.Friday, test.expectedResult.Friday)
			assert.Equal(t, applicableDays.Saturday, test.expectedResult.Saturday)
			assert.Equal(t, applicableDays.Sunday, test.expectedResult.Sunday)
		})
	}
}

func TestNewFloatingPeriod(t *testing.T) {
	cst, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	tests := []struct {
		name             string
		s, e             time.Duration
		d                ApplicableDays
		loc, expectedLoc *time.Location
	}{
		{
			"creating a new floating period works",
			time.Second, 2 * time.Second,
			ApplicableDays{Monday: true, Friday: true},
			cst,
			cst,
		}, {
			"creating a new floating period nil time zone loads UTC",
			time.Second, 2 * time.Second,
			ApplicableDays{Monday: true, Friday: true},
			nil,
			time.UTC,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fp := NewFloatingPeriod(test.s, test.e, test.d, test.loc)
			assert.Equal(t, test.s, fp.Start)
			assert.Equal(t, test.e, fp.End)
			assert.Equal(t, test.d, fp.Days)
			assert.Equal(t, test.expectedLoc, fp.Location)
		})
	}
}

func TestFloatingPeriod_Contiguous(t *testing.T) {
	tests := []struct {
		name           string
		expectedResult bool
		fp             FloatingPeriod
	}{
		{
			"Floating Period 00:00-00:00 is contiguous",
			true,
			FloatingPeriod{
				Start: time.Duration(0) * time.Hour,
				End:   time.Duration(0) * time.Hour,
			},
		}, {
			"Floating Period 12:34-12:34 is contiguous",
			true,
			FloatingPeriod{
				Start: (time.Duration(12) * time.Hour) + (time.Duration(34) * time.Minute),
				End:   (time.Duration(12) * time.Hour) + (time.Duration(34) * time.Minute),
			},
		}, {
			"Floating Period 00:00-00:01 is non-contiguous",
			false,
			FloatingPeriod{
				Start: time.Duration(0) * time.Hour,
				End:   time.Duration(1) * time.Minute,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.fp.Contiguous())
		})
	}
}

func TestFloatingPeriod_AtDate(t *testing.T) {
	chiTz, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	tests := []struct {
		name           string
		expectedResult *Period
		fp             FloatingPeriod
		d              time.Time
	}{
		{
			"Floating Period 05:00-21:00 at 11/13/2018 01:23:45 returns 11/13/2018 05:00-21:00",
			&Period{Start: time.Date(2018, 11, 13, 5, 0, 0, 0, time.UTC), End: time.Date(2018, 11, 13, 21, 00, 0, 0, time.UTC)},
			NewFloatingPeriod(5*time.Hour, 21*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			time.Date(2018, 11, 13, 1, 23, 45, 59, time.UTC),
		}, {
			`Floating Period 21:00-05:00 at 11/13/2018 01:23:45 returns
			11/12/2018 21:00 - 11/13/2018 05:00`,
			&Period{Start: time.Date(2018, 11, 12, 21, 0, 0, 0, time.UTC), End: time.Date(2018, 11, 13, 5, 00, 0, 0, time.UTC)},
			NewFloatingPeriod(21*time.Hour, 5*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			time.Date(2018, 11, 13, 1, 23, 45, 59, time.UTC),
		}, {
			`Floating Period 21:00-05:00 at 11/13/2018 22:00:00 returns 11/13/2018 21:00 - 11/14/2018 05:00`,
			&Period{Start: time.Date(2018, 11, 13, 21, 0, 0, 0, time.UTC), End: time.Date(2018, 11, 14, 5, 00, 0, 0, time.UTC)},
			NewFloatingPeriod(21*time.Hour, 5*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			time.Date(2018, 11, 13, 22, 0, 0, 0, time.UTC),
		}, {
			"Floating Period 15:00-0:00 returns 11/13/2018 15:00 - 11/14/2018 00:00 CST when requested with 11/14/2018 00:30 UTC",
			&Period{Start: time.Date(2018, 11, 13, 15, 0, 0, 0, chiTz), End: time.Date(2018, 11, 14, 0, 0, 0, 0, chiTz)},
			NewFloatingPeriod(15*time.Hour, 0, NewApplicableDaysMonStart(0, 6), chiTz),
			time.Date(2018, 11, 14, 0, 30, 0, 0, time.UTC),
		}, {
			"Floating period 09:00-12:00 MWF, request anytime on Tuesday returns period on Wednesday",
			&Period{Start: time.Date(2019, 1, 9, 9, 0, 0, 0, time.UTC), End: time.Date(2019, 1, 9, 12, 0, 0, 0, time.UTC)},
			NewFloatingPeriod(9*time.Hour, 12*time.Hour, ApplicableDays{Monday: true, Wednesday: true, Friday: true}, time.UTC),
			time.Date(2019, 1, 8, 8, 0, 0, 0, time.UTC),
		}, {
			"Floating period 09:00-12:00 MWF, request Monday 13:00 returns period on Wednesday",
			&Period{Start: time.Date(2019, 1, 9, 9, 0, 0, 0, time.UTC), End: time.Date(2019, 1, 9, 12, 0, 0, 0, time.UTC)},
			NewFloatingPeriod(9*time.Hour, 12*time.Hour, ApplicableDays{Monday: true, Wednesday: true, Friday: true}, time.UTC),
			time.Date(2019, 1, 7, 13, 0, 0, 0, time.UTC),
		}, {
			"Floating period 09:00-12:00 MWF, request Monday 12:00 returns period on Wednesday",
			&Period{Start: time.Date(2019, 1, 9, 9, 0, 0, 0, time.UTC), End: time.Date(2019, 1, 9, 12, 0, 0, 0, time.UTC)},
			NewFloatingPeriod(9*time.Hour, 12*time.Hour, ApplicableDays{Monday: true, Wednesday: true, Friday: true}, time.UTC),
			time.Date(2019, 1, 7, 12, 0, 0, 0, time.UTC),
		}, {
			"Floating period 09:00-12:00 M, request Monday 13:00 returns period on next Monday",
			&Period{Start: time.Date(2019, 1, 14, 9, 0, 0, 0, time.UTC), End: time.Date(2019, 1, 14, 12, 0, 0, 0, time.UTC)},
			NewFloatingPeriod(9*time.Hour, 12*time.Hour, ApplicableDays{Monday: true}, time.UTC),
			time.Date(2019, 1, 7, 13, 0, 0, 0, time.UTC),
		}, {
			"Floating period with no applicable days returns nil",
			nil,
			NewFloatingPeriod(0, 0, ApplicableDays{}, time.UTC),
			time.Date(2019, 1, 7, 13, 0, 0, 0, time.UTC),
		}, {
			"Floating period 20:00-02:00 M, request 1:00 Tuesday returns 20:00 M - 02:00 T",
			&Period{Start: time.Date(2019, 1, 7, 20, 0, 0, 0, time.UTC), End: time.Date(2019, 1, 8, 2, 0, 0, 0, time.UTC)},
			NewFloatingPeriod(20*time.Hour, 2*time.Hour, ApplicableDays{Monday: true}, time.UTC),
			time.Date(2019, 1, 8, 1, 0, 0, 0, time.UTC),
		}, {
			"Floating period 20:00-02:00 MTW, request 1:00 Tuesday returns 20:00 M - 02:00 T",
			&Period{Start: time.Date(2019, 1, 7, 20, 0, 0, 0, time.UTC), End: time.Date(2019, 1, 8, 2, 0, 0, 0, time.UTC)},
			NewFloatingPeriod(20*time.Hour, 2*time.Hour, ApplicableDays{Monday: true, Tuesday: true, Wednesday: true}, time.UTC),
			time.Date(2019, 1, 8, 1, 0, 0, 0, time.UTC),
		},
		{
			"Floating period 20:00-02:00 MTW, request 05:00 Tuesday returns 20:00 T - 02:00 W",
			&Period{Start: time.Date(2019, 1, 8, 20, 0, 0, 0, time.UTC), End: time.Date(2019, 1, 9, 2, 0, 0, 0, time.UTC)},
			NewFloatingPeriod(20*time.Hour, 2*time.Hour, ApplicableDays{Monday: true, Tuesday: true, Wednesday: true}, time.UTC),
			time.Date(2019, 1, 8, 5, 0, 0, 0, time.UTC),
		}, {
			"Floating period 20:00-02:00 MT, request 02:00 Tuesday returns 20:00 T - 02:00 W",
			&Period{Start: time.Date(2019, 1, 8, 20, 0, 0, 0, time.UTC), End: time.Date(2019, 1, 9, 2, 0, 0, 0, time.UTC)},
			NewFloatingPeriod(20*time.Hour, 2*time.Hour, ApplicableDays{Monday: true, Tuesday: true}, time.UTC),
			time.Date(2019, 1, 8, 2, 0, 0, 0, time.UTC),
		}, {
			"Floating period 09:00 - 09:00 MW, request 12:00 T returns 09:00 W - 09:00 Th",
			&Period{Start: time.Date(2019, 1, 9, 9, 0, 0, 0, time.UTC), End: time.Date(2019, 1, 10, 9, 0, 0, 0, time.UTC)},
			NewFloatingPeriod(9*time.Hour, 9*time.Hour, ApplicableDays{Monday: true, Wednesday: true}, time.UTC),
			time.Date(2019, 1, 8, 12, 0, 0, 0, time.UTC),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.fp.AtDate(test.d))
		})
	}
}

func TestFloatingPeriod_Contains(t *testing.T) {
	tests := []struct {
		name           string
		expectedResult bool
		fp             FloatingPeriod
		p              Period
	}{
		// Test when all days are valid (starts before ends)
		{
			"Floating Period 05:00-21:00, request for 05:00-20:59 is contained",
			true,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period 05:00-21:00, request for 05:00-21:00 is contained",
			true,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
		}, {
			"Floating Period 05:00-21:00, request for 1/1/2018 05:00 - 1/2/2018 20:59 is not contained",
			false,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 20, 59, 0, 0, time.UTC)),
		},
		// Test when all days are valid (starts AFTER ends)
		{
			"Floating Period 21:00-05:00, request for 05:00-20:59 is not contained",
			false,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period 21:00-05:00, request for 21:00-05:00 is contained",
			true,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 5, 0, 0, 0, time.UTC)),
		}, {
			"Floating Period 21:00-05:00, request for 1/1/2018 21:00 - 1/2/2018 04:59 is not contained",
			true,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 4, 59, 0, 0, time.UTC)),
		},
		{
			"Floating Period 00:00-00:00, request for 1/1/2018 05:00 - 1/2/2018 20:59 is not contained",
			false,
			NewFloatingPeriod(time.Duration(0)*time.Hour, time.Duration(0)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 20, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period 23:59-23:59, request for 1/1/2018 05:00 - 1/2/2018 20:59 is not contained since Friday is not applicable",
			false,
			NewFloatingPeriod(time.Duration(23)*time.Hour+time.Duration(59)*time.Minute,
				time.Duration(23)*time.Hour+time.Duration(59)*time.Minute,
				NewApplicableDaysMonStart(0, 3), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 5, 20, 59, 0, 0, time.UTC)),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.fp.Contains(test.p))
		})
	}
}

func TestFloatingPeriod_Intersects(t *testing.T) {
	tests := []struct {
		name           string
		expectedResult bool
		fp             FloatingPeriod
		p              Period
	}{
		// Test when all days are valid (starts before ends)
		{
			"Floating Period 05:00-21:00, request for 05:00-20:59 is intersected",
			true,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period 05:00-21:00, request for 20:59-05:00 is intersected",
			true,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC), time.Date(2018, 1, 2, 5, 0, 0, 0, time.UTC)),
		}, {
			"Floating Period 05:00-21:00, request for 00:00-4:59 is not intersected",
			false,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 4, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period 05:00-21:00, request for 21:00-21:01 is not intersected",
			false,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 1, 0, 0, time.UTC)),
		},
		// Test when all days are valid (starts AFTER ends)
		{
			"Floating Period 21:00-05:00, request for 05:00-20:59 is not intersected",
			false,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period 21:00-05:00, request for 20:59-05:00 is intersected",
			true,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC), time.Date(2018, 1, 2, 5, 0, 0, 0, time.UTC)),
		}, {
			"Floating Period 21:00-05:00, request for 00:00-4:59 is intersected",
			true,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 4, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period 21:00-05:00, request for 05:00-05:01 is intersected",
			false,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 05, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 05, 1, 0, 0, time.UTC)),
		},
		// Test when we have gaps in days (starts before ends)
		{
			"Floating Period 05:00-21:00, request for Mon 01/01/2018 05:00 - Mon 01/01/2018 20:59 is intersected",
			true,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, NewApplicableDaysMonStart(0, 5), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period 05:00-21:00, request for Sun 12/31/2017 05:00 - Sun 12/31/2017 20:59 is not intersected",
			false,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, NewApplicableDaysMonStart(0, 5), time.UTC),
			NewPeriod(time.Date(2017, 12, 31, 5, 0, 0, 0, time.UTC), time.Date(2017, 12, 31, 20, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period 05:00-21:00, request for Sun 12/31/2017 05:00 - Mon 01/01/2018 20:59 is intersected",
			true,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, NewApplicableDaysMonStart(0, 5), time.UTC),
			NewPeriod(time.Date(2017, 12, 31, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period 05:00-21:00, request for Sat 12/30/2017 20:59 - Sun 12/31/2018 00:00 is intersected",
			true,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, NewApplicableDaysMonStart(0, 5), time.UTC),
			NewPeriod(time.Date(2017, 12, 30, 20, 59, 0, 0, time.UTC), time.Date(2017, 12, 31, 0, 0, 0, 0, time.UTC)),
		},
		// Test when we have gaps in days (starts AFTER ends)
		{
			"Floating Period 21:00-05:00, request for Mon 01/01/2018 05:00 - Mon 01/01/2018 20:59 is not intersected",
			false,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(0, 5), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period 21:00-05:00, request for Sun 12/31/2017 05:00 - Sun 12/31/2017 20:59 is not intersected",
			false,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(0, 5), time.UTC),
			NewPeriod(time.Date(2017, 12, 31, 5, 0, 0, 0, time.UTC), time.Date(2017, 12, 31, 20, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period 21:00-05:00, request for Sun 12/31/2017 05:00 - Mon 01/01/2018 20:59 is not intersected",
			false,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(0, 5), time.UTC),
			NewPeriod(time.Date(2017, 12, 31, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period 21:00-05:00, request for Sat 12/30/2017 20:59 - Sun 12/31/2018 00:00 is intersected",
			true,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(0, 5), time.UTC),
			NewPeriod(time.Date(2017, 12, 30, 20, 59, 0, 0, time.UTC), time.Date(2017, 12, 31, 0, 0, 0, 0, time.UTC)),
		}, {
			"Floating Period Days 0-5 00:00-00:00, request for M-F at any time is intersected",
			true,
			NewFloatingPeriod(time.Duration(0)*time.Hour, time.Duration(0)*time.Hour, NewApplicableDaysMonStart(0, 4), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2018, 1, 5, 23, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period Days 0-5 00:00-00:00, request for Sa-Su at any time is not intersected",
			false,
			NewFloatingPeriod(time.Duration(0)*time.Hour, time.Duration(0)*time.Hour, NewApplicableDaysMonStart(0, 4), time.UTC),
			NewPeriod(time.Date(2018, 1, 6, 0, 0, 0, 0, time.UTC), time.Date(2018, 1, 7, 23, 59, 0, 0, time.UTC)),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.fp.Intersects(test.p))
		})
	}
}

func TestFloatingPeriod_ContainsTime(t *testing.T) {
	tests := []struct {
		name           string
		expectedResult bool
		fp             FloatingPeriod
		t              time.Time
	}{
		{
			"Floating Period 05:00-21:00, request for 05:00 is contained",
			true,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC),
		}, {
			"Floating Period 05:00-21:00, request for 04:59 is not contained",
			false,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			time.Date(2018, 1, 1, 4, 59, 0, 0, time.UTC),
		}, {
			"Floating Period 21:00-05:00, request for 21:00 is contained",
			true,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC),
		}, {
			"Floating Period 21:00-05:00, request for 20:59 is not contained",
			false,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC),
		}, {
			"Floating Period Tu-Su 21:00-05:00, request for 01/01/2018 20:59 is not contained",
			false,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(1, 6), time.UTC),
			time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.fp.ContainsTime(test.t))
		})
	}
}

func TestFloatingPeriod_ContainsStart(t *testing.T) {
	tests := []struct {
		name           string
		expectedResult bool
		fp             FloatingPeriod
		p              Period
	}{
		{
			"Floating Period 05:00-21:00, request for 05:00-20:59 is contained",
			true,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period 05:00-21:00, request for 05:00-21:00 is contained",
			true,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
		}, {
			"Floating Period 05:00-21:00, request for 04:59-21:00 is not contained",
			false,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 4, 59, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
		}, {
			"Floating Period 21:00-05:00, request for 21:00-04:59 is contained",
			true,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 4, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period 21:00-05:00, request for 20:59-04:59 is not contained",
			false,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC), time.Date(2018, 1, 2, 4, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period 21:00-05:00, request for 21:00-05:00 is contained",
			true,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 5, 0, 0, 0, time.UTC)),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.fp.ContainsStart(test.p))
		})
	}
}

func TestFloatingPeriod_ContainsEnd(t *testing.T) {
	tests := []struct {
		name           string
		expectedResult bool
		fp             FloatingPeriod
		p              Period
	}{
		{
			"Floating Period 05:00-21:00, request for 05:00-16:59 is contained",
			true,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period 05:00-21:00, request for 05:00-21:00 is not contained",
			false,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
		}, {
			"Floating Period 21:00-05:00, request for 21:00-04:59 is contained",
			true,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 4, 59, 0, 0, time.UTC)),
		}, {
			"Floating Period 21:00-05:00, request for 21:00-05:00 is not contained",
			false,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, NewApplicableDaysMonStart(0, 6), time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 5, 0, 0, 0, time.UTC)),
		}, {
			"Floating Period 21:00-05:00 Tu only, request for M - Tu 22:00-04:00 is not contained",
			false,
			NewFloatingPeriod(time.Duration(21)*time.Hour, time.Duration(5)*time.Hour, ApplicableDays{Tuesday: true}, time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 22, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 4, 0, 0, 0, time.UTC)),
		}, {
			"Floating Period 05:00-21:00 Tu only, request for M 12:00 - 17:00 is not contained",
			false,
			NewFloatingPeriod(time.Duration(5)*time.Hour, time.Duration(21)*time.Hour, ApplicableDays{Tuesday: true}, time.UTC),
			NewPeriod(time.Date(2018, 1, 1, 12, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 17, 0, 0, 0, time.UTC)),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.fp.ContainsEnd(test.p))
		})
	}
}

func TestNewContinuousPeriod(t *testing.T) {
	cst, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	tests := []struct {
		name             string
		s, e             time.Duration
		sDow, eDow       time.Weekday
		loc, expectedLoc *time.Location
	}{
		{
			"creating a new continuous period works",
			time.Second, 2 * time.Second,
			time.Monday, time.Thursday,
			cst,
			cst,
		}, {
			"creating a new continuous period nil time zone loads UTC",
			time.Second, 2 * time.Second,
			time.Friday, time.Monday,
			nil,
			time.UTC,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cp := NewContinuousPeriod(test.s, test.e, test.sDow, test.eDow, test.loc)
			assert.Equal(t, test.s, cp.Start)
			assert.Equal(t, test.e, cp.End)
			assert.Equal(t, test.sDow, cp.StartDOW)
			assert.Equal(t, test.eDow, cp.EndDOW)
			assert.Equal(t, test.expectedLoc, cp.Location)
		})
	}
}

func TestContinuousPeriod_AtDate(t *testing.T) {
	chiTz, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	laTz, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)
	tests := []struct {
		name           string
		expectedResult Period
		cp             ContinuousPeriod
		d              time.Time
	}{
		{
			"CP 0500 M - 1800 F is offset correctly from 2018-10-03T13:13:13Z",
			NewPeriod(time.Date(2018, 10, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 10, 5, 18, 0, 0, 0, time.UTC)),
			NewContinuousPeriod(time.Duration(5)*time.Hour, time.Duration(18)*time.Hour, time.Monday, time.Friday, time.UTC),
			time.Date(2018, 10, 3, 13, 13, 13, 13, time.UTC),
		}, {
			"CP 0500 M - 0400 M is offset correctly from 2018-10-03T13:13:13Z",
			NewPeriod(time.Date(2018, 10, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 10, 8, 4, 0, 0, 0, time.UTC)),
			NewContinuousPeriod(time.Duration(5)*time.Hour, time.Duration(4)*time.Hour, time.Monday, time.Monday, time.UTC),
			time.Date(2018, 10, 3, 13, 13, 13, 13, time.UTC),
		}, {
			"CP 0500 W - 0400 W is offset correctly from 2018-10-03T13:13:13Z",
			NewPeriod(time.Date(2018, 10, 3, 5, 0, 0, 0, time.UTC), time.Date(2018, 10, 10, 4, 0, 0, 0, time.UTC)),
			NewContinuousPeriod(time.Duration(5)*time.Hour, time.Duration(4)*time.Hour, time.Wednesday, time.Wednesday, time.UTC),
			time.Date(2018, 10, 3, 13, 13, 13, 13, time.UTC),
		}, {
			"CP 0400 W - 0500 W is offset correctly from 2018-10-03T13:13:13Z",
			NewPeriod(time.Date(2018, 10, 10, 4, 0, 0, 0, time.UTC), time.Date(2018, 10, 10, 5, 0, 0, 0, time.UTC)),
			NewContinuousPeriod(time.Duration(4)*time.Hour, time.Duration(5)*time.Hour, time.Wednesday, time.Wednesday, time.UTC),
			time.Date(2018, 10, 3, 13, 13, 13, 13, time.UTC),
		}, {
			"CP 0500 TH - 0400 F is offset correctly from 2018-10-03T13:13:13Z",
			NewPeriod(time.Date(2018, 10, 4, 5, 0, 0, 0, time.UTC), time.Date(2018, 10, 5, 4, 0, 0, 0, time.UTC)),
			NewContinuousPeriod(time.Duration(5)*time.Hour, time.Duration(4)*time.Hour, time.Thursday, time.Friday, time.UTC),
			time.Date(2018, 10, 3, 13, 13, 13, 13, time.UTC),
		}, {
			"CP M 0000 - M 0000 is offset correctly from 2018-10-23T1:00:00Z",
			NewPeriod(time.Date(2018, 10, 22, 0, 0, 0, 0, time.UTC), time.Date(2018, 10, 29, 0, 0, 0, 0, time.UTC)),
			NewContinuousPeriod(time.Duration(0), time.Duration(0), time.Monday, time.Monday, time.UTC),
			time.Date(2018, 10, 23, 1, 0, 0, 0, time.UTC),
		}, {
			"CP 0500 M - 1800 F PDT is offset correctly from 2018-10-03T13:13:13 CDT",
			NewPeriod(time.Date(2018, 10, 1, 5, 0, 0, 0, laTz), time.Date(2018, 10, 5, 18, 0, 0, 0, laTz)),
			NewContinuousPeriod(time.Duration(5)*time.Hour, time.Duration(18)*time.Hour, time.Monday, time.Friday, laTz),
			time.Date(2018, 10, 3, 13, 13, 13, 13, chiTz),
		}, {
			"CP 1200 Sa - 1200 Su is offset correctly from 2019-1-3T12:00Z",
			NewPeriod(time.Date(2019, 1, 5, 12, 0, 0, 0, time.UTC), time.Date(2019, 1, 6, 12, 0, 0, 0, time.UTC)),
			NewContinuousPeriod(12*time.Hour, 12*time.Hour, time.Saturday, time.Sunday, time.UTC),
			time.Date(2019, 1, 3, 12, 0, 0, 0, time.UTC),
		}, {
			"CP 1200 Sa - 1200 Su is offset correctly from 2019-1-7T12:00Z",
			NewPeriod(time.Date(2019, 1, 12, 12, 0, 0, 0, time.UTC), time.Date(2019, 1, 13, 12, 0, 0, 0, time.UTC)),
			NewContinuousPeriod(12*time.Hour, 12*time.Hour, time.Saturday, time.Sunday, time.UTC),
			time.Date(2019, 1, 7, 12, 0, 0, 0, time.UTC),
		}, {
			"CP 0100 W - 1200 F CST is offset correctly from 2019-01-02T02:00Z",
			NewPeriod(time.Date(2019, 1, 2, 1, 0, 0, 0, chiTz), time.Date(2019, 1, 4, 12, 0, 0, 0, chiTz)),
			NewContinuousPeriod(time.Hour, 12*time.Hour, time.Wednesday, time.Friday, chiTz),
			time.Date(2019, 1, 2, 2, 0, 0, 0, time.UTC),
		}, {
			"CP 0000 Sa - 0000 M UTC is offset correctly from 11/17/18T01:00Z",
			NewPeriod(time.Date(2018, 11, 17, 0, 0, 0, 0, time.UTC), time.Date(2018, 11, 19, 0, 0, 0, 0, time.UTC)),
			NewContinuousPeriod(0, 0, time.Saturday, time.Monday, time.UTC),
			time.Date(2018, 11, 17, 1, 0, 0, 0, time.UTC),
		}, {
			"CP 0100 Sa - 0000 M UTC is offset correctly from 11/17/18T01:00Z",
			NewPeriod(time.Date(2018, 11, 17, 1, 0, 0, 0, time.UTC), time.Date(2018, 11, 19, 0, 0, 0, 0, time.UTC)),
			NewContinuousPeriod(time.Hour, 0, time.Saturday, time.Monday, time.UTC),
			time.Date(2018, 11, 17, 0, 30, 0, 0, time.UTC),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, &test.expectedResult, test.cp.AtDate(test.d))
		})
	}
}

func TestContinuousPeriod_Contains(t *testing.T) {
	tests := []struct {
		name           string
		expectedResult bool
		p              Period
		cp             ContinuousPeriod
	}{
		{
			"CP 0500 M - 1800 F contains 2018-10-03T13:13:13Z - 2018-10-04T13:13:13Z",
			true,
			NewPeriod(time.Date(2018, 10, 3, 13, 13, 13, 13, time.UTC), time.Date(2018, 10, 4, 13, 13, 13, 13, time.UTC)),
			NewContinuousPeriod(time.Duration(5)*time.Hour, time.Duration(18)*time.Hour, time.Monday, time.Friday, time.UTC),
		}, {
			"CP 0500 M - 1800 F doesnt contain 2018-10-03T13:13:13Z - 2018-10-09T13:13:13Z",
			false,
			NewPeriod(time.Date(2018, 10, 3, 13, 13, 13, 13, time.UTC), time.Date(2018, 10, 9, 13, 13, 13, 13, time.UTC)),
			NewContinuousPeriod(time.Duration(5)*time.Hour, time.Duration(18)*time.Hour, time.Monday, time.Friday, time.UTC),
		}, {
			"CP 0500 M - 1800 F contains 2018-10-03T05:00:00Z - 2018-10-07T17:59:59Z",
			true,
			NewPeriod(time.Date(2018, 10, 3, 5, 0, 0, 0, time.UTC), time.Date(2018, 10, 5, 17, 59, 59, 59, time.UTC)),
			NewContinuousPeriod(time.Duration(5)*time.Hour, time.Duration(18)*time.Hour, time.Monday, time.Friday, time.UTC),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.cp.Contains(test.p))
		})
	}
}

func TestApplicableDays_TimeApplicable(t *testing.T) {
	chiTZ, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	tests := []struct {
		name    string
		t       time.Time
		l       *time.Location
		ad      ApplicableDays
		outcome bool
	}{
		{
			"2018-10-30T22:00 CDT is applicable on Tuesday CDT",
			time.Date(2018, 10, 30, 22, 0, 0, 0, chiTZ),
			chiTZ,
			ApplicableDays{Tuesday: true},
			true,
		}, {
			"2018-10-30T22:00 CDT is not applicable on Tuesday UTC",
			time.Date(2018, 10, 30, 22, 0, 0, 0, chiTZ),
			time.UTC,
			ApplicableDays{Tuesday: true},
			false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.outcome, test.ad.TimeApplicable(test.t, test.l))
		})
	}
}

func TestApplicableDays_AnyApplicable(t *testing.T) {
	tests := []struct {
		name     string
		ad       ApplicableDays
		expected bool
	}{
		{
			"Monday applicable returns true",
			ApplicableDays{Monday: true},
			true,
		}, {
			"Tuesday applicable returns true",
			ApplicableDays{Tuesday: true},
			true,
		}, {
			"Wednesday applicable returns true",
			ApplicableDays{Wednesday: true},
			true,
		}, {
			"Thursday applicable returns true",
			ApplicableDays{Thursday: true},
			true,
		}, {
			"Friday applicable returns true",
			ApplicableDays{Friday: true},
			true,
		}, {
			"Saturday applicable returns true",
			ApplicableDays{Saturday: true},
			true,
		}, {
			"Sunday applicable returns true",
			ApplicableDays{Sunday: true},
			true,
		}, {
			"No applicable days returns false",
			ApplicableDays{},
			false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, test.ad.AnyApplicable())
		})
	}
}

func TestContinuousPeriod_ContainsTime(t *testing.T) {
	tests := []struct {
		name    string
		t       time.Time
		cp      ContinuousPeriod
		outcome bool
	}{
		{
			"continuous period 8:00-20:00 M-F does not contain 11/10/18 12:00",
			time.Date(2018, 11, 10, 12, 0, 0, 0, time.UTC),
			NewContinuousPeriod(time.Duration(8)*time.Hour, time.Duration(20)*time.Hour, 1, 5, time.UTC),
			false,
		}, {
			"continuous period 8:00-20:00 M-F does contain 11/6/18 12:00",
			time.Date(2018, 11, 6, 12, 0, 0, 0, time.UTC),
			NewContinuousPeriod(time.Duration(8)*time.Hour, time.Duration(20)*time.Hour, 1, 5, time.UTC),
			true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.outcome, test.cp.ContainsTime(test.t))
		})
	}
}

func TestContinuousPeriod_FromTime(t *testing.T) {
	tests := []struct {
		name    string
		t       time.Time
		cp      ContinuousPeriod
		outcome *Period
	}{
		{
			"continuous period 8:00-20:00 M-F request time 11/8/18 12:00 returns period 11/9/18 12:00-20:00",
			time.Date(2018, 11, 8, 12, 0, 0, 0, time.UTC),
			NewContinuousPeriod(time.Duration(8)*time.Hour, time.Duration(20)*time.Hour, time.Monday, time.Friday, time.UTC),
			&Period{Start: time.Date(2018, 11, 8, 12, 0, 0, 0, time.UTC), End: time.Date(2018, 11, 9, 20, 0, 0, 0, time.UTC)},
		}, {
			"continuous period 8:00-20:00 M-F request time 11/9/18 22:00 returns nil",
			time.Date(2018, 11, 9, 22, 0, 0, 0, time.UTC),
			NewContinuousPeriod(time.Duration(8)*time.Hour, time.Duration(20)*time.Hour, time.Monday, time.Friday, time.UTC),
			nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.outcome, test.cp.FromTime(test.t))
		})
	}
}

func TestFloatingPeriod_FromTime(t *testing.T) {
	tests := []struct {
		name    string
		t       time.Time
		fp      FloatingPeriod
		outcome *Period
	}{
		{
			"floating period 8:00-20:00 M-F request time 11/8/18 12:00 returns period 11/8/18 12:00-20:00",
			time.Date(2018, 11, 8, 12, 0, 0, 0, time.UTC),
			NewFloatingPeriod(
				time.Duration(8)*time.Hour, time.Duration(20)*time.Hour, NewApplicableDaysMonStart(0, 4), time.UTC),
			&Period{Start: time.Date(2018, 11, 8, 12, 0, 0, 0, time.UTC), End: time.Date(2018, 11, 8, 20, 0, 0, 0, time.UTC)},
		}, {
			"floating period 8:00-20:00 M-F request time 11/8/18 22:00 returns nil",
			time.Date(2018, 11, 8, 22, 0, 0, 0, time.UTC),
			NewFloatingPeriod(
				time.Duration(8)*time.Hour, time.Duration(20)*time.Hour, NewApplicableDaysMonStart(0, 4), time.UTC),
			nil,
		}, {
			"floating period 8:00-20:00 M-F request time 11/10/18 12:00 returns nil",
			time.Date(2018, 11, 10, 12, 0, 0, 0, time.UTC),
			NewFloatingPeriod(
				time.Duration(8)*time.Hour, time.Duration(20)*time.Hour, NewApplicableDaysMonStart(0, 4), time.UTC),
			nil,
		}, {
			"floating period 17:00-03:00 Th request time 11/16/18 00:00-03:00 returns 11/16/18 00:00-03:00",
			time.Date(2018, 11, 16, 0, 0, 0, 0, time.UTC),
			NewFloatingPeriod(
				time.Duration(17)*time.Hour, time.Duration(3)*time.Hour, ApplicableDays{Thursday: true}, time.UTC),
			&Period{Start: time.Date(2018, 11, 16, 0, 0, 0, 0, time.UTC), End: time.Date(2018, 11, 16, 3, 0, 0, 0, time.UTC)},
		}, {
			"floating period with no applicable days returns nil",
			time.Time{},
			NewFloatingPeriod(0, 0, ApplicableDays{}, time.UTC),
			nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.outcome, test.fp.FromTime(test.t))
		})
	}
}

func TestPeriod_Equals(t *testing.T) {
	chiTz, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	p := NewPeriod(time.Date(2018, 12, 7, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 7, 17, 0, 0, 0, time.UTC))
	tests := []struct {
		name    string
		other   Period
		outcome bool
	}{
		{
			"periods with same starts and ends are equal",
			NewPeriod(time.Date(2018, 12, 7, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 7, 17, 0, 0, 0, time.UTC)),
			true,
		}, {
			"periods with the same start and different ends are not equal",
			NewPeriod(time.Date(2018, 12, 7, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 7, 17, 0, 0, 1, time.UTC)),
			false,
		}, {
			"periods with the different start and same ends are not equal",
			NewPeriod(time.Date(2018, 12, 7, 12, 0, 0, 1, time.UTC), time.Date(2018, 12, 7, 17, 0, 0, 0, time.UTC)),
			false,
		}, {
			"periods with the starts and ends in different times are the same when adjusted",
			NewPeriod(time.Date(2018, 12, 7, 6, 0, 0, 0, chiTz), time.Date(2018, 12, 7, 11, 0, 0, 0, chiTz)),
			true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.outcome, p.Equals(test.other))
		})
	}
}

func TestContinuousPeriod_Intersects(t *testing.T) {
	tests := []struct {
		name           string
		expectedResult bool
		cp             ContinuousPeriod
		p              Period
	}{
		{
			"period equivalent to cp intersects",
			true,
			NewContinuousPeriod(5*time.Hour, 20*time.Hour, time.Thursday, time.Thursday, time.UTC),
			NewPeriod(time.Date(2019, 1, 3, 5, 0, 0, 0, time.UTC), time.Date(2019, 1, 3, 20, 0, 0, 0, time.UTC)),
		}, {
			"period that starts before cp and ends at the same time intersects",
			true,
			NewContinuousPeriod(5*time.Hour, 20*time.Hour, time.Thursday, time.Thursday, time.UTC),
			NewPeriod(time.Date(2019, 1, 3, 4, 0, 0, 0, time.UTC), time.Date(2019, 1, 3, 20, 0, 0, 0, time.UTC)),
		}, {
			"period that ends before cp and starts at the same time intersects",
			true,
			NewContinuousPeriod(5*time.Hour, 20*time.Hour, time.Thursday, time.Thursday, time.UTC),
			NewPeriod(time.Date(2019, 1, 3, 5, 0, 0, 0, time.UTC), time.Date(2019, 1, 3, 10, 0, 0, 0, time.UTC)),
		}, {
			"period that overlaps cp on the same day intersects",
			true,
			NewContinuousPeriod(5*time.Hour, 20*time.Hour, time.Thursday, time.Thursday, time.UTC),
			NewPeriod(time.Date(2019, 1, 3, 4, 0, 0, 0, time.UTC), time.Date(2019, 1, 3, 10, 0, 0, 0, time.UTC)),
		}, {
			"multi-day period that overlaps cp on next day intersects",
			true,
			NewContinuousPeriod(5*time.Hour, 20*time.Hour, time.Thursday, time.Thursday, time.UTC),
			NewPeriod(time.Date(2019, 1, 2, 4, 0, 0, 0, time.UTC), time.Date(2019, 1, 3, 10, 0, 0, 0, time.UTC)),
		}, {
			"period that starts after cp end on the same week but overlaps on the next week intersects",
			true,
			NewContinuousPeriod(5*time.Hour, 20*time.Hour, time.Thursday, time.Thursday, time.UTC),
			NewPeriod(time.Date(2019, 1, 4, 4, 0, 0, 0, time.UTC), time.Date(2019, 1, 10, 10, 0, 0, 0, time.UTC)),
		}, {
			"cp that starts and ends on the same day with end before start intersects period on different day",
			true,
			NewContinuousPeriod(20*time.Hour, 5*time.Hour, time.Thursday, time.Thursday, time.UTC),
			NewPeriod(time.Date(2019, 1, 8, 4, 0, 0, 0, time.UTC), time.Date(2019, 1, 8, 10, 0, 0, 0, time.UTC)),
		}, {
			"period that does not overlap cp does not intersect",
			false,
			NewContinuousPeriod(5*time.Hour, 20*time.Hour, time.Thursday, time.Thursday, time.UTC),
			NewPeriod(time.Date(2019, 1, 8, 4, 0, 0, 0, time.UTC), time.Date(2019, 1, 8, 10, 0, 0, 0, time.UTC)),
		}, {
			"cp that starts and ends on the same day with end before start does not intersect period between end and start",
			false,
			NewContinuousPeriod(20*time.Hour, 5*time.Hour, time.Thursday, time.Thursday, time.UTC),
			NewPeriod(time.Date(2019, 1, 3, 10, 0, 0, 0, time.UTC), time.Date(2019, 1, 3, 18, 0, 0, 0, time.UTC)),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.cp.Intersects(test.p))
		})
	}
}

func TestMonStartToSunStart(t *testing.T) {
	tests := []struct {
		name        string
		monStartDow int
		sunStartDow time.Weekday
		err         bool
	}{
		{
			"0 turns into Monday", 0, time.Monday, false,
		}, {
			"1 turns into Tuesday", 1, time.Tuesday, false,
		}, {
			"2 turns into Wednesday", 2, time.Wednesday, false,
		}, {
			"3 turns into Thursday", 3, time.Thursday, false,
		}, {
			"4 turns into Friday", 4, time.Friday, false,
		}, {
			"5 turns into Saturday", 5, time.Saturday, false,
		}, {
			"6 turns into Sunday", 6, time.Sunday, false,
		}, {
			"anything not 0-6 returns an error", 7, time.Sunday, true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcome, err := MonStartToSunStart(test.monStartDow)
			assert.Equal(t, test.sunStartDow, outcome)
			if test.err {
				assert.Error(t, err)
			}
		})
	}
}

func TestFloatingPeriod_DayApplicable(t *testing.T) {
	chiTz, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	tests := []struct {
		name            string
		t               time.Time
		fp              FloatingPeriod
		expectedOutcome bool
	}{
		{
			"time on day covered by floating period returns true",
			time.Date(2019, 1, 2, 12, 0, 0, 0, time.UTC),
			NewFloatingPeriod(0, 0, ApplicableDays{Wednesday: true}, time.UTC),
			true,
		}, {
			"time on day not covered by floating period returns true",
			time.Date(2019, 1, 3, 12, 0, 0, 0, time.UTC),
			NewFloatingPeriod(0, 0, ApplicableDays{Wednesday: true}, time.UTC),
			false,
		}, {
			"time when adjusted to the period's time zone is covered by floating period returns true",
			time.Date(2019, 1, 3, 2, 0, 0, 0, time.UTC),
			NewFloatingPeriod(0, 0, ApplicableDays{Wednesday: true}, chiTz),
			true,
		}, {
			"time when adjusted to the period's time zone is not covered by floating period returns false",
			time.Date(2019, 1, 3, 2, 0, 0, 0, chiTz),
			NewFloatingPeriod(0, 0, ApplicableDays{Wednesday: true}, time.UTC),
			false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedOutcome, test.fp.DayApplicable(test.t))
		})
	}
}

func TestContinuousPeriod_DayApplicable(t *testing.T) {
	chiTz, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	tests := []struct {
		name            string
		t               time.Time
		cp              ContinuousPeriod
		expectedOutcome bool
	}{
		{
			"time on day covered by continuous period returns true",
			time.Date(2019, 1, 2, 12, 0, 0, 0, time.UTC),
			NewContinuousPeriod(0, 0, time.Wednesday, time.Thursday, time.UTC),
			true,
		}, {
			"time on day not covered by continuous period returns false",
			time.Date(2019, 1, 4, 12, 0, 0, 0, time.UTC),
			NewContinuousPeriod(0, 0, time.Wednesday, time.Thursday, time.UTC),
			false,
		}, {
			"time on day after start dow covered by continuous period that wraps around the week returns true",
			time.Date(2019, 1, 5, 12, 0, 0, 0, time.UTC),
			NewContinuousPeriod(0, 0, time.Friday, time.Wednesday, time.UTC),
			true,
		}, {
			"time on day before start dow covered by continuous period that wraps around the week returns true",
			time.Date(2019, 1, 2, 12, 0, 0, 0, time.UTC),
			NewContinuousPeriod(0, 0, time.Friday, time.Wednesday, time.UTC),
			true,
		}, {
			"time on day not covered by continuous period that wraps around the week returns false",
			time.Date(2019, 1, 3, 12, 0, 0, 0, time.UTC),
			NewContinuousPeriod(0, 0, time.Friday, time.Wednesday, time.UTC),
			false,
		}, {
			"time when adjusted to the period's time zone is covered by the continuous period returns true",
			time.Date(2019, 1, 3, 2, 0, 0, 0, time.UTC),
			NewContinuousPeriod(0, 0, time.Wednesday, time.Wednesday, chiTz),
			true,
		}, {
			"time when adjusted to the period's time zone is not covered by the continuous period returns false",
			time.Date(2019, 1, 2, 22, 0, 0, 0, chiTz),
			NewContinuousPeriod(0, 0, time.Wednesday, time.Wednesday, time.UTC),
			false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedOutcome, test.cp.DayApplicable(test.t))
		})
	}
}

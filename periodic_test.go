// Copyright 2023 SpotHero
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
		p              Period
		o              Period
		name           string
		expectedResult bool
	}{
		{
			name:           "True when start intersects",
			expectedResult: true,
			p:              p,
			o:              NewPeriod(p.Start, p.End.Add(time.Duration(1)*time.Minute)),
		}, {
			name:           "True when end intersects",
			expectedResult: true,
			p:              p,
			o:              NewPeriod(p.Start.Add(-time.Duration(1)*time.Minute), p.End.Add(-time.Duration(1)*time.Minute)),
		}, {
			name:           "True when start and end intersects through containment",
			expectedResult: true,
			p:              p,
			o:              NewPeriod(p.Start, p.End.Add(-time.Duration(1)*time.Minute)),
		}, {
			name:           "true when start and end contain the period",
			expectedResult: true,
			p:              p,
			o:              NewPeriod(p.Start.Add(-time.Duration(1)*time.Minute), p.End.Add(time.Duration(1)*time.Minute)),
		}, {
			name: "False when start and end are before",
			p:    p,
			o:    NewPeriod(p.Start.Add(-time.Duration(2)*time.Minute), p.Start.Add(-time.Duration(1)*time.Minute)),
		}, {
			name: "False when start and end are after",
			p:    p,
			o:    NewPeriod(p.End.Add(time.Duration(1)*time.Minute), p.End.Add(time.Duration(2)*time.Minute)),
		}, {
			name:           "True when start intersects and other end is unbounded",
			expectedResult: true,
			p:              p,
			o:              NewPeriod(p.Start, time.Time{}),
		}, {
			name:           "True when start intersects and end is unbounded",
			expectedResult: true,
			p:              NewPeriod(p.Start, time.Time{}),
			o:              p,
		}, {
			name: "False when end is unbounded and start comes after other end",
			p:    NewPeriod(p.Start, time.Time{}),
			o:    NewPeriod(time.Unix(0, 0), p.Start.Add(-time.Hour)),
		}, {
			name: "False when other end is unbounded and other start comes after end",
			p:    p,
			o:    NewPeriod(p.End.Add(time.Second), time.Time{}),
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
		p              Period
		o              Period
		name           string
		expectedResult bool
	}{
		{
			name: "False when only start intersects",
			p:    p,
			o:    NewPeriod(p.Start, p.End.Add(time.Duration(1)*time.Minute)),
		}, {
			name: "False when only end intersects",
			p:    p,
			o:    NewPeriod(p.Start.Add(-time.Duration(1)*time.Minute), p.End.Add(-time.Duration(1)*time.Minute)),
		}, {
			name:           "True when start and end intersects through containment",
			expectedResult: true,
			p:              p,
			o:              NewPeriod(p.Start, p.End.Add(-time.Duration(1)*time.Minute)),
		}, {
			name: "False when start and end contain the period",
			p:    p,
			o:    NewPeriod(p.Start.Add(-time.Duration(1)*time.Minute), p.End.Add(time.Duration(1)*time.Minute)),
		}, {
			name: "False when start and end are before",
			p:    p,
			o:    NewPeriod(p.Start.Add(-time.Duration(2)*time.Minute), p.Start.Add(-time.Duration(1)*time.Minute)),
		}, {
			name: "False when start and end are after",
			p:    p,
			o:    NewPeriod(p.End.Add(time.Duration(1)*time.Minute), p.End.Add(time.Duration(2)*time.Minute)),
		}, {
			name: "False when start is before and no end",
			p:    NewPeriod(testTime1, time.Time{}),
			o:    NewPeriod(testTime1.Add(-time.Minute), testTime2),
		}, {
			name:           "True when start is after and no end",
			expectedResult: true,
			p:              NewPeriod(testTime1, time.Time{}),
			o:              NewPeriod(testTime1.Add(time.Minute), testTime2),
		}, {
			name: "False when end is after and no start",
			p:    NewPeriod(time.Time{}, testTime2),
			o:    NewPeriod(testTime1, testTime2.Add(time.Minute)),
		}, {
			name:           "True when end is before and no start",
			expectedResult: true,
			p:              NewPeriod(time.Time{}, testTime2),
			o:              NewPeriod(testTime1, testTime2.Add(-time.Minute)),
		}, {
			name:           "True when period has no start and no end",
			expectedResult: true,
			p:              NewPeriod(time.Time{}, time.Time{}),
			o:              NewPeriod(testTime1, testTime2),
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
		p              Period
		o              Period
		name           string
		expectedResult bool
	}{
		{
			name:           "Identical time periods are contained (start)",
			expectedResult: true,
			p:              p,
			o:              p,
		}, {
			name:           "True when start is contained",
			expectedResult: true,
			p:              p,
			o:              NewPeriod(p.Start, p.End.Add(time.Duration(1)*time.Minute)),
		}, {
			name:           "True when end is contained",
			expectedResult: true,
			p:              p,
			o:              NewPeriod(p.Start.Add(-time.Duration(1)*time.Minute), p.End.Add(-time.Duration(1)*time.Minute)),
		}, {
			name:           "True when period is fully contained",
			expectedResult: true,
			p:              p,
			o:              NewPeriod(p.Start.Add(time.Duration(1)*time.Minute), p.End.Add(-time.Duration(1)*time.Minute)),
		}, {
			name: "False when period is fully before",
			p:    p,
			o:    NewPeriod(p.Start.Add(-time.Duration(2)*time.Minute), p.Start.Add(-time.Duration(1)*time.Minute)),
		}, {
			name: "False when period is fully after",
			p:    p,
			o:    NewPeriod(p.End.Add(time.Duration(1)*time.Minute), p.End.Add(time.Duration(2)*time.Minute)),
		}, {
			name:           "True when open starts period start time is before requested time",
			expectedResult: true,
			p:              pos,
			o:              NewPeriod(pos.Start.Add(-time.Duration(1)*time.Minute), pos.Start.AddDate(1, 0, 0)),
		}, {
			name: "False when open starts period start time is after requested time",
			p:    pos,
			o:    NewPeriod(pos.End, pos.End.Add(time.Duration(1)*time.Minute)),
		}, {
			name:           "True when open ends period end time is after requested time",
			expectedResult: true,
			p:              poe,
			o:              NewPeriod(poe.Start.Add(time.Duration(1)*time.Minute), poe.Start.AddDate(2, 0, 0)),
		}, {
			name: "False when open ends period end time is before requested time",
			p:    poe,
			o:    NewPeriod(poe.Start.AddDate(-1, 0, 0), poe.Start.Add(-time.Duration(1)*time.Minute)),
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
		p              Period
		name           string
		d              time.Duration
		expectedResult bool
	}{
		{
			name:           "01/01/2018 05:00 - 01/01/2018 21:00 is less than 24 hours",
			expectedResult: true,
			p:              NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
			d:              time.Duration(24) * time.Hour,
		}, {
			name: "01/01/2018 05:00 - 01/01/2018 21:00 is not less than 16 hours",
			p:    NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
			d:    time.Duration(16) * time.Hour,
		}, {
			name: "01/01/2018 05:00 - 01/01/2018 21:00 is not less than 1 hour",
			p:    NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
			d:    time.Duration(1) * time.Hour,
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
		p              Period
		t              time.Time
		name           string
		expectedResult bool
		endInclusive   bool
	}{
		{
			name:           "Period 01/01/2018 05:00-21:00, request for 05:00 is contained",
			expectedResult: true,
			p:              NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
			t:              time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC),
		}, {
			name: "Period 01/01/2018 05:00-21:00, request for 04:59 is not contained",
			p:    NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
			t:    time.Date(2018, 1, 1, 4, 59, 0, 0, time.UTC),
		}, {
			name:           "01/01/2018 Period 21:00 - 01/02/2018 05:00, request for 21:00 is contained",
			expectedResult: true,
			p:              NewPeriod(time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 5, 0, 0, 0, time.UTC)),
			t:              time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC),
		}, {
			name: "01/01/2018 Period 21:00 - 01/02/2018 05:00, request for 20:59 is not contained",
			p:    NewPeriod(time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 5, 0, 0, 0, time.UTC)),
			t:    time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC),
		}, {
			name:           "Period 0 - 0 contains any time",
			expectedResult: true,
			t:              time.Date(2018, 1, 1, 1, 1, 1, 1, time.UTC),
		}, {
			name:           "Period with only start time contains anything after start",
			expectedResult: true,
			p:              NewPeriod(time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC), time.Time{}),
			t:              time.Date(2018, 1, 1, 1, 1, 1, 1, time.UTC),
		}, {
			name: "Period with only start time does not contain anything before start",
			p:    NewPeriod(time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC), time.Time{}),
			t:    time.Date(2017, 12, 31, 23, 59, 0, 0, time.UTC),
		}, {
			name:           "Period with only end time contains anything before the end",
			expectedResult: true,
			p:              NewPeriod(time.Time{}, time.Date(2018, 1, 1, 1, 1, 1, 1, time.UTC)),
			t:              time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC),
		}, {
			name: "Period with only end time does not contain anything after the end",
			p:    NewPeriod(time.Time{}, time.Date(2018, 1, 1, 1, 1, 1, 1, time.UTC)),
			t:    time.Date(2018, 1, 2, 0, 0, 0, 0, time.UTC),
		}, {
			name: "Period 0 - 01/01/2018 05:00, request for 05:00 is not contained",
			p:    NewPeriod(time.Time{}, time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC)),
			t:    time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC),
		}, {
			name: "Period 01/01/2018 05:00-21:00, request for 21:00 is not contained",
			p:    NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
			t:    time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC),
		},
		{
			name:           "End inclusive period 0 - 01/01/2018 05:00, request for 05:00 is contained",
			expectedResult: true,
			p:              NewPeriod(time.Time{}, time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC)),
			t:              time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC),
			endInclusive:   true,
		}, {
			name:           "End inclusive period 01/01/2018 05:00-21:00, request for 21:00 is contained",
			expectedResult: true,
			p:              NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
			t:              time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC),
			endInclusive:   true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.p.ContainsTime(test.t, test.endInclusive))
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
		times          []time.Time
	}{
		{
			"T1 is returned when T1 and T2 are identical",
			t1,
			[]time.Time{t1, t2},
		}, {
			"T1 is returned when T1 is greater than T2",
			t1,
			[]time.Time{t1, t2.Add(-time.Duration(1) * time.Minute)},
		}, {
			"T2 is returned when T2 is greater than T1",
			t2,
			[]time.Time{t1.Add(-time.Duration(1) * time.Minute), t2},
		}, {
			"zero time is returned when no times are provided",
			time.Time{},
			[]time.Time{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, MaxTime(test.times...))
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
		times          []time.Time
	}{
		{
			"T1 is returned when T1 and T2 are identical",
			t1,
			[]time.Time{t1, t2},
		}, {
			"T2 is returned when T1 is greater than T2",
			t1,
			[]time.Time{t1.Add(time.Duration(1) * time.Minute), t2},
		}, {
			"T1 is returned when T2 is greater than T1",
			t2,
			[]time.Time{t1, t2.Add(time.Duration(1) * time.Minute)},
		}, {
			"zero time is returned when no times are provided",
			time.Time{},
			[]time.Time{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, MinTime(test.times...))
		})
	}
}

func TestNewApplicableDaysMonStart(t *testing.T) {
	continuousDayTests := []struct {
		expectedResult *ApplicableDays
		startDay       int
		endDay         int
	}{
		{expectedResult: &ApplicableDays{true, false, false, false, false, false, false}},
		{endDay: 4, expectedResult: &ApplicableDays{true, true, true, true, true, false, false}},
		{startDay: 5, endDay: 1, expectedResult: &ApplicableDays{true, true, false, false, false, true, true}},
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

func TestApplicableDays_DayApplicable(t *testing.T) {
	allApplicable := ApplicableDays{
		Monday:    true,
		Tuesday:   true,
		Wednesday: true,
		Thursday:  true,
		Friday:    true,
		Saturday:  true,
		Sunday:    true,
	}
	for i := 0; i < DaysInWeek; i++ {
		day := time.Weekday(i)
		t.Run(fmt.Sprintf("%v applicable", day.String()), func(t *testing.T) {
			assert.True(t, allApplicable.DayApplicable(day))
		})
		t.Run(fmt.Sprintf("%v not applicable", day.String()), func(t *testing.T) {
			assert.False(t, ApplicableDays{}.DayApplicable(day))
		})
	}
	t.Run("invalid weekday not applicable", func(t *testing.T) {
		assert.False(t, allApplicable.DayApplicable(time.Weekday(8)))
	})
}

func TestApplicableDays_TimeApplicable(t *testing.T) {
	chiTZ, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	tests := []struct {
		t       time.Time
		l       *time.Location
		name    string
		ad      ApplicableDays
		outcome bool
	}{
		{
			name:    "2018-10-30T22:00 CDT is applicable on Tuesday CDT",
			t:       time.Date(2018, 10, 30, 22, 0, 0, 0, chiTZ),
			l:       chiTZ,
			ad:      ApplicableDays{Tuesday: true},
			outcome: true,
		}, {
			name: "2018-10-30T22:00 CDT is not applicable on Tuesday UTC",
			t:    time.Date(2018, 10, 30, 22, 0, 0, 0, chiTZ),
			l:    time.UTC,
			ad:   ApplicableDays{Tuesday: true},
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

func TestPeriod_Equals(t *testing.T) {
	chiTz, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	p := NewPeriod(time.Date(2018, 12, 7, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 7, 17, 0, 0, 0, time.UTC))
	tests := []struct {
		other   Period
		name    string
		outcome bool
	}{
		{
			name:    "periods with same starts and ends are equal",
			other:   NewPeriod(time.Date(2018, 12, 7, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 7, 17, 0, 0, 0, time.UTC)),
			outcome: true,
		}, {
			name:  "periods with the same start and different ends are not equal",
			other: NewPeriod(time.Date(2018, 12, 7, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 7, 17, 0, 0, 1, time.UTC)),
		}, {
			name:  "periods with the different start and same ends are not equal",
			other: NewPeriod(time.Date(2018, 12, 7, 12, 0, 0, 1, time.UTC), time.Date(2018, 12, 7, 17, 0, 0, 0, time.UTC)),
		}, {
			name:    "periods with the starts and ends in different times are the same when adjusted",
			other:   NewPeriod(time.Date(2018, 12, 7, 6, 0, 0, 0, chiTz), time.Date(2018, 12, 7, 11, 0, 0, 0, chiTz)),
			outcome: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.outcome, p.Equals(test.other))
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

func TestPeriod_IsZero(t *testing.T) {
	tests := []struct {
		p       Period
		name    string
		outcome bool
	}{
		{
			name: "period where start and end are not equal is not zero",
			p:    Period{Start: time.Unix(1, 0), End: time.Unix(5, 0)},
		}, {
			name:    "period where start and end are equal is zero",
			p:       Period{Start: time.Unix(1, 0), End: time.Unix(1, 0)},
			outcome: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.outcome, test.p.IsZero())
		})
	}
}

func TestMergePeriods(t *testing.T) {
	tests := []struct {
		name     string
		periods  []Period
		expected []Period
	}{
		{
			"two overlapping periods returns single merged period",
			[]Period{
				NewPeriod(time.Unix(10, 0), time.Unix(50, 0)),
				NewPeriod(time.Unix(40, 0), time.Unix(90, 0)),
			},
			[]Period{
				NewPeriod(time.Unix(10, 0), time.Unix(90, 0)),
			},
		}, {
			"empty array returns empty array",
			[]Period{},
			[]Period{},
		}, {
			"array with same time periods are merged into one",
			[]Period{
				NewPeriod(time.Unix(10, 0), time.Unix(50, 0)),
				NewPeriod(time.Unix(10, 0), time.Unix(50, 0)),
				NewPeriod(time.Unix(10, 0), time.Unix(50, 0)),
			},
			[]Period{
				NewPeriod(time.Unix(10, 0), time.Unix(50, 0)),
			},
		}, {
			"unsorted array is merged correctly",
			[]Period{
				NewPeriod(time.Unix(90, 0), time.Unix(110, 0)),
				NewPeriod(time.Unix(50, 0), time.Unix(100, 0)),
				NewPeriod(time.Unix(60, 0), time.Unix(70, 0)),
			},
			[]Period{
				NewPeriod(time.Unix(50, 0), time.Unix(110, 0)),
			},
		}, {
			"non-overlapping periods are not merged",
			[]Period{
				NewPeriod(time.Unix(10, 0), time.Unix(30, 0)),
				NewPeriod(time.Unix(40, 0), time.Unix(50, 0)),
				NewPeriod(time.Unix(60, 0), time.Unix(70, 0)),
			},
			[]Period{
				NewPeriod(time.Unix(10, 0), time.Unix(30, 0)),
				NewPeriod(time.Unix(40, 0), time.Unix(50, 0)),
				NewPeriod(time.Unix(60, 0), time.Unix(70, 0)),
			},
		}, {
			"periods with start equal to end are merged",
			[]Period{
				NewPeriod(time.Unix(10, 0), time.Unix(30, 0)),
				NewPeriod(time.Unix(30, 0), time.Unix(50, 0)),
			},
			[]Period{
				NewPeriod(time.Unix(10, 0), time.Unix(50, 0)),
			},
		}, {
			"some overlapping and some non-overlapping are partially merged",
			[]Period{
				NewPeriod(time.Unix(10, 0), time.Unix(40, 0)),
				NewPeriod(time.Unix(30, 0), time.Unix(50, 0)),
				NewPeriod(time.Unix(70, 0), time.Unix(90, 0)),
			},
			[]Period{
				NewPeriod(time.Unix(10, 0), time.Unix(50, 0)),
				NewPeriod(time.Unix(70, 0), time.Unix(90, 0)),
			},
		}, {
			"single input period returns single output period",
			[]Period{
				NewPeriod(time.Unix(10, 0), time.Unix(40, 0)),
			},
			[]Period{
				NewPeriod(time.Unix(10, 0), time.Unix(40, 0)),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(
				t, test.expected, MergePeriods(test.periods))
		})
	}
}

func TestAddDSTAwareDuration(t *testing.T) {
	chiTz, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	tests := []struct {
		t        time.Time
		expected time.Time
		name     string
		d        time.Duration
	}{
		{
			name:     "result time with same timezone offset returns correct time",
			t:        time.Date(2019, 11, 1, 15, 0, 0, 0, chiTz),
			d:        24 * time.Hour,
			expected: time.Date(2019, 11, 2, 15, 0, 0, 0, chiTz),
		}, {
			name:     "time in DST, result in non-DST returns correct time",
			t:        time.Date(2019, 11, 2, 15, 0, 0, 0, chiTz),
			d:        24 * time.Hour,
			expected: time.Date(2019, 11, 3, 15, 0, 0, 0, chiTz),
		}, {
			name:     "time in non-DST, result in DST returns correct time",
			t:        time.Date(2019, 3, 9, 15, 0, 0, 0, chiTz),
			d:        24 * time.Hour,
			expected: time.Date(2019, 3, 10, 15, 0, 0, 0, chiTz),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(
				t, test.expected, AddDSTAwareDuration(test.t, test.d))
		})
	}
}

func TestPeriod_Difference(t *testing.T) {
	tests := []struct {
		name          string
		period, other Period
		expected      []Period
	}{
		{
			"other intersects first part of period",
			NewPeriod(time.Unix(10, 0), time.Unix(50, 0)),
			NewPeriod(time.Unix(5, 0), time.Unix(30, 0)),
			[]Period{
				NewPeriod(time.Unix(30, 0), time.Unix(50, 0)),
			},
		}, {
			"other intersects first part of period, start times equal",
			NewPeriod(time.Unix(10, 0), time.Unix(50, 0)),
			NewPeriod(time.Unix(10, 0), time.Unix(30, 0)),
			[]Period{
				NewPeriod(time.Unix(30, 0), time.Unix(50, 0)),
			},
		}, {
			"other intersects second part of period",
			NewPeriod(time.Unix(10, 0), time.Unix(50, 0)),
			NewPeriod(time.Unix(30, 0), time.Unix(70, 0)),
			[]Period{
				NewPeriod(time.Unix(10, 0), time.Unix(30, 0)),
			},
		}, {
			"other intersects second part of period, end times equal",
			NewPeriod(time.Unix(10, 0), time.Unix(50, 0)),
			NewPeriod(time.Unix(30, 0), time.Unix(50, 0)),
			[]Period{
				NewPeriod(time.Unix(10, 0), time.Unix(30, 0)),
			},
		}, {
			"other bisects period",
			NewPeriod(time.Unix(10, 0), time.Unix(50, 0)),
			NewPeriod(time.Unix(20, 0), time.Unix(30, 0)),
			[]Period{
				NewPeriod(time.Unix(10, 0), time.Unix(20, 0)),
				NewPeriod(time.Unix(30, 0), time.Unix(50, 0)),
			},
		}, {
			"other bisects period, period end is 0",
			NewPeriod(time.Unix(10, 0), time.Time{}),
			NewPeriod(time.Unix(20, 0), time.Unix(30, 0)),
			[]Period{
				NewPeriod(time.Unix(10, 0), time.Unix(20, 0)),
				NewPeriod(time.Unix(30, 0), time.Time{}),
			},
		}, {
			"other bisects period, period start is 0",
			NewPeriod(time.Time{}, time.Unix(50, 0)),
			NewPeriod(time.Unix(20, 0), time.Unix(30, 0)),
			[]Period{
				NewPeriod(time.Time{}, time.Unix(20, 0)),
				NewPeriod(time.Unix(30, 0), time.Unix(50, 0)),
			},
		}, {
			"other intersects period, other end is 0",
			NewPeriod(time.Unix(10, 0), time.Unix(50, 0)),
			NewPeriod(time.Unix(20, 0), time.Time{}),
			[]Period{
				NewPeriod(time.Unix(10, 0), time.Unix(20, 0)),
			},
		}, {
			"other does not intersect period",
			NewPeriod(time.Unix(10, 0), time.Unix(50, 0)),
			NewPeriod(time.Unix(60, 0), time.Unix(80, 0)),
			[]Period{
				NewPeriod(time.Unix(10, 0), time.Unix(50, 0)),
			},
		}, {
			"other is equal to period",
			NewPeriod(time.Unix(10, 0), time.Unix(50, 0)),
			NewPeriod(time.Unix(10, 0), time.Unix(50, 0)),
			[]Period{},
		}, {
			"other envelops period",
			NewPeriod(time.Unix(10, 0), time.Unix(50, 0)),
			NewPeriod(time.Unix(5, 0), time.Unix(60, 0)),
			[]Period{},
		}, {
			"period end is zero and other period end is zero, period start after other start",
			NewPeriod(time.Unix(10, 0), time.Time{}),
			NewPeriod(time.Unix(5, 0), time.Time{}),
			[]Period{},
		}, {
			"period end is zero and other period end is zero, other start after period start",
			NewPeriod(time.Unix(5, 0), time.Time{}),
			NewPeriod(time.Unix(10, 0), time.Time{}),
			[]Period{NewPeriod(time.Unix(5, 0), time.Unix(10, 0))},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(
				t, test.expected, test.period.Difference(test.other))
		})
	}
}

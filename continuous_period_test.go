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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContinuousPeriod_AtDate(t *testing.T) {
	chiTz, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	laTz, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)
	tests := []struct {
		expectedResult Period
		d              time.Time
		name           string
		cp             ContinuousPeriod
	}{
		{
			name:           "CP 0500 M - 1800 F is offset correctly from 2018-10-03T13:13:13Z",
			expectedResult: NewPeriod(time.Date(2018, 10, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 10, 5, 18, 0, 0, 0, time.UTC)),
			cp:             NewContinuousPeriod(5*time.Hour, 18*time.Hour, time.Monday, time.Friday, time.UTC),
			d:              time.Date(2018, 10, 3, 13, 13, 13, 13, time.UTC),
		}, {
			name:           "CP 0500 M - 0400 M is offset correctly from 2018-10-03T13:13:13Z",
			expectedResult: NewPeriod(time.Date(2018, 10, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 10, 8, 4, 0, 0, 0, time.UTC)),
			cp:             NewContinuousPeriod(5*time.Hour, 4*time.Hour, time.Monday, time.Monday, time.UTC),
			d:              time.Date(2018, 10, 3, 13, 13, 13, 13, time.UTC),
		}, {
			name:           "CP 0500 W - 0400 W is offset correctly from 2018-10-03T13:13:13Z",
			expectedResult: NewPeriod(time.Date(2018, 10, 3, 5, 0, 0, 0, time.UTC), time.Date(2018, 10, 10, 4, 0, 0, 0, time.UTC)),
			cp:             NewContinuousPeriod(5*time.Hour, 4*time.Hour, time.Wednesday, time.Wednesday, time.UTC),
			d:              time.Date(2018, 10, 3, 13, 13, 13, 13, time.UTC),
		}, {
			name:           "CP 0400 W - 0500 W is offset correctly from 2018-10-03T13:13:13Z",
			expectedResult: NewPeriod(time.Date(2018, 10, 10, 4, 0, 0, 0, time.UTC), time.Date(2018, 10, 10, 5, 0, 0, 0, time.UTC)),
			cp:             NewContinuousPeriod(4*time.Hour, 5*time.Hour, time.Wednesday, time.Wednesday, time.UTC),
			d:              time.Date(2018, 10, 3, 13, 13, 13, 13, time.UTC),
		}, {
			name:           "CP 0500 TH - 0400 F is offset correctly from 2018-10-03T13:13:13Z",
			expectedResult: NewPeriod(time.Date(2018, 10, 4, 5, 0, 0, 0, time.UTC), time.Date(2018, 10, 5, 4, 0, 0, 0, time.UTC)),
			cp:             NewContinuousPeriod(5*time.Hour, 4*time.Hour, time.Thursday, time.Friday, time.UTC),
			d:              time.Date(2018, 10, 3, 13, 13, 13, 13, time.UTC),
		}, {
			name:           "CP M 0000 - M 0000 is offset correctly from 2018-10-23T1:00:00Z",
			expectedResult: NewPeriod(time.Date(2018, 10, 22, 0, 0, 0, 0, time.UTC), time.Date(2018, 10, 29, 0, 0, 0, 0, time.UTC)),
			cp:             NewContinuousPeriod(0, 0, time.Monday, time.Monday, time.UTC),
			d:              time.Date(2018, 10, 23, 1, 0, 0, 0, time.UTC),
		}, {
			name:           "CP 0500 M - 1800 F PDT is offset correctly from 2018-10-03T13:13:13 CDT",
			expectedResult: NewPeriod(time.Date(2018, 10, 1, 5, 0, 0, 0, laTz), time.Date(2018, 10, 5, 18, 0, 0, 0, laTz)),
			cp:             NewContinuousPeriod(5*time.Hour, 18*time.Hour, time.Monday, time.Friday, laTz),
			d:              time.Date(2018, 10, 3, 13, 13, 13, 13, chiTz),
		}, {
			name:           "CP 1200 Sa - 1200 Su is offset correctly from 2019-1-3T12:00Z",
			expectedResult: NewPeriod(time.Date(2019, 1, 5, 12, 0, 0, 0, time.UTC), time.Date(2019, 1, 6, 12, 0, 0, 0, time.UTC)),
			cp:             NewContinuousPeriod(12*time.Hour, 12*time.Hour, time.Saturday, time.Sunday, time.UTC),
			d:              time.Date(2019, 1, 3, 12, 0, 0, 0, time.UTC),
		}, {
			name:           "CP 1200 Sa - 1200 Su is offset correctly from 2019-1-7T12:00Z",
			expectedResult: NewPeriod(time.Date(2019, 1, 12, 12, 0, 0, 0, time.UTC), time.Date(2019, 1, 13, 12, 0, 0, 0, time.UTC)),
			cp:             NewContinuousPeriod(12*time.Hour, 12*time.Hour, time.Saturday, time.Sunday, time.UTC),
			d:              time.Date(2019, 1, 7, 12, 0, 0, 0, time.UTC),
		}, {
			name:           "CP 0100 W - 1200 F CST is offset correctly from 2019-01-02T02:00Z",
			expectedResult: NewPeriod(time.Date(2019, 1, 2, 1, 0, 0, 0, chiTz), time.Date(2019, 1, 4, 12, 0, 0, 0, chiTz)),
			cp:             NewContinuousPeriod(time.Hour, 12*time.Hour, time.Wednesday, time.Friday, chiTz),
			d:              time.Date(2019, 1, 2, 2, 0, 0, 0, time.UTC),
		}, {
			name:           "CP 0000 Sa - 0000 M UTC is offset correctly from 11/17/18T01:00Z",
			expectedResult: NewPeriod(time.Date(2018, 11, 17, 0, 0, 0, 0, time.UTC), time.Date(2018, 11, 19, 0, 0, 0, 0, time.UTC)),
			cp:             NewContinuousPeriod(0, 0, time.Saturday, time.Monday, time.UTC),
			d:              time.Date(2018, 11, 17, 1, 0, 0, 0, time.UTC),
		}, {
			name:           "CP 0100 Sa - 0000 M UTC is offset correctly from 11/17/18T01:00Z",
			expectedResult: NewPeriod(time.Date(2018, 11, 17, 1, 0, 0, 0, time.UTC), time.Date(2018, 11, 19, 0, 0, 0, 0, time.UTC)),
			cp:             NewContinuousPeriod(time.Hour, 0, time.Saturday, time.Monday, time.UTC),
			d:              time.Date(2018, 11, 17, 0, 30, 0, 0, time.UTC),
		}, {
			name: "CP spanning dst fallback returns correct period",
			// DST change on 2019-11-03
			expectedResult: NewPeriod(time.Date(2019, 11, 1, 6, 0, 0, 0, chiTz), time.Date(2019, 11, 4, 0, 0, 0, 0, chiTz)),
			cp:             NewContinuousPeriod(6*time.Hour, 0, time.Friday, time.Monday, chiTz),
			d:              time.Date(2019, 11, 2, 0, 0, 0, 0, chiTz),
		}, {
			name: "CP spanning dst spring forward returns correct period",
			// DST change on 2019-03-10
			expectedResult: NewPeriod(time.Date(2019, 3, 8, 6, 0, 0, 0, chiTz), time.Date(2019, 3, 11, 0, 0, 0, 0, chiTz)),
			cp:             NewContinuousPeriod(6*time.Hour, 0, time.Friday, time.Monday, chiTz),
			d:              time.Date(2019, 3, 9, 0, 0, 0, 0, chiTz),
		}, {
			name: "CP spanning dst spring forward returns correct period if start time is 0",
			// DST change on 2020-03-10
			expectedResult: NewPeriod(time.Date(2020, 3, 8, 0, 0, 0, 0, chiTz), time.Date(2020, 3, 13, 0, 0, 0, 0, chiTz)),
			cp:             NewContinuousPeriod(0, 0, time.Sunday, time.Friday, chiTz),
			d:              time.Date(2020, 3, 12, 0, 0, 0, 0, chiTz),
		}, {
			name: "CP spanning dst fallback returns correct period if start time is 0",
			// DST change on 2019-11-03
			expectedResult: NewPeriod(time.Date(2019, 11, 3, 0, 0, 0, 0, chiTz), time.Date(2019, 11, 8, 0, 0, 0, 0, chiTz)),
			cp:             NewContinuousPeriod(0, 0, time.Sunday, time.Friday, chiTz),
			d:              time.Date(2019, 11, 2, 0, 0, 0, 0, chiTz),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.cp.AtDate(test.d))
		})
	}
}

func TestContinuousPeriod_Contains(t *testing.T) {
	tests := []struct {
		p              Period
		name           string
		cp             ContinuousPeriod
		expectedResult bool
	}{
		{
			name:           "CP 0500 M - 1800 F contains 2018-10-03T13:13:13Z - 2018-10-04T13:13:13Z",
			expectedResult: true,
			p:              NewPeriod(time.Date(2018, 10, 3, 13, 13, 13, 13, time.UTC), time.Date(2018, 10, 4, 13, 13, 13, 13, time.UTC)),
			cp:             NewContinuousPeriod(5*time.Hour, 18*time.Hour, time.Monday, time.Friday, time.UTC),
		}, {
			name: "CP 0500 M - 1800 F doesnt contain 2018-10-03T13:13:13Z - 2018-10-09T13:13:13Z",
			p:    NewPeriod(time.Date(2018, 10, 3, 13, 13, 13, 13, time.UTC), time.Date(2018, 10, 9, 13, 13, 13, 13, time.UTC)),
			cp:   NewContinuousPeriod(5*time.Hour, 18*time.Hour, time.Monday, time.Friday, time.UTC),
		}, {
			name:           "CP 0500 M - 1800 F contains 2018-10-03T05:00:00Z - 2018-10-07T17:59:59Z",
			expectedResult: true,
			p:              NewPeriod(time.Date(2018, 10, 3, 5, 0, 0, 0, time.UTC), time.Date(2018, 10, 5, 17, 59, 59, 59, time.UTC)),
			cp:             NewContinuousPeriod(5*time.Hour, 18*time.Hour, time.Monday, time.Friday, time.UTC),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.cp.Contains(test.p))
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
			name: "continuous period 8:00-20:00 M-F does not contain 11/10/18 12:00",
			t:    time.Date(2018, 11, 10, 12, 0, 0, 0, time.UTC),
			cp:   NewContinuousPeriod(8*time.Hour, 20*time.Hour, 1, 5, time.UTC),
		}, {
			name:    "continuous period 8:00-20:00 M-F does contain 11/6/18 12:00",
			t:       time.Date(2018, 11, 6, 12, 0, 0, 0, time.UTC),
			cp:      NewContinuousPeriod(8*time.Hour, 20*time.Hour, 1, 5, time.UTC),
			outcome: true,
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
		t       time.Time
		outcome *Period
		name    string
		cp      ContinuousPeriod
	}{
		{
			name:    "continuous period 8:00-20:00 M-F request time 11/8/18 12:00 returns period 11/9/18 12:00-20:00",
			t:       time.Date(2018, 11, 8, 12, 0, 0, 0, time.UTC),
			cp:      NewContinuousPeriod(8*time.Hour, 20*time.Hour, time.Monday, time.Friday, time.UTC),
			outcome: &Period{Start: time.Date(2018, 11, 8, 12, 0, 0, 0, time.UTC), End: time.Date(2018, 11, 9, 20, 0, 0, 0, time.UTC)},
		}, {
			name: "continuous period 8:00-20:00 M-F request time 11/9/18 22:00 returns nil",
			t:    time.Date(2018, 11, 9, 22, 0, 0, 0, time.UTC),
			cp:   NewContinuousPeriod(8*time.Hour, 20*time.Hour, time.Monday, time.Friday, time.UTC),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.outcome, test.cp.FromTime(test.t))
		})
	}
}
func TestContinuousPeriod_Intersects(t *testing.T) {
	tests := []struct {
		p              Period
		name           string
		cp             ContinuousPeriod
		expectedResult bool
	}{
		{
			name:           "period equivalent to cp intersects",
			expectedResult: true,
			cp:             NewContinuousPeriod(5*time.Hour, 20*time.Hour, time.Thursday, time.Thursday, time.UTC),
			p:              NewPeriod(time.Date(2019, 1, 3, 5, 0, 0, 0, time.UTC), time.Date(2019, 1, 3, 20, 0, 0, 0, time.UTC)),
		}, {
			name:           "period that starts before cp and ends at the same time intersects",
			expectedResult: true,
			cp:             NewContinuousPeriod(5*time.Hour, 20*time.Hour, time.Thursday, time.Thursday, time.UTC),
			p:              NewPeriod(time.Date(2019, 1, 3, 4, 0, 0, 0, time.UTC), time.Date(2019, 1, 3, 20, 0, 0, 0, time.UTC)),
		}, {
			name:           "period that ends before cp and starts at the same time intersects",
			expectedResult: true,
			cp:             NewContinuousPeriod(5*time.Hour, 20*time.Hour, time.Thursday, time.Thursday, time.UTC),
			p:              NewPeriod(time.Date(2019, 1, 3, 5, 0, 0, 0, time.UTC), time.Date(2019, 1, 3, 10, 0, 0, 0, time.UTC)),
		}, {
			name:           "period that overlaps cp on the same day intersects",
			expectedResult: true,
			cp:             NewContinuousPeriod(5*time.Hour, 20*time.Hour, time.Thursday, time.Thursday, time.UTC),
			p:              NewPeriod(time.Date(2019, 1, 3, 4, 0, 0, 0, time.UTC), time.Date(2019, 1, 3, 10, 0, 0, 0, time.UTC)),
		}, {
			name:           "multi-day period that overlaps cp on next day intersects",
			expectedResult: true,
			cp:             NewContinuousPeriod(5*time.Hour, 20*time.Hour, time.Thursday, time.Thursday, time.UTC),
			p:              NewPeriod(time.Date(2019, 1, 2, 4, 0, 0, 0, time.UTC), time.Date(2019, 1, 3, 10, 0, 0, 0, time.UTC)),
		}, {
			name:           "period that starts after cp end on the same week but overlaps on the next week intersects",
			expectedResult: true,
			cp:             NewContinuousPeriod(5*time.Hour, 20*time.Hour, time.Thursday, time.Thursday, time.UTC),
			p:              NewPeriod(time.Date(2019, 1, 4, 4, 0, 0, 0, time.UTC), time.Date(2019, 1, 10, 10, 0, 0, 0, time.UTC)),
		}, {
			name:           "cp that starts and ends on the same day with end before start intersects period on different day",
			expectedResult: true,
			cp:             NewContinuousPeriod(20*time.Hour, 5*time.Hour, time.Thursday, time.Thursday, time.UTC),
			p:              NewPeriod(time.Date(2019, 1, 8, 4, 0, 0, 0, time.UTC), time.Date(2019, 1, 8, 10, 0, 0, 0, time.UTC)),
		}, {
			name: "period that does not overlap cp does not intersect",
			cp:   NewContinuousPeriod(5*time.Hour, 20*time.Hour, time.Thursday, time.Thursday, time.UTC),
			p:    NewPeriod(time.Date(2019, 1, 8, 4, 0, 0, 0, time.UTC), time.Date(2019, 1, 8, 10, 0, 0, 0, time.UTC)),
		}, {
			name: "cp that starts and ends on the same day with end before start does not intersect period between end and start",
			cp:   NewContinuousPeriod(20*time.Hour, 5*time.Hour, time.Thursday, time.Thursday, time.UTC),
			p:    NewPeriod(time.Date(2019, 1, 3, 10, 0, 0, 0, time.UTC), time.Date(2019, 1, 3, 18, 0, 0, 0, time.UTC)),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.cp.Intersects(test.p))
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
			name:            "time on day covered by continuous period returns true",
			t:               time.Date(2019, 1, 2, 12, 0, 0, 0, time.UTC),
			cp:              NewContinuousPeriod(0, 0, time.Wednesday, time.Thursday, time.UTC),
			expectedOutcome: true,
		}, {
			name: "time on day not covered by continuous period returns false",
			t:    time.Date(2019, 1, 4, 12, 0, 0, 0, time.UTC),
			cp:   NewContinuousPeriod(0, 0, time.Wednesday, time.Thursday, time.UTC),
		}, {
			name:            "time on day after start dow covered by continuous period that wraps around the week returns true",
			t:               time.Date(2019, 1, 5, 12, 0, 0, 0, time.UTC),
			cp:              NewContinuousPeriod(0, 0, time.Friday, time.Wednesday, time.UTC),
			expectedOutcome: true,
		}, {
			name:            "time on day before start dow covered by continuous period that wraps around the week returns true",
			t:               time.Date(2019, 1, 2, 12, 0, 0, 0, time.UTC),
			cp:              NewContinuousPeriod(0, 0, time.Friday, time.Wednesday, time.UTC),
			expectedOutcome: true,
		}, {
			name: "time on day not covered by continuous period that wraps around the week returns false",
			t:    time.Date(2019, 1, 3, 12, 0, 0, 0, time.UTC),
			cp:   NewContinuousPeriod(0, 0, time.Friday, time.Wednesday, time.UTC),
		}, {
			name:            "time when adjusted to the period's time zone is covered by the continuous period returns true",
			t:               time.Date(2019, 1, 3, 2, 0, 0, 0, time.UTC),
			cp:              NewContinuousPeriod(0, 0, time.Wednesday, time.Wednesday, chiTz),
			expectedOutcome: true,
		}, {
			name: "time when adjusted to the period's time zone is not covered by the continuous period returns false",
			t:    time.Date(2019, 1, 2, 22, 0, 0, 0, chiTz),
			cp:   NewContinuousPeriod(0, 0, time.Wednesday, time.Wednesday, time.UTC),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedOutcome, test.cp.DayApplicable(test.t))
		})
	}
}

func TestNewContinuousPeriod(t *testing.T) {
	cst, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	tests := []struct {
		loc         *time.Location
		expectedLoc *time.Location
		name        string
		s           time.Duration
		e           time.Duration
		sDow        time.Weekday
		eDow        time.Weekday
	}{
		{
			name: "creating a new continuous period works",
			s:    time.Second, e: 2 * time.Second,
			sDow: time.Monday, eDow: time.Thursday,
			loc:         cst,
			expectedLoc: cst,
		}, {
			name: "creating a new continuous period nil time zone loads UTC",
			s:    time.Second, e: 2 * time.Second,
			sDow: time.Friday, eDow: time.Monday,
			expectedLoc: time.UTC,
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

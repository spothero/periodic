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

func TestFloatingPeriod_Contiguous(t *testing.T) {
	tests := []struct {
		name           string
		fp             FloatingPeriod
		expectedResult bool
	}{
		{
			name:           "Floating Period 00:00-00:00 is contiguous",
			expectedResult: true,
			fp: FloatingPeriod{
				Start: time.Duration(0) * time.Hour,
				End:   time.Duration(0) * time.Hour,
			},
		}, {
			name:           "Floating Period 12:34-12:34 is contiguous",
			expectedResult: true,
			fp: FloatingPeriod{
				Start: (time.Duration(12) * time.Hour) + (time.Duration(34) * time.Minute),
				End:   (time.Duration(12) * time.Hour) + (time.Duration(34) * time.Minute),
			},
		}, {
			name: "Floating Period 00:00-00:01 is non-contiguous",
			fp: FloatingPeriod{
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
		expectedResult Period
		d              time.Time
		name           string
		fp             FloatingPeriod
	}{
		{
			name:           "Floating Period 05:00-21:00 at 11/13/2018 01:23:45 returns 11/13/2018 05:00-21:00",
			expectedResult: Period{Start: time.Date(2018, 11, 13, 5, 0, 0, 0, time.UTC), End: time.Date(2018, 11, 13, 21, 00, 0, 0, time.UTC)},
			fp:             FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			d:              time.Date(2018, 11, 13, 1, 23, 45, 59, time.UTC),
		}, {
			name: `Floating Period 21:00-05:00 at 11/13/2018 01:23:45 returns
			11/12/2018 21:00 - 11/13/2018 05:00`,
			expectedResult: Period{Start: time.Date(2018, 11, 12, 21, 0, 0, 0, time.UTC), End: time.Date(2018, 11, 13, 5, 00, 0, 0, time.UTC)},
			fp:             FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			d:              time.Date(2018, 11, 13, 1, 23, 45, 59, time.UTC),
		}, {
			name:           `Floating Period 21:00-05:00 at 11/13/2018 22:00:00 returns 11/13/2018 21:00 - 11/14/2018 05:00`,
			expectedResult: Period{Start: time.Date(2018, 11, 13, 21, 0, 0, 0, time.UTC), End: time.Date(2018, 11, 14, 5, 00, 0, 0, time.UTC)},
			fp:             FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			d:              time.Date(2018, 11, 13, 22, 0, 0, 0, time.UTC),
		}, {
			name:           "Floating Period 15:00-0:00 returns 11/13/2018 15:00 - 11/14/2018 00:00 CST when requested with 11/14/2018 00:30 UTC",
			expectedResult: Period{Start: time.Date(2018, 11, 13, 15, 0, 0, 0, chiTz), End: time.Date(2018, 11, 14, 0, 0, 0, 0, chiTz)},
			fp:             FloatingPeriod{Start: 15 * time.Hour, End: 0, Days: NewApplicableDaysMonStart(0, 6), Location: chiTz},
			d:              time.Date(2018, 11, 14, 0, 30, 0, 0, time.UTC),
		}, {
			name:           "Floating period 09:00-12:00 MWF, request anytime on Tuesday returns period on Wednesday",
			expectedResult: Period{Start: time.Date(2019, 1, 9, 9, 0, 0, 0, time.UTC), End: time.Date(2019, 1, 9, 12, 0, 0, 0, time.UTC)},
			fp:             FloatingPeriod{Start: 9 * time.Hour, End: 12 * time.Hour, Days: ApplicableDays{Monday: true, Wednesday: true, Friday: true}, Location: time.UTC},
			d:              time.Date(2019, 1, 8, 8, 0, 0, 0, time.UTC),
		}, {
			name:           "Floating period 09:00-12:00 MWF, request Monday 13:00 returns period on Wednesday",
			expectedResult: Period{Start: time.Date(2019, 1, 9, 9, 0, 0, 0, time.UTC), End: time.Date(2019, 1, 9, 12, 0, 0, 0, time.UTC)},
			fp:             FloatingPeriod{Start: 9 * time.Hour, End: 12 * time.Hour, Days: ApplicableDays{Monday: true, Wednesday: true, Friday: true}, Location: time.UTC},
			d:              time.Date(2019, 1, 7, 13, 0, 0, 0, time.UTC),
		}, {
			name:           "Floating period 09:00-12:00 MWF, request Monday 12:00 returns period on Wednesday",
			expectedResult: Period{Start: time.Date(2019, 1, 9, 9, 0, 0, 0, time.UTC), End: time.Date(2019, 1, 9, 12, 0, 0, 0, time.UTC)},
			fp:             FloatingPeriod{Start: 9 * time.Hour, End: 12 * time.Hour, Days: ApplicableDays{Monday: true, Wednesday: true, Friday: true}, Location: time.UTC},
			d:              time.Date(2019, 1, 7, 12, 0, 0, 0, time.UTC),
		}, {
			name:           "Floating period 09:00-12:00 M, request Monday 13:00 returns period on next Monday",
			expectedResult: Period{Start: time.Date(2019, 1, 14, 9, 0, 0, 0, time.UTC), End: time.Date(2019, 1, 14, 12, 0, 0, 0, time.UTC)},
			fp:             FloatingPeriod{Start: 9 * time.Hour, End: 12 * time.Hour, Days: ApplicableDays{Monday: true}, Location: time.UTC},
			d:              time.Date(2019, 1, 7, 13, 0, 0, 0, time.UTC),
		}, {
			name:           "Floating period 20:00-02:00 M, request 1:00 Tuesday returns 20:00 M - 02:00 T",
			expectedResult: Period{Start: time.Date(2019, 1, 7, 20, 0, 0, 0, time.UTC), End: time.Date(2019, 1, 8, 2, 0, 0, 0, time.UTC)},
			fp:             FloatingPeriod{Start: 20 * time.Hour, End: 2 * time.Hour, Days: ApplicableDays{Monday: true}, Location: time.UTC},
			d:              time.Date(2019, 1, 8, 1, 0, 0, 0, time.UTC),
		}, {
			name:           "Floating period 20:00-02:00 MTW, request 1:00 Tuesday returns 20:00 M - 02:00 T",
			expectedResult: Period{Start: time.Date(2019, 1, 7, 20, 0, 0, 0, time.UTC), End: time.Date(2019, 1, 8, 2, 0, 0, 0, time.UTC)},
			fp:             FloatingPeriod{Start: 20 * time.Hour, End: 2 * time.Hour, Days: ApplicableDays{Monday: true, Tuesday: true, Wednesday: true}, Location: time.UTC},
			d:              time.Date(2019, 1, 8, 1, 0, 0, 0, time.UTC),
		},
		{
			name:           "Floating period 20:00-02:00 MTW, request 05:00 Tuesday returns 20:00 T - 02:00 W",
			expectedResult: Period{Start: time.Date(2019, 1, 8, 20, 0, 0, 0, time.UTC), End: time.Date(2019, 1, 9, 2, 0, 0, 0, time.UTC)},
			fp:             FloatingPeriod{Start: 20 * time.Hour, End: 2 * time.Hour, Days: ApplicableDays{Monday: true, Tuesday: true, Wednesday: true}, Location: time.UTC},
			d:              time.Date(2019, 1, 8, 5, 0, 0, 0, time.UTC),
		}, {
			name:           "Floating period 20:00-02:00 MT, request 02:00 Tuesday returns 20:00 T - 02:00 W",
			expectedResult: Period{Start: time.Date(2019, 1, 8, 20, 0, 0, 0, time.UTC), End: time.Date(2019, 1, 9, 2, 0, 0, 0, time.UTC)},
			fp:             FloatingPeriod{Start: 20 * time.Hour, End: 2 * time.Hour, Days: ApplicableDays{Monday: true, Tuesday: true}, Location: time.UTC},
			d:              time.Date(2019, 1, 8, 2, 0, 0, 0, time.UTC),
		}, {
			name:           "Floating period 09:00 - 09:00 MW, request 12:00 T returns 09:00 W - 09:00 Th",
			expectedResult: Period{Start: time.Date(2019, 1, 9, 9, 0, 0, 0, time.UTC), End: time.Date(2019, 1, 10, 9, 0, 0, 0, time.UTC)},
			fp:             FloatingPeriod{Start: 9 * time.Hour, End: 9 * time.Hour, Days: ApplicableDays{Monday: true, Wednesday: true}, Location: time.UTC},
			d:              time.Date(2019, 1, 8, 12, 0, 0, 0, time.UTC),
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
		p              Period
		name           string
		fp             FloatingPeriod
		expectedResult bool
	}{
		// Test when all days are valid (starts before ends)
		{
			name:           "Floating Period 05:00-21:00, request for 05:00-20:59 is contained",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:              NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			name:           "Floating Period 05:00-21:00, request for 05:00-21:00 is contained",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:              NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
		}, {
			name: "Floating Period 05:00-21:00, request for 1/1/2018 05:00 - 1/2/2018 20:59 is not contained",
			fp:   FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:    NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 20, 59, 0, 0, time.UTC)),
		},
		// Test when all days are valid (starts AFTER ends)
		{
			name: "Floating Period 21:00-05:00, request for 05:00-20:59 is not contained",
			fp:   FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:    NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			name:           "Floating Period 21:00-05:00, request for 21:00-05:00 is contained",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:              NewPeriod(time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 5, 0, 0, 0, time.UTC)),
		}, {
			name:           "Floating Period 21:00-05:00, request for 1/1/2018 21:00 - 1/2/2018 04:59 is not contained",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:              NewPeriod(time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 4, 59, 0, 0, time.UTC)),
		},
		{
			name: "Floating Period 00:00-00:00, request for 1/1/2018 05:00 - 1/2/2018 20:59 is not contained",
			fp:   FloatingPeriod{Start: 0, End: 0, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:    NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 20, 59, 0, 0, time.UTC)),
		}, {
			name: "Floating Period 23:59-23:59, request for 1/1/2018 05:00 - 1/2/2018 20:59 is not contained since Friday is not applicable",
			fp: FloatingPeriod{
				Start: 23*time.Hour + 59*time.Minute,
				End:   23*time.Hour + 59*time.Minute,
				Days:  NewApplicableDaysMonStart(0, 3), Location: time.UTC,
			},
			p: NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 5, 20, 59, 0, 0, time.UTC)),
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
		p              Period
		name           string
		fp             FloatingPeriod
		expectedResult bool
	}{
		// Test when all days are valid (starts before ends)
		{
			name:           "Floating Period 05:00-21:00, request for 05:00-20:59 is intersected",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:              NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			name:           "Floating Period 05:00-21:00, request for 20:59-05:00 is intersected",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:              NewPeriod(time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC), time.Date(2018, 1, 2, 5, 0, 0, 0, time.UTC)),
		}, {
			name: "Floating Period 05:00-21:00, request for 00:00-4:59 is not intersected",
			fp:   FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:    NewPeriod(time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 4, 59, 0, 0, time.UTC)),
		}, {
			name: "Floating Period 05:00-21:00, request for 21:00-21:01 is not intersected",
			fp:   FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:    NewPeriod(time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 1, 0, 0, time.UTC)),
		},
		// Test when all days are valid (starts AFTER ends)
		{
			name: "Floating Period 21:00-05:00, request for 05:00-20:59 is not intersected",
			fp:   FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:    NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			name:           "Floating Period 21:00-05:00, request for 20:59-05:00 is intersected",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:              NewPeriod(time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC), time.Date(2018, 1, 2, 5, 0, 0, 0, time.UTC)),
		}, {
			name:           "Floating Period 21:00-05:00, request for 00:00-4:59 is intersected",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:              NewPeriod(time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 4, 59, 0, 0, time.UTC)),
		}, {
			name: "Floating Period 21:00-05:00, request for 05:00-05:01 is intersected",
			fp:   FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:    NewPeriod(time.Date(2018, 1, 1, 05, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 05, 1, 0, 0, time.UTC)),
		},
		// Test when we have gaps in days (starts before ends)
		{
			name:           "Floating Period 05:00-21:00, request for Mon 01/01/2018 05:00 - Mon 01/01/2018 20:59 is intersected",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 5), Location: time.UTC},
			p:              NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			name: "Floating Period 05:00-21:00, request for Sun 12/31/2017 05:00 - Sun 12/31/2017 20:59 is not intersected",
			fp:   FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 5), Location: time.UTC},
			p:    NewPeriod(time.Date(2017, 12, 31, 5, 0, 0, 0, time.UTC), time.Date(2017, 12, 31, 20, 59, 0, 0, time.UTC)),
		}, {
			name:           "Floating Period 05:00-21:00, request for Sun 12/31/2017 05:00 - Mon 01/01/2018 20:59 is intersected",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 5), Location: time.UTC},
			p:              NewPeriod(time.Date(2017, 12, 31, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			name:           "Floating Period 05:00-21:00, request for Sat 12/30/2017 20:59 - Sun 12/31/2018 00:00 is intersected",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 5), Location: time.UTC},
			p:              NewPeriod(time.Date(2017, 12, 30, 20, 59, 0, 0, time.UTC), time.Date(2017, 12, 31, 0, 0, 0, 0, time.UTC)),
		},
		// Test when we have gaps in days (starts AFTER ends)
		{
			name: "Floating Period 21:00-05:00, request for Mon 01/01/2018 05:00 - Mon 01/01/2018 20:59 is not intersected",
			fp:   FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 5), Location: time.UTC},
			p:    NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			name: "Floating Period 21:00-05:00, request for Sun 12/31/2017 05:00 - Sun 12/31/2017 20:59 is not intersected",
			fp:   FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 5), Location: time.UTC},
			p:    NewPeriod(time.Date(2017, 12, 31, 5, 0, 0, 0, time.UTC), time.Date(2017, 12, 31, 20, 59, 0, 0, time.UTC)),
		}, {
			name: "Floating Period 21:00-05:00, request for Sun 12/31/2017 05:00 - Mon 01/01/2018 20:59 is not intersected",
			fp:   FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 5), Location: time.UTC},
			p:    NewPeriod(time.Date(2017, 12, 31, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			name:           "Floating Period 21:00-05:00, request for Sat 12/30/2017 20:59 - Sun 12/31/2018 00:00 is intersected",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 5), Location: time.UTC},
			p:              NewPeriod(time.Date(2017, 12, 30, 20, 59, 0, 0, time.UTC), time.Date(2017, 12, 31, 0, 0, 0, 0, time.UTC)),
		}, {
			name:           "Floating Period Days 0-5 00:00-00:00, request for M-F at any time is intersected",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 0, End: 0, Days: NewApplicableDaysMonStart(0, 4), Location: time.UTC},
			p:              NewPeriod(time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2018, 1, 5, 23, 59, 0, 0, time.UTC)),
		}, {
			name: "Floating Period Days 0-5 00:00-00:00, request for Sa-Su at any time is not intersected",
			fp:   FloatingPeriod{Start: 0, End: 0, Days: NewApplicableDaysMonStart(0, 4), Location: time.UTC},
			p:    NewPeriod(time.Date(2018, 1, 6, 0, 0, 0, 0, time.UTC), time.Date(2018, 1, 7, 23, 59, 0, 0, time.UTC)),
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
		t              time.Time
		name           string
		fp             FloatingPeriod
		expectedResult bool
	}{
		{
			name:           "Floating Period 05:00-21:00, request for 05:00 is contained",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			t:              time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC),
		}, {
			name: "Floating Period 05:00-21:00, request for 04:59 is not contained",
			fp:   FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			t:    time.Date(2018, 1, 1, 4, 59, 0, 0, time.UTC),
		}, {
			name:           "Floating Period 21:00-05:00, request for 21:00 is contained",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			t:              time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC),
		}, {
			name: "Floating Period 21:00-05:00, request for 20:59 is not contained",
			fp:   FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			t:    time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC),
		}, {
			name: "Floating Period Tu-Su 21:00-05:00, request for 01/01/2018 20:59 is not contained",
			fp:   FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(1, 6), Location: time.UTC},
			t:    time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC),
		}, {
			name: "Floating Period 05:00-21:00, request for 21:00 is not contained",
			fp:   FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			t:    time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC),
		}, {
			name:           "End inclusive floating Period 05:00-21:00, request for 21:00 is contained",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC, EndInclusive: true},
			t:              time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC),
		}, {
			name:           "End inclusive floating Period 14:00-02:00, request for 02:00 is contained",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 14 * time.Hour, End: 2 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC, EndInclusive: true},
			t:              time.Date(2018, 1, 1, 2, 0, 0, 0, time.UTC),
		}, {
			name:           "End inclusive floating Period 14:00-00:00, request for 00:00 is contained",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 14 * time.Hour, End: 0 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC, EndInclusive: true},
			t:              time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC),
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
		p              Period
		name           string
		fp             FloatingPeriod
		expectedResult bool
	}{
		{
			name:           "Floating Period 05:00-21:00, request for 05:00-20:59 is contained",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:              NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			name:           "Floating Period 05:00-21:00, request for 05:00-21:00 is contained",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:              NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
		}, {
			name: "Floating Period 05:00-21:00, request for 04:59-21:00 is not contained",
			fp:   FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:    NewPeriod(time.Date(2018, 1, 1, 4, 59, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
		}, {
			name:           "Floating Period 21:00-05:00, request for 21:00-04:59 is contained",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:              NewPeriod(time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 4, 59, 0, 0, time.UTC)),
		}, {
			name: "Floating Period 21:00-05:00, request for 20:59-04:59 is not contained",
			fp:   FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:    NewPeriod(time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC), time.Date(2018, 1, 2, 4, 59, 0, 0, time.UTC)),
		}, {
			name:           "Floating Period 21:00-05:00, request for 21:00-05:00 is contained",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:              NewPeriod(time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 5, 0, 0, 0, time.UTC)),
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
		p              Period
		name           string
		fp             FloatingPeriod
		expectedResult bool
	}{
		{
			name:           "Floating Period 05:00-21:00, request for 05:00-16:59 is contained",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:              NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 20, 59, 0, 0, time.UTC)),
		}, {
			name: "Floating Period 05:00-21:00, request for 05:00-21:00 is not contained",
			fp:   FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:    NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC)),
		}, {
			name:           "Floating Period 21:00-05:00, request for 21:00-04:59 is contained",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:              NewPeriod(time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 4, 59, 0, 0, time.UTC)),
		}, {
			name: "Floating Period 21:00-05:00, request for 21:00-05:00 is not contained",
			fp:   FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC},
			p:    NewPeriod(time.Date(2018, 1, 1, 21, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 5, 0, 0, 0, time.UTC)),
		}, {
			name: "Floating Period 21:00-05:00 Tu only, request for M - Tu 22:00-04:00 is not contained",
			fp:   FloatingPeriod{Start: 21 * time.Hour, End: 5 * time.Hour, Days: ApplicableDays{Tuesday: true}, Location: time.UTC},
			p:    NewPeriod(time.Date(2018, 1, 1, 22, 0, 0, 0, time.UTC), time.Date(2018, 1, 2, 4, 0, 0, 0, time.UTC)),
		}, {
			name: "Floating Period 05:00-21:00 Tu only, request for M 12:00 - 17:00 is not contained",
			fp:   FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: ApplicableDays{Tuesday: true}, Location: time.UTC},
			p:    NewPeriod(time.Date(2018, 1, 1, 12, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 17, 0, 0, 0, time.UTC)),
		}, {
			name:           "Floating Period 05:00-21:00 MWF only, request for M 12:00 - F 20:00 is contained",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: ApplicableDays{Monday: true, Wednesday: true, Friday: true}, Location: time.UTC},
			p:              NewPeriod(time.Date(2019, 1, 7, 12, 0, 0, 0, time.UTC), time.Date(2019, 1, 11, 20, 0, 0, 0, time.UTC)),
		}, {
			name:           "End inclusive floating Period 05:00-21:00, request for 05:00-21:00 is contained",
			expectedResult: true,
			fp:             FloatingPeriod{Start: 5 * time.Hour, End: 21 * time.Hour, Days: NewApplicableDaysMonStart(0, 6), Location: time.UTC, EndInclusive: true},
			p:              NewPeriod(time.Date(2018, 1, 1, 5, 0, 0, 0, time.UTC), time.Date(2018, 1, 1, 21, 00, 0, 0, time.UTC)),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedResult, test.fp.ContainsEnd(test.p))
		})
	}
}

func TestFloatingPeriod_FromTime(t *testing.T) {
	tests := []struct {
		t       time.Time
		outcome *Period
		name    string
		fp      FloatingPeriod
	}{
		{
			name:    "floating period 8:00-20:00 M-F request time 11/8/18 12:00 returns period 11/8/18 12:00-20:00",
			t:       time.Date(2018, 11, 8, 12, 0, 0, 0, time.UTC),
			fp:      FloatingPeriod{Start: 8 * time.Hour, End: 20 * time.Hour, Days: NewApplicableDaysMonStart(0, 4), Location: time.UTC},
			outcome: &Period{Start: time.Date(2018, 11, 8, 12, 0, 0, 0, time.UTC), End: time.Date(2018, 11, 8, 20, 0, 0, 0, time.UTC)},
		}, {
			name: "floating period 8:00-20:00 M-F request time 11/8/18 22:00 returns nil",
			t:    time.Date(2018, 11, 8, 22, 0, 0, 0, time.UTC),
			fp:   FloatingPeriod{Start: 8 * time.Hour, End: 20 * time.Hour, Days: NewApplicableDaysMonStart(0, 4), Location: time.UTC},
		}, {
			name: "floating period 8:00-20:00 M-F request time 11/10/18 12:00 returns nil",
			t:    time.Date(2018, 11, 10, 12, 0, 0, 0, time.UTC),
			fp:   FloatingPeriod{Start: 8 * time.Hour, End: 20 * time.Hour, Days: NewApplicableDaysMonStart(0, 4), Location: time.UTC},
		}, {
			name:    "floating period 17:00-03:00 Th request time 11/16/18 00:00-03:00 returns 11/16/18 00:00-03:00",
			t:       time.Date(2018, 11, 16, 0, 0, 0, 0, time.UTC),
			fp:      FloatingPeriod{Start: 17 * time.Hour, End: 3 * time.Hour, Days: ApplicableDays{Thursday: true}, Location: time.UTC},
			outcome: &Period{Start: time.Date(2018, 11, 16, 0, 0, 0, 0, time.UTC), End: time.Date(2018, 11, 16, 3, 0, 0, 0, time.UTC)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.outcome, test.fp.FromTime(test.t))
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
			FloatingPeriod{Start: 0, End: 0, Days: ApplicableDays{Wednesday: true}, Location: time.UTC},
			true,
		}, {
			"time on day not covered by floating period returns true",
			time.Date(2019, 1, 3, 12, 0, 0, 0, time.UTC),
			FloatingPeriod{Start: 0, End: 0, Days: ApplicableDays{Wednesday: true}, Location: time.UTC},
			false,
		}, {
			"time when adjusted to the period's time zone is covered by floating period returns true",
			time.Date(2019, 1, 3, 2, 0, 0, 0, time.UTC),
			FloatingPeriod{Start: 0, End: 0, Days: ApplicableDays{Wednesday: true}, Location: chiTz},
			true,
		}, {
			"time when adjusted to the period's time zone is not covered by floating period returns false",
			time.Date(2019, 1, 3, 2, 0, 0, 0, chiTz),
			FloatingPeriod{Start: 0, End: 0, Days: ApplicableDays{Wednesday: true}, Location: time.UTC},
			false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedOutcome, test.fp.DayApplicable(test.t))
		})
	}
}

func TestNewFloatingPeriod(t *testing.T) {
	cst, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	tests := []struct {
		loc         *time.Location
		expectedLoc *time.Location
		name        string
		s           time.Duration
		e           time.Duration
		d           ApplicableDays
		expectError bool
	}{
		{
			name: "creating a new floating period works",
			s:    time.Second, e: 2 * time.Second,
			d:           ApplicableDays{Monday: true, Friday: true},
			loc:         cst,
			expectedLoc: cst,
		}, {
			name: "creating a new floating period nil time zone loads UTC",
			s:    time.Second, e: 2 * time.Second,
			d:           ApplicableDays{Monday: true, Friday: true},
			expectedLoc: time.UTC,
		}, {
			name: "creating a new floating period without any applicable days returns error", expectError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fp, fpErr := NewFloatingPeriod(test.s, test.e, test.d, test.loc, false)
			if test.expectError {
				require.Error(t, fpErr)
				return
			}
			require.NoError(t, fpErr)
			assert.Equal(t, test.s, fp.Start)
			assert.Equal(t, test.e, fp.End)
			assert.Equal(t, test.d, fp.Days)
			assert.Equal(t, test.expectedLoc, fp.Location)
		})
	}
}

func TestFloatingPeriodConstructionError_Error(t *testing.T) {
	f := FloatingPeriodConstructionError("e")
	assert.Error(t, f)
	assert.Equal(t, "e", f.Error())
}

func TestTwelveHourDisplay(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		input    time.Duration
	}{
		{
			name:     "morning time is converted",
			input:    10 * time.Hour,
			expected: "10:00 AM",
		}, {
			name:     "afternoon time is converted",
			input:    23 * time.Hour,
			expected: "11:00 PM",
		}, {
			name:     "12:00 PM is converted",
			input:    12 * time.Hour,
			expected: "12:00 PM",
		}, {
			name:     "12:00 AM is converted",
			input:    0 * time.Hour,
			expected: "12:00 AM",
		}, {
			name:     "single digit morning hour is converted",
			input:    5 * time.Hour,
			expected: "5:00 AM",
		}, {
			name:     "single digit afternoon hour is converted",
			input:    14 * time.Hour,
			expected: "2:00 PM",
		}, {
			name:     "input in seconds is converted",
			input:    29400 * time.Second,
			expected: "8:10 AM",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, TwelveHourDisplay(test.input))
		})
	}
}

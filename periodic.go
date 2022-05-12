// Copyright 2021 SpotHero
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
	"reflect"
	"sort"
	"time"
)

const (
	// HoursInDay is the number of hours in a single day
	HoursInDay = 24
	// DaysInWeek is the number of days in a week
	DaysInWeek = 7
)

// Period defines a block of time bounded by a start and end.
type Period struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// RecurringPeriod defines an interface for converting periods that represent abstract points in time
// into concrete periods
type RecurringPeriod interface {
	AtDate(date time.Time) Period
	FromTime(t time.Time) *Period
	Contains(period Period) bool
	ContainsTime(t time.Time) bool
	DayApplicable(t time.Time) bool
	Intersects(period Period) bool
}

// ApplicableDays is a structure for storing what days of week something is valid for.
// This is particularly important when schedules are applicable (i.e. hours of operation &
// inventory rules)
type ApplicableDays struct {
	Monday    bool
	Tuesday   bool
	Wednesday bool
	Thursday  bool
	Friday    bool
	Saturday  bool
	Sunday    bool
}

// NewPeriod constructs a new time period from start and end times in a given location
func NewPeriod(start, end time.Time) Period {
	return Period{
		Start: start,
		End:   end,
	}
}

// Intersects returns true if the other time period intersects the Period upon
// which the method was called. Note that if the period's end time is the zero value, it is treated as if
// the time period is unbounded on the end.
func (p Period) Intersects(other Period) bool {
	if p.End.IsZero() && !other.End.IsZero() {
		return p.Start.Before(other.End)
	}
	if !p.End.IsZero() && other.End.IsZero() {
		return other.Start.Before(p.End)
	}
	// Calculate max(starts) < min(ends)
	return MaxTime(p.Start, other.Start).Before(MinTime(p.End, other.End))
}

// Contains returns true if the other time period is contained within the Period
// upon which the method was called. The time period is treated as inclusive on both ends
// eg [p.Start, p.End]
func (p Period) Contains(other Period) bool {
	if p.Start.IsZero() && p.End.IsZero() {
		return true
	}
	s := p.Start.Before(other.Start) || p.Start.Equal(other.Start)
	e := p.End.After(other.End) || p.End.Equal(other.End)
	if p.Start.IsZero() && !p.End.IsZero() {
		return e
	} else if !p.Start.IsZero() && p.End.IsZero() {
		return s
	}
	return s && e
}

// ContainsAny returns true if the other time periods start or end is contained within the Period
// upon which the method was called.
func (p Period) ContainsAny(other Period) bool {
	if p.Start.IsZero() {
		// If the start period is "empty" anything before our end time is contained
		return p.End.After(other.Start)
	} else if p.End.IsZero() {
		// If the end period is "empty" anything after or including our start time is contained
		return p.Start.Before(other.Start) || p.Start.Equal(other.Start)
	}
	// Otherwise, check for inclusion on start and ends times
	s := (p.Start.Before(other.Start) || p.Start.Equal(other.Start)) && p.End.After(other.Start)
	e := p.Start.Before(other.End) && p.End.After(other.End)
	return s || e
}

// Overlaps determines whether two different periods overlap each other, regardless of direction. This differs slightly
// from intersects in that the start from one period can share the same point in time as the end from the other.
func (p Period) Overlaps(other Period) bool {
	return p.ContainsAny(other) || other.ContainsAny(p)
}

// Less returns true if the duration of the period is less than the supplied duration
func (p Period) Less(d time.Duration) bool {
	return p.End.Sub(p.Start) < d
}

// ContainsTime determines if the Period contains the specified time.
func (p Period) ContainsTime(t time.Time, endInclusive bool) bool {
	if p.Start.IsZero() && p.End.IsZero() {
		return true
	} else if !p.Start.IsZero() && p.End.IsZero() {
		return p.Start.Before(t) || p.Start.Equal(t)
	} else if p.Start.IsZero() && !p.End.IsZero() {
		if endInclusive {
			return p.End.After(t) || p.End.Equal(t)
		}

		return p.End.After(t)
	}

	if endInclusive {
		return (p.Start.Before(t) || p.Start.Equal(t)) && (p.End.After(t) || p.End.Equal(t))
	}

	return (p.Start.Before(t) || p.Start.Equal(t)) && p.End.After(t)
}

// Equals returns whether or not two periods represent the same timespan. Periods are equal if their start time
// and end times are the same, even if they are located in different timezones. For example a period from 12:00 - 17:00
// UTC and a period from 7:00 - 12:00 UTC-5 on the same day are considered equal.
func (p Period) Equals(other Period) bool {
	return p.Start.Equal(other.Start) && p.End.Equal(other.End)
}

// IsZero returns whether the period encompasses no time; in other words, the time difference between the start and end
// of the period is zero.
func (p Period) IsZero() bool {
	return p.Start.Sub(p.End) == 0
}

// Difference returns a slice representing the set difference of (p - other). In other words, it will return any
// segments of p that are NOT in other. The possible scenarios and results are:
// * the periods do not intersect - the slice will contain p.
// * the periods intersect but are not fully overlapping - the slice will contain the subset of p that is not contained in other.
// * p fully envelops other - the slice will contain 2 elements: the subsets of p before/after other.
// * other fully envelops p - the slice will be empty
func (p Period) Difference(other Period) []Period {
	result := make([]Period, 0)
	if p.Start.Before(other.Start) {
		var end time.Time
		if p.End.IsZero() {
			end = other.Start
		} else {
			end = MinTime(p.End, other.Start)
		}
		result = append(result, NewPeriod(p.Start, end))
	}
	if (p.End.After(other.End) || p.End.IsZero()) && !other.End.IsZero() {
		result = append(result, NewPeriod(MaxTime(p.Start, other.End), p.End))
	}
	return result
}

// MaxTime returns the maximum of the provided times
func MaxTime(times ...time.Time) time.Time {
	if len(times) == 0 {
		return time.Time{}
	}
	maxTime := times[0]
	for _, t := range times[1:] {
		if t.After(maxTime) {
			maxTime = t
		}
	}
	return maxTime
}

// MinTime returns the minimum of the provided times
func MinTime(times ...time.Time) time.Time {
	if len(times) == 0 {
		return time.Time{}
	}
	minTime := times[0]
	for _, t := range times[1:] {
		if t.Before(minTime) {
			minTime = t
		}
	}
	return minTime
}

// MonStartToSunStart normalizes Monday Start Day of Week (Mon=0, Sun=6) to Sunday Start of Week (Sun=0, Sat=6)
func MonStartToSunStart(dow int) (time.Weekday, error) {
	switch dow {
	case 0:
		return time.Monday, nil
	case 1:
		return time.Tuesday, nil
	case 2:
		return time.Wednesday, nil
	case 3:
		return time.Thursday, nil
	case 4:
		return time.Friday, nil
	case 5:
		return time.Saturday, nil
	case 6:
		return time.Sunday, nil
	}
	return time.Sunday, fmt.Errorf("unknown day of week")
}

// DayApplicable returns whether the given weekday is contained within the set of applicable days.
func (ad ApplicableDays) DayApplicable(d time.Weekday) bool {
	switch d {
	case time.Sunday:
		return ad.Sunday
	case time.Monday:
		return ad.Monday
	case time.Tuesday:
		return ad.Tuesday
	case time.Wednesday:
		return ad.Wednesday
	case time.Thursday:
		return ad.Thursday
	case time.Friday:
		return ad.Friday
	case time.Saturday:
		return ad.Saturday
	}
	return false
}

// TimeApplicable determines if the given timestamp is valid on the associated day of the week in a given timezone
func (ad ApplicableDays) TimeApplicable(t time.Time, location *time.Location) bool {
	wd := t.In(location).Weekday()
	return ad.DayApplicable(wd)
}

// AnyApplicable returns whether or not there are any weekdays that are applicable.
func (ad ApplicableDays) AnyApplicable() bool {
	return ad.Sunday || ad.Monday || ad.Tuesday || ad.Wednesday || ad.Thursday || ad.Friday || ad.Saturday
}

// NewApplicableDaysMonStart translates continuous days of week to a struct with bools representing each
// day of the week. Note that this implementation is dependent on the ordering
// of days of the week in the applicableDaysOfWeek struct. Monday is 0, Sunday is 6.
func NewApplicableDaysMonStart(startDay int, endDay int) ApplicableDays {
	applicableDays := &ApplicableDays{}
	v := reflect.ValueOf(applicableDays).Elem()
	for i := 0; i < 7; i++ {
		var dayApplicable bool
		if startDay <= endDay {
			dayApplicable = startDay <= i && endDay >= i
		} else {
			dayApplicable = startDay <= i || endDay >= i
		}
		v.Field(i).SetBool(dayApplicable)
	}
	return *applicableDays
}

// MergePeriods accepts an array of time periods and will return a new list with intersecting periods merged together
func MergePeriods(periods []Period) []Period {
	sort.Slice(periods, func(i, j int) bool {
		return periods[i].Start.Before(periods[j].Start)
	})
	merged := make([]Period, 0, len(periods))
	for _, period := range periods {
		// If the merged array is empty, simply add the current period and skip to the next iteration
		if len(merged) == 0 {
			merged = append(merged, period)
			continue
		}
		// If the last merged period does not intersect the current period, add the current period to the merged
		// array. If they DO intersect, merge the periods by updating the end time of the last merged period.
		if merged[len(merged)-1].End.Before(period.Start) {
			merged = append(merged, period)
		} else {
			merged[len(merged)-1].End = MaxTime(merged[len(merged)-1].End, period.End)
		}
	}
	return merged
}

// AddDSTAwareDuration will add the given duration to the given time, adjusting for timezone offset changes due to DST and return
// the resulting time. As an example, adding 24 hours to 2019-11-02 15:00:00 -0500 CST will result in 2019-11-02 15:00:00 -0600 CST,
// whereas the time library Add method would result in 2019-11-03 14:00:00 -0600 CST because of the timezone offset change.
func AddDSTAwareDuration(t time.Time, d time.Duration) time.Time {
	result := t.Add(d)
	_, tOffset := t.Zone()
	_, resultOffset := result.Zone()
	return result.Add(time.Duration(tOffset-resultOffset) * time.Second)
}

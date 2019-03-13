// Copyright 2019 SpotHero
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
	"time"
)

// FloatingPeriod defines a period which defines a bound set of time which is applicable
// generically to any given date in a given location, but is not associated with any particular date.
type FloatingPeriod struct {
	// Time since midnight on the that the period begins
	Start time.Duration
	// Time since midnight on the that the period ends
	End time.Duration
	// Days on which the period applies
	Days ApplicableDays
	// Timezone where the period is located
	Location *time.Location
	// Indicates whether the end time is included in the period
	EndInclusive bool
}

// FloatingPeriodConstructionError is the error type returned if there is a problem constructing a FloatingPeriod
type FloatingPeriodConstructionError string

// Error implements the error interface for FloatingPeriodConstructionError
func (f FloatingPeriodConstructionError) Error() string {
	return string(f)
}

// NewFloatingPeriod constructs a new floating period
func NewFloatingPeriod(start, end time.Duration, days ApplicableDays, location *time.Location, endInclusive bool) (FloatingPeriod, error) {
	if !days.AnyApplicable() {
		return FloatingPeriod{}, FloatingPeriodConstructionError("floating period must have at least 1 applicable day")
	}
	l := location
	if location == nil {
		l = time.UTC
	}
	return FloatingPeriod{
		Start:        start,
		End:          end,
		Days:         days,
		Location:     l,
		EndInclusive: endInclusive,
	}, nil
}

// Contiguous returns true if starts time is equal to end time. It does not consider applicable
// days.
func (fp FloatingPeriod) Contiguous() bool {
	return fp.Start == fp.End
}

// AtDate returns the FloatingPeriod offset around the given date. If the date given is contained in a floating
// period, the period containing the date is the period that is returned. If the date given is not contained in a
// floating period, the period that is returned is the next occurrence of the floating period. Note that
// containment is inclusive on the continuous period start time but not on the end time.
func (fp FloatingPeriod) AtDate(date time.Time) Period {
	dateInLoc := date.In(fp.Location)
	midnight := time.Date(dateInLoc.Year(), dateInLoc.Month(), dateInLoc.Day(), 0, 0, 0, 0, fp.Location)
	durationSinceMidnight := dateInLoc.Sub(midnight)
	var scanForNextRecurrence bool
	if fp.Start >= fp.End {
		// The floating period spills over into the next day: if the given date is closer to midnight than the
		// end of the floating period, we actually want to check if the floating period was applicable on
		// the previous day. If it was not, we need to scan for the next recurrence of the floating period.
		if (durationSinceMidnight < fp.End) || (durationSinceMidnight == fp.End && fp.EndInclusive) {
			midnight = midnight.AddDate(0, 0, -1)
		}
		scanForNextRecurrence = !fp.Days.TimeApplicable(midnight, fp.Location)
	} else {
		// The start and end of the floating period occurs on the same day, so we only need to scan for the
		// next recurrence if the floating period is not applicable on the current day or if the time since midnight
		// of the given date comes after the end of the floating period.
		scanForNextRecurrence = !fp.Days.TimeApplicable(midnight, fp.Location)

		if fp.EndInclusive {
			scanForNextRecurrence = scanForNextRecurrence || durationSinceMidnight > fp.End
		} else {
			scanForNextRecurrence = scanForNextRecurrence || durationSinceMidnight >= fp.End
		}
	}

	// Scan until a day on which the floating period is applicable is found
	if scanForNextRecurrence {
		for i := 0; i < DaysInWeek; i++ {
			midnight = midnight.AddDate(0, 0, 1)
			if fp.Days.TimeApplicable(midnight, fp.Location) {
				break
			}
		}
	}

	if fp.Start >= fp.End {
		return Period{Start: midnight.Add(fp.Start), End: midnight.AddDate(0, 0, 1).Add(fp.End)}
	}
	return Period{Start: midnight.Add(fp.Start), End: midnight.Add(fp.End)}
}

// FromTime returns a period that extends from a given start time to the end of the floating period, or nil
// if the start time does not fall within the floating period
func (fp FloatingPeriod) FromTime(t time.Time) *Period {
	p := fp.AtDate(t)
	if !p.ContainsTime(t, false) {
		return nil
	}
	fromPeriod := NewPeriod(t, p.End)
	return &fromPeriod
}

// Contains determines if the FloatingPeriod contains the specified Period.
func (fp FloatingPeriod) Contains(period Period) bool {
	atDate := fp.AtDate(period.Start)
	return fp.DayApplicable(atDate.Start) && atDate.Contains(period)
}

// ContainsTime determines if the FloatingPeriod contains the specified time, excluding the end time of the period.
func (fp FloatingPeriod) ContainsTime(t time.Time) bool {
	return fp.AtDate(t).ContainsTime(t, fp.EndInclusive)
}

// Intersects determines if the FloatingPeriod intersects the specified Period.
func (fp FloatingPeriod) Intersects(period Period) bool {
	return fp.AtDate(period.Start).Intersects(period)
}

// ContainsStart determines if the FloatingPeriod contains the start of a given period. Note that
// this function is a convenience function equivalent to `fp.ContainsTime(period.Start)`.
func (fp FloatingPeriod) ContainsStart(period Period) bool {
	return fp.ContainsTime(period.Start)
}

// ContainsEnd determines if the FloatingPeriod contains the end of a given period. Note that this function is a
// convenience function  equivalent to `fp.ContainsTime(period.End)`.
func (fp FloatingPeriod) ContainsEnd(period Period) bool {
	return fp.ContainsTime(period.End)
}

// DayApplicable returns whether or not the given time falls on a day during which the floating period is applicable.
func (fp FloatingPeriod) DayApplicable(t time.Time) bool {
	return fp.Days.TimeApplicable(t, fp.Location)
}

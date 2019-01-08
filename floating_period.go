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
	"fmt"
	"time"
)

// FloatingPeriod defines a period which defines a bound set of time which is applicable
// generically to any given date in a given location, but is not associated with any particular date.
type FloatingPeriod struct {
	start    time.Duration
	end      time.Duration
	days     ApplicableDays
	location *time.Location
}

// NewFloatingPeriod constructs a new floating period
func NewFloatingPeriod(start, end time.Duration, days ApplicableDays, location *time.Location) (FloatingPeriod, error) {
	if !days.AnyApplicable() {
		return FloatingPeriod{}, fmt.Errorf("floating period must have at least 1 applicable day")
	}
	l := location
	if location == nil {
		l = time.UTC
	}
	return FloatingPeriod{
		start:    start,
		end:      end,
		days:     days,
		location: l,
	}, nil
}

// Contiguous returns true if starts time is equal to end time. It does not consider applicable
// days.
func (fp FloatingPeriod) Contiguous() bool {
	return fp.start == fp.end
}

// AtDate returns the FloatingPeriod offset around the given date. If the date given is contained in a floating
// period, the period containing the date is the period that is returned. If the date given is not contained in a
// floating period, the period that is returned is the next occurrence of the floating period. Note that
// containment is inclusive on the continuous period start time but not on the end time.
func (fp FloatingPeriod) AtDate(date time.Time) Period {
	dateInLoc := date.In(fp.location)
	midnight := time.Date(dateInLoc.Year(), dateInLoc.Month(), dateInLoc.Day(), 0, 0, 0, 0, fp.location)
	durationSinceMidnight := dateInLoc.Sub(midnight)
	var scanForNextRecurrence bool
	if fp.start >= fp.end {
		// The floating period spills over into the next day: if the given date is closer to midnight than the
		// end of the floating period, we actually want to check if the floating period was applicable on
		// the previous day. If it was not, we need to scan for the next recurrence of the floating period.
		if durationSinceMidnight < fp.end {
			midnight = midnight.AddDate(0, 0, -1)
		}
		scanForNextRecurrence = !fp.days.TimeApplicable(midnight, fp.location)
	} else {
		// The start and end of the floating period occurs on the same day, so we only need to scan for the
		// next recurrence if the floating period is not applicable on the current day or if the time since midnight
		// of the given date comes after the end of the floating period.
		scanForNextRecurrence = !fp.days.TimeApplicable(midnight, fp.location) || durationSinceMidnight >= fp.end
	}

	// Scan until a day on which the floating period is applicable is found
	if scanForNextRecurrence {
		for i := 0; i < DaysInWeek; i++ {
			midnight = midnight.AddDate(0, 0, 1)
			if fp.days.TimeApplicable(midnight, fp.location) {
				break
			}
		}
	}

	if fp.start >= fp.end {
		return Period{Start: midnight.Add(fp.start), End: midnight.AddDate(0, 0, 1).Add(fp.end)}
	}
	return Period{Start: midnight.Add(fp.start), End: midnight.Add(fp.end)}
}

// FromTime returns a period that extends from a given start time to the end of the floating period, or nil
// if the start time does not fall within the floating period
func (fp FloatingPeriod) FromTime(t time.Time) *Period {
	p := fp.AtDate(t)
	if !p.ContainsTime(t) {
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

// ContainsTime determines if the FloatingPeriod contains the specified time.
func (fp FloatingPeriod) ContainsTime(t time.Time) bool {
	if !fp.DayApplicable(t) {
		return false
	}
	return fp.AtDate(t).ContainsTime(t)
}

// Intersects determines if the FloatingPeriod intersects the specified Period.
func (fp FloatingPeriod) Intersects(period Period) bool {
	return fp.AtDate(period.Start).Intersects(period)
}

// ContainsStart determines if the FloatingPeriod contains the start of a given period. Note that
// this function is a convenience function is equivalent to `fp.containsTime(period.Start)`.
func (fp FloatingPeriod) ContainsStart(period Period) bool {
	return fp.ContainsTime(period.Start)
}

// ContainsEnd determines if the FloatingPeriod contains the end of a given period
func (fp FloatingPeriod) ContainsEnd(period Period) bool {
	return fp.AtDate(period.Start).ContainsTime(period.End)
}

// DayApplicable returns whether or not the given time falls on a day during which the floating period is applicable.
func (fp FloatingPeriod) DayApplicable(t time.Time) bool {
	return fp.days.TimeApplicable(t, fp.location)
}

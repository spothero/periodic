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

// ContinuousPeriod defines a period which defines a block of time in a given location, bounded by a week, which may
// span multiple days.
type ContinuousPeriod struct {
	start    time.Duration
	end      time.Duration
	startDOW time.Weekday
	endDOW   time.Weekday
	location *time.Location
}

// NewContinuousPeriod constructs a new continuous period
func NewContinuousPeriod(start, end time.Duration, startDow, endDow time.Weekday, location *time.Location) ContinuousPeriod {
	l := location
	if location == nil {
		l = time.UTC
	}
	return ContinuousPeriod{
		start:    start,
		end:      end,
		startDOW: startDow,
		endDOW:   endDow,
		location: l,
	}
}

// AtDate returns the ContinuousPeriod offset around the given date. If the date given is contained in a continuous
// period, the period containing d is the period that is returned. If the date given is not contained in a
// continuous period, the period that is returned is the next occurrence of the continuous period. Note that
// containment is inclusive on the continuous period start time but not on the end time.
func (cp ContinuousPeriod) AtDate(d time.Time) Period {
	var offsetDate Period
	var startDay time.Time
	dLoc := d.In(cp.location)

	// determine whether we should be looking for the next period or a current one -- findCurrent is true if
	// the continuous period overlaps itself, the given date occurs on a date not covered by the continuous period,
	// or the date is after the end time of the continuous period.
	var findCurrent bool
	if cp.startDOW == cp.endDOW && cp.start >= cp.end {
		// If start comes before end on the same day, then the continuous period overlaps itself so any date that
		// is contained within the period
		findCurrent = true
	} else {
		findCurrent = cp.DayApplicable(dLoc)
		sinceMidnight := dLoc.Sub(time.Date(dLoc.Year(), dLoc.Month(), dLoc.Day(), 0, 0, 0, 0, dLoc.Location()))
		if cp.endDOW == dLoc.Weekday() {
			// If the date is the same day of week as when the continuous period ends, it is within the period
			// if it is fewer hours from midnight than the end time of the continuous period.
			findCurrent = findCurrent && sinceMidnight < cp.end
		}
	}

	var offset time.Duration
	if cp.startDOW <= dLoc.Weekday() {
		if findCurrent {
			// offset to the beginning of the current period or the start of the period on the same day
			offset = time.Duration(HoursInDay*(dLoc.Weekday()-cp.startDOW)) * time.Hour
		} else {
			// offset to the beginning of the next period
			offset = time.Duration(HoursInDay*(dLoc.Weekday()-(DaysInWeek+cp.startDOW))) * time.Hour
		}
		startDay = dLoc.Add(-offset)
		offsetDate.Start = time.Date(startDay.Year(), startDay.Month(), startDay.Day(), 0, 0, 0, 0, cp.location)
		offsetDate.Start = offsetDate.Start.Add(cp.start)
	} else {
		if findCurrent {
			// offset to the beginning of the current period or the start of the period on the same day
			offset = time.Duration(HoursInDay*(dLoc.Weekday()+(DaysInWeek-cp.startDOW))) * time.Hour
		} else {
			// offset to the beginning of the next period
			offset = time.Duration(HoursInDay*(dLoc.Weekday()-cp.startDOW)) * time.Hour
		}
		startDay = dLoc.Add(-offset)
		offsetDate.Start = time.Date(startDay.Year(), startDay.Month(), startDay.Day(), 0, 0, 0, 0, cp.location)
		offsetDate.Start = offsetDate.Start.Add(cp.start)
	}

	if cp.endDOW > cp.startDOW {
		endDay := startDay.Add(time.Duration(HoursInDay*(cp.endDOW-cp.startDOW)) * time.Hour)
		offsetDate.End = time.Date(endDay.Year(), endDay.Month(), endDay.Day(), 0, 0, 0, 0, cp.location)
		offsetDate.End = offsetDate.End.Add(cp.end)
	} else if cp.endDOW < cp.startDOW {
		endDay := startDay.Add(time.Duration(HoursInDay*((DaysInWeek-cp.startDOW)+cp.endDOW)) * time.Hour)
		offsetDate.End = time.Date(endDay.Year(), endDay.Month(), endDay.Day(), 0, 0, 0, 0, cp.location)
		offsetDate.End = offsetDate.End.Add(cp.end)
	} else {
		endDay := startDay
		if cp.start >= cp.end {
			endDay = endDay.Add(time.Duration(DaysInWeek*HoursInDay) * time.Hour)
		}
		offsetDate.End = time.Date(endDay.Year(), endDay.Month(), endDay.Day(), 0, 0, 0, 0, cp.location)
		offsetDate.End = offsetDate.End.Add(cp.end)
	}
	return offsetDate
}

// FromTime returns a period that extends from a given start time to the end of the continuous period, or nil
// if the start time does not fall within the continuous period
func (cp ContinuousPeriod) FromTime(t time.Time) *Period {
	p := cp.AtDate(t)
	if !p.ContainsTime(t) {
		return nil
	}
	fromPeriod := NewPeriod(t, p.End)
	return &fromPeriod
}

// Contains determines if the ContinuousPeriod contains the specified Period.
func (cp ContinuousPeriod) Contains(period Period) bool {
	return cp.AtDate(period.Start).Contains(period)
}

// ContainsTime determines if the continuous period contains the specified time.
func (cp ContinuousPeriod) ContainsTime(t time.Time) bool {
	return cp.AtDate(t).ContainsTime(t)
}

// Intersects returns whether or not the given period has any overlap with any occurrence of a ContinuousPeriod.
func (cp ContinuousPeriod) Intersects(period Period) bool {
	return cp.AtDate(period.Start).Intersects(period)
}

// DayApplicable returns whether or not the given time falls within a day covered by the continuous period.
func (cp ContinuousPeriod) DayApplicable(t time.Time) bool {
	wd := t.In(cp.location).Weekday()
	if cp.startDOW <= cp.endDOW {
		return wd >= cp.startDOW && wd <= cp.endDOW
	}
	return (wd >= cp.startDOW && wd <= time.Saturday) || (wd >= time.Sunday && wd <= cp.endDOW)
}

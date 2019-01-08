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
	"reflect"
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
	Start time.Time
	End   time.Time
}

// FloatingPeriod defines a period which defines a bound set of time which is applicable
// generically to any given date in a given location, but is not associated with any particular date.
type FloatingPeriod struct {
	start    time.Duration
	end      time.Duration
	days     ApplicableDays
	location *time.Location
}

// ContinuousPeriod defines a period which defines a block of time in a given location, bounded by a week, which may
// span multiple days.
type ContinuousPeriod struct {
	start    time.Duration
	end      time.Duration
	startDOW time.Weekday
	endDOW   time.Weekday
	location *time.Location
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

// Less returns true if the duration of the period is less than the supplied duration
func (p Period) Less(d time.Duration) bool {
	return p.End.Sub(p.Start) < d
}

// Equals returns whether or not two periods represent the same timespan. Periods are equal if their start time
// and end times are the same, even if they are located in different timezones. For example a period from 12:00 - 17:00
// UTC and a period from 7:00 - 12:00 UTC-5 on the same day are considered equal.
func (p Period) Equals(other Period) bool {
	return p.Start.Equal(other.Start) && p.End.Equal(other.End)
}

// MaxTime returns the maximum of two timestamps, or the first timestamp if equal
func MaxTime(t1 time.Time, t2 time.Time) time.Time {
	if t2.After(t1) {
		return t2
	}
	return t1
}

// MinTime returns the minimum of two timestamps, or the first timestamp if equal
func MinTime(t1 time.Time, t2 time.Time) time.Time {
	if t2.Before(t1) {
		return t2
	}
	return t1
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

// TimeApplicable determines if the given timestamp is valid on the associated day of the week in a given timezone
func (ad ApplicableDays) TimeApplicable(t time.Time, location *time.Location) bool {
	wd := t.In(location).Weekday()
	switch wd {
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
	default:
		return ad.Saturday
	}
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

// Contains determines if the FloatingPeriod contains the specified Period.
func (fp FloatingPeriod) Contains(period Period) bool {
	atDate := fp.AtDate(period.Start)
	return fp.DayApplicable(atDate.Start) && atDate.Contains(period)
}

// ContainsTime determines if the Period contains the specified time.
func (p Period) ContainsTime(t time.Time) bool {
	if p.Start.IsZero() && p.End.IsZero() {
		return true
	} else if !p.Start.IsZero() && p.End.IsZero() {
		return p.Start.Before(t) || p.Start.Equal(t)
	} else if p.Start.IsZero() && !p.End.IsZero() {
		return p.End.After(t) || p.End.Equal(t)
	}
	return (p.Start.Before(t) || p.Start.Equal(t)) && p.End.After(t)
}

// ContainsTime determines if the FloatingPeriod contains the specified time.
func (fp FloatingPeriod) ContainsTime(t time.Time) bool {
	if !fp.DayApplicable(t) {
		return false
	}
	return fp.AtDate(t).ContainsTime(t)
}

// ContainsTime determines if the continuous period contains the specified time.
func (cp ContinuousPeriod) ContainsTime(t time.Time) bool {
	return cp.AtDate(t).ContainsTime(t)
}

// Intersects determines if the FloatingPeriod intersects the specified Period.
func (fp FloatingPeriod) Intersects(period Period) bool {
	return fp.AtDate(period.Start).Intersects(period)
}

// Intersects returns whether or not the given period has any overlap with any occurrence of a ContinuousPeriod.
func (cp ContinuousPeriod) Intersects(period Period) bool {
	return cp.AtDate(period.Start).Intersects(period)
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

// DayApplicable returns whether or not the given time falls within a day covered by the continuous period.
func (cp ContinuousPeriod) DayApplicable(t time.Time) bool {
	wd := t.In(cp.location).Weekday()
	if cp.startDOW <= cp.endDOW {
		return wd >= cp.startDOW && wd <= cp.endDOW
	}
	return (wd >= cp.startDOW && wd <= time.Saturday) || (wd >= time.Sunday && wd <= cp.endDOW)
}

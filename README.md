# Time Period Calculations for Go

[![GoDoc](https://godoc.org/github.com/spothero/periodic?status.svg)](https://godoc.org/github.com/spothero/periodic)
[![Build Status](https://circleci.com/gh/spothero/periodic/tree/master.svg?style=shield)](https://circleci.com/gh/spothero/periodic/tree/master)
[![codecov](https://codecov.io/gh/spothero/periodic/branch/master/graph/badge.svg)](https://codecov.io/gh/spothero/periodic)
[![Go Report Card](https://goreportcard.com/badge/github.com/spothero/periodic)](https://goreportcard.com/report/github.com/spothero/periodic)

Periodic provides an API for performing calculations on periods of time defined by a discrete start and end time as
well as recurring time periods.

Common use-cases for periodic:
* Representing recurring blocks of time; for example, hours of operation of a store or weekly calendar events.
* Determining if two periods of time intersect, overlap, contain, or are exclusive of each other.
* Storing a set objects with associated time periods and querying for objects that occur within another
  time period.

## Quick Start

```
go get github.com/spothero/periodic
```
or
```
dep ensure -add github.com/spothero/periodic
```

*********************************************************************************
️❗️ While this library is fully-tested and ready for production use, it is pre-1.0
and its API may change from release-to-release without prior notice.
*********************************************************************************

## Overview

Full API documentation is in [GoDoc](https://godoc.org/github.com/spothero/periodic)

### Period
`Period` is a basic type for storing discrete periods of time. Period consists of a start and end time and
has methods for checking for intersection, containment, and the like. Periods can also represent open-ended
periods of time. A `Period` with a zero-value start represents an open-ended time period with a discrete end time;
likewise a `Period` with a zero-value end time represents an open ended period with a discrete start time that ends
at infinity.

### Continuous Period
`ContinuousPeriod` is a data type that represents recurring blocks of time that may span multiple days. For example,
a `ContinuousPeriod` may be defined as "Monday at 9 am to Friday at 5 pm". `ContinuousPeriod` contains methods
for translating the abstract block of time into a real, defined time period, as well as methods for checking
membership.

### Floating Period
`FloatingPeriod` is a data type for represents recurring blocks of time that float from day-to-day. For example,
a `FloatingPeriod` may be defined as "every Monday, Wednesday, and Friday from 9 am to 5 pm". Like `ContinuousPeriod`,
`FloatingPeriod` contains methods for translating the abstract block of time into real time periods as well as
methods for checking membership.

### RecurringPeriod
This library defines an interface named `RecurringPeriod` which is implemented by both `ContinuousPeriod` and
`FloatingPeriod` so that the two types may be used interchangeably.

### PeriodCollection
`PeriodCollection` is a data structure for storing `Period`s and objects associated with those time periods.
Once populated, callers can quickly query for all stored objects whose associated time period intersects
with another period. `PeriodCollection` is backed by a self-balancing binary tree so query performance is at least as
good as linear time, but should approach logarithmic time in the average case.

## Linting

Run the linter using the command `make lint`.

A common linting error is the `fieldalignment` warning from the `govet` analyzer. `fieldalignment` errors arise when the order of a struct’s fields could be arranged differently to optimize the amount of allocated memory.

Imagine the following struct:
```
type MyObject struct {
    myBool   bool
    myString string
}
```

Running the linter would produce this output:
```
>> make lint
golangci-lint run

main.go:16:15: fieldalignment: struct with 16 pointer bytes could be 8 (govet)
type MyObject struct {
              ^
make: *** [lint] Error 1
```

The struct is more optimally arranged as:
```
type MyObject struct {
    myString string
    myBool   bool
}
```

A `fieldalignment` command line tool exists to help optimally arrange all the structs in a given file or package. Note that this tool will remove all existing comments within any structs it rearranges. Be sure to manually re-add any deleted comments after running the command.

Installation:
```
go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@latest
```

Utilization:
```
fieldalignment -fix {PATH_TO_FILE_OR_PACKAGE}
```

## License
Apache 2

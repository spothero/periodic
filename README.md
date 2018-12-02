# Time Period Calculations for Go

[![GoDoc](https://godoc.org/github.com/spothero/periodic?status.svg)](https://godoc.org/github.com/spothero/periodic)
[![Build Status](https://travis-ci.org/spothero/periodic.png?branch=master)](https://travis-ci.org/spothero/periodic)
[![codecov](https://codecov.io/gh/spothero/periodic/branch/master/graph/badge.svg)](https://codecov.io/gh/spothero/periodic)
[![Go Report Card](https://goreportcard.com/badge/github.com/spothero/periodic)](https://goreportcard.com/report/github.com/spothero/periodic)

Periodic provides a simple API for performing calculations around two or more points of time.

Common use-cases for periodic:
* Representing hours of operation for a store, facility, or otherwise
* Relating application functionality to a block of time
* Determining if two periods of time intersect, overlap, contain, or are exclusive of each other

This library provides the following:
* Closed-period analysis
* Open-period analysis
* Period analysis relative to a fixed set of time
* Period analysis unbound to an period in time
* Recurring period analysis
* Continuous period analysis

Beyond providing representations for the above concepts, the library also provides functionality
for interoperating between period-types.

API documentation and examples can be found in the [GoDoc](https://godoc.org/github.com/spothero/periodic)

## License
Apache 2

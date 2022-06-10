// Copyright 2022 SpotHero
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
)

func TestNode_isLeftChild(t *testing.T) {
	root := &node{}
	left := &node{parent: root}
	right := &node{parent: root}
	root.left = left
	root.right = right
	tests := []struct {
		name     string
		testNode *node
		outcome  bool
	}{
		{
			"root is not the left child",
			root,
			false,
		}, {
			"left is the left child",
			left,
			true,
		}, {
			"right is not the left child",
			right,
			false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.outcome, test.testNode.isLeftChild())
		})
	}
}

func TestNode_sibling(t *testing.T) {
	root := &node{}
	left := &node{parent: root}
	right := &node{parent: root}
	root.left = left
	root.right = right
	tests := []struct {
		name     string
		testNode *node
		outcome  *node
	}{
		{
			"root has no siblings",
			root,
			nil,
		}, {
			"sibling of left is right",
			left,
			right,
		}, {
			"sibling of right is left",
			right,
			left,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.outcome, test.testNode.sibling())
		})
	}
}

func TestNode_nodeColor(t *testing.T) {
	tests := []struct {
		name  string
		setup func() *node
		color color
	}{
		{
			"black node is black",
			func() *node {
				return &node{color: black}
			},
			black,
		}, {
			"red node is red",
			func() *node {
				return &node{color: red}
			},
			red,
		}, {
			"leaf node is black",
			func() *node {
				return &node{leaf: true}
			},
			black,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.color, test.setup().nodeColor())
		})
	}
}

func TestNode_maxEndOfSubtree(t *testing.T) {
	/*
		    A
		   / \
		  B   D
		 /     \
		C       E
	*/
	a := newNode(Period{time.Unix(20, 0), time.Unix(30, 0)}, nil, nil, black)
	b := newNode(Period{time.Unix(15, 0), time.Unix(25, 0)}, nil, nil, black)
	c := newNode(Period{time.Unix(5, 0), time.Unix(45, 0)}, nil, nil, black)
	d := newNode(Period{time.Unix(22, 0), time.Unix(101, 0)}, nil, nil, black)
	e := newNode(Period{time.Unix(25, 0), time.Unix(100, 0)}, nil, nil, black)
	a.left, a.right = b, d
	b.left = c
	d.right = e
	a.maxEnd = d.period.End
	b.maxEnd = c.period.End
	c.maxEnd = c.period.End
	d.maxEnd = d.period.End
	e.maxEnd = e.period.End

	f := newNode(Period{time.Unix(20, 0), time.Unix(30, 0)}, nil, nil, black)
	g := newNode(Period{time.Unix(15, 0), time.Time{}}, nil, nil, black)
	f.left = g
	g.maxEnd = g.period.End

	h := newNode(Period{time.Unix(20, 0), time.Unix(30, 0)}, nil, nil, black)
	i := newNode(Period{time.Unix(25, 0), time.Time{}}, nil, nil, black)
	h.right = i
	h.maxEnd = h.period.End

	j := newNode(Period{time.Unix(20, 0), time.Unix(30, 0)}, nil, nil, black)
	k := newNode(Period{time.Unix(15, 0), time.Unix(25, 0)}, nil, nil, black)
	l := newNode(Period{time.Unix(30, 0), time.Time{}}, nil, nil, black)
	j.left, j.right = k, l
	k.maxEnd = k.period.End
	j.maxEnd = j.period.End

	tests := []struct {
		name     string
		node     *node
		expected time.Time
	}{
		{
			"node with only child leafs returns its own max end time",
			c,
			c.period.End,
		}, {
			"node with only left child returns max of its period end and its left child's max end",
			b,
			c.maxEnd,
		}, {
			"node with only right child returns max of its period end and its right child's max end",
			d,
			d.maxEnd,
		}, {
			"node with left and right children returns the max of its period end and its children's max ends",
			a,
			d.maxEnd,
		}, {
			"node with only left child with period unbounded on the right returns the zero time",
			f,
			time.Time{},
		}, {
			"node with only right child with period unbounded on the right returns the zero time",
			h,
			time.Time{},
		}, {
			"node with left and right children where one node's max end is unbounded on the right returns the zero time",
			j,
			time.Time{},
		}, {
			"node with zero end time returns zero",
			newNode(Period{time.Unix(20, 0), time.Time{}}, nil, nil, black),
			time.Time{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, test.node.maxEndOfSubtree())
		})
	}
}

func TestNode_periodToLeft(t *testing.T) {
	n := &node{
		period: NewPeriod(time.Date(2018, 12, 7, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 8, 0, 0, 0, 0, time.UTC)),
	}
	tests := []struct {
		name    string
		p       Period
		outcome bool
	}{
		{
			"start before node start is to the left",
			Period{Start: time.Date(2018, 1, 1, 1, 1, 1, 1, time.UTC)},
			true,
		}, {
			"start after node start is not to the left",
			Period{Start: time.Date(2019, 1, 1, 1, 1, 1, 1, time.UTC)},
			false,
		}, {
			"start equal to node start is not to the left",
			Period{Start: time.Date(2018, 12, 7, 0, 0, 0, 0, time.UTC)},
			false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.outcome, n.periodToLeft(test.p))
		})
	}
}

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
)

func TestNode_isLeftChild(t *testing.T) {
	root := &node[int, any]{}
	left := &node[int, any]{parent: root}
	right := &node[int, any]{parent: root}
	root.left = left
	root.right = right
	tests := []struct {
		testNode *node[int, any]
		name     string
		outcome  bool
	}{
		{
			name:     "root is not the left child",
			testNode: root,
		}, {
			name:     "left is the left child",
			testNode: left,
			outcome:  true,
		}, {
			name:     "right is not the left child",
			testNode: right,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.outcome, test.testNode.isLeftChild())
		})
	}
}

func TestNode_sibling(t *testing.T) {
	root := &node[int, any]{}
	left := &node[int, any]{parent: root}
	right := &node[int, any]{parent: root}
	root.left = left
	root.right = right
	tests := []struct {
		testNode *node[int, any]
		outcome  *node[int, any]
		name     string
	}{
		{
			name:     "root has no siblings",
			testNode: root,
		}, {
			name:     "sibling of left is right",
			testNode: left,
			outcome:  right,
		}, {
			name:     "sibling of right is left",
			testNode: right,
			outcome:  left,
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
		setup func() *node[int, any]
		name  string
		color color
	}{
		{
			name: "black node is black",
			setup: func() *node[int, any] {
				return &node[int, any]{color: black}
			},
			color: black,
		}, {
			name: "red node is red",
			setup: func() *node[int, any] {
				return &node[int, any]{color: red}
			},
			color: red,
		}, {
			name: "leaf node is black",
			setup: func() *node[int, any] {
				return &node[int, any]{leaf: true}
			},
			color: black,
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
	a := newNode[int, any](Period{time.Unix(20, 0), time.Unix(30, 0)}, 0, nil, black)
	b := newNode[int, any](Period{time.Unix(15, 0), time.Unix(25, 0)}, 0, nil, black)
	c := newNode[int, any](Period{time.Unix(5, 0), time.Unix(45, 0)}, 0, nil, black)
	d := newNode[int, any](Period{time.Unix(22, 0), time.Unix(101, 0)}, 0, nil, black)
	e := newNode[int, any](Period{time.Unix(25, 0), time.Unix(100, 0)}, 0, nil, black)
	a.left, a.right = b, d
	b.left = c
	d.right = e
	a.maxEnd = d.period.End
	b.maxEnd = c.period.End
	c.maxEnd = c.period.End
	d.maxEnd = d.period.End
	e.maxEnd = e.period.End

	f := newNode[int, any](Period{time.Unix(20, 0), time.Unix(30, 0)}, 0, nil, black)
	g := newNode[int, any](Period{time.Unix(15, 0), time.Time{}}, 0, nil, black)
	f.left = g
	g.maxEnd = g.period.End

	h := newNode[int, any](Period{time.Unix(20, 0), time.Unix(30, 0)}, 0, nil, black)
	i := newNode[int, any](Period{time.Unix(25, 0), time.Time{}}, 0, nil, black)
	h.right = i
	h.maxEnd = h.period.End

	j := newNode[int, any](Period{time.Unix(20, 0), time.Unix(30, 0)}, 0, nil, black)
	k := newNode[int, any](Period{time.Unix(15, 0), time.Unix(25, 0)}, 0, nil, black)
	l := newNode[int, any](Period{time.Unix(30, 0), time.Time{}}, 0, nil, black)
	j.left, j.right = k, l
	k.maxEnd = k.period.End
	j.maxEnd = j.period.End

	tests := []struct {
		expected time.Time
		node     *node[int, any]
		name     string
	}{
		{
			name:     "node with only child leafs returns its own max end time",
			node:     c,
			expected: c.period.End,
		}, {
			name:     "node with only left child returns max of its period end and its left child's max end",
			node:     b,
			expected: c.maxEnd,
		}, {
			name:     "node with only right child returns max of its period end and its right child's max end",
			node:     d,
			expected: d.maxEnd,
		}, {
			name:     "node with left and right children returns the max of its period end and its children's max ends",
			node:     a,
			expected: d.maxEnd,
		}, {
			name: "node with only left child with period unbounded on the right returns the zero time",
			node: f,
		}, {
			name: "node with only right child with period unbounded on the right returns the zero time",
			node: h,
		}, {
			name: "node with left and right children where one node's max end is unbounded on the right returns the zero time",
			node: j,
		}, {
			name: "node with zero end time returns zero",
			node: newNode[int, any](Period{time.Unix(20, 0), time.Time{}}, 0, nil, black),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, test.node.maxEndOfSubtree())
		})
	}
}

func TestNode_periodToLeft(t *testing.T) {
	n := &node[int, any]{
		period: NewPeriod(time.Date(2018, 12, 7, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 8, 0, 0, 0, 0, time.UTC)),
	}
	tests := []struct {
		p       Period
		name    string
		outcome bool
	}{
		{
			name:    "start before node start is to the left",
			p:       Period{Start: time.Date(2018, 1, 1, 1, 1, 1, 1, time.UTC)},
			outcome: true,
		}, {
			name: "start after node start is not to the left",
			p:    Period{Start: time.Date(2019, 1, 1, 1, 1, 1, 1, time.UTC)},
		}, {
			name: "start equal to node start is not to the left",
			p:    Period{Start: time.Date(2018, 12, 7, 0, 0, 0, 0, time.UTC)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.outcome, n.periodToLeft(test.p))
		})
	}
}

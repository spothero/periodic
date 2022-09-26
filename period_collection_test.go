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
	"github.com/stretchr/testify/require"
)

func TestPeriodCollection_Insert(t *testing.T) {
	type insertions struct {
		period    Period
		key       int
		expectErr bool
	}
	tests := []struct {
		name         string
		setupTree    func() *PeriodCollection[int, insertions]
		insertions   []insertions
		validateTree func(t *testing.T, pc *PeriodCollection[int, insertions])
	}{
		{
			"inserting a single node creates a black root",
			func() *PeriodCollection[int, insertions] { return NewPeriodCollection[int, insertions]() },
			[]insertions{{NewPeriod(time.Unix(1, 0), time.Unix(5, 0)), 0, false}},
			func(t *testing.T, pc *PeriodCollection[int, insertions]) {
				assert.Equal(t, black, pc.root.color)
				assert.Contains(t, pc.nodes, 0)
				assert.Equal(t, time.Unix(5, 0), pc.root.maxEnd)
			},
		}, {
			"inserting a node into a tree with a sentinel root replaces the sentinel with a new root",
			func() *PeriodCollection[int, insertions] {
				pc := NewPeriodCollection[int, insertions]()
				pc.root = &node[int, insertions]{leaf: true}
				return pc
			},
			[]insertions{{NewPeriod(time.Unix(1, 0), time.Unix(5, 0)), 0, false}},
			func(t *testing.T, pc *PeriodCollection[int, insertions]) {
				assert.Equal(t, black, pc.root.color)
				assert.False(t, pc.root.leaf)
				assert.Equal(t, time.Unix(1, 0), pc.root.period.Start)
				assert.Contains(t, pc.nodes, 0)
				assert.Len(t, pc.nodes, 1)
				assert.Equal(t, time.Unix(5, 0), pc.root.maxEnd)
			},
		}, {
			/* after insertion, 1 and 3 are red and 2 is black
			  2
			 / \
			1   3
			*/
			"inserting 3 nodes creates red children",
			func() *PeriodCollection[int, insertions] { return NewPeriodCollection[int, insertions]() },
			[]insertions{
				{NewPeriod(time.Unix(2, 0), time.Unix(5, 0)), 0, false},
				{NewPeriod(time.Unix(1, 0), time.Unix(10, 0)), 1, false},
				{NewPeriod(time.Unix(3, 0), time.Unix(4, 0)), 2, false},
			},
			func(t *testing.T, pc *PeriodCollection[int, insertions]) {
				require.NotNil(t, pc.root.left)
				require.NotNil(t, pc.root.right)
				assert.Equal(t, time.Unix(2, 0), pc.root.period.Start)
				assert.Equal(t, time.Unix(1, 0), pc.root.left.period.Start)
				assert.Equal(t, time.Unix(3, 0), pc.root.right.period.Start)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, red, pc.root.left.color)
				assert.Equal(t, red, pc.root.right.color)
				for i := 0; i < 3; i++ {
					assert.Contains(t, pc.nodes, i)
				}
				assert.Len(t, pc.nodes, 3)
				assert.Equal(t, time.Unix(10, 0), pc.root.maxEnd)
				assert.Equal(t, time.Unix(10, 0), pc.root.left.maxEnd)
				assert.Equal(t, time.Unix(4, 0), pc.root.right.maxEnd)
			},
		}, {
			/* Nodes will be inserted and should be rotated and rebalanced such that 1 and 3 are red
			1
			 \         2
			  2   ->  / \
			   \     1   3
			    3
			*/
			"inserting 3 nodes creates red children after a rotation",
			func() *PeriodCollection[int, insertions] { return NewPeriodCollection[int, insertions]() },
			[]insertions{
				{NewPeriod(time.Unix(1, 0), time.Unix(5, 0)), 0, false},
				{NewPeriod(time.Unix(2, 0), time.Unix(4, 0)), 1, false},
				{NewPeriod(time.Unix(3, 0), time.Unix(10, 0)), 2, false},
			},
			func(t *testing.T, pc *PeriodCollection[int, insertions]) {
				require.NotNil(t, pc.root.left)
				require.NotNil(t, pc.root.right)
				assert.Equal(t, time.Unix(2, 0), pc.root.period.Start)
				assert.Equal(t, time.Unix(1, 0), pc.root.left.period.Start)
				assert.Equal(t, time.Unix(3, 0), pc.root.right.period.Start)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, red, pc.root.left.color)
				assert.Equal(t, red, pc.root.right.color)
				for i := 0; i < 3; i++ {
					assert.Contains(t, pc.nodes, i)
				}
				assert.Len(t, pc.nodes, 3)
				assert.Len(t, pc.nodes, 3)
				assert.Equal(t, time.Unix(10, 0), pc.root.maxEnd)
				assert.Equal(t, time.Unix(5, 0), pc.root.left.maxEnd)
				assert.Equal(t, time.Unix(10, 0), pc.root.right.maxEnd)
			},
		}, {
			/* 20 is black, 10, 30 are red to start, inserting 35 should make nodes 10 and 30 black
			   20
			  /  \
			10   30
			      \
			      35
			*/
			"inserting a new node beneath red nodes changes the parents to black",
			func() *PeriodCollection[int, insertions] {
				twenty := newNode[int, insertions](NewPeriod(time.Unix(20, 0), time.Unix(25, 0)), 0, insertions{}, black)
				ten := newNode(NewPeriod(time.Unix(10, 0), time.Unix(22, 0)), 0, insertions{}, red)
				thirty := newNode(NewPeriod(time.Unix(30, 0), time.Unix(100, 0)), 0, insertions{}, red)
				twenty.left, twenty.right, twenty.maxEnd = ten, thirty, thirty.period.End
				ten.parent, ten.maxEnd = twenty, ten.period.End
				thirty.parent, thirty.maxEnd = twenty, thirty.period.End
				pc := NewPeriodCollection[int, insertions]()
				pc.root = twenty
				return pc
			},
			[]insertions{{NewPeriod(time.Unix(35, 0), time.Unix(50, 0)), 0, false}},
			func(t *testing.T, pc *PeriodCollection[int, insertions]) {
				assert.Equal(t, time.Unix(20, 0), pc.root.period.Start)
				assert.Equal(t, time.Unix(10, 0), pc.root.left.period.Start)
				assert.Equal(t, time.Unix(30, 0), pc.root.right.period.Start)
				assert.Equal(t, time.Unix(35, 0), pc.root.right.right.period.Start)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, black, pc.root.left.color)
				assert.Equal(t, black, pc.root.right.color)
				assert.Equal(t, red, pc.root.right.right.color)
				assert.Equal(t, time.Unix(100, 0), pc.root.maxEnd)
				assert.Equal(t, time.Unix(22, 0), pc.root.left.maxEnd)
				assert.Equal(t, time.Unix(100, 0), pc.root.right.maxEnd)
				assert.Equal(t, time.Unix(50, 0), pc.root.right.right.maxEnd)
			},
		}, {
			/* 20 is black, 30 is red to start, inserting 25 should rebalance the tree with multiple left rotations
			20          25
			  \        /  \
			  30  ->  20  30
			 /
			25
			*/
			"inserting a new left inside performs multiple rotations to balance the tree",
			func() *PeriodCollection[int, insertions] {
				twenty := newNode[int, insertions](NewPeriod(time.Unix(20, 0), time.Unix(50, 0)), 0, insertions{}, black)
				thirty := newNode[int, insertions](NewPeriod(time.Unix(30, 0), time.Unix(75, 0)), 0, insertions{}, red)
				twenty.right, twenty.maxEnd = thirty, thirty.period.End
				thirty.parent, thirty.maxEnd = twenty, thirty.period.End
				pc := NewPeriodCollection[int, insertions]()
				pc.root = twenty
				return pc
			},
			[]insertions{{NewPeriod(time.Unix(25, 0), time.Unix(100, 0)), 0, false}},
			func(t *testing.T, pc *PeriodCollection[int, insertions]) {
				assert.Equal(t, time.Unix(25, 0), pc.root.period.Start)
				assert.Equal(t, time.Unix(20, 0), pc.root.left.period.Start)
				assert.Equal(t, time.Unix(30, 0), pc.root.right.period.Start)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, red, pc.root.left.color)
				assert.Equal(t, red, pc.root.right.color)
				assert.Equal(t, time.Unix(100, 0), pc.root.maxEnd)
				assert.Equal(t, time.Unix(50, 0), pc.root.left.maxEnd)
				assert.Equal(t, time.Unix(75, 0), pc.root.right.maxEnd)
			},
		}, {
			/* 25 is black, 15 is red to start, inserting 20 should rebalance the tree with multiple right rotations
			  25       20
			 /        /  \
			15   ->  15  25
			 \
			 20
			*/
			"inserting a new right inside performs multiple rotations to balance the tree",
			func() *PeriodCollection[int, insertions] {
				twentyFive := newNode[int, insertions](NewPeriod(time.Unix(25, 0), time.Unix(45, 0)), 0, insertions{}, black)
				fifteen := newNode[int, insertions](NewPeriod(time.Unix(15, 0), time.Unix(20, 0)), 0, insertions{}, red)
				twentyFive.left, twentyFive.maxEnd = fifteen, twentyFive.period.End
				fifteen.parent, fifteen.maxEnd = twentyFive, fifteen.period.End
				pc := NewPeriodCollection[int, insertions]()
				pc.root = twentyFive
				return pc
			},
			[]insertions{{NewPeriod(time.Unix(20, 0), time.Unix(40, 0)), 0, false}},
			func(t *testing.T, pc *PeriodCollection[int, insertions]) {
				assert.Equal(t, time.Unix(20, 0), pc.root.period.Start)
				assert.Equal(t, time.Unix(15, 0), pc.root.left.period.Start)
				assert.Equal(t, time.Unix(25, 0), pc.root.right.period.Start)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, red, pc.root.left.color)
				assert.Equal(t, red, pc.root.right.color)
				assert.Equal(t, time.Unix(45, 0), pc.root.maxEnd)
				assert.Equal(t, time.Unix(45, 0), pc.root.right.maxEnd)
				assert.Equal(t, time.Unix(20, 0), pc.root.left.maxEnd)
			},
		}, {
			"inserting a node with the same key as an existing node returns an error",
			func() *PeriodCollection[int, insertions] { return NewPeriodCollection[int, insertions]() },
			[]insertions{
				{NewPeriod(time.Unix(20, 0), time.Unix(40, 0)), 0, false},
				{NewPeriod(time.Unix(20, 0), time.Unix(40, 0)), 0, true},
			},
			func(t *testing.T, pc *PeriodCollection[int, insertions]) {
				assert.Len(t, pc.nodes, 1)
			},
		}, {
			"inserting a node on the left with an unbounded period updates maxEnd correctly",
			func() *PeriodCollection[int, insertions] {
				pc := NewPeriodCollection[int, insertions]()
				pc.root = newNode[int, insertions](NewPeriod(time.Unix(20, 0), time.Unix(25, 0)), 0, insertions{}, black)
				return pc
			},
			[]insertions{{NewPeriod(time.Unix(10, 0), time.Time{}), 0, false}},
			func(t *testing.T, pc *PeriodCollection[int, insertions]) {
				assert.Equal(t, time.Unix(20, 0), pc.root.period.Start)
				assert.Equal(t, time.Unix(10, 0), pc.root.left.period.Start)
				assert.Equal(t, time.Time{}, pc.root.maxEnd)
				assert.Equal(t, time.Time{}, pc.root.left.maxEnd)
			},
		}, {
			"inserting a node on the right with an unbounded period updates maxEnd correctly",
			func() *PeriodCollection[int, insertions] {
				pc := NewPeriodCollection[int, insertions]()
				pc.root = newNode[int, insertions](NewPeriod(time.Unix(20, 0), time.Unix(25, 0)), 0, insertions{}, black)
				return pc
			},
			[]insertions{{NewPeriod(time.Unix(30, 0), time.Time{}), 0, false}},
			func(t *testing.T, pc *PeriodCollection[int, insertions]) {
				assert.Equal(t, time.Unix(20, 0), pc.root.period.Start)
				assert.Equal(t, time.Unix(30, 0), pc.root.right.period.Start)
				assert.Equal(t, time.Time{}, pc.root.maxEnd)
				assert.Equal(t, time.Time{}, pc.root.right.maxEnd)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pc := test.setupTree()
			for _, i := range test.insertions {
				err := pc.Insert(i.key, i.period, insertions{})
				if i.expectErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
			}
			test.validateTree(t, pc)
		})
	}
}

func TestPeriodCollection_rotate(t *testing.T) {
	nodeA := &node[int, any]{}
	nodeB := &node[int, any]{}
	nodeC := &node[int, any]{}
	nodeD := &node[int, any]{}
	cleanupTree := func() {
		for _, n := range []*node[int, any]{nodeA, nodeB, nodeC, nodeD} {
			n.left, n.right, n.parent = &node[int, any]{leaf: true}, &node[int, any]{leaf: true}, nil
		}
	}
	setupLeftTree := func() *PeriodCollection[int, any] {
		cleanupTree()
		nodeD.left, nodeD.period.End, nodeD.maxEnd = nodeC, time.Unix(1, 0), time.Unix(10, 0)
		nodeC.left, nodeC.right, nodeC.parent, nodeC.period.End, nodeC.maxEnd =
			nodeA, nodeB, nodeD, time.Unix(2, 0), time.Unix(10, 0)
		nodeA.parent, nodeA.period.End, nodeA.maxEnd = nodeC, time.Unix(10, 0), time.Unix(10, 0)
		nodeB.parent, nodeB.period.End, nodeB.maxEnd = nodeC, time.Unix(5, 0), time.Unix(5, 0)
		return &PeriodCollection[int, any]{root: nodeD}
	}
	setupRightTree := func() *PeriodCollection[int, any] {
		cleanupTree()
		nodeD.right, nodeD.period.End, nodeD.maxEnd = nodeC, time.Unix(1, 0), time.Unix(10, 0)
		nodeC.left, nodeC.right, nodeC.parent, nodeC.period.End, nodeC.maxEnd =
			nodeA, nodeB, nodeD, time.Unix(2, 0), time.Unix(10, 0)
		nodeA.parent, nodeA.period.End, nodeA.maxEnd = nodeC, time.Unix(10, 0), time.Unix(10, 0)
		nodeB.parent, nodeB.period.End, nodeB.maxEnd = nodeC, time.Unix(5, 0), time.Unix(5, 0)
		return &PeriodCollection[int, any]{root: nodeD}
	}
	tests := []struct {
		name         string
		setupTree    func() *PeriodCollection[int, any]
		direction    rotationDirection
		nodeToRotate *node[int, any]
		validateTree func(t *testing.T)
	}{
		{
			/*
				    D                              D
				   /                              /
				  C    right rotate around C ->  A
				 / \                              \
				A   B                              C
				                                    \
				                                     B
			*/
			"right rotate works correctly",
			setupLeftTree,
			right,
			nodeC,
			func(t *testing.T) {
				assert.Equal(t, nodeA, nodeD.left)
				assert.True(t, nodeD.right.leaf)
				assert.Nil(t, nodeD.parent)
				assert.True(t, nodeA.left.leaf)
				assert.Equal(t, nodeC, nodeA.right)
				assert.Equal(t, nodeD, nodeA.parent)
				assert.True(t, nodeC.left.leaf)
				assert.Equal(t, nodeB, nodeC.right)
				assert.Equal(t, nodeA, nodeC.parent)
				assert.True(t, nodeB.left.leaf)
				assert.True(t, nodeB.right.leaf)
				assert.Equal(t, nodeC, nodeB.parent)
				assert.Equal(t, time.Unix(10, 0), nodeD.maxEnd)
				assert.Equal(t, time.Unix(10, 0), nodeA.maxEnd)
				assert.Equal(t, time.Unix(5, 0), nodeC.maxEnd)
				assert.Equal(t, time.Unix(5, 0), nodeB.maxEnd)
			},
		}, {
			/*
				    D                              D
				   /                              /
				  C    left rotate around C ->   B
				 / \                            /
				A   B                          C
				                              /
				                             A
			*/
			"left rotate works correctly",
			setupLeftTree,
			left,
			nodeC,
			func(t *testing.T) {
				assert.True(t, nodeD.right.leaf)
				assert.Equal(t, nodeB, nodeD.left)
				assert.Nil(t, nodeD.parent)
				assert.Equal(t, nodeC, nodeB.left)
				assert.True(t, nodeB.right.leaf)
				assert.Equal(t, nodeD, nodeB.parent)
				assert.Equal(t, nodeA, nodeC.left)
				assert.True(t, nodeC.right.leaf)
				assert.Equal(t, nodeB, nodeC.parent)
				assert.True(t, nodeA.left.leaf)
				assert.True(t, nodeA.right.leaf)
				assert.Equal(t, nodeC, nodeA.parent)
				assert.Equal(t, time.Unix(10, 0), nodeD.maxEnd)
				assert.Equal(t, time.Unix(10, 0), nodeB.maxEnd)
				assert.Equal(t, time.Unix(10, 0), nodeC.maxEnd)
				assert.Equal(t, time.Unix(10, 0), nodeA.maxEnd)
			},
		}, {
			/*
				    D                              C
				   /                              / \
				  C                              A   D
				 / \  right rotate around D ->      /
				A   B                              B
			*/
			"right rotate on root works correctly",
			setupLeftTree,
			right,
			nodeD,
			func(t *testing.T) {
				assert.Equal(t, nodeA, nodeC.left)
				assert.Equal(t, nodeD, nodeC.right)
				assert.Nil(t, nodeC.parent)
				assert.True(t, nodeA.left.leaf)
				assert.True(t, nodeA.right.leaf)
				assert.Equal(t, nodeC, nodeA.parent)
				assert.True(t, nodeB.left.leaf)
				assert.True(t, nodeB.right.leaf)
				assert.Equal(t, nodeD, nodeB.parent)
				assert.Equal(t, nodeB, nodeD.left)
				assert.True(t, nodeD.right.leaf)
				assert.Equal(t, nodeC, nodeD.parent)
				assert.Equal(t, time.Unix(10, 0), nodeC.maxEnd)
				assert.Equal(t, time.Unix(10, 0), nodeA.maxEnd)
				assert.Equal(t, time.Unix(5, 0), nodeD.maxEnd)
				assert.Equal(t, time.Unix(5, 0), nodeB.maxEnd)
			},
		}, {
			/*
				D                                 C
				 \                               / \
				  C                             D   B
				 / \   left rotate around D ->   \
				A 	B                             A
			*/
			"left rotate on root works correctly",
			setupRightTree,
			left,
			nodeD,
			func(t *testing.T) {
				assert.Equal(t, nodeD, nodeC.left)
				assert.Equal(t, nodeB, nodeC.right)
				assert.Nil(t, nodeC.parent)
				assert.True(t, nodeD.left.leaf)
				assert.Equal(t, nodeA, nodeD.right)
				assert.Equal(t, nodeC, nodeD.parent)
				assert.True(t, nodeA.left.leaf)
				assert.True(t, nodeA.right.leaf)
				assert.Equal(t, nodeD, nodeA.parent)
				assert.True(t, nodeB.left.leaf)
				assert.True(t, nodeB.right.leaf)
				assert.Equal(t, nodeC, nodeB.parent)
				assert.Equal(t, time.Unix(10, 0), nodeC.maxEnd)
				assert.Equal(t, time.Unix(10, 0), nodeD.maxEnd)
				assert.Equal(t, time.Unix(5, 0), nodeB.maxEnd)
				assert.Equal(t, time.Unix(10, 0), nodeA.maxEnd)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tree := test.setupTree()
			tree.rotate(test.nodeToRotate, test.direction)
			test.validateTree(t)
		})
	}
}

func TestPeriodCollection_successor(t *testing.T) {
	/* Tests performed using this tree
	    E
	   / \
	  B   F
	 / \
	A   C
	     \
	      D
	*/
	nodeA := newNode[int, any](Period{}, 0, nil, black)
	nodeB := newNode[int, any](Period{}, 0, nil, black)
	nodeC := newNode[int, any](Period{}, 0, nil, black)
	nodeD := newNode[int, any](Period{}, 0, nil, black)
	nodeE := newNode[int, any](Period{}, 0, nil, black)
	nodeF := newNode[int, any](Period{}, 0, nil, black)
	nodeE.left = nodeB
	nodeE.right = nodeF
	nodeB.parent = nodeE
	nodeF.parent = nodeE
	nodeB.left = nodeA
	nodeB.right = nodeC
	nodeA.parent = nodeB
	nodeC.parent = nodeB
	nodeC.right = nodeD
	nodeD.parent = nodeC
	tests := []struct {
		name              string
		successorOf       *node[int, any]
		expectedSuccessor *node[int, any]
	}{
		{
			"successor of A is B",
			nodeA,
			nodeB,
		}, {
			"successor of D is E",
			nodeD,
			nodeE,
		}, {
			"successor of C is D",
			nodeC,
			nodeD,
		}, {
			"successor of F is nil",
			nodeF,
			nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedSuccessor, test.successorOf.successor())
		})
	}
}

func TestPeriodCollection_Delete(t *testing.T) {
	tests := []struct {
		name string
		key  int
	}{
		{
			"deleting a node should also remove it from the external map",
			1,
		}, {
			"deleting a key with no node is a no-op",
			2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			n := newNode[int, any](Period{}, 1, nil, black)
			pc := PeriodCollection[int, any]{
				root:  n,
				nodes: map[int]*node[int, any]{1: n},
			}
			pc.Delete(test.key)
		})
	}
}

func TestPeriodCollection_deleteNode(t *testing.T) {
	tests := []struct {
		name      string
		setupTree func() (*PeriodCollection[int, any], *node[int, any])
		validate  func(t *testing.T, pc *PeriodCollection[int, any])
	}{
		{
			"deleting the a tree with only root leaves a leaf as the root",
			func() (*PeriodCollection[int, any], *node[int, any]) {
				root := newNode[int, any](Period{}, 1, nil, black)
				return &PeriodCollection[int, any]{root: root, nodes: map[int]*node[int, any]{1: root}}, root
			},
			func(t *testing.T, pc *PeriodCollection[int, any]) {
				assert.True(t, pc.root.leaf)
				assert.Nil(t, pc.root.left)
				assert.Nil(t, pc.root.right)
				assert.Nil(t, pc.root.parent)
				assert.NotContains(t, pc.nodes, 1)
			},
		}, {
			/* P, S, N are black, L, R are red; after deleting N, L is red with the rest black
			    P           R
			   / \         / \
			  S   N  ->   S   P
			 / \         /
			L   R       L
			*/
			"deleting a black right child with no children with a black sibling with red children rebalances the tree",
			func() (*PeriodCollection[int, any], *node[int, any]) {
				p := newNode[int, any](Period{End: time.Unix(40, 0)}, 1, "p", black)
				s := newNode[int, any](Period{End: time.Unix(50, 0)}, 2, "s", black)
				n := newNode[int, any](Period{End: time.Unix(45, 0)}, 3, "n", black)
				l := newNode[int, any](Period{End: time.Unix(25, 0)}, 4, "l", red)
				r := newNode[int, any](Period{End: time.Unix(60, 0)}, 5, "r", red)
				p.left, p.right, p.maxEnd = s, n, r.period.End
				s.left, s.right, s.parent, s.maxEnd = l, r, p, r.period.End
				l.parent, r.parent = s, s
				n.parent = p
				return &PeriodCollection[int, any]{
					root:  p,
					nodes: map[int]*node[int, any]{1: p, 2: s, 3: n, 4: l, 5: r},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection[int, any]) {
				assert.Equal(t, "r", pc.root.contents)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, "s", pc.root.left.contents)
				assert.Equal(t, black, pc.root.left.color)
				assert.Equal(t, "p", pc.root.right.contents)
				assert.Equal(t, black, pc.root.right.color)
				assert.Equal(t, "l", pc.root.left.left.contents)
				assert.Equal(t, red, pc.root.left.left.color)
				assert.True(t, pc.root.right.right.leaf)
				assert.Equal(t, time.Unix(60, 0), pc.root.maxEnd)
				assert.Equal(t, time.Unix(50, 0), pc.root.left.maxEnd)
				assert.Equal(t, time.Unix(25, 0), pc.root.left.left.maxEnd)
				assert.Equal(t, time.Unix(40, 0), pc.root.right.maxEnd)
				assert.NotContains(t, pc.nodes, 3)
				assert.Len(t, pc.nodes, 4)
			},
		}, {
			/* P, S, N are black, L, R are red; after deleting N, L is red with the rest black
			     P            L
			    / \          / \
			   N   S   ->   P   S
			      / \            \
			     L   R            R
			*/
			"deleting a black left child with no children with a black sibling with red children rebalances the tree",
			func() (*PeriodCollection[int, any], *node[int, any]) {
				p := newNode[int, any](Period{End: time.Unix(40, 0)}, 1, "p", black)
				s := newNode[int, any](Period{End: time.Unix(50, 0)}, 2, "s", black)
				n := newNode[int, any](Period{End: time.Unix(45, 0)}, 3, "n", black)
				l := newNode[int, any](Period{End: time.Unix(25, 0)}, 4, "l", red)
				r := newNode[int, any](Period{End: time.Unix(60, 0)}, 5, "r", red)
				p.left, p.right, p.maxEnd = n, s, r.period.End
				s.left, s.right, s.parent, s.maxEnd = l, r, p, r.period.End
				l.parent, r.parent = s, s
				n.parent = p
				return &PeriodCollection[int, any]{
					root:  p,
					nodes: map[int]*node[int, any]{1: p, 2: s, 3: n, 4: l, 5: r},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection[int, any]) {
				assert.Equal(t, "l", pc.root.contents)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, "p", pc.root.left.contents)
				assert.Equal(t, black, pc.root.left.color)
				assert.Equal(t, "s", pc.root.right.contents)
				assert.Equal(t, black, pc.root.left.color)
				assert.Equal(t, "r", pc.root.right.right.contents)
				assert.Equal(t, red, pc.root.right.right.color)
				assert.Equal(t, time.Unix(60, 0), pc.root.maxEnd)
				assert.Equal(t, time.Unix(40, 0), pc.root.left.maxEnd)
				assert.Equal(t, time.Unix(60, 0), pc.root.right.maxEnd)
				assert.Equal(t, time.Unix(60, 0), pc.root.right.right.maxEnd)
				assert.NotContains(t, pc.nodes, 3)
				assert.Len(t, pc.nodes, 4)
			},
		}, {
			/* P, N, L, R are black, S is red; after deleting N, P is red with the rest black
			    P           S
			   / \         / \
			  S   N  ->   L   P
			 / \             /
			L   R           R
			*/
			"deleting a black left node with a red sibling rebalances the tree",
			func() (*PeriodCollection[int, any], *node[int, any]) {
				p := newNode[int, any](Period{}, 1, "p", black)
				s := newNode[int, any](Period{}, 2, "s", red)
				n := newNode[int, any](Period{}, 3, "n", black)
				l := newNode[int, any](Period{}, 4, "l", black)
				r := newNode[int, any](Period{}, 5, "r", black)
				p.left, p.right = s, n
				s.left, s.right, s.parent = l, r, p
				l.parent = s
				r.parent = s
				n.parent = p
				return &PeriodCollection[int, any]{
					root:  p,
					nodes: map[int]*node[int, any]{1: p, 2: s, 3: n, 4: l, 5: r},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection[int, any]) {
				assert.Equal(t, "s", pc.root.contents)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, "l", pc.root.left.contents)
				assert.Equal(t, black, pc.root.left.color)
				assert.Equal(t, "p", pc.root.right.contents)
				assert.Equal(t, red, pc.root.right.color)
				assert.Equal(t, "r", pc.root.right.left.contents)
				assert.Equal(t, black, pc.root.right.left.color)
				assert.True(t, pc.root.right.right.leaf)
				assert.NotContains(t, pc.nodes, 3)
				assert.Len(t, pc.nodes, 4)
			},
		}, {
			/* P, N, L, R are black, S is red; after deleting N, P is red with the rest black
			  P           S
			 / \         / \
			N   S  ->   P   R
			   / \       \
			  L   R       L
			*/
			"deleting a black left node with a red sibling rebalances the tree",
			func() (*PeriodCollection[int, any], *node[int, any]) {
				p := newNode[int, any](Period{}, 1, "p", black)
				s := newNode[int, any](Period{}, 2, "s", red)
				n := newNode[int, any](Period{}, 3, "n", black)
				l := newNode[int, any](Period{}, 4, "l", black)
				r := newNode[int, any](Period{}, 5, "r", black)
				p.left, p.right = n, s
				s.left, s.right, s.parent = l, r, p
				l.parent = s
				r.parent = s
				n.parent = p
				return &PeriodCollection[int, any]{
					root:  p,
					nodes: map[int]*node[int, any]{1: p, 2: s, 3: n, 4: l, 5: r},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection[int, any]) {
				assert.Equal(t, "s", pc.root.contents)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, "p", pc.root.left.contents)
				assert.Equal(t, red, pc.root.left.color)
				assert.Equal(t, "r", pc.root.right.contents)
				assert.Equal(t, black, pc.root.right.color)
				assert.Equal(t, "l", pc.root.left.right.contents)
				assert.Equal(t, black, pc.root.right.left.color)
				assert.True(t, pc.root.left.left.leaf)
				assert.NotContains(t, pc.nodes, 3)
				assert.Len(t, pc.nodes, 4)
			},
		}, {
			/* L is red, P, N, S are black to start; after deleting N, all nodes are black
			    P         P
			   / \       / \
			  N   S  -> L   S
			 /
			L
			*/
			"delete left node with left child",
			func() (*PeriodCollection[int, any], *node[int, any]) {
				p := newNode[int, any](Period{End: time.Unix(15, 0)}, 1, "p", black)
				s := newNode[int, any](Period{End: time.Unix(10, 0)}, 2, "s", black)
				n := newNode[int, any](Period{End: time.Unix(45, 0)}, 3, "n", black)
				l := newNode[int, any](Period{End: time.Unix(25, 0)}, 4, "l", red)
				p.left, p.right, p.maxEnd = n, s, n.period.End
				n.left, n.parent = l, p
				s.parent = p
				l.parent = n
				return &PeriodCollection[int, any]{
					root:  p,
					nodes: map[int]*node[int, any]{1: p, 2: s, 3: n, 4: l},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection[int, any]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, "l", pc.root.left.contents)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, "s", pc.root.right.contents)
				assert.Equal(t, black, pc.root.right.color)
				assert.Equal(t, time.Unix(25, 0), pc.root.maxEnd)
				assert.Equal(t, time.Unix(25, 0), pc.root.left.maxEnd)
				assert.Equal(t, time.Unix(10, 0), pc.root.right.maxEnd)
				assert.NotContains(t, pc.nodes, 3)
				assert.Equal(t, pc.nodes[4], pc.root.left)
				assert.Len(t, pc.nodes, 3)
			},
		}, {
			/* R is red, P, N, S are black to start; after deleting N, all nodes are black
			  P         P
			 / \       / \
			S   N  -> S   R
			     \
			      R
			*/
			"delete right node right left child",
			func() (*PeriodCollection[int, any], *node[int, any]) {
				p := newNode[int, any](Period{End: time.Unix(15, 0)}, 1, "p", black)
				s := newNode[int, any](Period{End: time.Unix(10, 0)}, 2, "s", black)
				n := newNode[int, any](Period{End: time.Unix(45, 0)}, 3, "n", black)
				r := newNode[int, any](Period{End: time.Unix(25, 0)}, 4, "r", red)
				p.left, p.right, p.maxEnd = s, n, n.period.End
				n.right, n.parent = r, p
				s.parent = p
				r.parent = n
				return &PeriodCollection[int, any]{
					root:  p,
					nodes: map[int]*node[int, any]{1: p, 2: s, 3: n, 4: r},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection[int, any]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, "s", pc.root.left.contents)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, "r", pc.root.right.contents)
				assert.Equal(t, black, pc.root.right.color)
				assert.Equal(t, time.Unix(25, 0), pc.root.maxEnd)
				assert.Equal(t, time.Unix(25, 0), pc.root.right.maxEnd)
				assert.Equal(t, time.Unix(10, 0), pc.root.left.maxEnd)
				assert.NotContains(t, pc.nodes, 3)
				assert.Len(t, pc.nodes, 3)
			},
		}, {
			/* R is red, P, N, S are black to start; after deleting N, all nodes are black
			  P         P
			 / \       / \
			N   S  -> R   S
			 \
			  R
			*/
			"delete left node with right child",
			func() (*PeriodCollection[int, any], *node[int, any]) {
				p := newNode[int, any](Period{End: time.Unix(15, 0)}, 1, "p", black)
				s := newNode[int, any](Period{End: time.Unix(10, 0)}, 2, "s", black)
				n := newNode[int, any](Period{End: time.Unix(25, 0)}, 3, "n", black)
				r := newNode[int, any](Period{End: time.Unix(45, 0)}, 4, "r", red)
				p.left, p.right, p.maxEnd = n, s, r.maxEnd
				n.right, n.parent = r, p
				s.parent = p
				r.parent = n
				return &PeriodCollection[int, any]{
					root:  p,
					nodes: map[int]*node[int, any]{1: p, 2: s, 3: n, 4: r},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection[int, any]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, "r", pc.root.left.contents)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, "s", pc.root.right.contents)
				assert.Equal(t, black, pc.root.right.color)
				assert.Equal(t, time.Unix(45, 0), pc.root.maxEnd)
				assert.Equal(t, time.Unix(10, 0), pc.root.right.maxEnd)
				assert.Equal(t, time.Unix(45, 0), pc.root.left.maxEnd)
				assert.NotContains(t, pc.nodes, 3)
				assert.Len(t, pc.nodes, 3)
			},
		}, {
			/* R is red, P, N, S are black to start; after deleting N, all nodes are black
			  P         P
			 / \       / \
			S   N  -> S   L
			   /
			  L
			*/
			"delete right node with left child",
			func() (*PeriodCollection[int, any], *node[int, any]) {
				p := newNode[int, any](Period{End: time.Unix(15, 0)}, 1, "p", black)
				s := newNode[int, any](Period{End: time.Unix(10, 0)}, 2, "s", black)
				n := newNode[int, any](Period{End: time.Unix(45, 0)}, 3, "n", black)
				l := newNode[int, any](Period{End: time.Unix(25, 0)}, 4, "l", red)
				p.left, p.right, p.maxEnd = s, n, n.period.End
				n.left, n.parent = l, p
				s.parent = p
				l.parent = n
				return &PeriodCollection[int, any]{
					root:  p,
					nodes: map[int]*node[int, any]{1: p, 2: s, 3: n, 4: l},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection[int, any]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, "s", pc.root.left.contents)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, "l", pc.root.right.contents)
				assert.Equal(t, black, pc.root.right.color)
				assert.Equal(t, time.Unix(25, 0), pc.root.maxEnd)
				assert.Equal(t, time.Unix(25, 0), pc.root.right.maxEnd)
				assert.Equal(t, time.Unix(10, 0), pc.root.left.maxEnd)
				assert.NotContains(t, pc.nodes, 3)
				assert.Len(t, pc.nodes, 3)
			},
		}, {
			"deleting black node with leaf sibling and red parent makes parent black",
			func() (*PeriodCollection[int, any], *node[int, any]) {
				p := newNode[int, any](Period{}, 1, "p", red)
				n := newNode[int, any](Period{}, 2, "n", black)
				p.left = n
				n.parent = p
				return &PeriodCollection[int, any]{root: p, nodes: map[int]*node[int, any]{1: p, 2: n}}, n
			},
			func(t *testing.T, pc *PeriodCollection[int, any]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.True(t, pc.root.left.leaf)
				assert.Equal(t, black, pc.root.color)
				assert.NotContains(t, pc.nodes, 2)
				assert.Len(t, pc.nodes, 1)
			},
		}, {
			/* contrived example starting with an unbalanced tree:
			   all nodes are black, N is red; after deleting N, P is red with the rest black
			  P        P
			 / \        \
			N   S   ->   S
			   / \      / \
			  L   R    L   R
			*/
			"deleting a black node with a red sibling with 2 black child nodes rebalances the tree",
			func() (*PeriodCollection[int, any], *node[int, any]) {
				p := newNode[int, any](Period{}, 1, "p", black)
				s := newNode[int, any](Period{}, 2, "s", black)
				n := newNode[int, any](Period{}, 3, "n", black)
				l := newNode[int, any](Period{}, 4, "l", black)
				r := newNode[int, any](Period{}, 5, "r", black)
				p.left, p.right = n, s
				s.left, s.right, s.parent = l, r, p
				l.parent = s
				r.parent = s
				n.parent = p
				return &PeriodCollection[int, any]{
					root:  p,
					nodes: map[int]*node[int, any]{1: p, 2: s, 3: n, 4: l, 5: r},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection[int, any]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, "s", pc.root.right.contents)
				assert.Equal(t, red, pc.root.right.color)
				assert.Equal(t, "l", pc.root.right.left.contents)
				assert.Equal(t, black, pc.root.right.left.color)
				assert.Equal(t, "r", pc.root.right.right.contents)
				assert.Equal(t, black, pc.root.right.left.color)
				assert.True(t, pc.root.left.leaf)
				assert.NotContains(t, pc.nodes, 3)
				assert.Len(t, pc.nodes, 4)
			},
		},
		{
			/* RR is red, the rest are black; after deleting N, all are black
			  N          RL
			 / \        / \
			L   R  ->  L   R
			   /
			  RL
			*/
			"delete an internal node with a black successor",
			func() (*PeriodCollection[int, any], *node[int, any]) {
				n := newNode[int, any](Period{End: time.Unix(20, 0)}, 1, "n", black)
				l := newNode[int, any](Period{End: time.Unix(10, 0)}, 2, "l", black)
				r := newNode[int, any](Period{End: time.Unix(30, 0)}, 3, "r", black)
				rl := newNode[int, any](Period{End: time.Unix(50, 0)}, 4, "rl", red)
				n.left, n.right, n.maxEnd = l, r, rl.period.End
				r.left, r.parent = rl, n
				l.parent = n
				rl.parent = r
				return &PeriodCollection[int, any]{
					root:  n,
					nodes: map[int]*node[int, any]{1: n, 2: l, 3: r, 4: rl},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection[int, any]) {
				assert.Equal(t, "rl", pc.root.contents)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, "l", pc.root.left.contents)
				assert.Equal(t, black, pc.root.left.color)
				assert.Equal(t, "r", pc.root.right.contents)
				assert.Equal(t, black, pc.root.right.color)
				assert.Equal(t, time.Unix(50, 0), pc.root.maxEnd)
				assert.Equal(t, time.Unix(10, 0), pc.root.left.maxEnd)
				assert.Equal(t, time.Unix(30, 0), pc.root.right.maxEnd)
				assert.NotContains(t, pc.nodes, 1)
				assert.Len(t, pc.nodes, 3)
				assert.Equal(t, pc.nodes[4], pc.root)
				assert.Equal(t, pc.nodes[2], pc.root.left)
				assert.Equal(t, pc.nodes[3], pc.root.right)
			},
		},
		{
			/* RL is red, the rest are black; after deleting N, all are black
			  N          RL
			 / \        / \
			L   R  ->  L   R
			   /
			  RL
			*/
			"delete an internal node with a black successor with max end less than the internal node",
			func() (*PeriodCollection[int, any], *node[int, any]) {
				n := newNode[int, any](Period{End: time.Unix(50, 0)}, 1, "n", black)
				l := newNode[int, any](Period{End: time.Unix(10, 0)}, 2, "l", black)
				r := newNode[int, any](Period{End: time.Unix(30, 0)}, 3, "r", black)
				rl := newNode[int, any](Period{End: time.Unix(20, 0)}, 4, "rl", red)
				n.left, n.right = l, r
				r.left, r.parent, r.maxEnd = rl, n, rl.period.End
				l.parent = n
				rl.parent = r
				return &PeriodCollection[int, any]{
					root:  n,
					nodes: map[int]*node[int, any]{1: n, 2: l, 3: r, 4: rl},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection[int, any]) {
				assert.Equal(t, "rl", pc.root.contents)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, "l", pc.root.left.contents)
				assert.Equal(t, black, pc.root.left.color)
				assert.Equal(t, "r", pc.root.right.contents)
				assert.Equal(t, black, pc.root.right.color)
				assert.Equal(t, time.Unix(30, 0), pc.root.maxEnd)
				assert.Equal(t, time.Unix(10, 0), pc.root.left.maxEnd)
				assert.Equal(t, time.Unix(30, 0), pc.root.right.maxEnd)
				assert.NotContains(t, pc.nodes, 1)
				assert.Len(t, pc.nodes, 3)
				assert.Equal(t, pc.nodes[4], pc.root)
				assert.Equal(t, pc.nodes[2], pc.root.left)
				assert.Equal(t, pc.nodes[3], pc.root.right)
			},
		}, {
			"deleting the only child of the root updates max end correctly",
			func() (*PeriodCollection[int, any], *node[int, any]) {
				root := newNode[int, any](Period{End: time.Unix(20, 0)}, 1, "root", black)
				r := newNode[int, any](Period{End: time.Unix(30, 0)}, 2, "r", black)
				root.right, root.maxEnd = r, r.period.End
				r.parent = root
				return &PeriodCollection[int, any]{
					root:  root,
					nodes: map[int]*node[int, any]{1: root, 2: r},
				}, r
			},
			func(t *testing.T, pc *PeriodCollection[int, any]) {
				assert.Equal(t, "root", pc.root.contents)
				assert.True(t, pc.root.left.leaf)
				assert.True(t, pc.root.right.leaf)
				assert.Equal(t, time.Unix(20, 0), pc.root.maxEnd)
				assert.NotContains(t, pc.nodes, 2)
				assert.Len(t, pc.nodes, 1)
			},
		}, {
			/* P, L, and R are black, N is red; N has the earliest start and latest end;
			   after deleting N, maxEnd is updated all the way up to P.
			    P           P
			   / \         / \
			  L   R  ->   L   R
			 /
			N
			*/
			"maxend property correctly propagates up the tree",
			func() (*PeriodCollection[int, any], *node[int, any]) {
				p := newNode[int, any](Period{Start: time.Unix(30, 0), End: time.Unix(40, 0)}, 1, "p", black)
				l := newNode[int, any](Period{Start: time.Unix(20, 0), End: time.Unix(25, 0)}, 2, "l", black)
				r := newNode[int, any](Period{Start: time.Unix(50, 0), End: time.Unix(60, 0)}, 3, "r", black)
				n := newNode[int, any](Period{Start: time.Unix(10, 0), End: time.Unix(70, 0)}, 4, "n", red)
				p.left, p.right, p.maxEnd = l, r, n.period.End
				l.left, l.parent, l.maxEnd = n, p, n.period.End
				r.parent, r.maxEnd = p, r.period.End
				n.parent, n.maxEnd = l, n.period.End
				return &PeriodCollection[int, any]{
					root:  p,
					nodes: map[int]*node[int, any]{1: p, 2: l, 3: r, 4: n},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection[int, any]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, black, pc.root.color)
				assert.Equal(t, "l", pc.root.left.contents)
				assert.Equal(t, black, pc.root.left.color)
				assert.Equal(t, "r", pc.root.right.contents)
				assert.Equal(t, black, pc.root.right.color)
				assert.True(t, pc.root.left.left.leaf)
				assert.Equal(t, time.Unix(25, 0), pc.root.left.maxEnd)
				assert.Equal(t, time.Unix(60, 0), pc.root.maxEnd)
				assert.NotContains(t, pc.nodes, 4)
				assert.Len(t, pc.nodes, 3)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pc, nodeToDelete := test.setupTree()
			pc.deleteNode(nodeToDelete)
			test.validate(t, pc)
		})
	}
}

func TestPeriodCollection_deleteRepairCase1(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() (*PeriodCollection[int, string], *node[int, string])
		validate func(t *testing.T, pc *PeriodCollection[int, string])
	}{
		{
			/* N is deleted; S is red to start, everything else is black
				P                  S
			   / \                / \
			  S   N (leaf)  ->   L   P
			 / \                    / \
			L   R                  R   N (leaf)
			*/
			"deleted right child with red sibling rotates right around the parent",
			func() (*PeriodCollection[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "p", black)
				s := newNode[int, string](Period{}, 0, "s", red)
				l := newNode[int, string](Period{}, 0, "l", black)
				r := newNode[int, string](Period{}, 0, "r", black)
				n := &node[int, string]{leaf: true}
				p.left = s
				p.right = n
				s.left = l
				s.right = r
				s.parent = p
				n.parent = p
				l.parent = s
				r.parent = s
				n.parent = p
				return &PeriodCollection[int, string]{root: p}, n
			},
			func(t *testing.T, pc *PeriodCollection[int, string]) {
				assert.Equal(t, "s", pc.root.contents)
				assert.Equal(t, "l", pc.root.left.contents)
				assert.Equal(t, "p", pc.root.right.contents)
				assert.Equal(t, "r", pc.root.right.left.contents)
				assert.True(t, pc.root.right.right.leaf)
			},
		}, {
			/* N is deleted; S is red to start, everything else is black
			         P                  S
			        / \                / \
			(leaf) N   S      ->      P   R
			          / \            / \
			         L   R   (leaf) N   L
			*/
			"deleted left child with red sibling rotates left around the parent",
			func() (*PeriodCollection[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "p", black)
				s := newNode[int, string](Period{}, 0, "s", red)
				l := newNode[int, string](Period{}, 0, "l", black)
				r := newNode[int, string](Period{}, 0, "r", black)
				n := &node[int, string]{leaf: true}
				p.right = s
				p.left = n
				s.left = l
				s.right = r
				s.parent = p
				n.parent = p
				l.parent = s
				r.parent = s
				n.parent = p
				return &PeriodCollection[int, string]{root: p}, n
			},
			func(t *testing.T, pc *PeriodCollection[int, string]) {
				assert.Equal(t, "s", pc.root.contents)
				assert.Equal(t, "p", pc.root.left.contents)
				assert.Equal(t, "r", pc.root.right.contents)
				assert.Equal(t, "l", pc.root.left.right.contents)
				assert.True(t, pc.root.left.left.leaf)
			},
		}, {
			"deleted child with black sibling does nothing",
			func() (*PeriodCollection[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "p", black)
				s := newNode[int, string](Period{}, 0, "s", black)
				n := &node[int, string]{leaf: true}
				p.left = s
				p.right = n
				n.parent = p
				s.parent = p
				return &PeriodCollection[int, string]{root: p}, n
			},
			func(t *testing.T, pc *PeriodCollection[int, string]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "s", pc.root.left.contents)
				assert.True(t, pc.root.right.leaf)
			},
		}, {
			"deleted child with no sibling does nothing",
			func() (*PeriodCollection[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "p", black)
				n := &node[int, string]{leaf: true}
				p.right = n
				n.parent = p
				return &PeriodCollection[int, string]{root: p}, n
			},
			func(t *testing.T, pc *PeriodCollection[int, string]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.True(t, pc.root.left.leaf)
				assert.True(t, pc.root.right.leaf)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pc, n := test.setup()
			pc.deleteRepairCase1(n)
			test.validate(t, pc)
		})
	}
}

func TestPeriodCollection_deleteRepairCase2(t *testing.T) {
	tests := []struct {
		name            string
		setup           func() (*PeriodCollection[int, string], *node[int, string], *node[int, string])
		expectedOutcome bool
		expectRecolor   bool
	}{
		{
			"deleted node with black sibling with 2 black child nodes recolors the sibling and returns true",
			func() (*PeriodCollection[int, string], *node[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "p", black)
				s := newNode[int, string](Period{}, 0, "s", black)
				sl := newNode[int, string](Period{}, 0, "sl", black)
				sr := newNode[int, string](Period{}, 0, "sr", black)
				n := &node[int, string]{leaf: true, contents: "n"}
				p.left, p.right = s, n
				s.left, s.right, s.parent = sl, sr, p
				sl.parent, sr.parent = s, s
				n.parent = p
				return &PeriodCollection[int, string]{root: p}, n, s
			},
			true,
			true,
		}, {
			"deleted node with black sibling and 1 child does nothing",
			func() (*PeriodCollection[int, string], *node[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "", black)
				s := newNode[int, string](Period{}, 0, "", black)
				sr := newNode[int, string](Period{}, 0, "", black)
				n := &node[int, string]{leaf: true}
				p.left, p.right = s, n
				s.right, s.parent = sr, p
				sr.parent = s
				n.parent = p
				return &PeriodCollection[int, string]{root: p}, n, s
			},
			false,
			false,
		}, {
			"deleted node with leaf sibling returns true but does not recolor the leaf",
			func() (*PeriodCollection[int, string], *node[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "", black)
				s := &node[int, string]{leaf: true}
				n := &node[int, string]{leaf: true}
				p.left, p.right = s, n
				s.parent = p
				n.parent = p
				return &PeriodCollection[int, string]{root: p}, n, s
			},
			true,
			false,
		}, {
			"deleted node with red sibling does nothing",
			func() (*PeriodCollection[int, string], *node[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "", black)
				s := newNode[int, string](Period{}, 0, "", red)
				sl := newNode[int, string](Period{}, 0, "", black)
				sr := newNode[int, string](Period{}, 0, "", black)
				n := &node[int, string]{leaf: true}
				p.left, p.right = s, n
				s.left, s.right, s.parent = sl, sr, p
				sl.parent, sr.parent = s, s
				n.parent = p
				return &PeriodCollection[int, string]{root: p}, n, s
			},
			false,
			false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pc, n, s := test.setup()
			sColorBefore := s.color
			result := pc.deleteRepairCase2(n)
			assert.Equal(t, test.expectedOutcome, result)
			if test.expectRecolor {
				assert.Equal(t, red, s.color)
			} else {
				assert.Equal(t, sColorBefore, s.color)
			}
		})
	}
}

func TestPeriodCollection_deleteRepairCase3(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() (*PeriodCollection[int, string], *node[int, string])
		validate func(t *testing.T, pc *PeriodCollection[int, string])
	}{
		{
			"no action when sibling is a leaf",
			func() (*PeriodCollection[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "p", black)
				r := newNode[int, string](Period{}, 0, "r", black)
				r.parent = p
				p.right = r
				return &PeriodCollection[int, string]{root: p}, r
			},
			func(t *testing.T, pc *PeriodCollection[int, string]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "r", pc.root.right.contents)
				assert.True(t, pc.root.left.leaf)
			},
		}, {
			"no action when sibling is red",
			func() (*PeriodCollection[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "p", black)
				r := newNode[int, string](Period{}, 0, "r", red)
				l := newNode[int, string](Period{}, 0, "l", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				return &PeriodCollection[int, string]{root: p}, r
			},
			func(t *testing.T, pc *PeriodCollection[int, string]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "l", pc.root.left.contents)
				assert.Equal(t, "r", pc.root.right.contents)
			},
		}, {
			"no action when node is right child and sibling has no right child",
			func() (*PeriodCollection[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "p", black)
				r := newNode[int, string](Period{}, 0, "r", black)
				l := newNode[int, string](Period{}, 0, "l", black)
				ll := newNode[int, string](Period{}, 0, "ll", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				l.left = ll
				ll.parent = ll
				return &PeriodCollection[int, string]{root: p}, r
			},
			func(t *testing.T, pc *PeriodCollection[int, string]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "l", pc.root.left.contents)
				assert.Equal(t, "r", pc.root.right.contents)
				assert.Equal(t, "ll", pc.root.left.left.contents)
			},
		}, {
			"no action when node is left child and sibling has no left child",
			func() (*PeriodCollection[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "p", black)
				r := newNode[int, string](Period{}, 0, "r", black)
				l := newNode[int, string](Period{}, 0, "l", black)
				ll := newNode[int, string](Period{}, 0, "ll", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				l.left = ll
				ll.parent = ll
				return &PeriodCollection[int, string]{root: p}, l
			},
			func(t *testing.T, pc *PeriodCollection[int, string]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "l", pc.root.left.contents)
				assert.Equal(t, "r", pc.root.right.contents)
				assert.Equal(t, "ll", pc.root.left.left.contents)
			},
		}, {
			"left rotate around sibling and recolor when node is right child and sibling is black with red right child",
			func() (*PeriodCollection[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "p", black)
				r := newNode[int, string](Period{}, 0, "r", black)
				l := newNode[int, string](Period{}, 0, "l", black)
				lr := newNode[int, string](Period{}, 0, "lr", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				l.right = lr
				lr.parent = l
				return &PeriodCollection[int, string]{root: p}, r
			},
			func(t *testing.T, pc *PeriodCollection[int, string]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "lr", pc.root.left.contents)
				assert.Equal(t, "r", pc.root.right.contents)
				assert.Equal(t, "l", pc.root.left.left.contents)
				assert.Equal(t, black, pc.root.left.color)
				assert.Equal(t, red, pc.root.left.left.color)
			},
		}, {
			"right rotate around sibling and recolor when node is left child and sibling is black with red left child",
			func() (*PeriodCollection[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "p", black)
				r := newNode[int, string](Period{}, 0, "r", black)
				l := newNode[int, string](Period{}, 0, "l", black)
				rl := newNode[int, string](Period{}, 0, "rl", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				r.left = rl
				rl.parent = r
				return &PeriodCollection[int, string]{root: p}, l
			},
			func(t *testing.T, pc *PeriodCollection[int, string]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "rl", pc.root.right.contents)
				assert.Equal(t, "l", pc.root.left.contents)
				assert.Equal(t, "r", pc.root.right.right.contents)
				assert.Equal(t, black, pc.root.right.color)
				assert.Equal(t, red, pc.root.right.right.color)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pc, n := test.setup()
			pc.deleteRepairCase3(n)
			test.validate(t, pc)
		})
	}
}

func TestPeriodCollection_deleteRepairCase4(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() (*PeriodCollection[int, string], *node[int, string])
		validate func(t *testing.T, pc *PeriodCollection[int, string])
	}{
		{
			"no action when sibling is a leaf",
			func() (*PeriodCollection[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "p", black)
				r := newNode[int, string](Period{}, 0, "r", black)
				r.parent = p
				p.right = r
				return &PeriodCollection[int, string]{root: p}, r
			},
			func(t *testing.T, pc *PeriodCollection[int, string]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "r", pc.root.right.contents)
				assert.True(t, pc.root.left.leaf)
			},
		}, {
			"no action when sibling is red",
			func() (*PeriodCollection[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "p", black)
				r := newNode[int, string](Period{}, 0, "r", red)
				l := newNode[int, string](Period{}, 0, "l", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				return &PeriodCollection[int, string]{root: p}, r
			},
			func(t *testing.T, pc *PeriodCollection[int, string]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "l", pc.root.left.contents)
				assert.Equal(t, "r", pc.root.right.contents)
			},
		}, {
			"no action when right child and sibling has no left child",
			func() (*PeriodCollection[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "p", black)
				r := newNode[int, string](Period{}, 0, "r", black)
				l := newNode[int, string](Period{}, 0, "l", black)
				lr := newNode[int, string](Period{}, 0, "lr", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				l.right = lr
				lr.parent = l
				return &PeriodCollection[int, string]{root: p}, r
			},
			func(t *testing.T, pc *PeriodCollection[int, string]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "l", pc.root.left.contents)
				assert.Equal(t, "r", pc.root.right.contents)
				assert.Equal(t, "lr", pc.root.left.right.contents)
			},
		}, {
			"no action when left child and sibling has no right child",
			func() (*PeriodCollection[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "p", black)
				r := newNode[int, string](Period{}, 0, "r", black)
				l := newNode[int, string](Period{}, 0, "l", black)
				rl := newNode[int, string](Period{}, 0, "rl", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				r.left = rl
				rl.parent = r
				return &PeriodCollection[int, string]{root: p}, r
			},
			func(t *testing.T, pc *PeriodCollection[int, string]) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "l", pc.root.left.contents)
				assert.Equal(t, "r", pc.root.right.contents)
				assert.Equal(t, "rl", pc.root.right.left.contents)
			},
		}, {
			"right rotate around parent and recolor when right child and sibling is black with red left child",
			func() (*PeriodCollection[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "p", red)
				r := newNode[int, string](Period{}, 0, "r", black)
				l := newNode[int, string](Period{}, 0, "l", black)
				ll := newNode[int, string](Period{}, 0, "ll", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				l.left = ll
				ll.parent = l
				return &PeriodCollection[int, string]{root: p}, r
			},
			func(t *testing.T, pc *PeriodCollection[int, string]) {
				assert.Equal(t, "l", pc.root.contents)
				assert.Equal(t, "ll", pc.root.left.contents)
				assert.Equal(t, "p", pc.root.right.contents)
				assert.Equal(t, "r", pc.root.right.right.contents)
				assert.Equal(t, black, pc.root.left.color)
				assert.Equal(t, black, pc.root.right.color)
				assert.Equal(t, red, pc.root.color)
			},
		}, {
			"left rotate around parent and recolor when left child and sibling is black with red right child",
			func() (*PeriodCollection[int, string], *node[int, string]) {
				p := newNode[int, string](Period{}, 0, "p", red)
				r := newNode[int, string](Period{}, 0, "r", black)
				l := newNode[int, string](Period{}, 0, "l", black)
				rr := newNode[int, string](Period{}, 0, "rr", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				r.right = rr
				rr.parent = r
				return &PeriodCollection[int, string]{root: p}, l
			},
			func(t *testing.T, pc *PeriodCollection[int, string]) {
				assert.Equal(t, "r", pc.root.contents)
				assert.Equal(t, "p", pc.root.left.contents)
				assert.Equal(t, "rr", pc.root.right.contents)
				assert.Equal(t, "l", pc.root.left.left.contents)
				assert.Equal(t, black, pc.root.left.color)
				assert.Equal(t, black, pc.root.right.color)
				assert.Equal(t, red, pc.root.color)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pc, n := test.setup()
			pc.deleteRepairCase4(n)
			test.validate(t, pc)
		})
	}
}

func TestPeriodCollection_AnyContainsTime(t *testing.T) {
	periods := []Period{
		NewPeriod(time.Date(2018, 12, 6, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 7, 0, 0, 0, 0, time.UTC)),
		NewPeriod(time.Date(2018, 12, 7, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 8, 0, 0, 0, 0, time.UTC)),
		NewPeriod(time.Date(2018, 12, 8, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 9, 0, 0, 0, 0, time.UTC)),
		NewPeriod(time.Date(2018, 12, 9, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 10, 0, 0, 0, 0, time.UTC)),
	}
	tests := []struct {
		name            string
		periods         []Period
		query           time.Time
		expectedOutcome bool
	}{
		{
			"period that contains time in left subtree returns true",
			periods,
			time.Date(2018, 12, 6, 15, 0, 0, 0, time.UTC),
			true,
		}, {
			"period that contains time in right subtree returns true",
			periods,
			time.Date(2018, 12, 9, 15, 0, 0, 0, time.UTC),
			true,
		}, {
			"time not contained in any period in the tree returns false",
			periods,
			time.Date(2018, 12, 12, 0, 0, 0, 0, time.UTC),
			false,
		}, {
			"root that contains time returns true",
			[]Period{
				NewPeriod(time.Date(2018, 12, 6, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 7, 0, 0, 0, 0, time.UTC)),
			},
			time.Date(2018, 12, 6, 15, 0, 0, 0, time.UTC),
			true,
		}, {
			"time in unbounded period in left subtree returns true",
			[]Period{
				NewPeriod(time.Date(2018, 12, 6, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 7, 0, 0, 0, 0, time.UTC)),
				NewPeriod(time.Date(2018, 12, 5, 0, 0, 0, 0, time.UTC), time.Time{}),
			},
			time.Date(2018, 12, 27, 0, 0, 0, 0, time.UTC),
			true,
		}, {
			"time in unbounded period in right subtree returns true",
			[]Period{
				NewPeriod(time.Date(2018, 12, 6, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 7, 0, 0, 0, 0, time.UTC)),
				NewPeriod(time.Date(2018, 12, 8, 0, 0, 0, 0, time.UTC), time.Time{}),
			},
			time.Date(2018, 12, 27, 0, 0, 0, 0, time.UTC),
			true,
		}, {
			"time in unbounded period in root returns true",
			[]Period{NewPeriod(time.Date(2018, 12, 6, 0, 0, 0, 0, time.UTC), time.Time{})},
			time.Date(2018, 12, 27, 0, 0, 0, 0, time.UTC),
			true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pc := NewPeriodCollection[int, any]()
			for i, p := range test.periods {
				require.NoError(t, pc.Insert(i, p, nil))
			}
			assert.Equal(t, test.expectedOutcome, pc.AnyContainsTime(test.query))
		})
	}
}

func TestPeriodCollection_ContainsTime(t *testing.T) {
	nodes := []struct {
		contents string
		period   Period
	}{
		{"a", NewPeriod(time.Date(2018, 12, 5, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 7, 0, 0, 0, 0, time.UTC))},
		{"b", NewPeriod(time.Date(2018, 12, 7, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 8, 0, 0, 0, 0, time.UTC))},
		{"c", NewPeriod(time.Date(2018, 12, 7, 12, 0, 0, 0, time.UTC), time.Time{})},
	}
	pc := NewPeriodCollection[int, any]()
	for i, n := range nodes {
		require.NoError(t, pc.Insert(i, n.period, n.contents))
	}
	tests := []struct {
		name             string
		setupCollection  func() *PeriodCollection[int, any]
		time             time.Time
		expectedContents []any
	}{
		{
			"2018-12-05 00:00 contained in period a",
			func() *PeriodCollection[int, any] {
				return pc
			},
			time.Date(2018, 12, 5, 0, 0, 0, 0, time.UTC),
			[]any{"a"},
		}, {
			"2018-12-05 12:00 contained in period a",
			func() *PeriodCollection[int, any] {
				return pc
			},
			time.Date(2018, 12, 5, 12, 0, 0, 0, time.UTC),
			[]any{"a"},
		}, {
			"2018-12-07 00:00 contained in period b",
			func() *PeriodCollection[int, any] {
				return pc
			},
			time.Date(2018, 12, 7, 0, 0, 0, 0, time.UTC),
			[]any{"b"},
		}, {
			"2018-12-07 16:00 contained in periods b and c",
			func() *PeriodCollection[int, any] {
				return pc
			},
			time.Date(2018, 12, 7, 16, 0, 0, 0, time.UTC),
			[]any{"b", "c"},
		}, {
			"2018-12-04 00:00 not contained in any periods",
			func() *PeriodCollection[int, any] {
				return pc
			},
			time.Date(2018, 12, 4, 0, 0, 0, 0, time.UTC),
			[]any{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			collection := test.setupCollection()
			assert.Equal(t, test.expectedContents, collection.ContainsTime(test.time))
		})
	}
}

func TestPeriodCollection_Intersecting(t *testing.T) {
	chiTz, err := time.LoadLocation("America/Chicago")
	require.NoError(t, err)
	nodes := []struct {
		contents string
		period   Period
	}{
		{"a", NewPeriod(time.Date(2018, 12, 6, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 7, 0, 0, 0, 0, time.UTC))},
		{"b", NewPeriod(time.Date(2018, 12, 7, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 8, 0, 0, 0, 0, time.UTC))},
		{"c", NewPeriod(time.Date(2018, 12, 8, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 9, 0, 0, 0, 0, time.UTC))},
		{"d", NewPeriod(time.Date(2018, 12, 9, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 10, 0, 0, 0, 0, time.UTC))},
		{"e", NewPeriod(time.Date(2018, 12, 10, 0, 0, 0, 0, chiTz), time.Date(2018, 12, 10, 12, 0, 0, 0, chiTz))},
		{"f", NewPeriod(time.Date(2018, 12, 27, 0, 0, 0, 0, chiTz), time.Time{})},
	}
	pc := NewPeriodCollection[int, any]()
	for i, n := range nodes {
		require.NoError(t, pc.Insert(i, n.period, n.contents))
	}
	tests := []struct {
		name             string
		setupCollection  func() *PeriodCollection[int, any]
		query            Period
		expectedContents []any
	}{
		{
			"2018-12-05 12:00 - 2018-12-06 12:00 intersects period a",
			func() *PeriodCollection[int, any] {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 5, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 6, 12, 0, 0, 0, time.UTC)),
			[]any{"a"},
		}, {
			"2018-12-05 12:00 - 2018-12-07 12:00 intersects period a and b",
			func() *PeriodCollection[int, any] {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 5, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 7, 12, 0, 0, 0, time.UTC)),
			[]any{"a", "b"},
		}, {
			"2018-12-05 12:00 - 2018-12-12 12:00 intersects periods a, b, c, d, and e",
			func() *PeriodCollection[int, any] {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 5, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 12, 12, 0, 0, 0, time.UTC)),
			[]any{"a", "b", "c", "d", "e"},
		}, {
			"2018-12-05 12:00 - 2018-12-07 00:00 intersects period a",
			func() *PeriodCollection[int, any] {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 5, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 7, 0, 0, 0, 0, time.UTC)),
			[]any{"a"},
		}, {
			"2018-12-05 12:00 - 2018-12-05 14:00 does not intersect",
			func() *PeriodCollection[int, any] {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 5, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 5, 14, 0, 0, 0, time.UTC)),
			[]any{},
		}, {
			"2018-12-20 12:00 - 2018-12-20 14:00 does not intersect",
			func() *PeriodCollection[int, any] {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 20, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 20, 14, 0, 0, 0, time.UTC)),
			[]any{},
		}, {
			"2018-12-07 12:00 - 2018-12-07 14:00 intersects period b",
			func() *PeriodCollection[int, any] {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 7, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 7, 14, 0, 0, 0, time.UTC)),
			[]any{"b"},
		}, {
			"2018-12-10 02:00 - 10:00 CST intersects period e",
			func() *PeriodCollection[int, any] {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 10, 2, 0, 0, 0, chiTz), time.Date(2018, 12, 10, 10, 0, 0, 0, chiTz)),
			[]any{"e"},
		}, {
			"2018-12-09 20:00 - 2018-12-10 10:00 UTC intersects period d and e",
			func() *PeriodCollection[int, any] {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 9, 20, 0, 0, 0, time.UTC), time.Date(2018, 12, 10, 10, 0, 0, 0, time.UTC)),
			[]any{"d", "e"},
		}, {
			"tree with leaf root returns nothing",
			func() *PeriodCollection[int, any] {
				return NewPeriodCollection[int, any]()
			},
			Period{},
			[]any{},
		}, {
			"2018-12-28 12:00 - 2018-12-28 14:00 intersects period f",
			func() *PeriodCollection[int, any] {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 28, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 28, 14, 0, 0, 0, time.UTC)),
			[]any{"f"},
		}, {
			"2018-12-9 12:00 - 2018-12-28 14:00 intersects periods d, e, and f",
			func() *PeriodCollection[int, any] {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 9, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 28, 14, 0, 0, 0, time.UTC)),
			[]any{"d", "e", "f"},
		},
		/* Intersection set includes RL
		  N
		 / \
		L   R
		   /
		  RL
		*/
		{
			"2018-12-9 12:00 - 2018-12-28 14:00 intersects periods including in order successor of root",
			func() *PeriodCollection[int, any] {
				n := newNode[int, any](Period{time.Date(2019, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 4, 0, 0, 0, 0, time.UTC)}, 1, "n", black)
				l := newNode[int, any](Period{time.Date(2019, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 2, 0, 0, 0, 0, time.UTC)}, 2, "l", black)
				r := newNode[int, any](Period{time.Date(2019, 12, 5, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 6, 0, 0, 0, 0, time.UTC)}, 3, "r", black)
				rl := newNode[int, any](Period{time.Date(2019, 12, 3, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 4, 0, 0, 0, 0, time.UTC)}, 4, "rl", red)
				n.left, n.right = l, r
				r.left, r.parent, r.maxEnd = rl, n, rl.period.End
				l.parent = n
				rl.parent = r
				return &PeriodCollection[int, any]{
					root:  n,
					nodes: map[int]*node[int, any]{1: n, 2: l, 3: r, 4: rl},
				}
			},
			NewPeriod(time.Date(2019, 12, 3, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 4, 0, 0, 0, 0, time.UTC)),
			[]any{"n", "rl"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			collection := test.setupCollection()
			assert.Equal(t, test.expectedContents, collection.Intersecting(test.query))
		})
	}
}

func TestPeriodCollection_ContainsKey(t *testing.T) {
	pc := PeriodCollection[int, any]{
		nodes: map[int]*node[int, any]{
			1: {},
		},
	}
	tests := []struct {
		name    string
		k       int
		outcome bool
	}{
		{
			"key in nodes map returns true",
			1,
			true,
		}, {
			"key not in nodes map returns false",
			2,
			false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.outcome, pc.ContainsKey(test.k))
		})
	}
}

func TestPeriodCollection_Update(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() *PeriodCollection[int, int]
		updateKey   int
		newContents int
		newPeriod   Period
		validate    func(t *testing.T, pc *PeriodCollection[int, int])
	}{
		{
			"updating a key that doesn't exist inserts a new period",
			func() *PeriodCollection[int, int] { return NewPeriodCollection[int, int]() },
			1,
			1,
			Period{},
			func(t *testing.T, pc *PeriodCollection[int, int]) {
				assert.Len(t, pc.nodes, 1)
				assert.Equal(t, 1, pc.root.key)
				assert.True(t, pc.root.left.leaf)
				assert.True(t, pc.root.right.leaf)
			},
		}, {
			"updating contents without updating the period swaps contents",
			func() *PeriodCollection[int, int] {
				pc := NewPeriodCollection[int, int]()
				l := &node[int, int]{contents: 1}
				pc.root = &node[int, int]{
					left: l,
				}
				pc.nodes[0] = pc.root
				pc.nodes[1] = l
				return pc
			},
			1,
			2,
			Period{},
			func(t *testing.T, pc *PeriodCollection[int, int]) {
				l, ok := pc.nodes[1]
				require.True(t, ok)
				require.Equal(t, l, pc.root.left)
				assert.Equal(t, l.contents, 2)
				assert.Len(t, pc.nodes, 2)
			},
		}, {
			"updating the period deletes and reinserts the node",
			func() *PeriodCollection[int, int] {
				pc := NewPeriodCollection[int, int]()
				root := newNode[int, int](NewPeriod(time.Unix(10, 0), time.Unix(25, 0)), 0, 0, black)
				l := newNode[int, int](NewPeriod(time.Unix(5, 0), time.Unix(30, 0)), 1, 1, red)
				pc.root = root
				root.left, l.parent = l, root
				pc.nodes[0] = root
				pc.nodes[1] = l
				return pc
			},
			1,
			2,
			NewPeriod(time.Unix(20, 0), time.Unix(30, 0)),
			func(t *testing.T, pc *PeriodCollection[int, int]) {
				// the node should move from the the parent's left to right
				r, ok := pc.nodes[1]
				require.True(t, ok)
				require.Equal(t, r, pc.root.right)
				assert.Equal(t, r.contents, 2)
				assert.True(t, pc.root.left.leaf)
				assert.Len(t, pc.nodes, 2)
			},
		}, {
			"updating the root's period works",
			func() *PeriodCollection[int, int] {
				pc := NewPeriodCollection[int, int]()
				root := newNode(NewPeriod(time.Unix(10, 0), time.Unix(25, 0)), 0, 0, black)
				pc.root = root
				pc.nodes[0] = root
				return pc
			},
			0,
			1,
			NewPeriod(time.Unix(10, 0), time.Unix(30, 0)),
			func(t *testing.T, pc *PeriodCollection[int, int]) {
				root, ok := pc.nodes[0]
				require.True(t, ok)
				assert.Equal(t, root, pc.root)
				assert.Equal(t, 1, pc.root.contents)
				assert.Equal(t, time.Unix(30, 0), pc.root.period.End)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pc := test.setup()
			pc.Update(test.updateKey, test.newPeriod, test.newContents)
			test.validate(t, pc)
		})
	}
}

func TestPeriodCollection_AnyIntersecting(t *testing.T) {
	tests := []struct {
		name             string
		createCollection func(t *testing.T) *PeriodCollection[int, any]
		query            Period
		expectedOutcome  bool
	}{
		{
			"searching with intersection in left subtree works",
			func(t *testing.T) *PeriodCollection[int, any] {
				periods := []Period{
					// becomes left
					NewPeriod(time.Date(2018, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 15, 0, 0, 0, 0, time.UTC)),
					// becomes right
					NewPeriod(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 30, 0, 0, 0, 0, time.UTC)),
					// becomes root
					NewPeriod(time.Date(2018, 12, 27, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 27, 0, 0, 0, 1, time.UTC)),
				}
				pc := NewPeriodCollection[int, any]()
				for i, p := range periods {
					require.NoError(t, pc.Insert(i, p, nil))
				}
				return pc
			},
			NewPeriod(time.Date(2018, 12, 28, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 2, 0, 0, 0, 0, time.UTC)),
			true,
		}, {
			"searching with intersection in right subtree works",
			func(t *testing.T) *PeriodCollection[int, any] {
				periods := []Period{
					NewPeriod(time.Date(2018, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 15, 0, 0, 0, 0, time.UTC)),
					NewPeriod(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 30, 0, 0, 0, 0, time.UTC)),
					NewPeriod(time.Date(2018, 12, 27, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 27, 0, 0, 0, 1, time.UTC)),
				}
				pc := NewPeriodCollection[int, any]()
				for i, p := range periods {
					require.NoError(t, pc.Insert(i, p, nil))
				}
				return pc
			},
			NewPeriod(time.Date(2019, 1, 20, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 22, 0, 0, 0, 0, time.UTC)),
			true,
		}, {
			"searching when no intersection in tree works",
			func(t *testing.T) *PeriodCollection[int, any] {
				pc := NewPeriodCollection[int, any]()
				require.NoError(
					t, pc.Insert(
						1,
						NewPeriod(
							time.Date(2018, 12, 6, 0, 0, 0, 0, time.UTC),
							time.Date(2018, 12, 10, 0, 0, 0, 0, time.UTC),
						), nil))
				return pc
			},
			NewPeriod(time.Date(2018, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 2, 0, 0, 0, 0, time.UTC)),
			false,
		}, {
			"tree with leaf root does not intersect",
			func(t *testing.T) *PeriodCollection[int, any] { return NewPeriodCollection[int, any]() },
			Period{},
			false,
		}, {
			"searching with unbound intersection in right subtree works",
			func(t *testing.T) *PeriodCollection[int, any] {
				periods := []Period{
					NewPeriod(time.Date(2018, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 15, 0, 0, 0, 0, time.UTC)),
					NewPeriod(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), time.Time{}),
					NewPeriod(time.Date(2018, 12, 27, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 27, 0, 0, 0, 1, time.UTC)),
				}
				pc := NewPeriodCollection[int, any]()
				for i, p := range periods {
					require.NoError(t, pc.Insert(i, p, nil))
				}
				return pc
			},
			NewPeriod(time.Date(2019, 1, 20, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 22, 0, 0, 0, 0, time.UTC)),
			true,
		}, {
			"searching with unbound intersection in left subtree works",
			func(t *testing.T) *PeriodCollection[int, any] {
				periods := []Period{
					NewPeriod(time.Date(2018, 12, 1, 0, 0, 0, 0, time.UTC), time.Time{}),
					NewPeriod(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 30, 0, 0, 0, 0, time.UTC)),
					NewPeriod(time.Date(2018, 12, 27, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 27, 0, 0, 0, 1, time.UTC)),
				}
				pc := NewPeriodCollection[int, any]()
				for i, p := range periods {
					require.NoError(t, pc.Insert(i, p, nil))
				}
				return pc
			},
			NewPeriod(time.Date(2018, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 1, 1, 0, 0, 0, time.UTC)),
			true,
		}, {
			"searching with unbound intersection in root works",
			func(t *testing.T) *PeriodCollection[int, any] {
				periods := []Period{
					NewPeriod(time.Date(2018, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 15, 0, 0, 0, 0, time.UTC)),
					NewPeriod(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), time.Time{}),
					NewPeriod(time.Date(2018, 12, 27, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 27, 0, 0, 0, 1, time.UTC)),
				}
				pc := NewPeriodCollection[int, any]()
				for i, p := range periods {
					require.NoError(t, pc.Insert(i, p, nil))
				}
				return pc
			},
			NewPeriod(time.Date(2020, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2020, 12, 1, 1, 0, 0, 0, time.UTC)),
			true,
		}, {
			"searching with before unbound intersection returns false",
			func(t *testing.T) *PeriodCollection[int, any] {
				periods := []Period{
					NewPeriod(time.Date(2018, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 15, 0, 0, 0, 0, time.UTC)),
					NewPeriod(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), time.Time{}),
					NewPeriod(time.Date(2018, 12, 27, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 27, 0, 0, 0, 1, time.UTC)),
				}
				pc := NewPeriodCollection[int, any]()
				for i, p := range periods {
					require.NoError(t, pc.Insert(i, p, nil))
				}
				return pc
			},
			NewPeriod(time.Date(2017, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2017, 12, 1, 1, 0, 0, 0, time.UTC)),
			false,
		},
		{
			/* RL is only node that intersects query period
			  N
			 / \
			L   R
			   /
			  RL
			*/
			"searching with in order successor of root as only intersection",
			func(t *testing.T) *PeriodCollection[int, any] {
				n := newNode[int, any](Period{time.Date(2019, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 2, 0, 0, 0, 0, time.UTC)}, 1, "n", black)
				l := newNode[int, any](Period{time.Date(2019, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 2, 0, 0, 0, 0, time.UTC)}, 2, "l", black)
				r := newNode[int, any](Period{time.Date(2019, 12, 5, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 6, 0, 0, 0, 0, time.UTC)}, 3, "r", black)
				rl := newNode[int, any](Period{time.Date(2019, 12, 3, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 4, 0, 0, 0, 0, time.UTC)}, 4, "rl", red)
				n.left, n.right = l, r
				r.left, r.parent, r.maxEnd = rl, n, rl.period.End
				l.parent = n
				rl.parent = r
				return &PeriodCollection[int, any]{
					root:  n,
					nodes: map[int]*node[int, any]{1: n, 2: l, 3: r, 4: rl},
				}
			},
			NewPeriod(time.Date(2019, 12, 3, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 4, 0, 0, 0, 0, time.UTC)),
			true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pc := test.createCollection(t)
			assert.Equal(t, test.expectedOutcome, pc.AnyIntersecting(test.query))
		})
	}
}

func TestPeriodCollection_DepthFirstTraverse(t *testing.T) {
	/*
	     D
	    / \
	   C   E
	  / \
	 A   B
	*/
	tree := PeriodCollection[int, string]{
		root: &node[int, string]{
			contents: "D",
			left: &node[int, string]{
				contents: "C",
				left: &node[int, string]{
					contents: "A",
					left:     &node[int, string]{leaf: true},
					right:    &node[int, string]{leaf: true},
				},
				right: &node[int, string]{
					contents: "B",
					left:     &node[int, string]{leaf: true},
					right:    &node[int, string]{leaf: true},
				},
			},
			right: &node[int, string]{
				contents: "E",
				left:     &node[int, string]{leaf: true},
				right:    &node[int, string]{leaf: true},
			},
		},
	}
	tests := []struct {
		name            string
		order           TraversalOrder
		expectedOutcome []string
	}{
		{
			"traverse pre-order works",
			PreOrder,
			[]string{"D", "C", "A", "B", "E"},
		}, {
			"traverse in-order works",
			InOrder,
			[]string{"A", "C", "B", "D", "E"},
		}, {
			"traverse post-order works",
			PostOrder,
			[]string{"A", "B", "C", "E", "D"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedOutcome, tree.DepthFirstTraverse(test.order))
		})
	}
}

func TestPeriodCollection_DeleteOnCondition(t *testing.T) {
	setupTree := func() *PeriodCollection[int, any] {
		nodes := []struct {
			contents int
			period   Period
		}{
			{1, NewPeriod(time.Time{}, time.Time{})},
			{2, NewPeriod(time.Time{}, time.Time{})},
			{3, NewPeriod(time.Time{}, time.Time{})},
			{4, NewPeriod(time.Time{}, time.Time{})},
			{5, NewPeriod(time.Time{}, time.Time{})},
			{6, NewPeriod(time.Time{}, time.Time{})},
		}
		pc := NewPeriodCollection[int, any]()
		for _, n := range nodes {
			require.NoError(t, pc.Insert(n.contents, n.period, n.contents))
		}
		return pc
	}
	tests := []struct {
		name      string
		condition func(i any) bool
		validate  func(t *testing.T, pc *PeriodCollection[int, any])
	}{
		{
			"delete all nodes",
			func(i any) bool {
				return true
			},
			func(t *testing.T, pc *PeriodCollection[int, any]) {
				assert.Equal(t, 0, len(pc.nodes))
			},
		}, {
			"delete 0 nodes",
			func(i any) bool {
				return false
			},
			func(t *testing.T, pc *PeriodCollection[int, any]) {
				assert.Equal(t, 6, len(pc.nodes))
			},
		}, {
			"delete all even numbers",
			func(i any) bool {
				return i.(int)%2 == 0
			},
			func(t *testing.T, pc *PeriodCollection[int, any]) {
				assert.Equal(t, 3, len(pc.nodes))
				assert.True(t, pc.ContainsKey(1))
				assert.True(t, pc.ContainsKey(3))
				assert.True(t, pc.ContainsKey(5))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pc := setupTree()
			pc.DeleteOnCondition(test.condition)
			test.validate(t, pc)
		})
	}
}

func TestPeriodCollection_PrepareUpdate(t *testing.T) {
	pc := NewPeriodCollection[int, any]()
	assert.Equal(
		t,
		Update[int, any]{
			key:         1,
			newPeriod:   Period{Start: time.Unix(1, 0), End: time.Unix(2, 0)},
			newContents: 1,
			pc:          pc,
		},
		pc.PrepareUpdate(1, Period{Start: time.Unix(1, 0), End: time.Unix(2, 0)}, 1),
	)
}

func TestPeriodCollection_Execute(t *testing.T) {
	pc := NewPeriodCollection[int, any]()
	u1 := Update[int, any]{
		key:         1,
		newContents: 1,
		newPeriod:   Period{Start: time.Unix(1, 0), End: time.Unix(2, 0)},
		pc:          pc,
	}
	u2 := Update[int, any]{
		key:         2,
		newContents: 2,
		newPeriod:   Period{Start: time.Unix(1, 0), End: time.Unix(2, 0)},
		pc:          pc,
	}
	d1 := Delete[int, any]{key: 1, pc: pc}
	pc.Execute(u1, u2, d1)
	assert.Contains(t, pc.nodes, 2)
	assert.NotContains(t, pc.nodes, 1)
}

func TestPeriodCollection_PrepareDelete(t *testing.T) {
	pc := NewPeriodCollection[int, any]()
	assert.Equal(t, Delete[int, any]{key: 1, pc: pc}, pc.PrepareDelete(1))
}

func TestUpdate_execute(t *testing.T) {
	pc := NewPeriodCollection[int, any]()
	u := Update[int, any]{
		key:         1,
		newContents: 1,
		newPeriod:   Period{Start: time.Unix(1, 0), End: time.Unix(2, 0)},
		pc:          pc,
	}
	u.execute()
	assert.Contains(t, pc.nodes, 1)
}

func TestDelete_execute(t *testing.T) {
	pc := NewPeriodCollection[int, any]()
	pc.update(1, Period{}, 1)
	require.Contains(t, pc.nodes, 1)
	d := Delete[int, any]{key: 1, pc: pc}
	d.execute()
	assert.NotContains(t, pc.nodes, 1)
}

func TestPeriodCollection_ContentsOfKey(t *testing.T) {
	pc := PeriodCollection[int, any]{nodes: map[int]*node[int, any]{1: {contents: "contents"}}}
	tests := []struct {
		name             string
		key              int
		expectedContents any
		expectErr        bool
	}{
		{
			"should return contents of key",
			1,
			"contents",
			false,
		}, {
			"should return error when void of key",
			2,
			nil,
			true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			contents, err := pc.ContentsOfKey(test.key)
			if test.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expectedContents, contents)
			}
		})
	}
}

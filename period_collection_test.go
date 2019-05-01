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
		setupTree    func() *PeriodCollection
		insertions   []insertions
		validateTree func(t *testing.T, pc *PeriodCollection)
	}{
		{
			"inserting a single node creates a black root",
			func() *PeriodCollection { return NewPeriodCollection() },
			[]insertions{{NewPeriod(time.Unix(1, 0), time.Unix(5, 0)), 0, false}},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, black, pc.root.Color)
				assert.Contains(t, pc.nodes, 0)
				assert.Equal(t, time.Unix(5, 0), pc.root.MaxEnd)
			},
		}, {
			"inserting a node into a tree with a sentinel root replaces the sentinel with a new root",
			func() *PeriodCollection {
				pc := NewPeriodCollection()
				pc.root = &node{Leaf: true}
				return pc
			},
			[]insertions{{NewPeriod(time.Unix(1, 0), time.Unix(5, 0)), 0, false}},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, black, pc.root.Color)
				assert.False(t, pc.root.Leaf)
				assert.Equal(t, time.Unix(1, 0), pc.root.Period.Start)
				assert.Contains(t, pc.nodes, 0)
				assert.Len(t, pc.nodes, 1)
				assert.Equal(t, time.Unix(5, 0), pc.root.MaxEnd)
			},
		}, {
			/* after insertion, 1 and 3 are red and 2 is black
			  2
			 / \
			1   3
			*/
			"inserting 3 nodes creates red children",
			func() *PeriodCollection { return NewPeriodCollection() },
			[]insertions{
				{NewPeriod(time.Unix(2, 0), time.Unix(5, 0)), 0, false},
				{NewPeriod(time.Unix(1, 0), time.Unix(10, 0)), 1, false},
				{NewPeriod(time.Unix(3, 0), time.Unix(4, 0)), 2, false},
			},
			func(t *testing.T, pc *PeriodCollection) {
				require.NotNil(t, pc.root.Left)
				require.NotNil(t, pc.root.Right)
				assert.Equal(t, time.Unix(2, 0), pc.root.Period.Start)
				assert.Equal(t, time.Unix(1, 0), pc.root.Left.Period.Start)
				assert.Equal(t, time.Unix(3, 0), pc.root.Right.Period.Start)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, red, pc.root.Left.Color)
				assert.Equal(t, red, pc.root.Right.Color)
				for i := 0; i < 3; i++ {
					assert.Contains(t, pc.nodes, i)
				}
				assert.Len(t, pc.nodes, 3)
				assert.Equal(t, time.Unix(10, 0), pc.root.MaxEnd)
				assert.Equal(t, time.Unix(10, 0), pc.root.Left.MaxEnd)
				assert.Equal(t, time.Unix(4, 0), pc.root.Right.MaxEnd)
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
			func() *PeriodCollection { return NewPeriodCollection() },
			[]insertions{
				{NewPeriod(time.Unix(1, 0), time.Unix(5, 0)), 0, false},
				{NewPeriod(time.Unix(2, 0), time.Unix(4, 0)), 1, false},
				{NewPeriod(time.Unix(3, 0), time.Unix(10, 0)), 2, false},
			},
			func(t *testing.T, pc *PeriodCollection) {
				require.NotNil(t, pc.root.Left)
				require.NotNil(t, pc.root.Right)
				assert.Equal(t, time.Unix(2, 0), pc.root.Period.Start)
				assert.Equal(t, time.Unix(1, 0), pc.root.Left.Period.Start)
				assert.Equal(t, time.Unix(3, 0), pc.root.Right.Period.Start)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, red, pc.root.Left.Color)
				assert.Equal(t, red, pc.root.Right.Color)
				for i := 0; i < 3; i++ {
					assert.Contains(t, pc.nodes, i)
				}
				assert.Len(t, pc.nodes, 3)
				assert.Len(t, pc.nodes, 3)
				assert.Equal(t, time.Unix(10, 0), pc.root.MaxEnd)
				assert.Equal(t, time.Unix(5, 0), pc.root.Left.MaxEnd)
				assert.Equal(t, time.Unix(10, 0), pc.root.Right.MaxEnd)
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
			func() *PeriodCollection {
				twenty := newNode(NewPeriod(time.Unix(20, 0), time.Unix(25, 0)), nil, nil, black)
				ten := newNode(NewPeriod(time.Unix(10, 0), time.Unix(22, 0)), nil, nil, red)
				thirty := newNode(NewPeriod(time.Unix(30, 0), time.Unix(100, 0)), nil, nil, red)
				twenty.Left, twenty.Right, twenty.MaxEnd = ten, thirty, thirty.Period.End
				ten.parent, ten.MaxEnd = twenty, ten.Period.End
				thirty.parent, thirty.MaxEnd = twenty, thirty.Period.End
				pc := NewPeriodCollection()
				pc.root = twenty
				return pc
			},
			[]insertions{{NewPeriod(time.Unix(35, 0), time.Unix(50, 0)), 0, false}},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, time.Unix(20, 0), pc.root.Period.Start)
				assert.Equal(t, time.Unix(10, 0), pc.root.Left.Period.Start)
				assert.Equal(t, time.Unix(30, 0), pc.root.Right.Period.Start)
				assert.Equal(t, time.Unix(35, 0), pc.root.Right.Right.Period.Start)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, black, pc.root.Left.Color)
				assert.Equal(t, black, pc.root.Right.Color)
				assert.Equal(t, red, pc.root.Right.Right.Color)
				assert.Equal(t, time.Unix(100, 0), pc.root.MaxEnd)
				assert.Equal(t, time.Unix(22, 0), pc.root.Left.MaxEnd)
				assert.Equal(t, time.Unix(100, 0), pc.root.Right.MaxEnd)
				assert.Equal(t, time.Unix(50, 0), pc.root.Right.Right.MaxEnd)
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
			func() *PeriodCollection {
				twenty := newNode(NewPeriod(time.Unix(20, 0), time.Unix(50, 0)), nil, nil, black)
				thirty := newNode(NewPeriod(time.Unix(30, 0), time.Unix(75, 0)), nil, nil, red)
				twenty.Right, twenty.MaxEnd = thirty, thirty.Period.End
				thirty.parent, thirty.MaxEnd = twenty, thirty.Period.End
				pc := NewPeriodCollection()
				pc.root = twenty
				return pc
			},
			[]insertions{{NewPeriod(time.Unix(25, 0), time.Unix(100, 0)), 0, false}},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, time.Unix(25, 0), pc.root.Period.Start)
				assert.Equal(t, time.Unix(20, 0), pc.root.Left.Period.Start)
				assert.Equal(t, time.Unix(30, 0), pc.root.Right.Period.Start)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, red, pc.root.Left.Color)
				assert.Equal(t, red, pc.root.Right.Color)
				assert.Equal(t, time.Unix(100, 0), pc.root.MaxEnd)
				assert.Equal(t, time.Unix(50, 0), pc.root.Left.MaxEnd)
				assert.Equal(t, time.Unix(75, 0), pc.root.Right.MaxEnd)
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
			func() *PeriodCollection {
				twentyFive := newNode(NewPeriod(time.Unix(25, 0), time.Unix(45, 0)), nil, nil, black)
				fifteen := newNode(NewPeriod(time.Unix(15, 0), time.Unix(20, 0)), nil, nil, red)
				twentyFive.Left, twentyFive.MaxEnd = fifteen, twentyFive.Period.End
				fifteen.parent, fifteen.MaxEnd = twentyFive, fifteen.Period.End
				pc := NewPeriodCollection()
				pc.root = twentyFive
				return pc
			},
			[]insertions{{NewPeriod(time.Unix(20, 0), time.Unix(40, 0)), 0, false}},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, time.Unix(20, 0), pc.root.Period.Start)
				assert.Equal(t, time.Unix(15, 0), pc.root.Left.Period.Start)
				assert.Equal(t, time.Unix(25, 0), pc.root.Right.Period.Start)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, red, pc.root.Left.Color)
				assert.Equal(t, red, pc.root.Right.Color)
				assert.Equal(t, time.Unix(45, 0), pc.root.MaxEnd)
				assert.Equal(t, time.Unix(45, 0), pc.root.Right.MaxEnd)
				assert.Equal(t, time.Unix(20, 0), pc.root.Left.MaxEnd)
			},
		}, {
			"inserting a node with the same key as an existing node returns an error",
			func() *PeriodCollection { return NewPeriodCollection() },
			[]insertions{
				{NewPeriod(time.Unix(20, 0), time.Unix(40, 0)), 0, false},
				{NewPeriod(time.Unix(20, 0), time.Unix(40, 0)), 0, true},
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Len(t, pc.nodes, 1)
			},
		}, {
			"inserting a node on the left with an unbounded period updates maxEnd correctly",
			func() *PeriodCollection {
				pc := NewPeriodCollection()
				pc.root = newNode(NewPeriod(time.Unix(20, 0), time.Unix(25, 0)), nil, nil, black)
				return pc
			},
			[]insertions{{NewPeriod(time.Unix(10, 0), time.Time{}), 0, false}},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, time.Unix(20, 0), pc.root.Period.Start)
				assert.Equal(t, time.Unix(10, 0), pc.root.Left.Period.Start)
				assert.Equal(t, time.Time{}, pc.root.MaxEnd)
				assert.Equal(t, time.Time{}, pc.root.Left.MaxEnd)
			},
		}, {
			"inserting a node on the right with an unbounded period updates maxEnd correctly",
			func() *PeriodCollection {
				pc := NewPeriodCollection()
				pc.root = newNode(NewPeriod(time.Unix(20, 0), time.Unix(25, 0)), nil, nil, black)
				return pc
			},
			[]insertions{{NewPeriod(time.Unix(30, 0), time.Time{}), 0, false}},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, time.Unix(20, 0), pc.root.Period.Start)
				assert.Equal(t, time.Unix(30, 0), pc.root.Right.Period.Start)
				assert.Equal(t, time.Time{}, pc.root.MaxEnd)
				assert.Equal(t, time.Time{}, pc.root.Right.MaxEnd)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pc := test.setupTree()
			for _, i := range test.insertions {
				err := pc.Insert(i.key, i.period, nil)
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
	nodeA := &node{}
	nodeB := &node{}
	nodeC := &node{}
	nodeD := &node{}
	cleanupTree := func() {
		for _, n := range []*node{nodeA, nodeB, nodeC, nodeD} {
			n.Left, n.Right, n.parent = &node{Leaf: true}, &node{Leaf: true}, nil
		}
	}
	setupLeftTree := func() *PeriodCollection {
		cleanupTree()
		nodeD.Left, nodeD.Period.End, nodeD.MaxEnd = nodeC, time.Unix(1, 0), time.Unix(10, 0)
		nodeC.Left, nodeC.Right, nodeC.parent, nodeC.Period.End, nodeC.MaxEnd =
			nodeA, nodeB, nodeD, time.Unix(2, 0), time.Unix(10, 0)
		nodeA.parent, nodeA.Period.End, nodeA.MaxEnd = nodeC, time.Unix(10, 0), time.Unix(10, 0)
		nodeB.parent, nodeB.Period.End, nodeB.MaxEnd = nodeC, time.Unix(5, 0), time.Unix(5, 0)
		return &PeriodCollection{root: nodeD}
	}
	setupRightTree := func() *PeriodCollection {
		cleanupTree()
		nodeD.Right, nodeD.Period.End, nodeD.MaxEnd = nodeC, time.Unix(1, 0), time.Unix(10, 0)
		nodeC.Left, nodeC.Right, nodeC.parent, nodeC.Period.End, nodeC.MaxEnd =
			nodeA, nodeB, nodeD, time.Unix(2, 0), time.Unix(10, 0)
		nodeA.parent, nodeA.Period.End, nodeA.MaxEnd = nodeC, time.Unix(10, 0), time.Unix(10, 0)
		nodeB.parent, nodeB.Period.End, nodeB.MaxEnd = nodeC, time.Unix(5, 0), time.Unix(5, 0)
		return &PeriodCollection{root: nodeD}
	}
	tests := []struct {
		name         string
		setupTree    func() *PeriodCollection
		direction    rotationDirection
		nodeToRotate *node
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
				assert.Equal(t, nodeA, nodeD.Left)
				assert.True(t, nodeD.Right.Leaf)
				assert.Nil(t, nodeD.parent)
				assert.True(t, nodeA.Left.Leaf)
				assert.Equal(t, nodeC, nodeA.Right)
				assert.Equal(t, nodeD, nodeA.parent)
				assert.True(t, nodeC.Left.Leaf)
				assert.Equal(t, nodeB, nodeC.Right)
				assert.Equal(t, nodeA, nodeC.parent)
				assert.True(t, nodeB.Left.Leaf)
				assert.True(t, nodeB.Right.Leaf)
				assert.Equal(t, nodeC, nodeB.parent)
				assert.Equal(t, time.Unix(10, 0), nodeD.MaxEnd)
				assert.Equal(t, time.Unix(10, 0), nodeA.MaxEnd)
				assert.Equal(t, time.Unix(5, 0), nodeC.MaxEnd)
				assert.Equal(t, time.Unix(5, 0), nodeB.MaxEnd)
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
				assert.True(t, nodeD.Right.Leaf)
				assert.Equal(t, nodeB, nodeD.Left)
				assert.Nil(t, nodeD.parent)
				assert.Equal(t, nodeC, nodeB.Left)
				assert.True(t, nodeB.Right.Leaf)
				assert.Equal(t, nodeD, nodeB.parent)
				assert.Equal(t, nodeA, nodeC.Left)
				assert.True(t, nodeC.Right.Leaf)
				assert.Equal(t, nodeB, nodeC.parent)
				assert.True(t, nodeA.Left.Leaf)
				assert.True(t, nodeA.Right.Leaf)
				assert.Equal(t, nodeC, nodeA.parent)
				assert.Equal(t, time.Unix(10, 0), nodeD.MaxEnd)
				assert.Equal(t, time.Unix(10, 0), nodeB.MaxEnd)
				assert.Equal(t, time.Unix(10, 0), nodeC.MaxEnd)
				assert.Equal(t, time.Unix(10, 0), nodeA.MaxEnd)
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
				assert.Equal(t, nodeA, nodeC.Left)
				assert.Equal(t, nodeD, nodeC.Right)
				assert.Nil(t, nodeC.parent)
				assert.True(t, nodeA.Left.Leaf)
				assert.True(t, nodeA.Right.Leaf)
				assert.Equal(t, nodeC, nodeA.parent)
				assert.True(t, nodeB.Left.Leaf)
				assert.True(t, nodeB.Right.Leaf)
				assert.Equal(t, nodeD, nodeB.parent)
				assert.Equal(t, nodeB, nodeD.Left)
				assert.True(t, nodeD.Right.Leaf)
				assert.Equal(t, nodeC, nodeD.parent)
				assert.Equal(t, time.Unix(10, 0), nodeC.MaxEnd)
				assert.Equal(t, time.Unix(10, 0), nodeA.MaxEnd)
				assert.Equal(t, time.Unix(5, 0), nodeD.MaxEnd)
				assert.Equal(t, time.Unix(5, 0), nodeB.MaxEnd)
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
				assert.Equal(t, nodeD, nodeC.Left)
				assert.Equal(t, nodeB, nodeC.Right)
				assert.Nil(t, nodeC.parent)
				assert.True(t, nodeD.Left.Leaf)
				assert.Equal(t, nodeA, nodeD.Right)
				assert.Equal(t, nodeC, nodeD.parent)
				assert.True(t, nodeA.Left.Leaf)
				assert.True(t, nodeA.Right.Leaf)
				assert.Equal(t, nodeD, nodeA.parent)
				assert.True(t, nodeB.Left.Leaf)
				assert.True(t, nodeB.Right.Leaf)
				assert.Equal(t, nodeC, nodeB.parent)
				assert.Equal(t, time.Unix(10, 0), nodeC.MaxEnd)
				assert.Equal(t, time.Unix(10, 0), nodeD.MaxEnd)
				assert.Equal(t, time.Unix(5, 0), nodeB.MaxEnd)
				assert.Equal(t, time.Unix(10, 0), nodeA.MaxEnd)
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
	nodeA := newNode(Period{}, nil, nil, black)
	nodeB := newNode(Period{}, nil, nil, black)
	nodeC := newNode(Period{}, nil, nil, black)
	nodeD := newNode(Period{}, nil, nil, black)
	nodeE := newNode(Period{}, nil, nil, black)
	nodeF := newNode(Period{}, nil, nil, black)
	nodeE.Left = nodeB
	nodeE.Right = nodeF
	nodeB.parent = nodeE
	nodeF.parent = nodeE
	nodeB.Left = nodeA
	nodeB.Right = nodeC
	nodeA.parent = nodeB
	nodeC.parent = nodeB
	nodeC.Right = nodeD
	nodeD.parent = nodeC
	tests := []struct {
		name              string
		successorOf       *node
		expectedSuccessor *node
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
		key  interface{}
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
			n := newNode(Period{}, 1, nil, black)
			pc := PeriodCollection{
				root:  n,
				nodes: map[interface{}]*node{1: n},
			}
			pc.Delete(test.key)
		})
	}
}

func TestPeriodCollection_deleteNode(t *testing.T) {
	tests := []struct {
		name      string
		setupTree func() (*PeriodCollection, *node)
		validate  func(t *testing.T, pc *PeriodCollection)
	}{
		{
			"deleting the a tree with only root leaves a leaf as the root",
			func() (*PeriodCollection, *node) {
				root := newNode(Period{}, 1, nil, black)
				return &PeriodCollection{root: root, nodes: map[interface{}]*node{1: root}}, root
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.True(t, pc.root.Leaf)
				assert.Nil(t, pc.root.Left)
				assert.Nil(t, pc.root.Right)
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
			func() (*PeriodCollection, *node) {
				p := newNode(Period{End: time.Unix(40, 0)}, 1, "p", black)
				s := newNode(Period{End: time.Unix(50, 0)}, 2, "s", black)
				n := newNode(Period{End: time.Unix(45, 0)}, 3, "n", black)
				l := newNode(Period{End: time.Unix(25, 0)}, 4, "l", red)
				r := newNode(Period{End: time.Unix(60, 0)}, 5, "r", red)
				p.Left, p.Right, p.MaxEnd = s, n, r.Period.End
				s.Left, s.Right, s.parent, s.MaxEnd = l, r, p, r.Period.End
				l.parent, r.parent = s, s
				n.parent = p
				return &PeriodCollection{
					root:  p,
					nodes: map[interface{}]*node{1: p, 2: s, 3: n, 4: l, 5: r},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "r", pc.root.contents)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, "s", pc.root.Left.contents)
				assert.Equal(t, black, pc.root.Left.Color)
				assert.Equal(t, "p", pc.root.Right.contents)
				assert.Equal(t, black, pc.root.Right.Color)
				assert.Equal(t, "l", pc.root.Left.Left.contents)
				assert.Equal(t, red, pc.root.Left.Left.Color)
				assert.True(t, pc.root.Right.Right.Leaf)
				assert.Equal(t, time.Unix(60, 0), pc.root.MaxEnd)
				assert.Equal(t, time.Unix(50, 0), pc.root.Left.MaxEnd)
				assert.Equal(t, time.Unix(25, 0), pc.root.Left.Left.MaxEnd)
				assert.Equal(t, time.Unix(40, 0), pc.root.Right.MaxEnd)
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
			func() (*PeriodCollection, *node) {
				p := newNode(Period{End: time.Unix(40, 0)}, 1, "p", black)
				s := newNode(Period{End: time.Unix(50, 0)}, 2, "s", black)
				n := newNode(Period{End: time.Unix(45, 0)}, 3, "n", black)
				l := newNode(Period{End: time.Unix(25, 0)}, 4, "l", red)
				r := newNode(Period{End: time.Unix(60, 0)}, 5, "r", red)
				p.Left, p.Right, p.MaxEnd = n, s, r.Period.End
				s.Left, s.Right, s.parent, s.MaxEnd = l, r, p, r.Period.End
				l.parent, r.parent = s, s
				n.parent = p
				return &PeriodCollection{
					root:  p,
					nodes: map[interface{}]*node{1: p, 2: s, 3: n, 4: l, 5: r},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "l", pc.root.contents)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, "p", pc.root.Left.contents)
				assert.Equal(t, black, pc.root.Left.Color)
				assert.Equal(t, "s", pc.root.Right.contents)
				assert.Equal(t, black, pc.root.Left.Color)
				assert.Equal(t, "r", pc.root.Right.Right.contents)
				assert.Equal(t, red, pc.root.Right.Right.Color)
				assert.Equal(t, time.Unix(60, 0), pc.root.MaxEnd)
				assert.Equal(t, time.Unix(40, 0), pc.root.Left.MaxEnd)
				assert.Equal(t, time.Unix(60, 0), pc.root.Right.MaxEnd)
				assert.Equal(t, time.Unix(60, 0), pc.root.Right.Right.MaxEnd)
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
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, 1, "p", black)
				s := newNode(Period{}, 2, "s", red)
				n := newNode(Period{}, 3, "n", black)
				l := newNode(Period{}, 4, "l", black)
				r := newNode(Period{}, 5, "r", black)
				p.Left, p.Right = s, n
				s.Left, s.Right, s.parent = l, r, p
				l.parent = s
				r.parent = s
				n.parent = p
				return &PeriodCollection{
					root:  p,
					nodes: map[interface{}]*node{1: p, 2: s, 3: n, 4: l, 5: r},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "s", pc.root.contents)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, "l", pc.root.Left.contents)
				assert.Equal(t, black, pc.root.Left.Color)
				assert.Equal(t, "p", pc.root.Right.contents)
				assert.Equal(t, red, pc.root.Right.Color)
				assert.Equal(t, "r", pc.root.Right.Left.contents)
				assert.Equal(t, black, pc.root.Right.Left.Color)
				assert.True(t, pc.root.Right.Right.Leaf)
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
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, 1, "p", black)
				s := newNode(Period{}, 2, "s", red)
				n := newNode(Period{}, 3, "n", black)
				l := newNode(Period{}, 4, "l", black)
				r := newNode(Period{}, 5, "r", black)
				p.Left, p.Right = n, s
				s.Left, s.Right, s.parent = l, r, p
				l.parent = s
				r.parent = s
				n.parent = p
				return &PeriodCollection{
					root:  p,
					nodes: map[interface{}]*node{1: p, 2: s, 3: n, 4: l, 5: r},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "s", pc.root.contents)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, "p", pc.root.Left.contents)
				assert.Equal(t, red, pc.root.Left.Color)
				assert.Equal(t, "r", pc.root.Right.contents)
				assert.Equal(t, black, pc.root.Right.Color)
				assert.Equal(t, "l", pc.root.Left.Right.contents)
				assert.Equal(t, black, pc.root.Right.Left.Color)
				assert.True(t, pc.root.Left.Left.Leaf)
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
			func() (*PeriodCollection, *node) {
				p := newNode(Period{End: time.Unix(15, 0)}, 1, "p", black)
				s := newNode(Period{End: time.Unix(10, 0)}, 2, "s", black)
				n := newNode(Period{End: time.Unix(45, 0)}, 3, "n", black)
				l := newNode(Period{End: time.Unix(25, 0)}, 4, "l", red)
				p.Left, p.Right, p.MaxEnd = n, s, n.Period.End
				n.Left, n.parent = l, p
				s.parent = p
				l.parent = n
				return &PeriodCollection{
					root:  p,
					nodes: map[interface{}]*node{1: p, 2: s, 3: n, 4: l},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, "l", pc.root.Left.contents)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, "s", pc.root.Right.contents)
				assert.Equal(t, black, pc.root.Right.Color)
				assert.Equal(t, time.Unix(25, 0), pc.root.MaxEnd)
				assert.Equal(t, time.Unix(25, 0), pc.root.Left.MaxEnd)
				assert.Equal(t, time.Unix(10, 0), pc.root.Right.MaxEnd)
				assert.NotContains(t, pc.nodes, 3)
				assert.Equal(t, pc.nodes[4], pc.root.Left)
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
			func() (*PeriodCollection, *node) {
				p := newNode(Period{End: time.Unix(15, 0)}, 1, "p", black)
				s := newNode(Period{End: time.Unix(10, 0)}, 2, "s", black)
				n := newNode(Period{End: time.Unix(45, 0)}, 3, "n", black)
				r := newNode(Period{End: time.Unix(25, 0)}, 4, "r", red)
				p.Left, p.Right, p.MaxEnd = s, n, n.Period.End
				n.Right, n.parent = r, p
				s.parent = p
				r.parent = n
				return &PeriodCollection{
					root:  p,
					nodes: map[interface{}]*node{1: p, 2: s, 3: n, 4: r},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, "s", pc.root.Left.contents)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, "r", pc.root.Right.contents)
				assert.Equal(t, black, pc.root.Right.Color)
				assert.Equal(t, time.Unix(25, 0), pc.root.MaxEnd)
				assert.Equal(t, time.Unix(25, 0), pc.root.Right.MaxEnd)
				assert.Equal(t, time.Unix(10, 0), pc.root.Left.MaxEnd)
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
			func() (*PeriodCollection, *node) {
				p := newNode(Period{End: time.Unix(15, 0)}, 1, "p", black)
				s := newNode(Period{End: time.Unix(10, 0)}, 2, "s", black)
				n := newNode(Period{End: time.Unix(25, 0)}, 3, "n", black)
				r := newNode(Period{End: time.Unix(45, 0)}, 4, "r", red)
				p.Left, p.Right, p.MaxEnd = n, s, r.MaxEnd
				n.Right, n.parent = r, p
				s.parent = p
				r.parent = n
				return &PeriodCollection{
					root:  p,
					nodes: map[interface{}]*node{1: p, 2: s, 3: n, 4: r},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, "r", pc.root.Left.contents)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, "s", pc.root.Right.contents)
				assert.Equal(t, black, pc.root.Right.Color)
				assert.Equal(t, time.Unix(45, 0), pc.root.MaxEnd)
				assert.Equal(t, time.Unix(10, 0), pc.root.Right.MaxEnd)
				assert.Equal(t, time.Unix(45, 0), pc.root.Left.MaxEnd)
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
			func() (*PeriodCollection, *node) {
				p := newNode(Period{End: time.Unix(15, 0)}, 1, "p", black)
				s := newNode(Period{End: time.Unix(10, 0)}, 2, "s", black)
				n := newNode(Period{End: time.Unix(45, 0)}, 3, "n", black)
				l := newNode(Period{End: time.Unix(25, 0)}, 4, "l", red)
				p.Left, p.Right, p.MaxEnd = s, n, n.Period.End
				n.Left, n.parent = l, p
				s.parent = p
				l.parent = n
				return &PeriodCollection{
					root:  p,
					nodes: map[interface{}]*node{1: p, 2: s, 3: n, 4: l},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, "s", pc.root.Left.contents)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, "l", pc.root.Right.contents)
				assert.Equal(t, black, pc.root.Right.Color)
				assert.Equal(t, time.Unix(25, 0), pc.root.MaxEnd)
				assert.Equal(t, time.Unix(25, 0), pc.root.Right.MaxEnd)
				assert.Equal(t, time.Unix(10, 0), pc.root.Left.MaxEnd)
				assert.NotContains(t, pc.nodes, 3)
				assert.Len(t, pc.nodes, 3)
			},
		}, {
			"deleting black node with leaf sibling and red parent makes parent black",
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, 1, "p", red)
				n := newNode(Period{}, 2, "n", black)
				p.Left = n
				n.parent = p
				return &PeriodCollection{root: p, nodes: map[interface{}]*node{1: p, 2: n}}, n
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "p", pc.root.contents)
				assert.True(t, pc.root.Left.Leaf)
				assert.Equal(t, black, pc.root.Color)
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
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, 1, "p", black)
				s := newNode(Period{}, 2, "s", black)
				n := newNode(Period{}, 3, "n", black)
				l := newNode(Period{}, 4, "l", black)
				r := newNode(Period{}, 5, "r", black)
				p.Left, p.Right = n, s
				s.Left, s.Right, s.parent = l, r, p
				l.parent = s
				r.parent = s
				n.parent = p
				return &PeriodCollection{
					root:  p,
					nodes: map[interface{}]*node{1: p, 2: s, 3: n, 4: l, 5: r},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, "s", pc.root.Right.contents)
				assert.Equal(t, red, pc.root.Right.Color)
				assert.Equal(t, "l", pc.root.Right.Left.contents)
				assert.Equal(t, black, pc.root.Right.Left.Color)
				assert.Equal(t, "r", pc.root.Right.Right.contents)
				assert.Equal(t, black, pc.root.Right.Left.Color)
				assert.True(t, pc.root.Left.Leaf)
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
			func() (*PeriodCollection, *node) {
				n := newNode(Period{End: time.Unix(20, 0)}, 1, "n", black)
				l := newNode(Period{End: time.Unix(10, 0)}, 2, "l", black)
				r := newNode(Period{End: time.Unix(30, 0)}, 3, "r", black)
				rl := newNode(Period{End: time.Unix(50, 0)}, 4, "rl", red)
				n.Left, n.Right, n.MaxEnd = l, r, rl.Period.End
				r.Left, r.parent = rl, n
				l.parent = n
				rl.parent = r
				return &PeriodCollection{
					root:  n,
					nodes: map[interface{}]*node{1: n, 2: l, 3: r, 4: rl},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "rl", pc.root.contents)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, "l", pc.root.Left.contents)
				assert.Equal(t, black, pc.root.Left.Color)
				assert.Equal(t, "r", pc.root.Right.contents)
				assert.Equal(t, black, pc.root.Right.Color)
				assert.Equal(t, time.Unix(50, 0), pc.root.MaxEnd)
				assert.Equal(t, time.Unix(10, 0), pc.root.Left.MaxEnd)
				assert.Equal(t, time.Unix(30, 0), pc.root.Right.MaxEnd)
				assert.NotContains(t, pc.nodes, 1)
				assert.Len(t, pc.nodes, 3)
				assert.Equal(t, pc.nodes[4], pc.root)
				assert.Equal(t, pc.nodes[2], pc.root.Left)
				assert.Equal(t, pc.nodes[3], pc.root.Right)
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
			"delete an internal node with a black successor with max end less than the internal node",
			func() (*PeriodCollection, *node) {
				n := newNode(Period{End: time.Unix(50, 0)}, 1, "n", black)
				l := newNode(Period{End: time.Unix(10, 0)}, 2, "l", black)
				r := newNode(Period{End: time.Unix(30, 0)}, 3, "r", black)
				rl := newNode(Period{End: time.Unix(20, 0)}, 4, "rl", red)
				n.Left, n.Right = l, r
				r.Left, r.parent, r.MaxEnd = rl, n, rl.Period.End
				l.parent = n
				rl.parent = r
				return &PeriodCollection{
					root:  n,
					nodes: map[interface{}]*node{1: n, 2: l, 3: r, 4: rl},
				}, n
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "rl", pc.root.contents)
				assert.Equal(t, black, pc.root.Color)
				assert.Equal(t, "l", pc.root.Left.contents)
				assert.Equal(t, black, pc.root.Left.Color)
				assert.Equal(t, "r", pc.root.Right.contents)
				assert.Equal(t, black, pc.root.Right.Color)
				assert.Equal(t, time.Unix(30, 0), pc.root.MaxEnd)
				assert.Equal(t, time.Unix(10, 0), pc.root.Left.MaxEnd)
				assert.Equal(t, time.Unix(30, 0), pc.root.Right.MaxEnd)
				assert.NotContains(t, pc.nodes, 1)
				assert.Len(t, pc.nodes, 3)
				assert.Equal(t, pc.nodes[4], pc.root)
				assert.Equal(t, pc.nodes[2], pc.root.Left)
				assert.Equal(t, pc.nodes[3], pc.root.Right)
			},
		}, {
			"deleting the only child of the root updates max end correctly",
			func() (*PeriodCollection, *node) {
				root := newNode(Period{End: time.Unix(20, 0)}, 1, "root", black)
				r := newNode(Period{End: time.Unix(30, 0)}, 2, "r", black)
				root.Right, root.MaxEnd = r, r.Period.End
				r.parent = root
				return &PeriodCollection{
					root:  root,
					nodes: map[interface{}]*node{1: root, 2: r},
				}, r
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "root", pc.root.contents)
				assert.True(t, pc.root.Left.Leaf)
				assert.True(t, pc.root.Right.Leaf)
				assert.Equal(t, time.Unix(20, 0), pc.root.MaxEnd)
				assert.NotContains(t, pc.nodes, 2)
				assert.Len(t, pc.nodes, 1)
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
		setup    func() (*PeriodCollection, *node)
		validate func(t *testing.T, pc *PeriodCollection)
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
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, nil, "p", black)
				s := newNode(Period{}, nil, "s", red)
				l := newNode(Period{}, nil, "l", black)
				r := newNode(Period{}, nil, "r", black)
				n := &node{Leaf: true}
				p.Left = s
				p.Right = n
				s.Left = l
				s.Right = r
				s.parent = p
				n.parent = p
				l.parent = s
				r.parent = s
				n.parent = p
				return &PeriodCollection{root: p}, n
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "s", pc.root.contents)
				assert.Equal(t, "l", pc.root.Left.contents)
				assert.Equal(t, "p", pc.root.Right.contents)
				assert.Equal(t, "r", pc.root.Right.Left.contents)
				assert.True(t, pc.root.Right.Right.Leaf)
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
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, nil, "p", black)
				s := newNode(Period{}, nil, "s", red)
				l := newNode(Period{}, nil, "l", black)
				r := newNode(Period{}, nil, "r", black)
				n := &node{Leaf: true}
				p.Right = s
				p.Left = n
				s.Left = l
				s.Right = r
				s.parent = p
				n.parent = p
				l.parent = s
				r.parent = s
				n.parent = p
				return &PeriodCollection{root: p}, n
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "s", pc.root.contents)
				assert.Equal(t, "p", pc.root.Left.contents)
				assert.Equal(t, "r", pc.root.Right.contents)
				assert.Equal(t, "l", pc.root.Left.Right.contents)
				assert.True(t, pc.root.Left.Left.Leaf)
			},
		}, {
			"deleted child with black sibling does nothing",
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, nil, "p", black)
				s := newNode(Period{}, nil, "s", black)
				n := &node{Leaf: true}
				p.Left = s
				p.Right = n
				n.parent = p
				s.parent = p
				return &PeriodCollection{root: p}, n
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "s", pc.root.Left.contents)
				assert.True(t, pc.root.Right.Leaf)
			},
		}, {
			"deleted child with no sibling does nothing",
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, nil, "p", black)
				n := &node{Leaf: true}
				p.Right = n
				n.parent = p
				return &PeriodCollection{root: p}, n
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "p", pc.root.contents)
				assert.True(t, pc.root.Left.Leaf)
				assert.True(t, pc.root.Right.Leaf)
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
		setup           func() (*PeriodCollection, *node, *node)
		expectedOutcome bool
		expectRecolor   bool
	}{
		{
			"deleted node with black sibling with 2 black child nodes recolors the sibling and returns true",
			func() (*PeriodCollection, *node, *node) {
				p := newNode(Period{}, nil, "p", black)
				s := newNode(Period{}, nil, "s", black)
				sl := newNode(Period{}, nil, "sl", black)
				sr := newNode(Period{}, nil, "sr", black)
				n := &node{Leaf: true, contents: "n"}
				p.Left, p.Right = s, n
				s.Left, s.Right, s.parent = sl, sr, p
				sl.parent, sr.parent = s, s
				n.parent = p
				return &PeriodCollection{root: p}, n, s
			},
			true,
			true,
		}, {
			"deleted node with black sibling and 1 child does nothing",
			func() (*PeriodCollection, *node, *node) {
				p := newNode(Period{}, nil, nil, black)
				s := newNode(Period{}, nil, nil, black)
				sr := newNode(Period{}, nil, nil, black)
				n := &node{Leaf: true}
				p.Left, p.Right = s, n
				s.Right, s.parent = sr, p
				sr.parent = s
				n.parent = p
				return &PeriodCollection{root: p}, n, s
			},
			false,
			false,
		}, {
			"deleted node with leaf sibling returns true but does not recolor the leaf",
			func() (*PeriodCollection, *node, *node) {
				p := newNode(Period{}, nil, nil, black)
				s := &node{Leaf: true}
				n := &node{Leaf: true}
				p.Left, p.Right = s, n
				s.parent = p
				n.parent = p
				return &PeriodCollection{root: p}, n, s
			},
			true,
			false,
		}, {
			"deleted node with red sibling does nothing",
			func() (*PeriodCollection, *node, *node) {
				p := newNode(Period{}, nil, nil, black)
				s := newNode(Period{}, nil, nil, red)
				sl := newNode(Period{}, nil, nil, black)
				sr := newNode(Period{}, nil, nil, black)
				n := &node{Leaf: true}
				p.Left, p.Right = s, n
				s.Left, s.Right, s.parent = sl, sr, p
				sl.parent, sr.parent = s, s
				n.parent = p
				return &PeriodCollection{root: p}, n, s
			},
			false,
			false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pc, n, s := test.setup()
			sColorBefore := s.Color
			result := pc.deleteRepairCase2(n)
			assert.Equal(t, test.expectedOutcome, result)
			if test.expectRecolor {
				assert.Equal(t, red, s.Color)
			} else {
				assert.Equal(t, sColorBefore, s.Color)
			}
		})
	}
}

func TestPeriodCollection_deleteRepairCase3(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() (*PeriodCollection, *node)
		validate func(t *testing.T, pc *PeriodCollection)
	}{
		{
			"no action when sibling is a leaf",
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", black)
				r.parent = p
				p.Right = r
				return &PeriodCollection{root: p}, r
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "r", pc.root.Right.contents)
				assert.True(t, pc.root.Left.Leaf)
			},
		}, {
			"no action when sibling is red",
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", red)
				l := newNode(Period{}, nil, "l", red)
				l.parent, r.parent = p, p
				p.Left, p.Right = l, r
				return &PeriodCollection{root: p}, r
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "l", pc.root.Left.contents)
				assert.Equal(t, "r", pc.root.Right.contents)
			},
		}, {
			"no action when node is right child and sibling has no right child",
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", black)
				l := newNode(Period{}, nil, "l", black)
				ll := newNode(Period{}, nil, "ll", red)
				l.parent, r.parent = p, p
				p.Left, p.Right = l, r
				l.Left = ll
				ll.parent = ll
				return &PeriodCollection{root: p}, r
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "l", pc.root.Left.contents)
				assert.Equal(t, "r", pc.root.Right.contents)
				assert.Equal(t, "ll", pc.root.Left.Left.contents)
			},
		}, {
			"no action when node is left child and sibling has no left child",
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", black)
				l := newNode(Period{}, nil, "l", black)
				ll := newNode(Period{}, nil, "ll", red)
				l.parent, r.parent = p, p
				p.Left, p.Right = l, r
				l.Left = ll
				ll.parent = ll
				return &PeriodCollection{root: p}, l
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "l", pc.root.Left.contents)
				assert.Equal(t, "r", pc.root.Right.contents)
				assert.Equal(t, "ll", pc.root.Left.Left.contents)
			},
		}, {
			"left rotate around sibling and recolor when node is right child and sibling is black with red right child",
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", black)
				l := newNode(Period{}, nil, "l", black)
				lr := newNode(Period{}, nil, "lr", red)
				l.parent, r.parent = p, p
				p.Left, p.Right = l, r
				l.Right = lr
				lr.parent = l
				return &PeriodCollection{root: p}, r
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "lr", pc.root.Left.contents)
				assert.Equal(t, "r", pc.root.Right.contents)
				assert.Equal(t, "l", pc.root.Left.Left.contents)
				assert.Equal(t, black, pc.root.Left.Color)
				assert.Equal(t, red, pc.root.Left.Left.Color)
			},
		}, {
			"right rotate around sibling and recolor when node is left child and sibling is black with red left child",
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", black)
				l := newNode(Period{}, nil, "l", black)
				rl := newNode(Period{}, nil, "rl", red)
				l.parent, r.parent = p, p
				p.Left, p.Right = l, r
				r.Left = rl
				rl.parent = r
				return &PeriodCollection{root: p}, l
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "rl", pc.root.Right.contents)
				assert.Equal(t, "l", pc.root.Left.contents)
				assert.Equal(t, "r", pc.root.Right.Right.contents)
				assert.Equal(t, black, pc.root.Right.Color)
				assert.Equal(t, red, pc.root.Right.Right.Color)
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
		setup    func() (*PeriodCollection, *node)
		validate func(t *testing.T, pc *PeriodCollection)
	}{
		{
			"no action when sibling is a leaf",
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", black)
				r.parent = p
				p.Right = r
				return &PeriodCollection{root: p}, r
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "r", pc.root.Right.contents)
				assert.True(t, pc.root.Left.Leaf)
			},
		}, {
			"no action when sibling is red",
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", red)
				l := newNode(Period{}, nil, "l", red)
				l.parent, r.parent = p, p
				p.Left, p.Right = l, r
				return &PeriodCollection{root: p}, r
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "l", pc.root.Left.contents)
				assert.Equal(t, "r", pc.root.Right.contents)
			},
		}, {
			"no action when right child and sibling has no left child",
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", black)
				l := newNode(Period{}, nil, "l", black)
				lr := newNode(Period{}, nil, "lr", red)
				l.parent, r.parent = p, p
				p.Left, p.Right = l, r
				l.Right = lr
				lr.parent = l
				return &PeriodCollection{root: p}, r
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "l", pc.root.Left.contents)
				assert.Equal(t, "r", pc.root.Right.contents)
				assert.Equal(t, "lr", pc.root.Left.Right.contents)
			},
		}, {
			"no action when left child and sibling has no right child",
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", black)
				l := newNode(Period{}, nil, "l", black)
				rl := newNode(Period{}, nil, "rl", red)
				l.parent, r.parent = p, p
				p.Left, p.Right = l, r
				r.Left = rl
				rl.parent = r
				return &PeriodCollection{root: p}, r
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "p", pc.root.contents)
				assert.Equal(t, "l", pc.root.Left.contents)
				assert.Equal(t, "r", pc.root.Right.contents)
				assert.Equal(t, "rl", pc.root.Right.Left.contents)
			},
		}, {
			"right rotate around parent and recolor when right child and sibling is black with red left child",
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, nil, "p", red)
				r := newNode(Period{}, nil, "r", black)
				l := newNode(Period{}, nil, "l", black)
				ll := newNode(Period{}, nil, "ll", red)
				l.parent, r.parent = p, p
				p.Left, p.Right = l, r
				l.Left = ll
				ll.parent = l
				return &PeriodCollection{root: p}, r
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "l", pc.root.contents)
				assert.Equal(t, "ll", pc.root.Left.contents)
				assert.Equal(t, "p", pc.root.Right.contents)
				assert.Equal(t, "r", pc.root.Right.Right.contents)
				assert.Equal(t, black, pc.root.Left.Color)
				assert.Equal(t, black, pc.root.Right.Color)
				assert.Equal(t, red, pc.root.Color)
			},
		}, {
			"left rotate around parent and recolor when left child and sibling is black with red right child",
			func() (*PeriodCollection, *node) {
				p := newNode(Period{}, nil, "p", red)
				r := newNode(Period{}, nil, "r", black)
				l := newNode(Period{}, nil, "l", black)
				rr := newNode(Period{}, nil, "rr", red)
				l.parent, r.parent = p, p
				p.Left, p.Right = l, r
				r.Right = rr
				rr.parent = r
				return &PeriodCollection{root: p}, l
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, "r", pc.root.contents)
				assert.Equal(t, "p", pc.root.Left.contents)
				assert.Equal(t, "rr", pc.root.Right.contents)
				assert.Equal(t, "l", pc.root.Left.Left.contents)
				assert.Equal(t, black, pc.root.Left.Color)
				assert.Equal(t, black, pc.root.Right.Color)
				assert.Equal(t, red, pc.root.Color)
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

func TestPeriodCollection_ContainsTime(t *testing.T) {
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
			pc := NewPeriodCollection()
			for i, p := range test.periods {
				require.NoError(t, pc.Insert(i, p, nil))
			}
			assert.Equal(t, test.expectedOutcome, pc.ContainsTime(test.query))
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
	pc := NewPeriodCollection()
	for i, n := range nodes {
		require.NoError(t, pc.Insert(i, n.period, n.contents))
	}
	tests := []struct {
		name             string
		setupCollection  func() *PeriodCollection
		query            Period
		expectedContents []interface{}
	}{
		{
			"2018-12-05 12:00 - 2018-12-06 12:00 intersects period a",
			func() *PeriodCollection {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 5, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 6, 12, 0, 0, 0, time.UTC)),
			[]interface{}{"a"},
		}, {
			"2018-12-05 12:00 - 2018-12-07 12:00 intersects period a and b",
			func() *PeriodCollection {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 5, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 7, 12, 0, 0, 0, time.UTC)),
			[]interface{}{"a", "b"},
		}, {
			"2018-12-05 12:00 - 2018-12-12 12:00 intersects periods a, b, c, d, and e",
			func() *PeriodCollection {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 5, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 12, 12, 0, 0, 0, time.UTC)),
			[]interface{}{"a", "b", "c", "d", "e"},
		}, {
			"2018-12-05 12:00 - 2018-12-07 00:00 intersects period a",
			func() *PeriodCollection {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 5, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 7, 0, 0, 0, 0, time.UTC)),
			[]interface{}{"a"},
		}, {
			"2018-12-05 12:00 - 2018-12-05 14:00 does not intersect",
			func() *PeriodCollection {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 5, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 5, 14, 0, 0, 0, time.UTC)),
			[]interface{}{},
		}, {
			"2018-12-20 12:00 - 2018-12-20 14:00 does not intersect",
			func() *PeriodCollection {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 20, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 20, 14, 0, 0, 0, time.UTC)),
			[]interface{}{},
		}, {
			"2018-12-07 12:00 - 2018-12-07 14:00 intersects period b",
			func() *PeriodCollection {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 7, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 7, 14, 0, 0, 0, time.UTC)),
			[]interface{}{"b"},
		}, {
			"2018-12-10 02:00 - 10:00 CST intersects period e",
			func() *PeriodCollection {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 10, 2, 0, 0, 0, chiTz), time.Date(2018, 12, 10, 10, 0, 0, 0, chiTz)),
			[]interface{}{"e"},
		}, {
			"2018-12-09 20:00 - 2018-12-10 10:00 UTC intersects period d and e",
			func() *PeriodCollection {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 9, 20, 0, 0, 0, time.UTC), time.Date(2018, 12, 10, 10, 0, 0, 0, time.UTC)),
			[]interface{}{"d", "e"},
		}, {
			"tree with leaf root returns nothing",
			func() *PeriodCollection {
				return NewPeriodCollection()
			},
			Period{},
			[]interface{}{},
		}, {
			"2018-12-28 12:00 - 2018-12-28 14:00 intersects period f",
			func() *PeriodCollection {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 28, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 28, 14, 0, 0, 0, time.UTC)),
			[]interface{}{"f"},
		}, {
			"2018-12-9 12:00 - 2018-12-28 14:00 intersects periods d, e, and f",
			func() *PeriodCollection {
				return pc
			},
			NewPeriod(time.Date(2018, 12, 9, 12, 0, 0, 0, time.UTC), time.Date(2018, 12, 28, 14, 0, 0, 0, time.UTC)),
			[]interface{}{"d", "e", "f"},
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
			func() *PeriodCollection {
				n := newNode(Period{time.Date(2019, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 4, 0, 0, 0, 0, time.UTC)}, 1, "n", black)
				l := newNode(Period{time.Date(2019, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 2, 0, 0, 0, 0, time.UTC)}, 2, "l", black)
				r := newNode(Period{time.Date(2019, 12, 5, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 6, 0, 0, 0, 0, time.UTC)}, 3, "r", black)
				rl := newNode(Period{time.Date(2019, 12, 3, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 4, 0, 0, 0, 0, time.UTC)}, 4, "rl", red)
				n.Left, n.Right = l, r
				r.Left, r.parent, r.MaxEnd = rl, n, rl.Period.End
				l.parent = n
				rl.parent = r
				return &PeriodCollection{
					root:  n,
					nodes: map[interface{}]*node{1: n, 2: l, 3: r, 4: rl},
				}
			},
			NewPeriod(time.Date(2019, 12, 3, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 4, 0, 0, 0, 0, time.UTC)),
			[]interface{}{"n", "rl"},
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
	pc := PeriodCollection{
		nodes: map[interface{}]*node{
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
		setup       func() *PeriodCollection
		updateKey   int
		newContents int
		newPeriod   Period
		validate    func(t *testing.T, pc *PeriodCollection)
	}{
		{
			"updating a key that doesn't exist inserts a new period",
			func() *PeriodCollection { return NewPeriodCollection() },
			1,
			1,
			Period{},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Len(t, pc.nodes, 1)
				assert.Equal(t, 1, pc.root.Key)
				assert.True(t, pc.root.Left.Leaf)
				assert.True(t, pc.root.Right.Leaf)
			},
		}, {
			"updating contents without updating the period swaps contents",
			func() *PeriodCollection {
				pc := NewPeriodCollection()
				l := &node{contents: 1}
				pc.root = &node{
					Left: l,
				}
				pc.nodes[0] = pc.root
				pc.nodes[1] = l
				return pc
			},
			1,
			2,
			Period{},
			func(t *testing.T, pc *PeriodCollection) {
				l, ok := pc.nodes[1]
				require.True(t, ok)
				require.Equal(t, l, pc.root.Left)
				assert.Equal(t, l.contents, 2)
				assert.Len(t, pc.nodes, 2)
			},
		}, {
			"updating the period deletes and reinserts the node",
			func() *PeriodCollection {
				pc := NewPeriodCollection()
				root := newNode(NewPeriod(time.Unix(10, 0), time.Unix(25, 0)), 0, 0, black)
				l := newNode(NewPeriod(time.Unix(5, 0), time.Unix(30, 0)), 1, 1, red)
				pc.root = root
				root.Left, l.parent = l, root
				pc.nodes[0] = root
				pc.nodes[1] = l
				return pc
			},
			1,
			2,
			NewPeriod(time.Unix(20, 0), time.Unix(30, 0)),
			func(t *testing.T, pc *PeriodCollection) {
				// the node should move from the the parent's left to right
				r, ok := pc.nodes[1]
				require.True(t, ok)
				require.Equal(t, r, pc.root.Right)
				assert.Equal(t, r.contents, 2)
				assert.True(t, pc.root.Left.Leaf)
				assert.Len(t, pc.nodes, 2)
			},
		}, {
			"updating the root's period works",
			func() *PeriodCollection {
				pc := NewPeriodCollection()
				root := newNode(NewPeriod(time.Unix(10, 0), time.Unix(25, 0)), 0, 0, black)
				pc.root = root
				pc.nodes[0] = root
				return pc
			},
			0,
			1,
			NewPeriod(time.Unix(10, 0), time.Unix(30, 0)),
			func(t *testing.T, pc *PeriodCollection) {
				root, ok := pc.nodes[0]
				require.True(t, ok)
				assert.Equal(t, root, pc.root)
				assert.Equal(t, 1, pc.root.contents)
				assert.Equal(t, time.Unix(30, 0), pc.root.Period.End)
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
		createCollection func(t *testing.T) *PeriodCollection
		query            Period
		expectedOutcome  bool
	}{
		{
			"searching with intersection in left subtree works",
			func(t *testing.T) *PeriodCollection {
				periods := []Period{
					// becomes left
					NewPeriod(time.Date(2018, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 15, 0, 0, 0, 0, time.UTC)),
					// becomes right
					NewPeriod(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 30, 0, 0, 0, 0, time.UTC)),
					// becomes root
					NewPeriod(time.Date(2018, 12, 27, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 27, 0, 0, 0, 1, time.UTC)),
				}
				pc := NewPeriodCollection()
				for i, p := range periods {
					require.NoError(t, pc.Insert(i, p, nil))
				}
				return pc
			},
			NewPeriod(time.Date(2018, 12, 28, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 2, 0, 0, 0, 0, time.UTC)),
			true,
		}, {
			"searching with intersection in right subtree works",
			func(t *testing.T) *PeriodCollection {
				periods := []Period{
					NewPeriod(time.Date(2018, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 15, 0, 0, 0, 0, time.UTC)),
					NewPeriod(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 30, 0, 0, 0, 0, time.UTC)),
					NewPeriod(time.Date(2018, 12, 27, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 27, 0, 0, 0, 1, time.UTC)),
				}
				pc := NewPeriodCollection()
				for i, p := range periods {
					require.NoError(t, pc.Insert(i, p, nil))
				}
				return pc
			},
			NewPeriod(time.Date(2019, 1, 20, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 22, 0, 0, 0, 0, time.UTC)),
			true,
		}, {
			"searching when no intersection in tree works",
			func(t *testing.T) *PeriodCollection {
				pc := NewPeriodCollection()
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
			func(t *testing.T) *PeriodCollection { return NewPeriodCollection() },
			Period{},
			false,
		}, {
			"searching with unbound intersection in right subtree works",
			func(t *testing.T) *PeriodCollection {
				periods := []Period{
					NewPeriod(time.Date(2018, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 15, 0, 0, 0, 0, time.UTC)),
					NewPeriod(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), time.Time{}),
					NewPeriod(time.Date(2018, 12, 27, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 27, 0, 0, 0, 1, time.UTC)),
				}
				pc := NewPeriodCollection()
				for i, p := range periods {
					require.NoError(t, pc.Insert(i, p, nil))
				}
				return pc
			},
			NewPeriod(time.Date(2019, 1, 20, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 22, 0, 0, 0, 0, time.UTC)),
			true,
		}, {
			"searching with unbound intersection in left subtree works",
			func(t *testing.T) *PeriodCollection {
				periods := []Period{
					NewPeriod(time.Date(2018, 12, 1, 0, 0, 0, 0, time.UTC), time.Time{}),
					NewPeriod(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 30, 0, 0, 0, 0, time.UTC)),
					NewPeriod(time.Date(2018, 12, 27, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 27, 0, 0, 0, 1, time.UTC)),
				}
				pc := NewPeriodCollection()
				for i, p := range periods {
					require.NoError(t, pc.Insert(i, p, nil))
				}
				return pc
			},
			NewPeriod(time.Date(2018, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 1, 1, 0, 0, 0, time.UTC)),
			true,
		}, {
			"searching with unbound intersection in root works",
			func(t *testing.T) *PeriodCollection {
				periods := []Period{
					NewPeriod(time.Date(2018, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 15, 0, 0, 0, 0, time.UTC)),
					NewPeriod(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), time.Time{}),
					NewPeriod(time.Date(2018, 12, 27, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 27, 0, 0, 0, 1, time.UTC)),
				}
				pc := NewPeriodCollection()
				for i, p := range periods {
					require.NoError(t, pc.Insert(i, p, nil))
				}
				return pc
			},
			NewPeriod(time.Date(2020, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2020, 12, 1, 1, 0, 0, 0, time.UTC)),
			true,
		}, {
			"searching with before unbound intersection returns false",
			func(t *testing.T) *PeriodCollection {
				periods := []Period{
					NewPeriod(time.Date(2018, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 1, 15, 0, 0, 0, 0, time.UTC)),
					NewPeriod(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), time.Time{}),
					NewPeriod(time.Date(2018, 12, 27, 0, 0, 0, 0, time.UTC), time.Date(2018, 12, 27, 0, 0, 0, 1, time.UTC)),
				}
				pc := NewPeriodCollection()
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
			func(t *testing.T) *PeriodCollection {
				n := newNode(Period{time.Date(2019, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 2, 0, 0, 0, 0, time.UTC)}, 1, "n", black)
				l := newNode(Period{time.Date(2019, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 2, 0, 0, 0, 0, time.UTC)}, 2, "l", black)
				r := newNode(Period{time.Date(2019, 12, 5, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 6, 0, 0, 0, 0, time.UTC)}, 3, "r", black)
				rl := newNode(Period{time.Date(2019, 12, 3, 0, 0, 0, 0, time.UTC), time.Date(2019, 12, 4, 0, 0, 0, 0, time.UTC)}, 4, "rl", red)
				n.Left, n.Right = l, r
				r.Left, r.parent, r.MaxEnd = rl, n, rl.Period.End
				l.parent = n
				rl.parent = r
				return &PeriodCollection{
					root:  n,
					nodes: map[interface{}]*node{1: n, 2: l, 3: r, 4: rl},
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
	tree := PeriodCollection{
		root: &node{
			contents: "D",
			Left: &node{
				contents: "C",
				Left: &node{
					contents: "A",
					Left:     &node{Leaf: true},
					Right:    &node{Leaf: true},
				},
				Right: &node{
					contents: "B",
					Left:     &node{Leaf: true},
					Right:    &node{Leaf: true},
				},
			},
			Right: &node{
				contents: "E",
				Left:     &node{Leaf: true},
				Right:    &node{Leaf: true},
			},
		},
	}
	tests := []struct {
		name            string
		order           TraversalOrder
		expectedOutcome []interface{}
	}{
		{
			"traverse pre-order works",
			PreOrder,
			[]interface{}{"D", "C", "A", "B", "E"},
		}, {
			"traverse in-order works",
			InOrder,
			[]interface{}{"A", "C", "B", "D", "E"},
		}, {
			"traverse post-order works",
			PostOrder,
			[]interface{}{"A", "B", "C", "E", "D"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedOutcome, tree.DepthFirstTraverse(test.order))
		})
	}
}

func TestPeriodCollection_DeleteOnCondition(t *testing.T) {
	setupTree := func() *PeriodCollection {
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
		pc := NewPeriodCollection()
		for _, n := range nodes {
			require.NoError(t, pc.Insert(n.contents, n.period, n.contents))
		}
		return pc
	}
	tests := []struct {
		name      string
		condition func(i interface{}) bool
		validate  func(t *testing.T, pc *PeriodCollection)
	}{
		{
			"delete all nodes",
			func(i interface{}) bool {
				return true
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, 0, len(pc.nodes))
			},
		}, {
			"delete 0 nodes",
			func(i interface{}) bool {
				return false
			},
			func(t *testing.T, pc *PeriodCollection) {
				assert.Equal(t, 6, len(pc.nodes))
			},
		}, {
			"delete all even numbers",
			func(i interface{}) bool {
				return i.(int)%2 == 0
			},
			func(t *testing.T, pc *PeriodCollection) {
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
	pc := NewPeriodCollection()
	assert.Equal(
		t,
		Update{
			key:         1,
			newPeriod:   Period{Start: time.Unix(1, 0), End: time.Unix(2, 0)},
			newContents: 1,
			pc:          pc,
		},
		pc.PrepareUpdate(1, Period{Start: time.Unix(1, 0), End: time.Unix(2, 0)}, 1),
	)
}

func TestPeriodCollection_Execute(t *testing.T) {
	pc := NewPeriodCollection()
	u1 := Update{
		key:         1,
		newContents: 1,
		newPeriod:   Period{Start: time.Unix(1, 0), End: time.Unix(2, 0)},
		pc:          pc,
	}
	u2 := Update{
		key:         2,
		newContents: 2,
		newPeriod:   Period{Start: time.Unix(1, 0), End: time.Unix(2, 0)},
		pc:          pc,
	}
	d1 := Delete{key: 1, pc: pc}
	pc.Execute(u1, u2, d1)
	assert.Contains(t, pc.nodes, 2)
	assert.NotContains(t, pc.nodes, 1)
}

func TestPeriodCollection_PrepareDelete(t *testing.T) {
	pc := NewPeriodCollection()
	assert.Equal(t, Delete{key: 1, pc: pc}, pc.PrepareDelete(1))
}

func TestUpdate_execute(t *testing.T) {
	pc := NewPeriodCollection()
	u := Update{
		key:         1,
		newContents: 1,
		newPeriod:   Period{Start: time.Unix(1, 0), End: time.Unix(2, 0)},
		pc:          pc,
	}
	u.execute()
	assert.Contains(t, pc.nodes, 1)
}

func TestDelete_execute(t *testing.T) {
	pc := NewPeriodCollection()
	pc.update(1, Period{}, 1)
	require.Contains(t, pc.nodes, 1)
	d := Delete{key: 1, pc: pc}
	d.execute()
	assert.NotContains(t, pc.nodes, 1)
}

func TestPeriodCollection_ContentsOfKey(t *testing.T) {
	pc := PeriodCollection{nodes: map[interface{}]*node{1: {contents: "contents"}}}
	tests := []struct {
		name             string
		key              interface{}
		expectedContents interface{}
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

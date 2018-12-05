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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntervalTree_Insert(t *testing.T) {
	tests := []struct {
		name          string
		setupTree     func() IntervalTree
		insertPeriods []Period // start time of the periods to insert (tree uses start time as node key)
		validateTree  func(t *testing.T, it IntervalTree)
	}{
		{
			"inserting a single node creates a black root",
			func() IntervalTree { return NewIntervalTree() },
			[]Period{NewPeriod(time.Unix(1, 0), time.Unix(5, 0))},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, black, it.root.color)
				assert.Contains(t, it.nodes, 0)
				assert.Equal(t, time.Unix(5, 0), it.root.maxEnd)
			},
		},
		{
			"inserting a node into a tree with a sentinel root replaces the sentinel with a new root",
			func() IntervalTree {
				it := NewIntervalTree()
				it.root = &node{leaf: true}
				return it
			},
			[]Period{NewPeriod(time.Unix(1, 0), time.Unix(5, 0))},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, black, it.root.color)
				assert.False(t, it.root.leaf)
				assert.Equal(t, time.Unix(1, 0), it.root.period.Start)
				assert.Contains(t, it.nodes, 0)
				assert.Len(t, it.nodes, 1)
				assert.Equal(t, time.Unix(5, 0), it.root.maxEnd)
			},
		},
		{
			/* after insertion, 1 and 3 are red and 2 is black
			  2
			 / \
			1   3
			*/
			"inserting 3 nodes creates red children",
			func() IntervalTree { return NewIntervalTree() },
			[]Period{
				NewPeriod(time.Unix(2, 0), time.Unix(5, 0)),
				NewPeriod(time.Unix(1, 0), time.Unix(10, 0)),
				NewPeriod(time.Unix(3, 0), time.Unix(4, 0)),
			},
			func(t *testing.T, it IntervalTree) {
				require.NotNil(t, it.root.left)
				require.NotNil(t, it.root.right)
				assert.Equal(t, time.Unix(2, 0), it.root.period.Start)
				assert.Equal(t, time.Unix(1, 0), it.root.left.period.Start)
				assert.Equal(t, time.Unix(3, 0), it.root.right.period.Start)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, red, it.root.left.color)
				assert.Equal(t, red, it.root.right.color)
				for i := 0; i < 3; i++ {
					assert.Contains(t, it.nodes, i)
				}
				assert.Len(t, it.nodes, 3)
				assert.Equal(t, time.Unix(10, 0), it.root.maxEnd)
				assert.Equal(t, time.Unix(10, 0), it.root.left.maxEnd)
				assert.Equal(t, time.Unix(4, 0), it.root.right.maxEnd)
			},
		},
		{
			/* Nodes will be inserted and should be rotated and rebalanced such that 1 and 3 are red
			1
			 \         2
			  2   ->  / \
			   \     1   3
			    3
			*/
			"inserting 3 nodes creates red children after a rotation",
			func() IntervalTree { return NewIntervalTree() },
			[]Period{
				NewPeriod(time.Unix(1, 0), time.Unix(5, 0)),
				NewPeriod(time.Unix(2, 0), time.Unix(4, 0)),
				NewPeriod(time.Unix(3, 0), time.Unix(10, 0)),
			},
			func(t *testing.T, it IntervalTree) {
				require.NotNil(t, it.root.left)
				require.NotNil(t, it.root.right)
				assert.Equal(t, time.Unix(2, 0), it.root.period.Start)
				assert.Equal(t, time.Unix(1, 0), it.root.left.period.Start)
				assert.Equal(t, time.Unix(3, 0), it.root.right.period.Start)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, red, it.root.left.color)
				assert.Equal(t, red, it.root.right.color)
				for i := 0; i < 3; i++ {
					assert.Contains(t, it.nodes, i)
				}
				assert.Len(t, it.nodes, 3)
				assert.Len(t, it.nodes, 3)
				assert.Equal(t, time.Unix(10, 0), it.root.maxEnd)
				assert.Equal(t, time.Unix(5, 0), it.root.left.maxEnd)
				assert.Equal(t, time.Unix(10, 0), it.root.right.maxEnd)
			},
		},
		{
			/* 20 is black, 10, 30 are red to start, inserting 35 should make nodes 10 and 30 black
			   20
			  /  \
			10   30
			      \
			      35
			*/
			"inserting a new node beneath red nodes changes the parents to black",
			func() IntervalTree {
				twenty := newNode(NewPeriod(time.Unix(20, 0), time.Unix(25, 0)), nil, nil, black)
				ten := newNode(NewPeriod(time.Unix(10, 0), time.Unix(22, 0)), nil, nil, red)
				thirty := newNode(NewPeriod(time.Unix(30, 0), time.Unix(100, 0)), nil, nil, red)
				twenty.left, twenty.right, twenty.maxEnd = ten, thirty, thirty.period.End
				ten.parent, ten.maxEnd = twenty, ten.period.End
				thirty.parent, thirty.maxEnd = twenty, thirty.period.End
				it := NewIntervalTree()
				it.root = twenty
				return it
			},
			[]Period{NewPeriod(time.Unix(35, 0), time.Unix(50, 0))},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, time.Unix(20, 0), it.root.period.Start)
				assert.Equal(t, time.Unix(10, 0), it.root.left.period.Start)
				assert.Equal(t, time.Unix(30, 0), it.root.right.period.Start)
				assert.Equal(t, time.Unix(35, 0), it.root.right.right.period.Start)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, black, it.root.left.color)
				assert.Equal(t, black, it.root.right.color)
				assert.Equal(t, red, it.root.right.right.color)
				assert.Equal(t, time.Unix(100, 0), it.root.maxEnd)
				assert.Equal(t, time.Unix(22, 0), it.root.left.maxEnd)
				assert.Equal(t, time.Unix(100, 0), it.root.right.maxEnd)
				assert.Equal(t, time.Unix(50, 0), it.root.right.right.maxEnd)
			},
		},
		{
			/* 20 is black, 30 is red to start, inserting 25 should rebalance the tree with multiple left rotations
			20          25
			  \        /  \
			  30  ->  20  30
			 /
			25
			*/
			"inserting a new left inside performs multiple rotations to balance the tree",
			func() IntervalTree {
				twenty := newNode(NewPeriod(time.Unix(20, 0), time.Unix(50, 0)), nil, nil, black)
				thirty := newNode(NewPeriod(time.Unix(30, 0), time.Unix(75, 0)), nil, nil, red)
				twenty.right, twenty.maxEnd = thirty, thirty.period.End
				thirty.parent, thirty.maxEnd = twenty, thirty.period.End
				it := NewIntervalTree()
				it.root = twenty
				return it
			},
			[]Period{NewPeriod(time.Unix(25, 0), time.Unix(100, 0))},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, time.Unix(25, 0), it.root.period.Start)
				assert.Equal(t, time.Unix(20, 0), it.root.left.period.Start)
				assert.Equal(t, time.Unix(30, 0), it.root.right.period.Start)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, red, it.root.left.color)
				assert.Equal(t, red, it.root.right.color)
				assert.Equal(t, time.Unix(100, 0), it.root.maxEnd)
				assert.Equal(t, time.Unix(50, 0), it.root.left.maxEnd)
				assert.Equal(t, time.Unix(75, 0), it.root.right.maxEnd)
			},
		},
		{
			/* 25 is black, 15 is red to start, inserting 20 should rebalance the tree with multiple right rotations
			  25       20
			 /        /  \
			15   ->  15  25
			 \
			 20
			*/
			"inserting a new right inside performs multiple rotations to balance the tree",
			func() IntervalTree {
				twentyFive := newNode(NewPeriod(time.Unix(25, 0), time.Unix(45, 0)), nil, nil, black)
				fifteen := newNode(NewPeriod(time.Unix(15, 0), time.Unix(20, 0)), nil, nil, red)
				twentyFive.left, twentyFive.maxEnd = fifteen, twentyFive.period.End
				fifteen.parent, fifteen.maxEnd = twentyFive, fifteen.period.End
				it := NewIntervalTree()
				it.root = twentyFive
				return it
			},
			[]Period{NewPeriod(time.Unix(20, 0), time.Unix(40, 0))},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, time.Unix(20, 0), it.root.period.Start)
				assert.Equal(t, time.Unix(15, 0), it.root.left.period.Start)
				assert.Equal(t, time.Unix(25, 0), it.root.right.period.Start)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, red, it.root.left.color)
				assert.Equal(t, red, it.root.right.color)
				assert.Equal(t, time.Unix(45, 0), it.root.maxEnd)
				assert.Equal(t, time.Unix(45, 0), it.root.right.maxEnd)
				assert.Equal(t, time.Unix(20, 0), it.root.left.maxEnd)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			it := test.setupTree()
			for i, p := range test.insertPeriods {
				it.Insert(p, i, i)
			}
			test.validateTree(t, it)
		})
	}
}

func TestIntervalTree_rotate(t *testing.T) {
	nodeA := &node{}
	nodeB := &node{}
	nodeC := &node{}
	nodeD := &node{}
	cleanupTree := func() {
		for _, n := range []*node{nodeA, nodeB, nodeC, nodeD} {
			n.left, n.right, n.parent = &node{leaf: true}, &node{leaf: true}, nil
		}
	}
	setupLeftTree := func() IntervalTree {
		cleanupTree()
		nodeD.left, nodeD.period.End, nodeD.maxEnd = nodeC, time.Unix(1, 0), time.Unix(10, 0)
		nodeC.left, nodeC.right, nodeC.parent, nodeC.period.End, nodeC.maxEnd =
			nodeA, nodeB, nodeD, time.Unix(2, 0), time.Unix(10, 0)
		nodeA.parent, nodeA.period.End, nodeA.maxEnd = nodeC, time.Unix(10, 0), time.Unix(10, 0)
		nodeB.parent, nodeB.period.End, nodeB.maxEnd = nodeC, time.Unix(5, 0), time.Unix(5, 0)
		return IntervalTree{root: nodeD}
	}
	setupRightTree := func() IntervalTree {
		cleanupTree()
		nodeD.right, nodeD.period.End, nodeD.maxEnd = nodeC, time.Unix(1, 0), time.Unix(10, 0)
		nodeC.left, nodeC.right, nodeC.parent, nodeC.period.End, nodeC.maxEnd =
			nodeA, nodeB, nodeD, time.Unix(2, 0), time.Unix(10, 0)
		nodeA.parent, nodeA.period.End, nodeA.maxEnd = nodeC, time.Unix(10, 0), time.Unix(10, 0)
		nodeB.parent, nodeB.period.End, nodeB.maxEnd = nodeC, time.Unix(5, 0), time.Unix(5, 0)
		return IntervalTree{root: nodeD}
	}
	tests := []struct {
		name         string
		setupTree    func() IntervalTree
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

func TestIntervalTree_successor(t *testing.T) {
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

func TestIntervalTree_Delete(t *testing.T) {
	tests := []struct {
		name   string
		key    interface{}
		result error
	}{
		{
			"deleting a node should also remove it from the external map",
			1,
			nil,
		}, {
			"deleting a key with no node returns an error",
			2,
			fmt.Errorf("could not delete node with key %v: key does not exist", 2),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			n := newNode(Period{}, 1, nil, black)
			it := IntervalTree{
				root:  n,
				nodes: map[interface{}]*node{1: n},
			}
			err := it.Delete(test.key)
			assert.Equal(t, test.result, err)
		})
	}
}

func TestIntervalTree_delete(t *testing.T) {
	tests := []struct {
		name      string
		setupTree func() (IntervalTree, *node)
		validate  func(t *testing.T, it IntervalTree)
	}{
		{
			"deleting the a tree with only root leaves a leaf as the root",
			func() (IntervalTree, *node) {
				root := newNode(Period{}, nil, nil, black)
				return IntervalTree{root: root}, root
			},
			func(t *testing.T, it IntervalTree) {
				assert.True(t, it.root.leaf)
				assert.Nil(t, it.root.left)
				assert.Nil(t, it.root.right)
				assert.Nil(t, it.root.parent)
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
			func() (IntervalTree, *node) {
				p := newNode(Period{End: time.Unix(40, 0)}, nil, "p", black)
				s := newNode(Period{End: time.Unix(50, 0)}, nil, "s", black)
				n := newNode(Period{End: time.Unix(45, 0)}, nil, "n", black)
				l := newNode(Period{End: time.Unix(25, 0)}, nil, "l", red)
				r := newNode(Period{End: time.Unix(60, 0)}, nil, "r", red)
				p.left, p.right, p.maxEnd = s, n, r.period.End
				s.left, s.right, s.parent, s.maxEnd = l, r, p, r.period.End
				l.parent, r.parent = s, s
				n.parent = p
				return IntervalTree{root: p}, n
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "r", it.root.contents)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, "s", it.root.left.contents)
				assert.Equal(t, black, it.root.left.color)
				assert.Equal(t, "p", it.root.right.contents)
				assert.Equal(t, black, it.root.right.color)
				assert.Equal(t, "l", it.root.left.left.contents)
				assert.Equal(t, red, it.root.left.left.color)
				assert.True(t, it.root.right.right.leaf)
				assert.Equal(t, time.Unix(60, 0), it.root.maxEnd)
				assert.Equal(t, time.Unix(50, 0), it.root.left.maxEnd)
				assert.Equal(t, time.Unix(25, 0), it.root.left.left.maxEnd)
				assert.Equal(t, time.Unix(40, 0), it.root.right.maxEnd)
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
			func() (IntervalTree, *node) {
				p := newNode(Period{End: time.Unix(40, 0)}, nil, "p", black)
				s := newNode(Period{End: time.Unix(50, 0)}, nil, "s", black)
				n := newNode(Period{End: time.Unix(45, 0)}, nil, "n", black)
				l := newNode(Period{End: time.Unix(25, 0)}, nil, "l", red)
				r := newNode(Period{End: time.Unix(60, 0)}, nil, "r", red)
				p.left, p.right, p.maxEnd = n, s, r.period.End
				s.left, s.right, s.parent, s.maxEnd = l, r, p, r.period.End
				l.parent, r.parent = s, s
				n.parent = p
				return IntervalTree{root: p}, n
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "l", it.root.contents)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, "p", it.root.left.contents)
				assert.Equal(t, black, it.root.left.color)
				assert.Equal(t, "s", it.root.right.contents)
				assert.Equal(t, black, it.root.left.color)
				assert.Equal(t, "r", it.root.right.right.contents)
				assert.Equal(t, red, it.root.right.right.color)
				assert.Equal(t, time.Unix(60, 0), it.root.maxEnd)
				assert.Equal(t, time.Unix(40, 0), it.root.left.maxEnd)
				assert.Equal(t, time.Unix(60, 0), it.root.right.maxEnd)
				assert.Equal(t, time.Unix(60, 0), it.root.right.right.maxEnd)
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
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", black)
				s := newNode(Period{}, nil, "s", red)
				n := newNode(Period{}, nil, "n", black)
				l := newNode(Period{}, nil, "l", black)
				r := newNode(Period{}, nil, "r", black)
				p.left, p.right = s, n
				s.left, s.right, s.parent = l, r, p
				l.parent = s
				r.parent = s
				n.parent = p
				return IntervalTree{root: p}, n
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "s", it.root.contents)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, "l", it.root.left.contents)
				assert.Equal(t, black, it.root.left.color)
				assert.Equal(t, "p", it.root.right.contents)
				assert.Equal(t, red, it.root.right.color)
				assert.Equal(t, "r", it.root.right.left.contents)
				assert.Equal(t, black, it.root.right.left.color)
				assert.True(t, it.root.right.right.leaf)
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
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", black)
				s := newNode(Period{}, nil, "s", red)
				n := newNode(Period{}, nil, "n", black)
				l := newNode(Period{}, nil, "l", black)
				r := newNode(Period{}, nil, "r", black)
				p.left, p.right = n, s
				s.left, s.right, s.parent = l, r, p
				l.parent = s
				r.parent = s
				n.parent = p
				return IntervalTree{root: p}, n
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "s", it.root.contents)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, "p", it.root.left.contents)
				assert.Equal(t, red, it.root.left.color)
				assert.Equal(t, "r", it.root.right.contents)
				assert.Equal(t, black, it.root.right.color)
				assert.Equal(t, "l", it.root.left.right.contents)
				assert.Equal(t, black, it.root.right.left.color)
				assert.True(t, it.root.left.left.leaf)
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
			func() (IntervalTree, *node) {
				p := newNode(Period{End: time.Unix(15, 0)}, nil, "p", black)
				s := newNode(Period{End: time.Unix(10, 0)}, nil, "s", black)
				n := newNode(Period{End: time.Unix(45, 0)}, nil, "n", black)
				l := newNode(Period{End: time.Unix(25, 0)}, nil, "l", red)
				p.left, p.right, p.maxEnd = n, s, n.period.End
				n.left, n.parent = l, p
				s.parent = p
				l.parent = n
				return IntervalTree{root: p}, n
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "p", it.root.contents)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, "l", it.root.left.contents)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, "s", it.root.right.contents)
				assert.Equal(t, black, it.root.right.color)
				assert.Equal(t, time.Unix(25, 0), it.root.maxEnd)
				assert.Equal(t, time.Unix(25, 0), it.root.left.maxEnd)
				assert.Equal(t, time.Unix(10, 0), it.root.right.maxEnd)
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
			func() (IntervalTree, *node) {
				p := newNode(Period{End: time.Unix(15, 0)}, nil, "p", black)
				s := newNode(Period{End: time.Unix(10, 0)}, nil, "s", black)
				n := newNode(Period{End: time.Unix(45, 0)}, nil, "n", black)
				r := newNode(Period{End: time.Unix(25, 0)}, nil, "r", red)
				p.left, p.right, p.maxEnd = s, n, n.period.End
				n.right, n.parent = r, p
				s.parent = p
				r.parent = n
				return IntervalTree{root: p}, n
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "p", it.root.contents)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, "s", it.root.left.contents)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, "r", it.root.right.contents)
				assert.Equal(t, black, it.root.right.color)
				assert.Equal(t, time.Unix(25, 0), it.root.maxEnd)
				assert.Equal(t, time.Unix(25, 0), it.root.right.maxEnd)
				assert.Equal(t, time.Unix(10, 0), it.root.left.maxEnd)
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
			func() (IntervalTree, *node) {
				p := newNode(Period{End: time.Unix(15, 0)}, nil, "p", black)
				s := newNode(Period{End: time.Unix(10, 0)}, nil, "s", black)
				n := newNode(Period{End: time.Unix(25, 0)}, nil, "n", black)
				r := newNode(Period{End: time.Unix(45, 0)}, nil, "r", red)
				p.left, p.right, p.maxEnd = n, s, r.maxEnd
				n.right, n.parent = r, p
				s.parent = p
				r.parent = n
				return IntervalTree{root: p}, n
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "p", it.root.contents)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, "r", it.root.left.contents)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, "s", it.root.right.contents)
				assert.Equal(t, black, it.root.right.color)
				assert.Equal(t, time.Unix(45, 0), it.root.maxEnd)
				assert.Equal(t, time.Unix(10, 0), it.root.right.maxEnd)
				assert.Equal(t, time.Unix(45, 0), it.root.left.maxEnd)
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
			func() (IntervalTree, *node) {
				p := newNode(Period{End: time.Unix(15, 0)}, nil, "p", black)
				s := newNode(Period{End: time.Unix(10, 0)}, nil, "s", black)
				n := newNode(Period{End: time.Unix(45, 0)}, nil, "n", black)
				l := newNode(Period{End: time.Unix(25, 0)}, nil, "l", red)
				p.left, p.right, p.maxEnd = s, n, n.period.End
				n.left, n.parent = l, p
				s.parent = p
				l.parent = n
				return IntervalTree{root: p}, n
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "p", it.root.contents)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, "s", it.root.left.contents)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, "l", it.root.right.contents)
				assert.Equal(t, black, it.root.right.color)
				assert.Equal(t, time.Unix(25, 0), it.root.maxEnd)
				assert.Equal(t, time.Unix(25, 0), it.root.right.maxEnd)
				assert.Equal(t, time.Unix(10, 0), it.root.left.maxEnd)
			},
		}, {
			"deleting black node with leaf sibling and red parent makes parent black",
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", red)
				n := newNode(Period{}, nil, "n", black)
				p.left = n
				n.parent = p
				return IntervalTree{root: p}, n
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "p", it.root.contents)
				assert.True(t, it.root.left.leaf)
				assert.Equal(t, black, it.root.color)
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
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", black)
				s := newNode(Period{}, nil, "s", black)
				n := newNode(Period{}, nil, "n", black)
				l := newNode(Period{}, nil, "l", black)
				r := newNode(Period{}, nil, "r", black)
				p.left, p.right = n, s
				s.left, s.right, s.parent = l, r, p
				l.parent = s
				r.parent = s
				n.parent = p
				return IntervalTree{root: p}, n
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "p", it.root.contents)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, "s", it.root.right.contents)
				assert.Equal(t, red, it.root.right.color)
				assert.Equal(t, "l", it.root.right.left.contents)
				assert.Equal(t, black, it.root.right.left.color)
				assert.Equal(t, "r", it.root.right.right.contents)
				assert.Equal(t, black, it.root.right.left.color)
				assert.True(t, it.root.left.leaf)
			},
		}, {
			/* RR is red, the rest are black; after deleting N, all are black
			  N          RL
			 / \        / \
			L   R  ->  L   R
			   /
			  RL
			*/
			"delete an internal node with a black successor",
			func() (IntervalTree, *node) {
				n := newNode(Period{End: time.Unix(20, 0)}, nil, "n", black)
				l := newNode(Period{End: time.Unix(10, 0)}, nil, "l", black)
				r := newNode(Period{End: time.Unix(30, 0)}, nil, "r", black)
				rl := newNode(Period{End: time.Unix(50, 0)}, nil, "rl", red)
				n.left, n.right, n.maxEnd = l, r, rl.period.End
				r.left, r.parent = rl, n
				l.parent = n
				rl.parent = r
				return IntervalTree{root: n}, n
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "rl", it.root.contents)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, "l", it.root.left.contents)
				assert.Equal(t, black, it.root.left.color)
				assert.Equal(t, "r", it.root.right.contents)
				assert.Equal(t, black, it.root.right.color)
				assert.Equal(t, time.Unix(50, 0), it.root.maxEnd)
				assert.Equal(t, time.Unix(10, 0), it.root.left.maxEnd)
				assert.Equal(t, time.Unix(30, 0), it.root.right.maxEnd)
			},
		}, {
			/* RR is red, the rest are black; after deleting N, all are black
			  N          RL
			 / \        / \
			L   R  ->  L   R
			   /
			  RL
			*/
			"delete an internal node with a black successor with max end less than the internal node",
			func() (IntervalTree, *node) {
				n := newNode(Period{End: time.Unix(50, 0)}, nil, "n", black)
				l := newNode(Period{End: time.Unix(10, 0)}, nil, "l", black)
				r := newNode(Period{End: time.Unix(30, 0)}, nil, "r", black)
				rl := newNode(Period{End: time.Unix(20, 0)}, nil, "rl", red)
				n.left, n.right = l, r
				r.left, r.parent, r.maxEnd = rl, n, rl.period.End
				l.parent = n
				rl.parent = r
				return IntervalTree{root: n}, n
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "rl", it.root.contents)
				assert.Equal(t, black, it.root.color)
				assert.Equal(t, "l", it.root.left.contents)
				assert.Equal(t, black, it.root.left.color)
				assert.Equal(t, "r", it.root.right.contents)
				assert.Equal(t, black, it.root.right.color)
				assert.Equal(t, time.Unix(30, 0), it.root.maxEnd)
				assert.Equal(t, time.Unix(10, 0), it.root.left.maxEnd)
				assert.Equal(t, time.Unix(30, 0), it.root.right.maxEnd)
			},
		}, {
			"deleting the only child of the root updates max end correctly",
			func() (IntervalTree, *node) {
				root := newNode(Period{End: time.Unix(20, 0)}, nil, "root", black)
				r := newNode(Period{End: time.Unix(30, 0)}, nil, "r", black)
				root.right, root.maxEnd = r, r.period.End
				r.parent = root
				return IntervalTree{root: root}, r
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "root", it.root.contents)
				assert.True(t, it.root.left.leaf)
				assert.True(t, it.root.right.leaf)
				assert.Equal(t, time.Unix(20, 0), it.root.maxEnd)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			it, nodeToDelete := test.setupTree()
			it.delete(nodeToDelete)
			test.validate(t, it)
		})
	}
}

func TestIntervalTree_deleteRepairCase1(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() (IntervalTree, *node)
		validate func(t *testing.T, it IntervalTree)
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
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", black)
				s := newNode(Period{}, nil, "s", red)
				l := newNode(Period{}, nil, "l", black)
				r := newNode(Period{}, nil, "r", black)
				n := &node{leaf: true}
				p.left = s
				p.right = n
				s.left = l
				s.right = r
				s.parent = p
				n.parent = p
				l.parent = s
				r.parent = s
				n.parent = p
				return IntervalTree{root: p}, n
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "s", it.root.contents)
				assert.Equal(t, "l", it.root.left.contents)
				assert.Equal(t, "p", it.root.right.contents)
				assert.Equal(t, "r", it.root.right.left.contents)
				assert.True(t, it.root.right.right.leaf)
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
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", black)
				s := newNode(Period{}, nil, "s", red)
				l := newNode(Period{}, nil, "l", black)
				r := newNode(Period{}, nil, "r", black)
				n := &node{leaf: true}
				p.right = s
				p.left = n
				s.left = l
				s.right = r
				s.parent = p
				n.parent = p
				l.parent = s
				r.parent = s
				n.parent = p
				return IntervalTree{root: p}, n
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "s", it.root.contents)
				assert.Equal(t, "p", it.root.left.contents)
				assert.Equal(t, "r", it.root.right.contents)
				assert.Equal(t, "l", it.root.left.right.contents)
				assert.True(t, it.root.left.left.leaf)
			},
		}, {
			"deleted child with black sibling does nothing",
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", black)
				s := newNode(Period{}, nil, "s", black)
				n := &node{leaf: true}
				p.left = s
				p.right = n
				n.parent = p
				s.parent = p
				return IntervalTree{root: p}, n
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "p", it.root.contents)
				assert.Equal(t, "s", it.root.left.contents)
				assert.True(t, it.root.right.leaf)
			},
		}, {
			"deleted child with no sibling does nothing",
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", black)
				n := &node{leaf: true}
				p.right = n
				n.parent = p
				return IntervalTree{root: p}, n
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "p", it.root.contents)
				assert.True(t, it.root.left.leaf)
				assert.True(t, it.root.right.leaf)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			it, n := test.setup()
			it.deleteRepairCase1(n)
			test.validate(t, it)
		})
	}
}

func TestIntervalTree_deleteRepairCase2(t *testing.T) {
	tests := []struct {
		name            string
		setup           func() (IntervalTree, *node, *node)
		expectedOutcome bool
		expectRecolor   bool
	}{
		{
			"deleted node with black sibling with 2 black child nodes recolors the sibling and returns true",
			func() (IntervalTree, *node, *node) {
				p := newNode(Period{}, nil, "p", black)
				s := newNode(Period{}, nil, "s", black)
				sl := newNode(Period{}, nil, "sl", black)
				sr := newNode(Period{}, nil, "sr", black)
				n := &node{leaf: true, contents: "n"}
				p.left, p.right = s, n
				s.left, s.right, s.parent = sl, sr, p
				sl.parent, sr.parent = s, s
				n.parent = p
				return IntervalTree{root: p}, n, s
			},
			true,
			true,
		}, {
			"deleted node with black sibling and 1 child does nothing",
			func() (IntervalTree, *node, *node) {
				p := newNode(Period{}, nil, nil, black)
				s := newNode(Period{}, nil, nil, black)
				sr := newNode(Period{}, nil, nil, black)
				n := &node{leaf: true}
				p.left, p.right = s, n
				s.right, s.parent = sr, p
				sr.parent = s
				n.parent = p
				return IntervalTree{root: p}, n, s
			},
			false,
			false,
		}, {
			"deleted node with leaf sibling returns true but does not recolor the leaf",
			func() (IntervalTree, *node, *node) {
				p := newNode(Period{}, nil, nil, black)
				s := &node{leaf: true}
				n := &node{leaf: true}
				p.left, p.right = s, n
				s.parent = p
				n.parent = p
				return IntervalTree{root: p}, n, s
			},
			true,
			false,
		}, {
			"deleted node with red sibling does nothing",
			func() (IntervalTree, *node, *node) {
				p := newNode(Period{}, nil, nil, black)
				s := newNode(Period{}, nil, nil, red)
				sl := newNode(Period{}, nil, nil, black)
				sr := newNode(Period{}, nil, nil, black)
				n := &node{leaf: true}
				p.left, p.right = s, n
				s.left, s.right, s.parent = sl, sr, p
				sl.parent, sr.parent = s, s
				n.parent = p
				return IntervalTree{root: p}, n, s
			},
			false,
			false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			it, n, s := test.setup()
			sColorBefore := s.color
			result := it.deleteRepairCase2(n)
			assert.Equal(t, test.expectedOutcome, result)
			if test.expectRecolor {
				assert.Equal(t, red, s.color)
			} else {
				assert.Equal(t, sColorBefore, s.color)
			}
		})
	}
}

func TestIntervalTree_deleteRepairCase3(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() (IntervalTree, *node)
		validate func(t *testing.T, it IntervalTree)
	}{
		{
			"no action when sibling is a leaf",
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", black)
				r.parent = p
				p.right = r
				return IntervalTree{root: p}, r
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "p", it.root.contents)
				assert.Equal(t, "r", it.root.right.contents)
				assert.True(t, it.root.left.leaf)
			},
		}, {
			"no action when sibling is red",
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", red)
				l := newNode(Period{}, nil, "l", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				return IntervalTree{root: p}, r
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "p", it.root.contents)
				assert.Equal(t, "l", it.root.left.contents)
				assert.Equal(t, "r", it.root.right.contents)
			},
		}, {
			"no action when node is right child and sibling has no right child",
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", black)
				l := newNode(Period{}, nil, "l", black)
				ll := newNode(Period{}, nil, "ll", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				l.left = ll
				ll.parent = ll
				return IntervalTree{root: p}, r
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "p", it.root.contents)
				assert.Equal(t, "l", it.root.left.contents)
				assert.Equal(t, "r", it.root.right.contents)
				assert.Equal(t, "ll", it.root.left.left.contents)
			},
		}, {
			"no action when node is left child and sibling has no left child",
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", black)
				l := newNode(Period{}, nil, "l", black)
				ll := newNode(Period{}, nil, "ll", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				l.left = ll
				ll.parent = ll
				return IntervalTree{root: p}, l
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "p", it.root.contents)
				assert.Equal(t, "l", it.root.left.contents)
				assert.Equal(t, "r", it.root.right.contents)
				assert.Equal(t, "ll", it.root.left.left.contents)
			},
		}, {
			"left rotate around sibling and recolor when node is right child and sibling is black with red right child",
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", black)
				l := newNode(Period{}, nil, "l", black)
				lr := newNode(Period{}, nil, "lr", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				l.right = lr
				lr.parent = l
				return IntervalTree{root: p}, r
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "p", it.root.contents)
				assert.Equal(t, "lr", it.root.left.contents)
				assert.Equal(t, "r", it.root.right.contents)
				assert.Equal(t, "l", it.root.left.left.contents)
				assert.Equal(t, black, it.root.left.color)
				assert.Equal(t, red, it.root.left.left.color)
			},
		}, {
			"right rotate around sibling and recolor when node is left child and sibling is black with red left child",
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", black)
				l := newNode(Period{}, nil, "l", black)
				rl := newNode(Period{}, nil, "rl", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				r.left = rl
				rl.parent = r
				return IntervalTree{root: p}, l
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "p", it.root.contents)
				assert.Equal(t, "rl", it.root.right.contents)
				assert.Equal(t, "l", it.root.left.contents)
				assert.Equal(t, "r", it.root.right.right.contents)
				assert.Equal(t, black, it.root.right.color)
				assert.Equal(t, red, it.root.right.right.color)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			it, n := test.setup()
			it.deleteRepairCase3(n)
			test.validate(t, it)
		})
	}
}

func TestIntervalTree_deleteRepairCase4(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() (IntervalTree, *node)
		validate func(t *testing.T, it IntervalTree)
	}{
		{
			"no action when sibling is a leaf",
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", black)
				r.parent = p
				p.right = r
				return IntervalTree{root: p}, r
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "p", it.root.contents)
				assert.Equal(t, "r", it.root.right.contents)
				assert.True(t, it.root.left.leaf)
			},
		}, {
			"no action when sibling is red",
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", red)
				l := newNode(Period{}, nil, "l", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				return IntervalTree{root: p}, r
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "p", it.root.contents)
				assert.Equal(t, "l", it.root.left.contents)
				assert.Equal(t, "r", it.root.right.contents)
			},
		}, {
			"no action when right child and sibling has no left child",
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", black)
				l := newNode(Period{}, nil, "l", black)
				lr := newNode(Period{}, nil, "lr", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				l.right = lr
				lr.parent = l
				return IntervalTree{root: p}, r
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "p", it.root.contents)
				assert.Equal(t, "l", it.root.left.contents)
				assert.Equal(t, "r", it.root.right.contents)
				assert.Equal(t, "lr", it.root.left.right.contents)
			},
		}, {
			"no action when left child and sibling has no right child",
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", black)
				r := newNode(Period{}, nil, "r", black)
				l := newNode(Period{}, nil, "l", black)
				rl := newNode(Period{}, nil, "rl", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				r.left = rl
				rl.parent = r
				return IntervalTree{root: p}, r
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "p", it.root.contents)
				assert.Equal(t, "l", it.root.left.contents)
				assert.Equal(t, "r", it.root.right.contents)
				assert.Equal(t, "rl", it.root.right.left.contents)
			},
		}, {
			"right rotate around parent and recolor when right child and sibling is black with red left child",
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", red)
				r := newNode(Period{}, nil, "r", black)
				l := newNode(Period{}, nil, "l", black)
				ll := newNode(Period{}, nil, "ll", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				l.left = ll
				ll.parent = l
				return IntervalTree{root: p}, r
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "l", it.root.contents)
				assert.Equal(t, "ll", it.root.left.contents)
				assert.Equal(t, "p", it.root.right.contents)
				assert.Equal(t, "r", it.root.right.right.contents)
				assert.Equal(t, black, it.root.left.color)
				assert.Equal(t, black, it.root.right.color)
				assert.Equal(t, red, it.root.color)
			},
		}, {
			"left rotate around parent and recolor when left child and sibling is black with red right child",
			func() (IntervalTree, *node) {
				p := newNode(Period{}, nil, "p", red)
				r := newNode(Period{}, nil, "r", black)
				l := newNode(Period{}, nil, "l", black)
				rr := newNode(Period{}, nil, "rr", red)
				l.parent, r.parent = p, p
				p.left, p.right = l, r
				r.right = rr
				rr.parent = r
				return IntervalTree{root: p}, l
			},
			func(t *testing.T, it IntervalTree) {
				assert.Equal(t, "r", it.root.contents)
				assert.Equal(t, "p", it.root.left.contents)
				assert.Equal(t, "rr", it.root.right.contents)
				assert.Equal(t, "l", it.root.left.left.contents)
				assert.Equal(t, black, it.root.left.color)
				assert.Equal(t, black, it.root.right.color)
				assert.Equal(t, red, it.root.color)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			it, n := test.setup()
			it.deleteRepairCase4(n)
			test.validate(t, it)
		})
	}
}

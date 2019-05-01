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

type color int

const (
	black color = iota
	red
)

// node is the private interval tree node type that contains the node's subtree, the maximum end time of the node's
// subtree, and its color. Instead of storing leaves as nil, leaves are stored as sentinel nodes with the leaf property
// set to make deletions easier.
type node struct {
	period   Period
	key      interface{}
	contents interface{}
	maxEnd   time.Time
	color    color
	left     *node
	right    *node
	parent   *node
	leaf     bool
}

// newNode creates a new node with data and a color, making sure to construct its left and right children as
// sentinel nil nodes.
func newNode(period Period, key, contents interface{}, color color) *node {
	n := &node{
		period:   period,
		key:      key,
		contents: contents,
		color:    color,
		maxEnd:   period.End,
	}
	l, r := &node{leaf: true, parent: n}, &node{leaf: true, parent: n}
	n.left, n.right = l, r
	return n
}

// isLeftChild returns whether a node is the left child of its parent.
func (n *node) isLeftChild() bool {
	if n.parent == nil {
		return false
	}
	return n.parent.left == n
}

// sibling returns the node's sibling; i.e. if the node is the parent's left child, the sibling
// is the parent's right child, and vice versa.
func (n *node) sibling() *node {
	if n.parent == nil {
		return nil
	}
	if n.isLeftChild() {
		return n.parent.right
	}
	return n.parent.left
}

// nodeColor returns the color of the node, taking into account that nil nodes are black
func (n *node) nodeColor() color {
	if n.leaf {
		return black
	}
	return n.color
}

// successor returns the next node that would be traversed in an in-order traversal.
// This is either the the minimum value in node n's right subtree or the first ancestor that is to the left of n.
func (n *node) successor() *node {
	if !n.right.leaf {
		successor := n.right
		for !successor.left.leaf {
			successor = successor.left
		}
		return successor
	}

	parent := n.parent
	cur := n
	for parent != nil {
		if cur.isLeftChild() {
			break
		}
		cur = parent
		parent = parent.parent
	}
	return parent
}

// maxEndOfSubtree returns the latest end time of the node and its subtree.
func (n *node) maxEndOfSubtree() time.Time {
	if n.period.End.IsZero() {
		return time.Time{}
	}
	if n.left.leaf && n.right.leaf {
		return n.period.End
	}
	if n.left.leaf && !n.right.leaf {
		if n.right.maxEnd.IsZero() {
			return n.right.maxEnd
		}
		return MaxTime(n.period.End, n.right.maxEnd)
	}
	if !n.left.leaf && n.right.leaf {
		if n.left.maxEnd.IsZero() {
			return n.left.maxEnd
		}
		return MaxTime(n.period.End, n.left.maxEnd)
	}
	if n.right.maxEnd.IsZero() || n.left.maxEnd.IsZero() {
		return time.Time{}
	}
	return MaxTime(n.period.End, n.left.maxEnd, n.right.maxEnd)
}

// periodToLeft decides whether a period belongs to the left of the node.
func (n *node) periodToLeft(p Period) bool {
	return p.Start.Before(n.period.Start)
}

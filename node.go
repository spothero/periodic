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
	Period   Period      `json:"period"`
	Key      interface{} `json:"key"`
	contents interface{}
	MaxEnd   time.Time `json:"max_end"`
	Color    color     `json:"color"`
	Left     *node     `json:"left"`
	Right    *node     `json:"right"`
	parent   *node
	Leaf     bool `json:"leaf"`
}

// newNode creates a new node with data and a color, making sure to construct its left and right children as
// sentinel nil nodes.
func newNode(period Period, key, contents interface{}, color color) *node {
	n := &node{
		Period:   period,
		Key:      key,
		contents: contents,
		Color:    color,
		MaxEnd:   period.End,
	}
	l, r := &node{Leaf: true, parent: n}, &node{Leaf: true, parent: n}
	n.Left, n.Right = l, r
	return n
}

// isLeftChild returns whether a node is the left child of its parent.
func (n *node) isLeftChild() bool {
	if n.parent == nil {
		return false
	}
	return n.parent.Left == n
}

// sibling returns the node's sibling; i.e. if the node is the parent's left child, the sibling
// is the parent's right child, and vice versa.
func (n *node) sibling() *node {
	if n.parent == nil {
		return nil
	}
	if n.isLeftChild() {
		return n.parent.Right
	}
	return n.parent.Left
}

// nodeColor returns the color of the node, taking into account that nil nodes are black
func (n *node) nodeColor() color {
	if n.Leaf {
		return black
	}
	return n.Color
}

// successor returns the next node that would be traversed in an in-order traversal.
// This is either the the minimum value in node n's right subtree or the first ancestor that is to the left of n.
func (n *node) successor() *node {
	if !n.Right.Leaf {
		successor := n.Right
		for !successor.Left.Leaf {
			successor = successor.Left
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

// maxEndOfSubtree returns the latest end time of the node's subtree.
func (n *node) maxEndOfSubtree() time.Time {
	if n.Left.Leaf && n.Right.Leaf {
		return n.Period.End
	}
	if n.Left.Leaf && !n.Right.Leaf {
		if n.Right.MaxEnd.IsZero() {
			return n.Right.MaxEnd
		}
		return MaxTime(n.Period.End, n.Right.MaxEnd)
	}
	if !n.Left.Leaf && n.Right.Leaf {
		if n.Left.MaxEnd.IsZero() {
			return n.Left.MaxEnd
		}
		return MaxTime(n.Period.End, n.Left.MaxEnd)
	}
	if n.Right.MaxEnd.IsZero() || n.Left.MaxEnd.IsZero() {
		return time.Time{}
	}
	return MaxTime(n.Period.End, MaxTime(n.Left.MaxEnd, n.Right.MaxEnd))
}

// periodToLeft decides whether a period belongs to the left of the node.
func (n *node) periodToLeft(p Period) bool {
	return p.Start.Before(n.Period.Start)
}

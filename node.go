package periodic

import (
	"time"
)

type color int

const (
	black color = iota
	red
)

// Node is the interval tree node type that contains any object and its corresponding period.
type Node struct {
	Period   Period
	Contents interface{}
}

// node is the private interval tree node type that contains the node's subtree, the maximum end time of the node's
// subtree, and its color. Instead of storing leaves as nil, leaves are stored as nodes with the leaf property
// set to make deletions easier.
type node struct {
	Node
	maxEnd time.Time
	color  color
	left   *node
	right  *node
	parent *node
	leaf   bool
}

// newNode creates a new node with data and a color, making sure to construct its left and right children as
// sentinel nil nodes.
func newNode(data Node, color color) *node {
	n := &node{
		Node:  data,
		color: color,
	}
	l, r := &node{leaf: true, parent: n}, &node{leaf: true, parent: n}
	n.left, n.right = l, r
	return n
}

func (n *node) isLeftChild() bool {
	if n.parent == nil {
		return false
	}
	return n.parent.left == n
}

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

// successor the next node that would be traversed in an in-order traversal.
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

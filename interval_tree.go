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

// IntervalTree is a data structure for storing objects that contain time intervals (periods). It is implemented
// as an augmented red-black binary search tree.
type IntervalTree struct {
	root *node
	size int
}

// less decides if p1 comes before or after p2 for the purposes of tree traversal. The start of the period
// is used as the key in the tree.
func less(p1 Period, p2 Period) bool {
	return p1.Start.Before(p2.Start)
}

// Insert adds a new node into the tree
func (it *IntervalTree) Insert(data Node) {
	if it.root == nil || it.root.leaf {
		it.root = newNode(data, black)
		return
	}
	var inserted *node
	it.insert(data, it.root, &inserted)
	it.size++
	it.insertRepair(inserted)
}

// insert recursively a new red node containing a period and ID into the tree. The inserted node is stored in the
// the inserted parameter.
func (it *IntervalTree) insert(data Node, root *node, inserted **node) *node {
	if root.leaf {
		*inserted = newNode(data, red)
		return *inserted
	}
	// augment the tree with the maximum end time of its subtree
	root.maxEnd = MaxTime(data.Period.End, root.Period.End)

	if less(data.Period, root.Period) {
		root.left = it.insert(data, root.left, inserted)
		root.left.parent = root
	} else {
		root.right = it.insert(data, root.right, inserted)
		root.right.parent = root
	}
	return root
}

// insertRepair rebalances the tree to maintain the red-black property after an insertion
// TODO: maintain the augmented maxEnd property when repairing -- should be able to accomplish in O(1)
func (it *IntervalTree) insertRepair(n *node) {
	if n == it.root {
		// n is the actual root of the tree, by definition it is always black
		n.color = black
		return
	}

	if n.parent.color == black {
		// the parent is already black so nothing has to be done
		return
	}

	uncle := n.parent.sibling()
	uncleColor := uncle.nodeColor()

	if uncleColor == red {
		// the parent is red; if it has a red sibling, change parent & uncle to black and change grandparent to red
		uncle.color = black
		n.parent.color = black
		if n.parent.parent != nil {
			n.parent.parent.color = red
			it.insertRepair(n.parent.parent)
			return
		}
	}

	if uncleColor == black && n.parent.color == red {
		// move n so that it is on the same side of its parent as its parent is to its grandparent (i.e. it is on the
		// outside of the subtree)
		isInsideRight := n.parent.isLeftChild() && !n.isLeftChild()
		isInsideLeft := !n.parent.isLeftChild() && n.isLeftChild()
		if isInsideRight {
			it.rotate(n.parent, left)
			n = n.left
		} else if isInsideLeft {
			it.rotate(n.parent, right)
			n = n.right
		}

		// rotate again to move n into the grandparent's spot
		n.parent.color = black
		if n.parent.parent != nil {
			n.parent.parent.color = red
			if n.isLeftChild() {
				it.rotate(n.parent.parent, right)
			} else {
				it.rotate(n.parent.parent, left)
			}
		}
	}
}

type rotationDirection int

const (
	right rotationDirection = iota
	left
)

func (it *IntervalTree) rotate(n *node, direction rotationDirection) {
	// y is the node that is going to take the place of n in the tree
	var y *node
	switch direction {
	case right:
		y = n.left
	case left:
		y = n.right
	}

	// move y into n's position
	if n == it.root {
		it.root = y
	} else {
		if n.isLeftChild() {
			n.parent.left = y
		} else {
			n.parent.right = y
		}
	}
	y.parent = n.parent

	// rotate about n
	switch direction {
	case right:
		n.left = y.right
		y.right.parent = n
		y.right = n
	case left:
		n.right = y.left
		y.left.parent = n
		y.left = n
	}
	n.parent = y
}

// InOrder traverses the tree from the root and visits all the nodes in order
func (it *IntervalTree) InOrder() []Node {
	nodes := make([]Node, 0, it.size)
	it.inOrder(it.root, &nodes)
	return nodes
}

func (it *IntervalTree) inOrder(n *node, visited *[]Node) {
	if n.leaf {
		return
	}
	it.inOrder(n.left, visited)
	*visited = append(*visited, n.Node)
	it.inOrder(n.right, visited)
}

func (it *IntervalTree) delete(n *node) {
	// y is the node that is going to be deleted, z is the node that will be moved into y's place
	var y *node
	var z *node

	if n.left.leaf || n.right.leaf {
		// n has 0 or 1 children so it can be deleted
		y = n
	} else {
		// n is an internal node, delete its successor and swap the contents of its successor into n
		y = n.successor()
	}
	if !y.left.leaf {
		z = y.left
	} else if !y.right.leaf {
		z = y.right
	} else {
		z = &node{leaf: true}
	}
	z.parent = y.parent

	if y.parent != nil {
		if y.isLeftChild() {
			y.parent.left = z
		} else {
			y.parent.right = z
		}
	} else {
		it.root = z
	}
	n.Node = y.Node
	if y.color == black {
		it.deleteRepair(z)
	}
}

// deleteRepair rebalances the tree to maintain the red-black property after a deletion
// TODO: maintain the augmented maxEnd property when repairing -- should be able to accomplish in O(1)
func (it *IntervalTree) deleteRepair(n *node) {
	if n == it.root || n.color == red {
		n.color = black
		return
	}
	it.deleteRepairCase1(n)
	if it.deleteRepairCase2(n) {
		it.deleteRepair(n.parent)
		return
	}
	it.deleteRepairCase3(n)
	it.deleteRepairCase4(n)
}

// deleteRepairCase1 handles the case of the deleted node's sibling being red. changes the parent's color to red and
// the sibling's color to black and rotates to make the sibling the parent.
func (it *IntervalTree) deleteRepairCase1(n *node) {
	sibling := n.sibling()
	if sibling.nodeColor() == red {
		sibling.color = black
		n.parent.color = red
		if n.isLeftChild() {
			it.rotate(n.parent, left)
		} else {
			it.rotate(n.parent, right)
		}
	}
}

// deleteRepairCase2 handles the case of the deleted node's sibling being a leaf or the sibling and its children
// colored black. It handles this case by changing the sibling to red and returns whether the parent needs
// to be repaired.
func (it *IntervalTree) deleteRepairCase2(n *node) bool {
	sibling := n.sibling()
	if sibling.leaf {
		return true
	}
	numChildren := 0
	if !sibling.left.leaf {
		numChildren++
	}
	if !sibling.right.leaf {
		numChildren++
	}
	if sibling.color == black && numChildren == 2 && sibling.left.color == black && sibling.right.color == black {
		sibling.color = red
		return true
	}
	return false
}

// deleteRepairCase3 handles the case of the node's sibling colored black with a red child on the right if the deleted
// node is on the right, or a red child on the left if the deleted node is on the left. It handles this case by
// recoloring the sibling red and recoloring the appropriate child to black then rotating to move the sibling up.
func (it *IntervalTree) deleteRepairCase3(n *node) {
	sibling := n.sibling()
	if !sibling.leaf && sibling.nodeColor() == black {
		if sibling.right.nodeColor() == red && !n.isLeftChild() {
			sibling.color = red
			sibling.right.color = black
			it.rotate(sibling, left)
		} else if sibling.left.nodeColor() == red && n.isLeftChild() {
			sibling.color = red
			sibling.left.color = black
			it.rotate(sibling, right)
		}
	}
}

// deleteRepairCase4 handles the case of the sibling of n colored black with a red child on the right if the deleted
// node is on the left, or the sibling having a red child on the left if the deleted node is on the right. It recolors
// the appropriate child of the sibling, makes the sibling the same color as the parent, makes the parent black, and
// rotates to move the sibling up.
func (it *IntervalTree) deleteRepairCase4(n *node) {
	sibling := n.sibling()
	if !sibling.leaf && sibling.nodeColor() == black {
		if sibling.left.nodeColor() == red && !n.isLeftChild() {
			sibling.left.color = black
			sibling.color = n.parent.color
			n.parent.color = black
			it.rotate(n.parent, right)
		} else if sibling.right.nodeColor() == red && n.isLeftChild() {
			sibling.right.color = black
			sibling.color = n.parent.color
			n.parent.color = black
			it.rotate(n.parent, left)
		}
	}
}

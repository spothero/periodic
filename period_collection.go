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
	"fmt"
	"sync"
	"time"
)

// PeriodCollection is a data structure for storing time periods and arbitrary data objects associated with the
// period.Once populated, PeriodCollection allows callers to quickly identify subsets of the collection
// that intersect with another period or find periods that contain a given time.
//
// PeriodCollection is implemented on top of a self-balancing red-black tree.
// This means that insertion and deletion operations take logarithmic time while querying can never exceed linear
// time. But on average, as long as the query period is not large relative to the total time range stored, querying
// should perform in better than linear time.
type PeriodCollection struct {
	root  *node
	mutex sync.RWMutex
	// nodes is an external mapping of a node's key to a pointer of the node since the interval tree is keyed on
	// the node's period start time
	nodes map[interface{}]*node
}

type rotationDirection int

const (
	right rotationDirection = iota
	left
)

// TraversalOrder is the type of depth-first search to use when traversing PeriodCollection's backing tree
type TraversalOrder int

const (
	// PreOrder corresponds to a pre-order depth-first traversal (i.e. root, left, right)
	PreOrder = iota
	// InOrder corresponds to an in-order depth-first traversal (i.e. left, root, right)
	InOrder
	// PostOrder corresponds to a post-order depth-first traversal (i.e. left, right, root)
	PostOrder
)

// NewPeriodCollection constructs a new PeriodCollection
func NewPeriodCollection() *PeriodCollection {
	return &PeriodCollection{nodes: make(map[interface{}]*node), root: &node{leaf: true}}
}

// Command is an interface that allows multiple operations on the PeriodCollection to be
// queued up and executed in order within the context of one write lock.
type Command interface {
	execute()
}

// Update is a command that runs an update on the PeriodCollection
type Update struct {
	key, newContents interface{}
	newPeriod        Period
	pc               *PeriodCollection
}

// Delete is a command that runs a deletion on the PeriodCollection
type Delete struct {
	key interface{}
	pc  *PeriodCollection
}

// Insert adds a new period into the collection. The key parameter is a unique identifier that must be supplied
// when inserting a new period. contents is an arbitrary object associated with the period inserted. If a period
// already exists with the given key, an error will be returned.
func (pc *PeriodCollection) Insert(key interface{}, period Period, contents interface{}) error {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	if _, ok := pc.nodes[key]; ok {
		return fmt.Errorf("period with key %v already exists", key)
	}
	pc.insert(key, period, contents)
	return nil
}

// insert is the internal function that adds a new red node to the tree. Note this function does not lock the mutex,
// that must be done by the caller.
func (pc *PeriodCollection) insert(key interface{}, period Period, contents interface{}) {
	inserted := newNode(period, key, contents, red)
	pc.nodes[key] = inserted
	if pc.root == nil || pc.root.leaf {
		inserted.color = black
		pc.root = inserted
	} else {
		pc.insertRecursive(pc.root, inserted)
		pc.insertRepair(inserted)
	}
}

// insertRecursive recursively adds new red node containing a period and ID into the tree. The inserted node is stored in the
// the inserted parameter.
func (pc *PeriodCollection) insertRecursive(root, inserted *node) {
	// augment the tree with the maximum end time of its subtree
	if inserted.period.End.IsZero() {
		root.maxEnd = inserted.period.End
	} else if !root.maxEnd.IsZero() {
		root.maxEnd = MaxTime(root.maxEnd, inserted.period.End)
	}

	if root.periodToLeft(inserted.period) {
		if root.left.leaf {
			inserted.parent = root
			root.left = inserted
			return
		}
		pc.insertRecursive(root.left, inserted)

	} else {
		if root.right.leaf {
			inserted.parent = root
			root.right = inserted
			return
		}
		pc.insertRecursive(root.right, inserted)
	}
}

// insertRepair rebalances the tree to maintain the red-black property after an insertion
func (pc *PeriodCollection) insertRepair(n *node) {
	if n == pc.root {
		// n is the actual root of the tree, by definition pc is always black
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
		// the parent is red; if pc has a red sibling, change parent & uncle to black and change grandparent to red
		uncle.color = black
		n.parent.color = black
		if n.parent.parent != nil {
			n.parent.parent.color = red
			pc.insertRepair(n.parent.parent)
			return
		}
	}

	if uncleColor == black && n.parent.color == red {
		// move n so that pc is on the same side of its parent as its parent is to its grandparent (i.e. pc is on the
		// outside of the subtree)
		isInsideRight := n.parent.isLeftChild() && !n.isLeftChild()
		isInsideLeft := !n.parent.isLeftChild() && n.isLeftChild()
		if isInsideRight {
			pc.rotate(n.parent, left)
			n = n.left
		} else if isInsideLeft {
			pc.rotate(n.parent, right)
			n = n.right
		}

		// rotate again to move n into the grandparent's spot
		n.parent.color = black
		if n.parent.parent != nil {
			n.parent.parent.color = red
			if n.isLeftChild() {
				pc.rotate(n.parent.parent, right)
			} else {
				pc.rotate(n.parent.parent, left)
			}
		}
	}
}

// rotate rotates a node in the tree about node n either left or right.
func (pc *PeriodCollection) rotate(n *node, direction rotationDirection) {
	// y is the node that is going to take the place of n in the tree
	var y *node
	switch direction {
	case right:
		y = n.left
	case left:
		y = n.right
	}

	// move y into n's position
	if n == pc.root {
		pc.root = y
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
	n.maxEnd = n.maxEndOfSubtree()
	if !y.leaf {
		y.maxEnd = y.maxEndOfSubtree()
	}
}

// Delete removes the period and its associated contents with the provided key. If no period with the provided
// key exists, this function is a no-op.
func (pc *PeriodCollection) Delete(key interface{}) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	pc.delete(key)
}

// delete is the internal method that determines if a deletion is necessary, and if so, executes the deletion
func (pc *PeriodCollection) delete(key interface{}) {
	node, ok := pc.nodes[key]
	if !ok {
		return
	}
	pc.deleteNode(node)
}

// deleteNode removes the specified node from the tree
func (pc *PeriodCollection) deleteNode(n *node) {
	// y is the node that is going to be deleted, z is the node that will be moved into y's place
	var y *node
	var z *node

	delete(pc.nodes, n.key)
	if n.left.leaf || n.right.leaf {
		// n has 0 or 1 children so pc can be deleted
		y = n
	} else {
		// n is an internal node, delete its successor and swap the contents of its successor into n
		y = n.successor()
		pc.nodes[y.key] = n
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
		pc.root = z
	}
	n.period, n.key, n.contents = y.period, y.key, y.contents

	// update maxEnd all the way up the tree
	parent := z.parent
	for parent != nil {
		parent.maxEnd = parent.maxEndOfSubtree()
		parent = parent.parent
	}
	n.maxEnd = n.maxEndOfSubtree()

	if y.color == black {
		pc.deleteRepair(z)
	}
}

// deleteRepair rebalances the tree to maintain the red-black property after a deletion
func (pc *PeriodCollection) deleteRepair(n *node) {
	if n == pc.root || n.color == red {
		n.color = black
		return
	}
	pc.deleteRepairCase1(n)
	if pc.deleteRepairCase2(n) {
		pc.deleteRepair(n.parent)
		return
	}
	pc.deleteRepairCase3(n)
	pc.deleteRepairCase4(n)
}

// deleteRepairCase1 handles the case of the deleted node's sibling being red. changes the parent's color to red and
// the sibling's color to black and rotates to make the sibling the parent.
func (pc *PeriodCollection) deleteRepairCase1(n *node) {
	sibling := n.sibling()
	if sibling.nodeColor() == red {
		sibling.color = black
		n.parent.color = red
		if n.isLeftChild() {
			pc.rotate(n.parent, left)
		} else {
			pc.rotate(n.parent, right)
		}
	}
}

// deleteRepairCase2 handles the case of the deleted node's sibling being a leaf or the sibling and its children
// colored black. It handles this case by changing the sibling to red and returns whether the parent needs
// to be repaired.
func (pc *PeriodCollection) deleteRepairCase2(n *node) bool {
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
func (pc *PeriodCollection) deleteRepairCase3(n *node) {
	sibling := n.sibling()
	if !sibling.leaf && sibling.nodeColor() == black {
		if sibling.right.nodeColor() == red && !n.isLeftChild() {
			sibling.color = red
			sibling.right.color = black
			pc.rotate(sibling, left)
		} else if sibling.left.nodeColor() == red && n.isLeftChild() {
			sibling.color = red
			sibling.left.color = black
			pc.rotate(sibling, right)
		}
	}
}

// deleteRepairCase4 handles the case of the sibling of n colored black with a red child on the right if the deleted
// node is on the left, or the sibling having a red child on the left if the deleted node is on the right. It recolors
// the appropriate child of the sibling, makes the sibling the same color as the parent, makes the parent black, and
// rotates to move the sibling up.
func (pc *PeriodCollection) deleteRepairCase4(n *node) {
	sibling := n.sibling()
	if !sibling.leaf && sibling.nodeColor() == black {
		if sibling.left.nodeColor() == red && !n.isLeftChild() {
			sibling.left.color = black
			sibling.color = n.parent.color
			n.parent.color = black
			pc.rotate(n.parent, right)
		} else if sibling.right.nodeColor() == red && n.isLeftChild() {
			sibling.right.color = black
			sibling.color = n.parent.color
			n.parent.color = black
			pc.rotate(n.parent, left)
		}
	}
}

// Update the period and associated contents with the given key. If no period with the given key exists,
// a new period is inserted.
func (pc *PeriodCollection) Update(key interface{}, newPeriod Period, newContents interface{}) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	pc.update(key, newPeriod, newContents)
}

// update is the internal function that performs the update on the tree.
func (pc *PeriodCollection) update(key interface{}, newPeriod Period, newContents interface{}) {
	oldNode, ok := pc.nodes[key]
	if !ok {
		pc.insert(key, newPeriod, newContents)
		return
	}
	if oldNode.period.Equals(newPeriod) {
		// if the period hasn't changed, just swap the contents
		oldNode.contents = newContents
		return
	}
	// if the period has changed, delete the old node and insert a new one
	pc.deleteNode(oldNode)
	pc.insert(key, newPeriod, newContents)
}

// AnyContainsTime returns whether there is any stored period that contains the supplied time.
func (pc *PeriodCollection) AnyContainsTime(time time.Time) bool {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()
	return pc.anyContainsTime(pc.root, time)
}

// anyContainsTime is the internal function that recursively searches the tree for the supplied time.
func (pc *PeriodCollection) anyContainsTime(root *node, time time.Time) bool {
	if root.leaf {
		return false
	}
	if root.period.ContainsTime(time, false) {
		return true
	}
	if !root.left.leaf && (root.left.maxEnd.After(time) || root.left.maxEnd.IsZero()) {
		return pc.anyContainsTime(root.left, time)
	}
	return pc.anyContainsTime(root.right, time)
}

// ContainsTime will find and return the contents of all objects in a periodic collection that contain the given time.
func (pc *PeriodCollection) ContainsTime(time time.Time) []interface{} {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()
	results := make([]interface{}, 0)
	if pc.root.leaf {
		return results
	}
	pc.containsTime(time, pc.root, &results)
	return results
}

// containsTime is the recursive step of ContainsTime that will determine which branches should be traversed in the
// periodic collection and append the contents of all nodes that contain the queried time.
// This method traverses the tree in-order, meaning that the results returned are sorted by start time ascending.
func (pc *PeriodCollection) containsTime(time time.Time, root *node, results *[]interface{}) {
	if !root.left.leaf && (root.left.maxEnd.After(time) || root.left.maxEnd.IsZero()) {
		pc.containsTime(time, root.left, results)
	}
	if root.period.ContainsTime(time, false) {
		*results = append(*results, root.contents)
	}
	// The current node (root) has the earliest start time of any node in the right subtree.
	// If the period root.period.Start to root.right.maxEnd does not contain the queried time, it is guaranteed
	// that there are no nodes in the right subtree that contain the time so the traversal can be skipped.
	if !root.right.leaf && NewPeriod(root.period.Start, root.right.maxEnd).ContainsTime(time, false) {
		pc.containsTime(time, root.right, results)
	}
}

// Intersecting returns the contents of all objects whose associated periods intersect the supplied query period.
// Period intersection is inclusive on the start time but exclusive on the end time. The results returned by
// Intersecting are sorted in ascending order by the associated period's start time.
func (pc *PeriodCollection) Intersecting(query Period) []interface{} {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()
	results := make([]interface{}, 0)
	if pc.root.leaf {
		return results
	}
	pc.intersecting(query, pc.root, &results)
	return results
}

// intersecting is the recursive step of Intersecting that will determine which branches should be traversed in the
// periodic collection and append the contents of all nodes that intersect the queried period.
// This method traverses the tree in-order, meaning that the results returned are sorted by start time ascending.
func (pc *PeriodCollection) intersecting(query Period, root *node, results *[]interface{}) {
	if !root.left.leaf && (root.left.maxEnd.After(query.Start) || root.left.maxEnd.IsZero()) {
		pc.intersecting(query, root.left, results)
	}
	if root.period.Intersects(query) {
		*results = append(*results, root.contents)
	}
	// The current node (root) has the earliest start time of any node in the right subtree.
	// If the period root.period.Start to root.right.maxEnd does not intersect the queried period, it is guaranteed
	// that there are no nodes in the right subtree that intersect the period so the traversal can be skipped.
	if !root.right.leaf && NewPeriod(root.period.Start, root.right.maxEnd).Intersects(query) {
		pc.intersecting(query, root.right, results)
	}
}

// AnyIntersecting returns whether or not there are any periods in the collection that intersect the query period.
// Compared to Intersecting, this method is more efficient because it will terminate early once an intersection is
// found.
func (pc *PeriodCollection) AnyIntersecting(query Period) bool {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()
	if pc.root.leaf {
		return false
	}
	return pc.anyIntersecting(query, pc.root)
}

// anyIntersecting is the internal function that recursively searches the tree and notifies the caller on the
// found channel whether or not it has found an intersection.
func (pc *PeriodCollection) anyIntersecting(query Period, root *node) bool {
	if root.period.Intersects(query) {
		return true
	}
	if !root.left.leaf && (root.left.maxEnd.After(query.Start) || root.left.maxEnd.IsZero()) {
		return pc.anyIntersecting(query, root.left)
	}
	if !root.right.leaf && (root.right.maxEnd.After(query.Start) || root.right.maxEnd.IsZero()) {
		return pc.anyIntersecting(query, root.right)
	}
	return false
}

// ContainsKey returns whether or not a period with a corresponding key exists.
func (pc *PeriodCollection) ContainsKey(key interface{}) bool {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()
	_, ok := pc.nodes[key]
	return ok
}

// DepthFirstTraverse traverses the period collection's backing tree depth-first and returns the contents of every
// node in the tree by the ordering given.
func (pc *PeriodCollection) DepthFirstTraverse(order TraversalOrder) []interface{} {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()
	nodeContents := make([]interface{}, 0, len(pc.nodes))
	pc.depthFirstTraverse(pc.root, order, &nodeContents)
	return nodeContents
}

// depthFirstTraverse is the internal recursive function for traversing the interval tree.
func (pc *PeriodCollection) depthFirstTraverse(n *node, order TraversalOrder, visitedContents *[]interface{}) {
	if n.leaf {
		return
	}
	if order == PreOrder {
		*visitedContents = append(*visitedContents, n.contents)
	}
	pc.depthFirstTraverse(n.left, order, visitedContents)
	if order == InOrder {
		*visitedContents = append(*visitedContents, n.contents)
	}
	pc.depthFirstTraverse(n.right, order, visitedContents)
	if order == PostOrder {
		*visitedContents = append(*visitedContents, n.contents)
	}
}

// ContentsOfKey returns the contents stored at the provided key in the collection. This method
// runs in O(1) time and can be used if the key is known but not the period.
func (pc *PeriodCollection) ContentsOfKey(key interface{}) (interface{}, error) {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()
	node, ok := pc.nodes[key]
	if !ok {
		return nil, fmt.Errorf("key %v does not exist", key)
	}
	return node.contents, nil
}

// DeleteOnCondition will delete all nodes in the collection with contents that satisfy the given condition
// Note that this method can be time consuming for large trees and multiple deletions as it may involve multiple
// tree rotations.
func (pc *PeriodCollection) DeleteOnCondition(condition func(contents interface{}) bool) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	for _, node := range pc.nodes {
		if condition(node.contents) {
			pc.deleteNode(node)
		}
	}
}

// PrepareUpdate returns an Update command that can be later be used for bulk actions on the collection
func (pc *PeriodCollection) PrepareUpdate(key interface{}, newPeriod Period, newContents interface{}) Update {
	return Update{
		key:         key,
		newContents: newContents,
		newPeriod:   newPeriod,
		pc:          pc,
	}
}

// PrepareDelete returns a Delete command that can be later used for bulk actions on the collection
func (pc *PeriodCollection) PrepareDelete(key interface{}) Delete {
	return Delete{key: key, pc: pc}
}

// Execute takes a list of commands and runs all of them on the PeriodCollection within the context
// of one write lock.
func (pc *PeriodCollection) Execute(commands ...Command) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	for _, command := range commands {
		command.execute()
	}
}

// execute the update on the tree
func (u Update) execute() {
	u.pc.update(u.key, u.newPeriod, u.newContents)
}

// execute the delete on the tree
func (d Delete) execute() {
	d.pc.delete(d.key)
}

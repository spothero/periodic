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
	"context"
	"fmt"
	"sync"
	"time"
)

// PeriodCollection is a data structure for storing time periods and arbitrary data objects associated with the
// period.Once populated, PeriodCollection allows callers to quickly identify subsets of the collection
// that intersect with another period or find periods that contain a given time.

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

// NewPeriodCollection constructs a new PeriodCollection
func NewPeriodCollection() *PeriodCollection {
	return &PeriodCollection{nodes: make(map[interface{}]*node), root: &node{leaf: true}}
}

// Insert adds a new period into the collection. The key parameter is a unique identifier that must be supplied
// when inserting a new period. contents is an arbitrary object associated with the period inserted. If a period
// already exists with the given key, an error will be returned.
func (pc *PeriodCollection) Insert(period Period, key, contents interface{}) error {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	if _, ok := pc.nodes[key]; ok {
		return fmt.Errorf("period with key %v already exists", key)
	}
	var inserted *node
	if pc.root == nil || pc.root.leaf {
		inserted = newNode(period, key, contents, black)
		inserted.maxEnd = period.End
		pc.root = inserted
	} else {
		pc.insert(period, key, contents, pc.root, &inserted)
		pc.insertRepair(inserted)
	}
	pc.nodes[inserted.key] = inserted
	return nil
}

// insert recursively adds new red node containing a period and ID into the tree. The inserted node is stored in the
// the inserted parameter.
func (pc *PeriodCollection) insert(period Period, key, contents interface{}, root *node, inserted **node) *node {
	if root.leaf {
		*inserted = newNode(period, key, contents, red)
		return *inserted
	}
	// augment the tree with the maximum end time of its subtree
	root.maxEnd = MaxTime(root.maxEnd, period.End)

	if root.periodToLeft(period) {
		root.left = pc.insert(period, key, contents, root.left, inserted)
		root.left.parent = root
	} else {
		root.right = pc.insert(period, key, contents, root.right, inserted)
		root.right.parent = root
	}
	return root
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
// key exists, an error is returned.
func (pc *PeriodCollection) Delete(key interface{}) error {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	node, ok := pc.nodes[key]
	if !ok {
		return fmt.Errorf("could not delete node with key %v: key does not exist", key)
	}
	pc.delete(node)
	delete(pc.nodes, key)
	return nil
}

func (pc *PeriodCollection) delete(n *node) {
	// y is the node that is going to be deleted, z is the node that will be moved into y's place
	var y *node
	var z *node

	if n.left.leaf || n.right.leaf {
		// n has 0 or 1 children so pc can be deleted
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
		pc.root = z
	}
	n.period, n.key, n.contents = y.period, y.key, y.contents

	if z.parent != nil {
		z.parent.maxEnd = z.parent.maxEndOfSubtree()
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

// Update the period and associated contents with the given key. An error is returned if no period with the given
// key exists.
func (pc *PeriodCollection) Update(key interface{}, newPeriod Period, newContents interface{}) error {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	oldNode, ok := pc.nodes[key]
	if !ok {
		return fmt.Errorf("could not update node with key %v: key does not exist", key)
	}
	if oldNode.period.Equals(newPeriod) {
		// if the period hasn't changed, just swap the contents
		oldNode.contents = newContents
		return nil
	}
	// if the period has changed, delete the old node and insert a new one
	pc.delete(oldNode)
	var replacement *node
	pc.insert(newPeriod, key, newContents, pc.root, &replacement)
	pc.insertRepair(replacement)
	pc.nodes[key] = replacement
	return nil
}

// ContainsTime returns whether there is any stored period that contains the supplied time.
func (pc *PeriodCollection) ContainsTime(time time.Time) bool {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()
	return pc.containsTime(pc.root, time)
}

// containsTime is the internal function that recursively searches the tree for the supplied time.
func (pc *PeriodCollection) containsTime(root *node, time time.Time) bool {
	if root.leaf {
		return false
	}
	if root.period.ContainsTime(time) {
		return true
	}
	if !root.left.leaf && root.left.maxEnd.After(time) {
		return pc.containsTime(root.left, time)
	}
	return pc.containsTime(root.right, time)
}

// Intersecting returns the contents of all objects whose associated periods intersect the supplied query period.
// Period intersection is inclusive on the start time but exclusive on the end time.
func (pc *PeriodCollection) Intersecting(query Period) []interface{} {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()

	if pc.root.leaf {
		return make([]interface{}, 0)
	}

	var wg sync.WaitGroup
	intersections := make(chan interface{})
	resultsChan := make(chan []interface{})
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		results := make([]interface{}, 0, len(pc.nodes))
		for {
			select {
			case intersection := <-intersections:
				results = append(results, intersection)
			case <-ctx.Done():
				resultsChan <- results
			}
		}
	}()

	wg.Add(1)
	go pc.intersecting(query, pc.root, intersections, &wg)
	wg.Wait()
	cancel()

	return <-resultsChan
}

// intersecting is the internal function that recursively searches the tree and adds all node contents to results
func (pc *PeriodCollection) intersecting(query Period, root *node, results chan interface{}, wg *sync.WaitGroup) {
	if root.period.Intersects(query) {
		results <- root.contents
	}
	if !root.left.leaf && root.left.maxEnd.After(query.Start) {
		wg.Add(1)
		go pc.intersecting(query, root.left, results, wg)
	}
	if !root.right.leaf && root.right.maxEnd.After(query.Start) && root.right.period.Start.Before(query.End) {
		wg.Add(1)
		go pc.intersecting(query, root.right, results, wg)
	}
	wg.Done()
}

// AnyIntersecting returns whether or not there are any periods in the collection that intersect the query period.
// Compared to Intersecting, this method is more efficient because it will terminate early once an intersection is
// found.
func (pc *PeriodCollection) AnyIntersecting(query Period) bool {
	pc.mutex.RLock()
	pc.mutex.RUnlock()
	if pc.root.leaf {
		return false
	}
	found := make(chan bool)
	go pc.anyIntersecting(query, pc.root, found)
	result := <-found
	return result
}

// anyIntersecting is the internal function that recursively searches the tree and notifies the caller on the
// found channel whether or not it has found an intersection.
func (pc *PeriodCollection) anyIntersecting(query Period, root *node, found chan bool) {
	if root.period.Intersects(query) {
		select {
		case found <- true:
			return
		default:
			// intersection found in another branch searched in parallel
			return
		}
	}
	searchLeft := !root.left.leaf && root.left.maxEnd.After(query.Start)
	searchRight := !root.right.leaf && root.right.maxEnd.After(query.Start) && root.right.period.Start.Before(query.End)
	if searchLeft || searchRight {
		if searchLeft {
			go pc.anyIntersecting(query, root.left, found)
		}
		if searchRight {
			go pc.anyIntersecting(query, root.right, found)
		}
		return
	}
	found <- false
}

// ContainsKey returns whether or not a period with a corresponding key exists.
func (pc *PeriodCollection) ContainsKey(key interface{}) bool {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()
	_, ok := pc.nodes[key]
	return ok
}

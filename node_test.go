package periodic

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNode_isLeftChild(t *testing.T) {
	root := &node{}
	left := &node{parent: root}
	right := &node{parent: root}
	root.left = left
	root.right = right
	tests := []struct {
		name     string
		testNode *node
		outcome  bool
	}{
		{
			"root is not the left child",
			root,
			false,
		}, {
			"left is the left child",
			left,
			true,
		}, {
			"right is not the left child",
			right,
			false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.outcome, test.testNode.isLeftChild())
		})
	}
}

func TestNode_sibling(t *testing.T) {
	root := &node{}
	left := &node{parent: root}
	right := &node{parent: root}
	root.left = left
	root.right = right
	tests := []struct {
		name     string
		testNode *node
		outcome  *node
	}{
		{
			"root has no siblings",
			root,
			nil,
		}, {
			"sibling of left is right",
			left,
			right,
		}, {
			"sibling of right is left",
			right,
			left,
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
		name  string
		setup func() *node
		color color
	}{
		{
			"black node is black",
			func() *node {
				return &node{color: black}
			},
			black,
		}, {
			"red node is red",
			func() *node {
				return &node{color: red}
			},
			red,
		}, {
			"leaf node is black",
			func() *node {
				return &node{leaf: true}
			},
			black,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.color, test.setup().nodeColor())
		})
	}
}

// Package go_bk_tree is a tree data structure (implemented in Golang) specialized to index data in a metric space.
// The BK-tree data structure was proposed by Burkhard and Keller in 1973 as a solution to the problem of
// searching a set of keys to find a key which is closest to a given query key. (Doc reference: http://signal-to-noise.xyz/post/bk-tree/)
package go_bk_tree

import (
	"runtime"
	"time"

	"github.com/pquerna/ffjson/ffjson"
)

type Distance int

// MetricTensor is an interface of data that needs to be indexed
//
// Example:
//  import l "github.com/texttheater/golang-levenshtein/levenshtein"
//
//  type Word string
//
//  func (w Word) DistanceFrom(w2 MetricTensor) Distance {
// 	 return Distance(l.DistanceForStrings([]rune(string(w)), []rune(string(w2.(Word))), l.DefaultOptions))
//  }
type MetricTensor interface {
	DistanceFrom(other MetricTensor) Distance
	ToString() string
}

type BkTreeNode struct {
	MetricTensor
	Children map[Distance]*BkTreeNode
}

func (node *BkTreeNode) MarshalJSON() ([]byte, error) {
	var array = make([]interface{}, 2)
	array[0] = node.MetricTensor.ToString()
	array[1] = node.Children
	return ffjson.Marshal(array)
}

func newbkTreeNode(v MetricTensor) *BkTreeNode {
	return &BkTreeNode{
		MetricTensor: v,
		Children:     make(map[Distance]*BkTreeNode),
	}
}

func (node *BkTreeNode) getSize() int {
	if len(node.Children) == 0 {
		return 1
	}
	count := 1
	for _, child := range node.Children {
		count += child.getSize()
	}
	return count
}

type BKTree struct {
	Size int
	Root *BkTreeNode
}

func (tree *BKTree) ToJson() ([]byte, error) {
	return ffjson.Marshal(tree.Root)
}

// Add a node to BK-Tree, the location of the new node
// depends on how distance between different tensors are defined
func (tree *BKTree) Add(val MetricTensor) {
	node := newbkTreeNode(val)
	if tree.Root == nil {
		tree.Root = node
		return
	}
	curNode := tree.Root
	for {
		dist := curNode.DistanceFrom(val)
		// If distance is zero which means two Metrics
		// are exactly the same, return directly
		if dist == 0 {
			break
		}
		target := curNode.Children[dist]
		if target == nil {
			curNode.Children[dist] = node
			tree.Size += 1
			break
		}
		curNode = target
	}
}

func (tree *BKTree) CalculateSize() {
	tree.Size = tree.Root.getSize()
}

func (tree *BKTree) Search(val MetricTensor, radius Distance) ([]MetricTensor, int) {
	count := 0
	candidates := make([]*BkTreeNode, 0, 10)
	candidates = append(candidates, tree.Root)
	results := make([]MetricTensor, 0, 5)
	for {
		cand := candidates[0]
		candidates = candidates[1:]
		dist := cand.DistanceFrom(val)
		count += 1
		if dist <= radius {
			results = append(results, cand.MetricTensor)
		}
		low, high := dist-radius, dist+radius
		for dist, child := range cand.Children {
			if dist >= low && dist <= high {
				candidates = append(candidates, child)
			}
		}
		if len(candidates) == 0 {
			break
		}
	}
	return results, count
}

var numCPU = runtime.NumCPU()

// Notice: this is an async implementation using goroutines for fun in order to see if async will out-perform the traditional
// implementation. Turns out it DID NOT.
func (tree *BKTree) SearchAsync(val MetricTensor, radius Distance) []MetricTensor {
	results := make([]MetricTensor, 0, 5)
	candsChan := make(chan *BkTreeNode, 100)
	candsChan <- tree.Root
LOOP:
	for {
		select {
		case cand := <-candsChan:
			go func() {
				dist := cand.DistanceFrom(val)
				if dist <= radius {
					results = append(results, cand.MetricTensor)
				}
				low, high := dist-radius, dist+radius
				for dist, child := range cand.Children {
					if dist >= low && dist <= high {
						candsChan <- child
					}
				}
			}()
		case <-time.After(time.Millisecond * 1):
			break LOOP
		}
	}
	return results
}

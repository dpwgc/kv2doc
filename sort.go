package kv2doc

import (
	"sort"
)

func Sort(rows []Doc, compare func(l, r Doc) bool) {
	sort.Sort(sortBase{rows, compare})
}

type sortBase struct {
	rows    []Doc
	compare func(l, r Doc) bool
}

func (s sortBase) Len() int           { return len(s.rows) }
func (s sortBase) Less(i, j int) bool { return s.compare(s.rows[i], s.rows[j]) }
func (s sortBase) Swap(i, j int)      { s.rows[i], s.rows[j] = s.rows[j], s.rows[i] }

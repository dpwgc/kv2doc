package kv2doc

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Query struct {
	db          *DB
	table       string
	expressions []string
	index       index
	limit       limit
	filter      func(doc Doc) bool
	parser      *Parser
	orderBy     func(l, r Doc) bool
}

type index struct {
	field string
	value string
}

type limit struct {
	enable bool
	cursor int
	size   int
}

// Limit 分页方法，逻辑和 MySQL 的 Limit 相同，limit 10 或 limit 0,10
func (c *Query) Limit(values ...int) *Query {
	cursor := 0
	size := 0
	if len(values) > 1 {
		cursor = values[0]
		size = values[1]
	} else if len(values) > 0 {
		size = values[0]
	} else {
		return c
	}
	if cursor < 0 || size < 0 {
		return c
	}
	c.limit.enable = true
	c.limit.cursor = cursor
	c.limit.size = size
	return c
}

const (
	eq = iota
	in
	leftLike
)

// Eq 等于
func (c *Query) Eq(field, value string) *Query {
	c.expressions = append(c.expressions, `(`+field+` == "`+value+`")`)
	c.selectIndex(eq, field, value)
	return c
}

// Ne 不等于
func (c *Query) Ne(field, value string) *Query {
	c.expressions = append(c.expressions, `(`+field+` != "`+value+`")`)
	return c
}

// Gt 大于
func (c *Query) Gt(field, value string) *Query {
	c.expressions = append(c.expressions, `(float(`+field+`) > `+value+`)`)
	return c
}

// Gte 大于或等于
func (c *Query) Gte(field, value string) *Query {
	c.expressions = append(c.expressions, `(float(`+field+`) >= `+value+`)`)
	return c
}

// Lt 小于
func (c *Query) Lt(field, value string) *Query {
	c.expressions = append(c.expressions, `(float(`+field+`) < `+value+`)`)
	return c
}

// Lte 小于或等于
func (c *Query) Lte(field, value string) *Query {
	c.expressions = append(c.expressions, `(float(`+field+`) <= `+value+`)`)
	return c
}

// In 包含
func (c *Query) In(field string, values ...string) *Query {
	var els []string
	for _, v := range values {
		els = append(els, `(`+field+` == "`+v+`")`)
	}
	c.expressions = append(c.expressions, `(`+strings.Join(els, ` || `)+`)`)
	c.selectIndex(in, field, values...)
	return c
}

// NotIn 不包含
func (c *Query) NotIn(field string, values ...string) *Query {
	var els []string
	for _, v := range values {
		els = append(els, `(`+field+` != "`+v+`")`)
	}
	c.expressions = append(c.expressions, `(`+strings.Join(els, ` && `)+`)`)
	return c
}

// Like 模糊匹配
func (c *Query) Like(field, value string) *Query {
	c.expressions = append(c.expressions, `(indexOf(`+field+`, "`+value+`") >= 0)`)
	return c
}

// LeftLike 模糊匹配-具有相同的前缀
// 此方法会走字段索引
func (c *Query) LeftLike(field, value string) *Query {
	c.expressions = append(c.expressions, `(hasPrefix(`+field+`, "`+value+`") == true)`)
	c.selectIndex(leftLike, field, value)
	return c
}

// RightLike 模糊匹配-具有相同的后缀
func (c *Query) RightLike(field, value string) *Query {
	c.expressions = append(c.expressions, `(hasSuffix(`+field+`, "`+value+`") == true)`)
	return c
}

// Exist 存在该字段
func (c *Query) Exist(field string) *Query {
	c.expressions = append(c.expressions, `(`+field+` != nil && len(`+field+`) > 0)`)
	return c
}

// NotExist 不存在该字段
// TODO
/*
func (c *Query) NotExist(field string) *Query {
	c.expressions = append(c.expressions, `(`+field+` == nil || len(`+field+`) <= 0)`)
	return c
}
*/

// Must 交集拼接
func (c *Query) Must(sc *SubQuery) *Query {
	c.expressions = append(c.expressions, `(`+strings.Join(sc.expressions, " && ")+`)`)
	return c
}

// Should 并集拼接
func (c *Query) Should(sc *SubQuery) *Query {
	c.expressions = append(c.expressions, `(`+strings.Join(sc.expressions, " || ")+`)`)
	return c
}

type SortRule int

const (
	Asc SortRule = iota
	Desc
)

func (c *Query) Asc(fields ...string) *Query {
	return c.Sort(Asc, fields...)
}

func (c *Query) Desc(fields ...string) *Query {
	return c.Sort(Desc, fields...)
}

func (c *Query) Sort(rule SortRule, fields ...string) *Query {
	c.orderBy = func(l, r Doc) bool {
		for _, v := range fields {
			if l[v] == r[v] {
				continue
			}
			ld, le := toDouble(l[v])
			rd, re := toDouble(r[v])
			if le == nil && re == nil {
				if rule == Asc {
					if ld < rd {
						return true
					}
				}
				if rule == Desc {
					if ld > rd {
						return true
					}
				}
			} else {
				if rule == Asc {
					if l[v] < r[v] {
						return true
					}
				}
				if rule == Desc {
					if l[v] > r[v] {
						return true
					}
				}
			}
		}
		return false
	}
	return c
}

func toDouble(s string) (float64, error) {
	if len(s) <= 0 {
		return 0, errors.New("s is empty")
	}
	if !strings.Contains(s, ".") {
		s = s + ".0"
	}
	return strconv.ParseFloat(s, 64)
}

// 传入一个判断方法，入参是文档内容，返回值是bool
// return true 则表明该文档符合查询条件，会将该文档加入到返回结果里
func (c *Query) setFilter() *Query {
	expr := strings.Join(c.expressions, " && ")
	fmt.Println("expr", expr)
	fmt.Println("idx", c.index.field, c.index.value)
	c.filter = func(doc Doc) bool {
		match, _ := c.parser.Match(expr, doc)
		return match
	}
	return c
}

// One 查询单个文档
func (c *Query) One() (doc Doc, err error) {
	cc := *c
	cc.limit.enable = true
	cc.limit.size = 1
	_, list, err := query(cc, false)
	if err != nil {
		return nil, err
	}
	if len(list) <= 0 {
		return nil, nil
	}
	return list[0], nil
}

// List 查询多个文档
func (c *Query) List() (docs []Doc, err error) {
	cc := *c
	_, list, err := query(cc, false)
	if err != nil {
		return nil, err
	}
	return list, nil
}

// Count 文档数量统计
func (c *Query) Count() (int64, error) {
	cc := *c
	cc.limit.enable = false
	count, _, err := query(cc, true)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// Scroll 滚动查询
func (c *Query) Scroll(fn func(doc Doc) bool) error {
	cc := *c
	return scan(cc, fn)
}

func (c *Query) selectIndex(operator uint8, field string, values ...string) {
	if len(field) <= 0 || len(values) <= 0 {
		return
	}
	var vs []string
	for _, v := range values {
		if len(v) > 0 {
			vs = append(vs, v)
		}
	}
	if len(vs) <= 0 {
		return
	}
	// 如果当前没有命中的索引值
	if len(c.index.field) <= 0 {
		// 如果是等于或者左like查询，走索引
		if operator == eq || operator == leftLike {
			c.index.field = field
			c.index.value = vs[0]
		} else if operator == in {
			// 如果是in查询，并且有共同前缀的话，走索引
			prefix := getCommonPrefix(vs)
			if len(prefix) > 0 {
				c.index.field = field
				c.index.value = prefix
			}
		}
	}
}

func getCommonPrefix(ss []string) (prefix string) {
	if len(ss) == 0 {
		return ""
	}
	for i := 0; i < len(ss[0]); i++ {
		firstStringChar := string(ss[0][i])
		match := true
		for j := 1; j < len(ss); j++ {
			if (len(ss[j]) - 1) < i {
				match = false
				break
			}
			if string(ss[j][i]) != firstStringChar {
				match = false
				break
			}
		}
		if match {
			prefix += firstStringChar
		} else {
			break
		}
	}
	return prefix
}

type SubQuery struct {
	expressions []string
}

func Expr() *SubQuery {
	return &SubQuery{}
}

// Eq 等于
func (c *SubQuery) Eq(field, value string) *SubQuery {
	c.expressions = append(c.expressions, `(`+field+` == "`+value+`")`)
	return c
}

// Ne 不等于
func (c *SubQuery) Ne(field, value string) *SubQuery {
	c.expressions = append(c.expressions, `(`+field+` != "`+value+`")`)
	return c
}

// Gt 大于
func (c *SubQuery) Gt(field, value string) *SubQuery {
	c.expressions = append(c.expressions, `(float(`+field+`) > `+value+`)`)
	return c
}

// Gte 大于或等于
func (c *SubQuery) Gte(field, value string) *SubQuery {
	c.expressions = append(c.expressions, `(float(`+field+`) >= `+value+`)`)
	return c
}

// Lt 小于
func (c *SubQuery) Lt(field, value string) *SubQuery {
	c.expressions = append(c.expressions, `(float(`+field+`) < `+value+`)`)
	return c
}

// Lte 小于或等于
func (c *SubQuery) Lte(field, value string) *SubQuery {
	c.expressions = append(c.expressions, `(float(`+field+`) <= `+value+`)`)
	return c
}

// In 包含
func (c *SubQuery) In(field string, values ...string) *SubQuery {
	var els []string
	for _, v := range values {
		els = append(els, `(`+field+` == "`+v+`")`)
	}
	c.expressions = append(c.expressions, `(`+strings.Join(els, ` || `)+`)`)
	return c
}

// NotIn 不包含
func (c *SubQuery) NotIn(field string, values ...string) *SubQuery {
	var els []string
	for _, v := range values {
		els = append(els, `(`+field+` != "`+v+`")`)
	}
	c.expressions = append(c.expressions, `(`+strings.Join(els, ` && `)+`)`)
	return c
}

// Like 模糊匹配
func (c *SubQuery) Like(field, value string) *SubQuery {
	c.expressions = append(c.expressions, `(indexOf(`+field+`, "`+value+`") >= 0)`)
	return c
}

// LeftLike 模糊匹配-具有相同的前缀
// 此方法会走字段索引
func (c *SubQuery) LeftLike(field, value string) *SubQuery {
	c.expressions = append(c.expressions, `(hasPrefix(`+field+`, "`+value+`") == true)`)
	return c
}

// RightLike 模糊匹配-具有相同的后缀
func (c *SubQuery) RightLike(field, value string) *SubQuery {
	c.expressions = append(c.expressions, `(hasSuffix(`+field+`, "`+value+`") == true)`)
	return c
}

// Exist 存在该字段
func (c *SubQuery) Exist(field string) *SubQuery {
	c.expressions = append(c.expressions, `(`+field+` != nil && len(`+field+`) > 0)`)
	return c
}

// NotExist 不存在该字段
// TODO
/*
func (c *SubQuery) NotExist(field string) *SubQuery {
	c.expressions = append(c.expressions, `(`+field+` == nil || len(`+field+`) <= 0)`)
	return c
}
*/

// Must 交集拼接
func (c *SubQuery) Must(sc *SubQuery) *SubQuery {
	c.expressions = append(c.expressions, `(`+strings.Join(sc.expressions, ` && `)+`)`)
	return c
}

// Should 并集拼接
func (c *SubQuery) Should(sc *SubQuery) *SubQuery {
	c.expressions = append(c.expressions, `(`+strings.Join(sc.expressions, ` || `)+`)`)
	return c
}

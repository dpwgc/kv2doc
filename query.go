package kv2doc

import (
	"strconv"
	"strings"
)

const (
	eq = iota
	in
	leftLike
)

type Query struct {
	db          *DB
	table       string
	expressions []string
	index       Index
	limit       limit
	parser      *Parser
	sort        func(l, r Doc) bool
	isChild     bool
}

type Index struct {
	field string
	value string
}

type Explain struct {
	Expr  string
	Index Index
}

type limit struct {
	enable bool
	cursor int
	size   int
}

func Expr() *Query {
	return &Query{
		isChild: true,
	}
}

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
	if field != primaryKey && field != createdAt && field != updatedAt {
		c.expressions = append(c.expressions, `(indexOf(_fields, "/`+field+`") >= 0)`)
	}
	return c
}

// NotExist 不存在该字段
func (c *Query) NotExist(field string) *Query {
	if field != primaryKey && field != createdAt && field != updatedAt {
		c.expressions = append(c.expressions, `(indexOf(_fields, "/`+field+`") < 0)`)
	} else {
		c.expressions = append(c.expressions, `(false)`)
	}
	return c
}

// Must 交集拼接
func (c *Query) Must(sc *Query) *Query {
	c.expressions = append(c.expressions, `(`+strings.Join(sc.expressions, " && ")+`)`)
	return c
}

// Should 并集拼接
func (c *Query) Should(sc *Query) *Query {
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
	if c.isChild {
		return c
	}
	c.sort = func(l, r Doc) bool {
		for _, v := range fields {
			if l[v] == r[v] {
				continue
			}
			ld, lb := toDouble(l[v])
			rd, rb := toDouble(r[v])
			if lb && rb {
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

// Limit 分页方法，逻辑和 MySQL 的 Limit 相同，limit 10 或 limit 0,10
func (c *Query) Limit(values ...int) *Query {
	if c.isChild {
		return c
	}
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

// One 查询单个文档
func (c *Query) One() (doc Doc, err error) {
	if c.isChild {
		return Doc{}, nil
	}
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

// List 返回多个文档
func (c *Query) List() (docs []Doc, err error) {
	if c.isChild {
		return nil, nil
	}
	cc := *c
	_, docs, err = query(cc, false)
	return docs, err
}

// Count 返回文档数量
func (c *Query) Count() (count int64, err error) {
	if c.isChild {
		return 0, nil
	}
	cc := *c
	cc.limit.enable = false
	count, _, err = query(cc, true)
	return count, err
}

// Scroll 滚动查询
func (c *Query) Scroll(fn func(doc Doc) bool) error {
	if c.isChild {
		return nil
	}
	cc := *c
	return scan(cc, fn)
}

// Explain 执行计划
func (c *Query) Explain() Explain {
	return Explain{
		Expr:  strings.Join(c.expressions, " && "),
		Index: c.index,
	}
}

func (c *Query) selectIndex(operator uint8, field string, values ...string) {
	if c.isChild || len(field) <= 0 || len(values) <= 0 {
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

func toDouble(s string) (float64, bool) {
	if len(s) <= 0 {
		return 0, false
	}
	if !strings.Contains(s, ".") {
		s = s + ".0"
	}
	fl, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return fl, true
}

package kv2doc

import (
	"fmt"
	"strings"
)

type Query struct {
	db         *DB
	table      string
	conditions []condition
	hit        hit
	limit      limit
	filters    []func(doc Doc) bool
	parser     *Parser
}

type hit struct {
	field string
	value string
}

func (c *hit) HasField() bool {
	return len(c.field) > 0
}

func (c *hit) HasValue() bool {
	return len(c.value) > 0
}

type limit struct {
	enable bool
	cursor int
	size   int
}

type condition struct {
	Field    string
	Operator uint8
	Values   []string
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

const eq = 1
const ne = 2
const in = 3
const notIn = 4
const gt = 5
const gte = 6
const lt = 7
const lte = 8
const like = 9
const leftLike = 10
const rightLike = 11
const exist = 12
const notExist = 13

// Eq 等于
func (c *Query) Eq(field, value string) *Query {
	c.add(eq, field, value)
	return c
}

// Ne 不等于
func (c *Query) Ne(field, value string) *Query {
	c.add(ne, field, value)
	return c
}

// Gt 大于
func (c *Query) Gt(field, value string) *Query {
	c.add(gt, field, value)
	return c
}

// Gte 大于或等于
func (c *Query) Gte(field, value string) *Query {
	c.add(gte, field, value)
	return c
}

// Lt 小于
func (c *Query) Lt(field, value string) *Query {
	c.add(lt, field, value)
	return c
}

// Lte 小于或等于
func (c *Query) Lte(field, value string) *Query {
	c.add(lte, field, value)
	return c
}

// In 包含
func (c *Query) In(field string, values ...string) *Query {
	c.add(in, field, values...)
	return c
}

// NotIn 不包含
func (c *Query) NotIn(field string, values ...string) *Query {
	c.add(notIn, field, values...)
	return c
}

// Like 模糊匹配
func (c *Query) Like(field, value string) *Query {
	c.add(like, field, value)
	return c
}

// LeftLike 模糊匹配-具有相同的前缀
// 此方法会走字段索引
func (c *Query) LeftLike(field, value string) *Query {
	c.add(leftLike, field, value)
	return c
}

// RightLike 模糊匹配-具有相同的后缀
func (c *Query) RightLike(field, value string) *Query {
	c.add(rightLike, field, value)
	return c
}

// Exist 存在该字段
func (c *Query) Exist(field, value string) *Query {
	c.add(exist, field, value)
	return c
}

// NotExist 不存在该字段
func (c *Query) NotExist(field, value string) *Query {
	c.add(notExist, field, value)
	return c
}

// And 交集拼接
func (c *Query) And(sc *SubQuery) *Query {
	return c.addFilter(strings.Join(sc.expr, " && "))
}

// Or 并集拼接
func (c *Query) Or(sc *SubQuery) *Query {
	return c.addFilter(strings.Join(sc.expr, " || "))
}

// 传入一个判断方法，入参是文档内容，返回值是bool
// return true 则表明该文档符合查询条件，会将该文档加入到返回结果里
func (c *Query) addFilter(expr string) *Query {
	fmt.Println("expr", expr)
	c.filters = append(c.filters, func(doc Doc) bool {
		match, _ := c.parser.Match(expr, doc)
		return match
	})
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

func (c *Query) add(operator uint8, field string, values ...string) {
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
	c.conditions = append(c.conditions, condition{
		Field:    field,
		Operator: operator,
		Values:   vs,
	})
	// 如果当前没有命中的索引值
	if !c.hit.HasValue() {
		// 如果是等于或者左like查询，走索引
		if operator == eq || operator == leftLike {
			c.hit.field = field
			c.hit.value = vs[0]
		} else if operator == in {
			// 如果是in查询，并且有共同前缀的话，走索引
			prefix := getCommonPrefix(vs)
			if len(prefix) > 0 {
				c.hit.field = field
				c.hit.value = prefix
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
	expr []string
}

func Sub() *SubQuery {
	return &SubQuery{}
}

// Eq 等于
func (c *SubQuery) Eq(field, value string) *SubQuery {
	c.expr = append(c.expr, `(`+field+` == "`+value+`")`)
	return c
}

// Ne 不等于
func (c *SubQuery) Ne(field, value string) *SubQuery {
	c.expr = append(c.expr, `(`+field+` != "`+value+`")`)
	return c
}

// Gt 大于
func (c *SubQuery) Gt(field, value string) *SubQuery {
	c.expr = append(c.expr, `(float(`+field+`) > `+value+`)`)
	return c
}

// Gte 大于或等于
func (c *SubQuery) Gte(field, value string) *SubQuery {
	c.expr = append(c.expr, `(float(`+field+`) >= `+value+`)`)
	return c
}

// Lt 小于
func (c *SubQuery) Lt(field, value string) *SubQuery {
	c.expr = append(c.expr, `(float(`+field+`) < `+value+`)`)
	return c
}

// Lte 小于或等于
func (c *SubQuery) Lte(field, value string) *SubQuery {
	c.expr = append(c.expr, `(float(`+field+`) <= `+value+`)`)
	return c
}

// In 包含
func (c *SubQuery) In(field string, values ...string) *SubQuery {
	var els []string
	for _, v := range values {
		els = append(els, `(`+field+` == "`+v+`")`)
	}
	c.expr = append(c.expr, `(`+strings.Join(els, ` || `)+`)`)
	return c
}

// NotIn 不包含
func (c *SubQuery) NotIn(field string, values ...string) *SubQuery {
	var els []string
	for _, v := range values {
		els = append(els, `(`+field+` != "`+v+`")`)
	}
	c.expr = append(c.expr, `(`+strings.Join(els, ` && `)+`)`)
	return c
}

// Like 模糊匹配
func (c *SubQuery) Like(field, value string) *SubQuery {
	c.expr = append(c.expr, `(indexOf(`+field+`, "`+value+`") >= 0)`)
	return c
}

// LeftLike 模糊匹配-具有相同的前缀
// 此方法会走字段索引
func (c *SubQuery) LeftLike(field, value string) *SubQuery {
	c.expr = append(c.expr, `(hasPrefix(`+field+`, "`+value+`") == true)`)
	return c
}

// RightLike 模糊匹配-具有相同的后缀
func (c *SubQuery) RightLike(field, value string) *SubQuery {
	c.expr = append(c.expr, `(hasSuffix(`+field+`, "`+value+`") == true)`)
	return c
}

// Exist 存在该字段
func (c *SubQuery) Exist(field string) *SubQuery {
	c.expr = append(c.expr, `(len(`+field+`) > 0)`)
	return c
}

// NotExist 不存在该字段
func (c *SubQuery) NotExist(field string) *SubQuery {
	c.expr = append(c.expr, `(len(`+field+`) <= 0)`)
	return c
}

// And 交集拼接
func (c *SubQuery) And(sc *SubQuery) *SubQuery {
	c.expr = append(c.expr, `(`+strings.Join(sc.expr, ` && `)+`)`)
	return c
}

// Or 并集拼接
func (c *SubQuery) Or(sc *SubQuery) *SubQuery {
	c.expr = append(c.expr, `(`+strings.Join(sc.expr, ` || `)+`)`)
	return c
}

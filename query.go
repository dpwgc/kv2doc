package kv2doc

type Query struct {
	conditions []condition
	hit        hit
	limit      limit
	customize  func(doc Doc) bool
}

func Filter() *Query {
	return &Query{}
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
	Left     string
	Operator uint8
	Right    []string
}

// Limit 分页方法，逻辑和 MySQL 的 Limit 相同
func (c *Query) Limit(cursor, size int) *Query {
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

// Customize 自定义过滤逻辑，可以用这个方法来写一些复杂的 or/and 嵌套逻辑
// 传入一个判断方法，入参是文档内容，返回值是bool
// return true 则表明该文档符合查询条件，会将该文档加入到返回结果里
func (c *Query) Customize(logic func(doc Doc) bool) *Query {
	c.customize = logic
	return c
}

// Hit 自定义命中哪个字段索引
// 程序默认会取第一个符合条件（Eq、LeftLike、In）的查询字段走索引
// 可以通过此方法指定走哪个字段的索引
func (c *Query) Hit(field, value string) *Query {
	c.hit.field = field
	c.hit.value = value
	return c
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
		Left:     field,
		Operator: operator,
		Right:    vs,
	})
	// 如果当前没有命中的索引值
	if !c.hit.HasValue() {
		// 如果是等于或者左like查询，走索引
		if operator == eq || operator == leftLike {
			c.Hit(field, vs[0])
		} else if operator == in {
			// 如果是in查询，并且有共同前缀的话，走索引
			pre := getCommonPrefix(vs)
			if len(pre) > 0 {
				c.Hit(field, pre)
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

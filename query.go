package kv2doc

type Query struct {
	conditions []condition
	hit        hit
	limit      limit
}

func Filter() *Query {
	return &Query{}
}

type hit struct {
	field string
	value string
}

func (c *hit) IsExist() bool {
	return len(c.field) > 0 && len(c.value) > 0
}

type limit struct {
	enable bool
	cursor int
	size   int
}

type condition struct {
	Left   string
	Middle uint8
	Right  []string
}

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

func (c *Query) Eq(field, value string) *Query {
	c.add(eq, field, value)
	return c
}

func (c *Query) Ne(field, value string) *Query {
	c.add(ne, field, value)
	return c
}

func (c *Query) Gt(field, value string) *Query {
	c.add(gt, field, value)
	return c
}

func (c *Query) Gte(field, value string) *Query {
	c.add(gte, field, value)
	return c
}

func (c *Query) Lt(field, value string) *Query {
	c.add(lt, field, value)
	return c
}

func (c *Query) Lte(field, value string) *Query {
	c.add(lte, field, value)
	return c
}

func (c *Query) In(field string, values ...string) *Query {
	c.add(in, field, values...)
	return c
}

func (c *Query) NotIn(field string, values ...string) *Query {
	c.add(notIn, field, values...)
	return c
}

func (c *Query) Like(field, value string) *Query {
	c.add(like, field, value)
	return c
}

func (c *Query) LeftLike(field, value string) *Query {
	c.add(leftLike, field, value)
	return c
}

func (c *Query) RightLike(field, value string) *Query {
	c.add(rightLike, field, value)
	return c
}

func (c *Query) add(middle uint8, field string, values ...string) {
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
		Left:   field,
		Middle: middle,
		Right:  vs,
	})
	if middle == eq || middle == leftLike {
		if !c.hit.IsExist() {
			c.hit.field = field
			c.hit.value = vs[0]
		}
	}
}

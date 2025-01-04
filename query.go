package kv2doc

type Query struct {
	expressions []expression
	hit         hit
	limit       limit
}

func NewQuery() *Query {
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

type expression struct {
	Left   string
	Middle uint8
	Right  string
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
const gt = 2
const gte = 3
const lt = 4
const lte = 5
const like = 6
const leftLike = 7
const rightLike = 8

func (c *Query) Eq(field, value string) *Query {
	c.add(eq, field, value)
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

func (c *Query) add(middle uint8, field, value string) {
	if len(field) <= 0 || len(value) <= 0 {
		return
	}
	c.expressions = append(c.expressions, expression{
		Left:   field,
		Middle: middle,
		Right:  value,
	})
	if middle == eq || middle == leftLike {
		if !c.hit.IsExist() {
			c.hit.field = field
			c.hit.value = value
		}
	}
}

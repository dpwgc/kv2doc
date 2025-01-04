package kv2doc

type Query struct {
	limit       limit
	expressions []expression
	hit         hit
}

func NewQuery() *Query {
	return &Query{}
}

type hit struct {
	field string
	value string
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
const like = 2
const leftLike = 3
const rightLike = 4

func (c *Query) Eq(field, value string) *Query {
	if len(field) <= 0 || len(value) <= 0 {
		return c
	}
	c.expressions = append(c.expressions, expression{
		Left:   field,
		Middle: eq,
		Right:  value,
	})
	if len(c.hit.field) <= 0 {
		c.hit.field = field
		c.hit.value = value
	}
	return c
}

func (c *Query) Like(field, value string) *Query {
	if len(field) <= 0 || len(value) <= 0 || field == primaryKey {
		return c
	}
	c.expressions = append(c.expressions, expression{
		Left:   field,
		Middle: like,
		Right:  value,
	})
	return c
}

func (c *Query) LeftLike(field, value string) *Query {
	if len(field) <= 0 || len(value) <= 0 || field == primaryKey {
		return c
	}
	c.expressions = append(c.expressions, expression{
		Left:   field,
		Middle: leftLike,
		Right:  value,
	})
	return c
}

func (c *Query) RightLike(field, value string) *Query {
	if len(field) <= 0 || len(value) <= 0 || field == primaryKey {
		return c
	}
	c.expressions = append(c.expressions, expression{
		Left:   field,
		Middle: rightLike,
		Right:  value,
	})
	return c
}

package kv2doc

import (
	"github.com/expr-lang/expr"
)

type Parser struct {
}

func NewParser() *Parser {
	return &Parser{}
}

func (c *Parser) Match(code string, doc Doc) (bool, error) {
	program, err := expr.Compile(code, expr.Env(doc))
	if err != nil {
		// fmt.Println(err)
		return false, err
	}
	output, err := expr.Run(program, doc)
	if err != nil {
		// fmt.Println(err)
		return false, err
	}
	// fmt.Println(output)
	return output.(bool), err
}

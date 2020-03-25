package aggregation

import (
	"github.com/pingcap/check"
	"github.com/cookieY/parser/ast"
	"github.com/cookieY/parser/mysql"
	"github.com/cookieY/tidb/expression"
	"github.com/cookieY/tidb/sessionctx"
	"github.com/cookieY/tidb/types"
	"github.com/cookieY/tidb/util/mock"
)

var _ = check.Suite(&testBaseFuncSuite{})

type testBaseFuncSuite struct {
	ctx sessionctx.Context
}

func (s *testBaseFuncSuite) SetUpSuite(c *check.C) {
	s.ctx = mock.NewContext()
}

func (s *testBaseFuncSuite) TestClone(c *check.C) {
	col := &expression.Column{
		UniqueID: 0,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	desc, err := newBaseFuncDesc(s.ctx, ast.AggFuncFirstRow, []expression.Expression{col})
	c.Assert(err, check.IsNil)
	cloned := desc.clone()
	c.Assert(desc.equal(s.ctx, cloned), check.IsTrue)

	col1 := &expression.Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeVarchar),
	}
	cloned.Args[0] = col1

	c.Assert(desc.Args[0], check.Equals, col)
	c.Assert(desc.equal(s.ctx, cloned), check.IsFalse)
}

func (s *testBaseFuncSuite) TestMaxMin(c *check.C) {
	col := &expression.Column{
		UniqueID: 0,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	col.RetType.Flag |= mysql.NotNullFlag
	desc, err := newBaseFuncDesc(s.ctx, ast.AggFuncMax, []expression.Expression{col})
	c.Assert(err, check.IsNil)
	c.Assert(mysql.HasNotNullFlag(desc.RetTp.Flag), check.IsFalse)
}

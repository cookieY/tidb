// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package failtest

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/cookieY/tidb/domain"
	"github.com/cookieY/tidb/kv"
	"github.com/cookieY/tidb/session"
	"github.com/cookieY/tidb/sessionctx"
	"github.com/cookieY/tidb/store/mockstore"
	"github.com/cookieY/tidb/util/mock"
	"github.com/cookieY/tidb/util/testkit"
	"github.com/cookieY/tidb/util/testleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testFailPointSuit{})

type testFailPointSuit struct {
	store kv.Storage
	dom   *domain.Domain
	ctx   sessionctx.Context
}

func (s *testFailPointSuit) SetUpSuite(c *C) {
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	c.Assert(store, NotNil)

	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	c.Assert(dom, NotNil)

	s.store, s.dom, s.ctx = store, dom, mock.NewContext()
}

func (s *testFailPointSuit) TearDownSuite(c *C) {
	s.dom.Close()
	err := s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testFailPointSuit) SetUpTest(c *C) {
	testleak.BeforeTest()
}

func (s *testFailPointSuit) TearDownTest(c *C) {
	testleak.AfterTest(c)()
}

func (s *testFailPointSuit) TestColumnPruningError(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a int, b int);`)
	tk.MustExec(`insert into t values(1,1);`)

	// test normal behavior
	tk.MustQuery(`select a from t;`).Check(testkit.Rows(`1`))

	// test the injected fail point
	c.Assert(failpoint.Enable("github.com/cookieY/tidb/planner/core/enableGetUsedListErr", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/cookieY/tidb/planner/core/enableGetUsedListErr"), IsNil)
	}()
	err := tk.ExecToErr(`select a from t;`)
	c.Assert(err.Error(), Equals, "getUsedList failed, triggered by gofail enableGetUsedListErr")
}

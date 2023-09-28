// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package extensions

import (
	"fmt"
	"github.com/xdblab/xdb/config"
)

var sqlRegistry = map[string]SQLDBExtension{}

// RegisterSQLDBExtension will register a SQL extension
func RegisterSQLDBExtension(name string, ext SQLDBExtension) {
	if _, ok := sqlRegistry[name]; ok {
		panic("SQL extension " + name + " already registered")
	}
	sqlRegistry[name] = ext
}

// NewSQLSession returns a regular session
func NewSQLSession(cfg *config.SQL) (SQLDBSession, error) {
	ext, ok := sqlRegistry[cfg.DBExtensionName]

	if !ok {
		return nil, fmt.Errorf("not supported SQLDBExtensionName %v, only supported: %v", cfg.DBExtensionName, sqlRegistry)
	}

	return ext.StartDBSession(cfg)
}

// NewSQLAdminSession returns a AdminDB
func NewSQLAdminSession(cfg *config.SQL) (SQLAdminDBSession, error) {
	ext, ok := sqlRegistry[cfg.DBExtensionName]

	if !ok {
		return nil, fmt.Errorf("not supported SQLDBExtensionName %v, only supported: %v", cfg.DBExtensionName, sqlRegistry)
	}

	return ext.StartAdminDBSession(cfg)
}

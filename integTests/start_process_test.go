// Copyright 2023 XDBLab organization
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package integTests

import (
	"github.com/xdblab/xdb-golang-sdk/integTests/basic"
	"testing"
	"time"
)

func TestStartBasicProcess(t *testing.T) {
	basic.TestStartIOProcess(t, client)
	time.Sleep(time.Second * 5)
}

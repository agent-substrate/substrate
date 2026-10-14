// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package combinedvolumes

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"testing"

	"github.com/agent-substrate/substrate/internal/e2e/probeapi"
	"github.com/agent-substrate/substrate/internal/resources"
)

func TestRuntimeGoldenVolumes(t *testing.T) {
	requireRuntimeGolden(t)
	f := newRuntimeGoldenFixture(t)
	ctx := context.Background()
	// Distinct actor-selected volumes must receive no placeholder/application
	// state. The same boot ID across clones proves a resumed kernel; distinct
	// process nonces prove applications start after the snapshot boundary.
	var goldenBoot, firstNonce string
	for i, v := range f.volumes {
		a := f.create(t, f.golden, fmt.Sprintf("check-%d-%s", i, f.name), v)
		func() {
			defer f.remove(t, a)
			if len(a.GetStatus().GetSnapshots()) == 0 {
				t.Fatal("runtime actor has no source snapshot")
			}
			f.resume(t, a)
			ref := resources.ActorRefFromActor(a)
			state := probeJSON(ctx, t, f.router, ref, "/runtime-state")
			if state["boot_id"] == "" || state["process_nonce"] == "" {
				t.Fatal("missing runtime identity")
			}
			if i == 0 {
				goldenBoot = state["boot_id"]
				firstNonce = state["process_nonce"]
			} else if state["boot_id"] != goldenBoot || state["process_nonce"] == firstNonce {
				t.Fatalf("not distinct processes from one resumed kernel: %v", state)
			}
			requireUnreadable(ctx, t, f.router, ref, sharedPath+"/actor-"+strconv.Itoa(1-i))
			requireWrite(ctx, t, f.router, ref, sharedPath+"/actor-"+strconv.Itoa(i))
			root := sharedPath + "/state"
			challenge := "challenge-" + v
			f.post(t, a, "/volume-fixture", map[string]string{"root": root, "value": v})
			f.post(t, a, "/volume-state?root="+url.QueryEscape(root)+"&challenge="+url.QueryEscape(challenge), nil)
			for generation := 1; generation <= 2; generation++ {
				a = f.suspend(t, a)
				f.resume(t, a)
				after := probeJSON(ctx, t, f.router, ref, "/runtime-state")
				if after["boot_id"] != goldenBoot || after["process_nonce"] != state["process_nonce"] {
					t.Fatal("own restore restarted application or guest")
				}
				resp, err := f.router.Get(ctx, ref, "/volume-state")
				if err != nil {
					t.Fatal(err)
				}
				var obs probeapi.VolumeStateResponse
				err = json.NewDecoder(resp.Body).Decode(&obs)
				resp.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("retained state actor=%s generation=%d: %+v", v, generation, obs)
				if err := probeapi.ValidateRetainedState(obs, challenge, v, generation); err != nil {
					t.Fatal(err)
				}
				if obs.Unlinked.ErrorCode == "EIO" {
					t.Log("known limitation: deleted-open file returned EIO on generation two")
				}
			}
		}()
	}
}

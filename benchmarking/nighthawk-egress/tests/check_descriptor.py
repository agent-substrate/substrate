# Copyright 2026 The Agent Substrate Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import spec as egress_spec

def main():
    desc = os.environ.get("NIGHTHAWK_DESC", "/app/nighthawk.desc")
    if not Path(desc).is_file():
        sys.exit(f"{desc} not present")
    fake_ca = "-----BEGIN CERTIFICATE-----\nMIIBfake\n-----END CERTIFICATE-----\n"
    for mode in egress_spec.MODE_URIS:
        s = egress_spec.build_egress_session_spec(
            mode,
            servicedns_ca_pem=fake_ca if mode == "passthrough" else None,
        )
        txt = egress_spec.spec_dict_to_textproto(s, desc)
        if "nighthawk_traffic_template" not in txt:
            sys.exit("nighthawk_traffic_template missing")
    print("Descriptor roundtrip successful")

if __name__ == "__main__":
    main()

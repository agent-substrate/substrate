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

"""Records how long each benchmark template took to build its golden snapshot.

The template controller starts a golden actor, waits for it to warm up,
suspends it and tags the snapshot. The template's status records the end of
the warmup as snapshot_time, which for a template without a wakeup probe
includes a fixed wait. The status write that saves the tag follows deleting
the golden actor and is the template's last update, so update_time marks the
tag. Both are read against
the template's create_time. workloads/deploy.sh creates the templates just
before the run, so they have no later writes.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

API_HOST = "api.ate-system.svc.cluster.local:443"
TEMPLATE_ATESPACE = "benchmark-workloads"


def golden_row(
    template: str,
    create: datetime,
    snapshot: datetime | None,
    tagged: datetime | None,
    data_ts: str,
    tag: str,
    test_name: str,
) -> dict[str, Any]:
    """The golden_snapshot row for one template. warmup_s runs to the end of
    the warmup and golden_snapshot_s to the tag; each is None when the
    template has not reached that point."""

    def since_create(t: datetime | None) -> str | None:
        return None if t is None else str(round((t - create).total_seconds(), 3))

    return {
        "timestamp": data_ts,
        "tag": tag,
        "test_name": test_name,
        "metric": "golden_snapshot",
        "measurements": {
            "template": template,
            "warmup_s": since_create(snapshot),
            "golden_snapshot_s": since_create(tagged),
        },
    }


def fetch_template_times(
    name: str,
) -> tuple[datetime, datetime | None, datetime | None]:
    """create_time, snapshot_time and, once the golden tag is saved,
    update_time of benchmark-workloads/<name>."""
    # Imported here: the generated protos and the pod certificate exist only
    # in the runner image.
    from common import ateapi_pb2, ateapi_pb2_grpc
    from common.ateapi_channel import ateapi_channel

    with ateapi_channel(API_HOST) as channel:
        tmpl = ateapi_pb2_grpc.ControlStub(channel).GetActorTemplate(
            ateapi_pb2.GetActorTemplateRequest(
                actor_template=ateapi_pb2.ObjectRef(
                    atespace=TEMPLATE_ATESPACE, name=name
                )
            ),
            timeout=10,
        )
    golden = tmpl.status.golden_snapshot_status
    snapshot = (
        golden.snapshot_time.ToDatetime() if golden.HasField("snapshot_time") else None
    )
    tagged = tmpl.metadata.update_time.ToDatetime() if golden.HasField("golden_tag") else None
    return tmpl.metadata.create_time.ToDatetime(), snapshot, tagged


def append_golden_snapshots(
    jsonl_path: Path,
    templates: list[str],
    data_ts: str,
    tag: str,
    test_name: str,
) -> int:
    """Appends one golden_snapshot row per template and returns how many."""
    rows = []
    for name in templates:
        create, snapshot, tagged = fetch_template_times(name)
        rows.append(golden_row(name, create, snapshot, tagged, data_ts, tag, test_name))
    with open(jsonl_path, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    return len(rows)

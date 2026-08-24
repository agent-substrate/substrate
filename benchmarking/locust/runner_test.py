# Copyright 2026 Google LLC

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

import tempfile
import unittest
from pathlib import Path

import runner


class RecordingProvider:
    def __init__(self) -> None:
        self.uploads: list[tuple[Path, str]] = []

    def upload(self, local_path: Path, destination: str) -> None:
        self.uploads.append((local_path, destination))


class StorageProviderTest(unittest.TestCase):
    def test_registered_provider_receives_upload(self) -> None:
        provider = RecordingProvider()
        runner.register_storage_provider("custom", provider)

        source = Path("results.jsonl")
        runner.upload(source, "custom://bucket/run/results.jsonl")

        self.assertEqual(
            provider.uploads,
            [(source, "custom://bucket/run/results.jsonl")],
        )

    def test_local_destination_copies_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "source"
            destination = Path(temp_dir) / "nested" / "destination"
            source.write_text("results")

            runner.upload(source, str(destination))

            self.assertEqual(destination.read_text(), "results")

    def test_unknown_scheme_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported storage scheme"):
            runner.upload(Path("results.jsonl"), "unknown://bucket/results")


if __name__ == "__main__":
    unittest.main()

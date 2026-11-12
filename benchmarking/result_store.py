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

"""Read and publish benchmark artifacts using local paths or object-store URLs."""

import argparse
import fnmatch
import sys
from pathlib import Path
from urllib.parse import urlsplit

import obstore
from obstore.store import LocalStore, from_url


class ResultStore:
    """An artifact root. Paths passed to its methods are relative to this root."""

    def __init__(self, root: str):
        url = urlsplit(root)
        if not url.scheme:
            self.store = LocalStore(Path(root).resolve(), mkdir=True)
        elif url.scheme in {"gs", "s3", "az", "memory"}:
            if url.query or url.fragment:
                raise ValueError("Result-store URLs cannot contain a query or fragment")
            self.store = from_url(root)
        else:
            raise ValueError(f"Unsupported result-store scheme: {url.scheme}")

    def write(self, path: str, source: Path) -> None:
        obstore.put(self.store, path, source)

    def read(self, path: str) -> bytes:
        return bytes(obstore.get(self.store, path).bytes())

    def list(self, prefix: str = ""):
        """Yield artifact paths relative to the root, recursively."""
        for batch in obstore.list(self.store, prefix=prefix):
            for item in batch:
                yield item["path"]


def upload(source: Path, destination: str) -> None:
    """Upload one artifact; propagate failures to the runner's exit status."""
    root, _, name = destination.rpartition("/")
    ResultStore(root or ("/" if destination.startswith("/") else ".")).write(name, source)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("list", help="List artifact URLs under a root")
    listing.add_argument("root")
    listing.add_argument("--match", default="*", help="Glob matched against relative paths")
    cat = commands.add_parser("cat", help="Write one artifact to stdout")
    cat.add_argument("url")
    download = commands.add_parser("download", help="Download all artifacts under a root")
    download.add_argument("root")
    download.add_argument("directory", type=Path)
    args = parser.parse_args()
    if args.command == "cat":
        root, _, name = args.url.rpartition("/")
        sys.stdout.buffer.write(ResultStore(root or ("/" if args.url.startswith("/") else ".")).read(name))
        return
    store = ResultStore(args.root)
    for path in store.list():
        if args.command == "list":
            if fnmatch.fnmatchcase(path, args.match):
                print(f"{args.root.rstrip('/')}/{path}")
        else:
            target = (args.directory / path).resolve()
            if not target.is_relative_to(args.directory.resolve()):
                raise ValueError(f"Artifact path escapes the download directory: {path}")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(store.read(path))


if __name__ == "__main__":
    main()

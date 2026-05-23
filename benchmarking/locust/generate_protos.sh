#!/usr/bin/env bash

#  Copyright 2026 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Source the environment variables if configured
if [[ -f .ate-dev-env.sh ]]; then
  source .ate-dev-env.sh
fi

PROTO_PATH="pkg/proto/ateapipb"
PROTO_FILE="$PROTO_PATH/ateapi.proto"

# Create and activate virtual environment if it doesn't exist
VENV_DIR="benchmarking/locust/venv"
if [ ! -d "$VENV_DIR" ]; then
  echo "Creating virtual environment in $VENV_DIR..."
  python3 -m venv "$VENV_DIR"
  source "$VENV_DIR/bin/activate"
  echo "Installing dependencies..."
  pip install --upgrade pip
  pip install grpcio-tools
else
  echo "Activating virtual environment..."
  source "$VENV_DIR/bin/activate"
fi

echo "Generating Python proto clients from $PROTO_FILE..."

python3 -m grpc_tools.protoc -I"$PROTO_PATH" --python_out=benchmarking/locust/common/ --grpc_python_out=benchmarking/locust/common/ "$PROTO_FILE"

# Prepend ASLv2 header to generated files
for file in benchmarking/locust/common/ateapi_pb2.py benchmarking/locust/common/ateapi_pb2_grpc.py; do
  if [ -f "$file" ]; then
    cat hack/boilerplate/python.txt "$file" > "${file}.tmp"
    mv "${file}.tmp" "$file"
  fi
done

# Fix relative import in generated grpc file
GRPC_FILE="benchmarking/locust/common/ateapi_pb2_grpc.py"
if [ -f "$GRPC_FILE" ]; then
  sed -i 's/^import ateapi_pb2 as ateapi__pb2/from . import ateapi_pb2 as ateapi__pb2/' "$GRPC_FILE"
fi

echo "Done!"

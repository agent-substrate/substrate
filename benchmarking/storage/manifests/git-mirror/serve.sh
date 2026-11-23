#!/bin/sh
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

# Serves linux-head.git from /srv on port 80 as static files, so clients use
# the dumb HTTP protocol and many actors can clone it at once.

set -o errexit -o nounset

apt-get update -qq
apt-get install -y -qq nginx > /dev/null
cat > /etc/nginx/sites-enabled/default <<'EOF'
server {
  listen 80;
  location /linux-head.git/ {
    root /srv;
  }
}
EOF
exec nginx -g 'daemon off;'

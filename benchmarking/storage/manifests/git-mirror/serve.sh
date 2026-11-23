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

# Serves the git mirror's repositories from /srv on port 80. kubernetes.git
# and linux.git go through git http-backend (smart HTTP). linux-head.git is
# served as static files, so clients use the dumb HTTP protocol and many
# actors can clone it at once.

set -o errexit -o nounset

apt-get update -qq
apt-get install -y -qq git nginx fcgiwrap > /dev/null
git config --system safe.directory '*'
cat > /etc/nginx/sites-enabled/default <<'EOF'
server {
  listen 80;
  location /linux-head.git/ {
    root /srv;
  }
  location / {
    include fastcgi_params;
    fastcgi_param SCRIPT_FILENAME /usr/lib/git-core/git-http-backend;
    fastcgi_param GIT_PROJECT_ROOT /srv;
    fastcgi_param GIT_HTTP_EXPORT_ALL "";
    fastcgi_param PATH_INFO $uri;
    fastcgi_read_timeout 3600;
    fastcgi_pass unix:/run/fcgiwrap.sock;
  }
}
EOF
fcgiwrap -c 4 -s unix:/run/fcgiwrap.sock &
while [ ! -S /run/fcgiwrap.sock ]; do sleep 0.1; done
chown www-data /run/fcgiwrap.sock
exec nginx -g 'daemon off;'

# Testing extprocd end to end

`egress-plugin-example` is the egress gateway's **inner** checkpoint: the ext_proc server
that runs on a tunneled request *after* Envoy has minted a leaf for the SNI,
terminated the actor's TLS, and decrypted it. It is the first and only point on
the egress path where the destination hostname, method, and path exist in the
clear.

```
actor process
  │  plain HTTP/HTTPS to a destination
  ▼
nftables REDIRECT ─► atunnel ─► mTLS + CONNECT <IP>:<port>
  ▼
atenet-egress :443  "egress" listener
  │  set_filter_state   publishes the peer cert's URI SAN as ate.actor
  │  ext_proc           CONNECT checkpoint: is this a running actor?
  ▼
mitm_internal cluster (internal_upstream carries ate.actor across the hop)
  ▼
"mitm_listener"       TLS chain (sdsmint leaf per SNI) or cleartext chain
  │  ext_proc  ───────────────►  extprocd     ◄── the thing under test
  │                              reads :authority + filter_state['ate.actor']
  │                              returns allow / 403 / header mutations
  ▼
dynamic_forward_proxy ─► the real destination
```

## The hop to extprocd is mutually authenticated

| | presents | verifies the peer with |
| --- | --- | --- |
| extprocd | `servicedns` leaf, DNS SAN `atenet-egress-extproc.ate-system.svc` | `podidentity` trust bundle, narrowed to `spiffe://cluster.local/ns/ate-system/sa/atenet-egress` |
| gateway (generated cluster `transport_socket`) | `podidentity` leaf, URI SAN `…/sa/atenet-egress` | `servicedns` trust bundle, `match_typed_subject_alt_names` on the DNS name above |


## Unit tests

```bash
go test ./egress-plugin-example/...
```

Covers the wire contract — message kinds, immediate responses, header mutation,
duplicate credential collapse — the actor-URI parser, and the serving mTLS
config It does not cover any Envoy configuration, which is
where most of the real failures live.

## E2E tests

## Step 1: Install Substrate with the additional egress extproc configured in the egress Envoy configuration

```shell
./hack/install-ate.sh --deploy-ate-system \
  --experimental-use-sdsmint \
  --experimental-additional-egress-extproc-service nous-dev/atenet-egress-extproc:50051
```

`--experimental-use-sdsmint` is needed because the default egress Envoy config does not include sdsmint currently.

## Step 2: Deploy the additional egress extproc

```
kubectl create namespace nous-dev

# Nous needs to replace KO_DOCKER_REPO
KO_DOCKER_REPO="gcr.io/haiyanmeng-gke-dev/ate-images" \
./hack/run-tool.sh ko apply -f egress-plugin-example/atenet-egress-extproc.yaml

kubectl -n nous-dev rollout status deployment/atenet-egress-extproc
```
Currently, the extproc allows all the HTTP requests.

Nous needs to run this step every time they modify the extproc Go code.

## Step 3: HTTP traffic

**Drive traffic.** Four steps: a destination, an actor, a request that makes
the actor fetch the destination, and a check that it went through the gateway.

```bash
# 1. The actor fixture: a WorkerPool and an ActorTemplate in ate-demo-egress.
#    The actor is a service that takes {"url":"…"}, GETs it, and returns the
#    response -- so a request to it is an egress request from it.
#    Needs BUCKET_NAME set (the template's snapshot location).
./hack/install-ate.sh --deploy-demo-egress

# 2. Something to fetch. Any in-cluster HTTP server; whoami echoes its client
#    address, which is how you prove the traffic was proxied.
kubectl create namespace egress-target
kubectl -n egress-target create deployment whoami --image=traefik/whoami
kubectl -n egress-target expose deployment whoami --port=80
kubectl -n egress-target rollout status deployment/whoami --timeout=120s
TARGET_IP=$(kubectl -n egress-target get svc whoami -o jsonpath='{.spec.clusterIP}')

# 3. An actor from that template. Egress is only tunneled for a real, running
#    actor -- the CONNECT checkpoint refuses anything else.
#    Needs kubectl-ate: go install ./cmd/kubectl-ate
kubectl ate create atespace demo
kubectl ate create actor egress-demo -a demo --template ate-demo-egress/egress
kubectl ate resume actor egress-demo -a demo
until kubectl ate get actors -a demo | grep -q STATUS_RUNNING; do sleep 3; done

# 4. Reach the actor through the ingress router and tell it what to fetch.
kubectl -n ate-system port-forward service/atenet-router 18099:80 &
sleep 4
curl -s -w '\nHTTP %{http_code}\n' -X POST http://localhost:18099/ \
  -H 'Host: egress-demo.demo.actors.resources.substrate.ate.dev' \
  -H 'Content-Type: application/json' \
  -d "{\"url\":\"http://${TARGET_IP}:80/\"}"
```

Expect `HTTP 200` and, in the body, `RemoteAddr: <atenet-egress pod IP>` — the
target seeing the gateway rather than the actor is the proof the request was
tunneled instead of dialled directly:


**Observe, on three surfaces.**

```bash
kubectl -n nous-dev logs -l app=atenet-egress-extproc --prefix --tail=-1 \
  | grep 'egress allowed\|egress denied'
# msg="egress allowed" authority=34.118.228.70:80 method=GET path=/ scheme=http actor=spiffe://substrate-actor.local/atespace/demo/actor/egress-demo

# 2. The MITM leg logged the same identity. "-" means it never arrived.
kubectl -n ate-system logs deploy/atenet-egress -c envoy | grep '"leg":"mitm"\|"leg":"cleartext"'
#   {"leg":"cleartext","actor":"spiffe://…/atespace/demo/actor/egress-demo","authority":"10.96.…:80",…}

# 3. The CONNECT leg, for comparison: authority is an IP:port here, by design.
kubectl -n ate-system logs deploy/atenet-egress -c envoy | grep '\[egress\]'
```

## Step 4: HTTPS traffic

**1. Extract the CA certificate.** The certificate only — the pool JSON also
holds `SigningKeyPKCS8`, so never dump the decoded pool to a terminal, a shared
log, or a paste. Note `RootCertificatePEM` in that JSON is empty; the DER field
is the populated one, hence the `openssl` conversion:

```bash
kubectl -n ate-system get secret egress-mitm-ca-pool -o jsonpath='{.data.pool}' | base64 -d \
  | jq -r '.CAs[0].RootCertificateDER' | base64 -d \
  | openssl x509 -inform DER > /tmp/mitm-ca.pem

openssl x509 -in /tmp/mitm-ca.pem -noout -subject -dates   # sanity check
```

**2. Drive it**, at a **public** origin. Not the in-cluster `whoami` from §2 and
not a self-signed test server: `egress_forward_proxy` validates the *origin's*
certificate against `/etc/ssl/certs/ca-certificates.crt` with `auto_sni` and
`auto_san_validation`, so anything whose certificate does not chain to a public
root fails on the far side of the MITM even when the near side is fine. That
shows up as a `503` with `upstream_failure` populated in the `"leg":"mitm"` log,
which is a different failure from the pin ones in the table below.
`https://example.com/` is the URL used here.

`--rawfile` is what gets a multi-line PEM into JSON intact:

```bash
jq -n --arg url https://example.com/ --rawfile ca /tmp/mitm-ca.pem \
     '{url:$url, caPem:$ca}' \
  | curl -s -w '\nHTTP %{http_code}\n' -X POST http://localhost:18099/ \
      -H 'Host: egress-demo.demo.actors.resources.substrate.ate.dev' \
      -H 'Content-Type: application/json' --data-binary @-
```

Expect `HTTP 200` with `example.com`'s HTML in `body`.

### What confirms the path, rather than just the fetch

```bash
# The MITM leg, with an SNI -- so a leaf was minted -- and the actor identity
# carried across from the CONNECT leg.
kubectl -n ate-system logs deploy/atenet-egress -c envoy | grep '"leg":"mitm"'
#   {"leg":"mitm","sni":"example.com","actor":"spiffe://…/actor/egress-demo",…}

# extprocd saw the decrypted request: the real hostname, not an IP:port.
kubectl -n nous-dev logs -l app=atenet-egress-extproc --prefix --tail=-1 \
  | grep 'egress allowed'
#   msg="egress allowed" authority=example.com method=GET path=/ scheme=https actor=spiffe://substrate-actor.local/atespace/demo/actor/egress-demo

# The CONNECT leg, same request, for contrast.
kubectl -n ate-system logs deploy/atenet-egress -c envoy | grep '\[egress\]'
#   [egress] authority=<resolved-ip>:443 peer_san=spiffe://… code=200
```

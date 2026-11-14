# Dataplane Timeouts

Substrate dataplane timeouts are tuned to support streaming or long running requests, such as inference.
Timeouts for ingress and egress traffic are the same. On the ingress path timeouts are defined for
connections and requests between client and actor, and on the egress path between actor and origin
server. Internal CONNECT tunnels between Atunnel and Ingress or Egress gateways are transparent to
the traffic and do not have configured timeouts.

| Timeout | Description | Default Value | User Configurable |
|---|---|---|---|
| Idle connection timeout | Timeout for connections without outstanding requests. | 5 minutes | No |
| Maximum connection duration | Maximum lifetime of a connection. | Disabled | No |
| Request timeout | Maximum time for request headers, body and trailers (if present) to be received. | Disabled | No |
| Request headers timeout | Maximum time for request headers to be received. | 10 seconds | No |
| Request idle timeout | Timeout for requests without network activity. | Disabled | No |
| Maximum request duration | Maximum lifetime of a request from the start to the end of response. | Disabled | No |
| Response timeout | Timeout for response to be fully received. | 5 minutes | Yes. See --route-timeout |
| TLS handshake timeout | Timeout for compltetion of TLS handshake. | 10 seconds | No |


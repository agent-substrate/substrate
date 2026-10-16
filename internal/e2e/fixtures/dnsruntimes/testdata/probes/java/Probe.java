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

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.resolver.ResolvedAddressTypes;
import io.netty.resolver.dns.DefaultDnsCache;
import io.netty.resolver.dns.DnsNameResolver;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.resolver.dns.SequentialDnsServerAddressStreamProvider;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Probe {
  private static String extractString(String json, String key, String def) {
    Pattern p = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*\"([^\"]*)\"");
    Matcher m = p.matcher(json);
    return m.find() ? m.group(1) : def;
  }

  private static int extractInt(String json, String key, int def) {
    Pattern p = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*(\\d+)");
    Matcher m = p.matcher(json);
    return m.find() ? Integer.parseInt(m.group(1)) : def;
  }

  private static boolean extractBool(String json, String key, boolean def) {
    Pattern p = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*(true|false)");
    Matcher m = p.matcher(json);
    return m.find() ? Boolean.parseBoolean(m.group(1)) : def;
  }

  private static String jsonEscape(String s) {
    if (s == null) {
      return "\"\"";
    }
    return "\""
        + s.replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
        + "\"";
  }

  private static List<InetSocketAddress> readNameservers() {
    List<InetSocketAddress> servers = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader("/etc/resolv.conf"))) {
      String line;
      while ((line = br.readLine()) != null) {
        line = line.trim();
        if (line.startsWith("nameserver ")) {
          String ip = line.substring("nameserver ".length()).trim();
          if (!ip.isEmpty()) {
            servers.add(new InetSocketAddress(ip, 53));
          }
        }
      }
    } catch (Exception ignored) {
    }
    return servers;
  }

  private static List<String> readSearchDomains() {
    List<String> domains = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader("/etc/resolv.conf"))) {
      String line;
      while ((line = br.readLine()) != null) {
        line = line.trim();
        if (line.startsWith("search ")) {
          String[] parts = line.substring("search ".length()).trim().split("\\s+");
          for (String p : parts) {
            if (!p.isEmpty()) {
              domains.add(p);
            }
          }
        }
      }
    } catch (Exception ignored) {
    }
    return domains;
  }

  private static int readNdots() {
    try (BufferedReader br = new BufferedReader(new FileReader("/etc/resolv.conf"))) {
      String line;
      while ((line = br.readLine()) != null) {
        line = line.trim();
        if (line.startsWith("options ")) {
          String[] parts = line.substring("options ".length()).trim().split("\\s+");
          for (String p : parts) {
            if (p.startsWith("ndots:")) {
              return Integer.parseInt(p.substring("ndots:".length()).trim());
            }
          }
        }
      }
    } catch (Exception ignored) {
    }
    return 1;
  }

  public static void main(String[] args) throws Exception {
    String req = args.length > 0 ? args[0] : "{}";
    String runtime = extractString(req, "runtime", "java-inetaddress");
    String op = extractString(req, "op", "resolve");
    String host = extractString(req, "host", "");
    String urlStr = extractString(req, "url", "");
    int attempts = extractInt(req, "attempts", 1);
    int intervalMs = extractInt(req, "intervalMs", 0);
    boolean enableCache = extractBool(req, "enableCache", false);
    if (attempts <= 0) {
      attempts = 1;
    }

    NioEventLoopGroup group = null;
    DnsNameResolver nettyResolver = null;
    if ("java-netty".equals(runtime)) {
      group = new NioEventLoopGroup(1);
      List<InetSocketAddress> servers = readNameservers();
      List<String> searchDomains = readSearchDomains();
      DnsNameResolverBuilder builder =
          new DnsNameResolverBuilder(group.next())
              .channelType(NioDatagramChannel.class)
              .resolvedAddressTypes(ResolvedAddressTypes.IPV4_ONLY)
              .queryTimeoutMillis(1500)
              .maxQueriesPerResolve(Math.max(4, (servers.size() + searchDomains.size()) * 2));
      if (!servers.isEmpty()) {
        builder.nameServerProvider(new SequentialDnsServerAddressStreamProvider(servers));
      }
      if (!searchDomains.isEmpty()) {
        builder.searchDomains(searchDomains);
        builder.ndots(readNdots());
      }
      if (enableCache) {
        builder.resolveCache(new DefaultDnsCache());
      }
      nettyResolver = builder.build();
    }

    StringBuilder out = new StringBuilder("{\"attempts\":[");
    try {
      for (int i = 0; i < attempts; i++) {
        if (i > 0) {
          out.append(",");
          if (intervalMs > 0) {
            Thread.sleep(intervalMs);
          }
        }
        long start = System.currentTimeMillis();
        try {
          if ("http_get".equals(op)) {
            int statusCode;
            if ("java-netty".equals(runtime)) {
              URI uri = URI.create(urlStr);
              String targetHost = uri.getHost();
              int targetPort = uri.getPort() > 0 ? uri.getPort() : 80;
              String targetPath =
                  (uri.getRawPath() == null || uri.getRawPath().isEmpty()) ? "/" : uri.getRawPath();
              InetAddress resolved = nettyResolver.resolve(targetHost).get();
              try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(resolved, targetPort), 5000);
                s.setSoTimeout(5000);
                OutputStream os = s.getOutputStream();
                String httpReq =
                    "GET "
                        + targetPath
                        + " HTTP/1.1\r\nHost: "
                        + targetHost
                        + "\r\nConnection: close\r\n\r\n";
                os.write(httpReq.getBytes(StandardCharsets.UTF_8));
                os.flush();
                InputStream is = s.getInputStream();
                byte[] buf = new byte[256];
                int n = is.read(buf);
                String statusLine = n > 0 ? new String(buf, 0, n, StandardCharsets.UTF_8) : "";
                String[] parts = statusLine.split(" ");
                statusCode = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
              }
            } else {
              URL u = URI.create(urlStr).toURL();
              HttpURLConnection conn = (HttpURLConnection) u.openConnection();
              conn.setConnectTimeout(5000);
              conn.setReadTimeout(5000);
              conn.setRequestMethod("GET");
              statusCode = conn.getResponseCode();
              conn.disconnect();
            }
            long dur = System.currentTimeMillis() - start;
            boolean ok = statusCode >= 200 && statusCode < 400;
            out.append("{\"success\":")
                .append(ok)
                .append(",\"statusCode\":")
                .append(statusCode)
                .append(",\"durationMs\":")
                .append(dur)
                .append("}");
          } else {
            List<String> addrs = new ArrayList<>();
            if ("java-netty".equals(runtime)) {
              List<InetAddress> list = nettyResolver.resolveAll(host).get();
              for (InetAddress a : list) {
                addrs.add(a.getHostAddress());
              }
            } else {
              InetAddress[] list = InetAddress.getAllByName(host);
              for (InetAddress a : list) {
                addrs.add(a.getHostAddress());
              }
            }
            long dur = System.currentTimeMillis() - start;
            out.append("{\"success\":true,\"addresses\":[");
            for (int j = 0; j < addrs.size(); j++) {
              if (j > 0) out.append(",");
              out.append(jsonEscape(addrs.get(j)));
            }
            out.append("],\"durationMs\":").append(dur).append("}");
          }
        } catch (Throwable t) {
          long dur = System.currentTimeMillis() - start;
          Throwable cause = (t instanceof ExecutionException && t.getCause() != null) ? t.getCause() : t;
          if ("java-netty".equals(runtime) && cause.getCause() == null && !host.isEmpty() && !host.endsWith(".")) {
            try {
              nettyResolver.resolveAll(host + ".").get();
            } catch (Throwable directErr) {
              Throwable directCause =
                  (directErr instanceof ExecutionException && directErr.getCause() != null)
                      ? directErr.getCause()
                      : directErr;
              if (directCause.getCause() != null) {
                try {
                  cause.initCause(directCause.getCause());
                } catch (Exception ignored) {
                }
              }
            }
          }
          StringBuilder fullMsg = new StringBuilder(cause.getClass().getName() + ": " + cause.getMessage());
          Throwable cur = cause.getCause();
          while (cur != null) {
            fullMsg.append(" <- ").append(cur.getClass().getName()).append(": ").append(cur.getMessage());
            cur = cur.getCause();
          }
          out.append("{\"success\":false,\"errorCode\":")
              .append(jsonEscape(cause.getClass().getSimpleName()))
              .append(",\"error\":")
              .append(jsonEscape(fullMsg.toString()))
              .append(",\"durationMs\":")
              .append(dur)
              .append("}");
        }
      }
    } finally {
      if (nettyResolver != null) {
        nettyResolver.close();
      }
      if (group != null) {
        group.shutdownGracefully().syncUninterruptibly();
      }
    }
    out.append("]}\n");
    System.out.print(out.toString());
  }
}

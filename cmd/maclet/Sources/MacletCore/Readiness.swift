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

import Foundation

public struct HTTPProbe: Sendable {
  public let port: Int
  public let path: String
  public let timeout: TimeInterval

  public init(port: Int, path: String, timeout: TimeInterval) throws {
    let allowed = CharacterSet(
      charactersIn:
        "/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~!$&'()*+,;=:@%")
    guard (1...65535).contains(port), (1...3600).contains(timeout),
      path.hasPrefix("/"), path.utf8.count <= 1024,
      path.unicodeScalars.allSatisfy({ allowed.contains($0) })
    else {
      throw MacletError(
        "Probe needs port 1...65535, an absolute URL path, and timeout 1...3600 seconds")
    }
    self.port = port
    self.path = path
    self.timeout = timeout
  }

  public func url(ip: String) -> URL? {
    guard DHCPLeases.validIPv4(ip) else { return nil }
    return URL(string: "http://\(ip):\(port)\(path)")
  }

  public static func accepts(statusCode: Int) -> Bool { (200..<300).contains(statusCode) }
}

public enum DHCPLeases {
  private static func normalizedMAC(_ value: String) -> [UInt8]? {
    let parts = value.split(separator: ":", omittingEmptySubsequences: false)
    let bytes = parts.compactMap { UInt8($0, radix: 16) }
    return parts.count == 6 && bytes.count == 6 ? bytes : nil
  }

  public static func validIPv4(_ value: String) -> Bool {
    let parts = value.split(separator: ".", omittingEmptySubsequences: false)
    return parts.count == 4 && parts.allSatisfy { UInt8($0) != nil }
  }

  public static func address(in leases: String, mac: String, now: Date = Date()) -> String? {
    guard let target = normalizedMAC(mac) else { return nil }
    for block in leases.components(separatedBy: "}").reversed() {
      var values: [String: String] = [:]
      for line in block.components(separatedBy: .newlines) {
        let pair = line.trimmingCharacters(in: .whitespaces).split(separator: "=", maxSplits: 1)
        if pair.count == 2 { values[String(pair[0])] = String(pair[1]) }
      }
      guard let hardware = values["hw_address"], hardware.hasPrefix("1,"),
        normalizedMAC(String(hardware.dropFirst(2))) == target,
        let ip = values["ip_address"], validIPv4(ip),
        let lease = values["lease"], lease.hasPrefix("0x"),
        let expiration = UInt64(lease.dropFirst(2), radix: 16),
        Double(expiration) > now.timeIntervalSince1970
      else { continue }
      return ip
    }
    return nil
  }
}

public struct VMStatus: Codable, Sendable {
  public let actorID: String
  public let runID: UUID
  public var phase: String
  public var ready: Bool
  public var ipAddress: String?
  public var detail: String

  public init(
    actorID: String, runID: UUID = UUID(), phase: String = "starting", ready: Bool = false,
    detail: String = "Waiting for guest HTTP readiness"
  ) {
    self.actorID = actorID
    self.runID = runID
    self.phase = phase
    self.ready = ready
    self.detail = detail
  }

  public static func read(_ directory: URL) throws -> Self {
    let actor = try ActorBundle.read(directory)
    let owned = try BundleLock.isOwned(directory)
    let url = directory.appendingPathComponent("status.json")
    if !FileManager.default.fileExists(atPath: url.path) {
      return Self(
        actorID: actor.actorID, phase: owned ? "starting" : "stopped",
        detail: "No readiness result yet")
    }
    var result = try JSONDecoder().decode(Self.self, from: Data(contentsOf: url))
    if !owned {
      result.phase = "stopped"
      result.ready = false
      result.detail = "No maclet owns this bundle. Last result: " + result.detail
    }
    return result
  }
}

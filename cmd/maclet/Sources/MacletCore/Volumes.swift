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
import Virtualization

public struct VMVolume: Codable, Equatable, Sendable {
  public let name: String
  public let mountPath: String
  public let tag: String
  public let hostPath: String

  public static func read(from bundle: URL) throws -> [Self] {
    let config = bundle.appendingPathComponent("macletd-volumes.json")
    guard FileManager.default.fileExists(atPath: config.path) else { return [] }
    let volumes = try JSONDecoder().decode([Self].self, from: Data(contentsOf: config))
    guard volumes.count <= 32 else { throw MacletError("At most 32 durable volumes are supported") }
    var tags = Set<String>()
    var mountPaths = Set<String>()
    for volume in volumes {
      guard !volume.name.isEmpty, volume.name.utf8.count <= 128,
        validTag(volume.tag), tags.insert(volume.tag).inserted,
        validMountPath(volume.mountPath), mountPaths.insert(volume.mountPath).inserted
      else { throw MacletError("Invalid durable volume configuration") }
      let host = URL(fileURLWithPath: volume.hostPath, isDirectory: true).standardizedFileURL
      guard host.path == volume.hostPath else {
        throw MacletError("Durable volume host path is not canonical")
      }
      let values = try host.resourceValues(forKeys: [.isDirectoryKey, .isSymbolicLinkKey])
      guard values.isDirectory == true, values.isSymbolicLink != true else {
        throw MacletError("Durable volume host path is not a real directory")
      }
    }
    return volumes
  }

  private static func validTag(_ value: String) -> Bool {
    do {
      try VZVirtioFileSystemDeviceConfiguration.validateTag(value)
      return true
    } catch {
      return false
    }
  }

  private static func validMountPath(_ value: String) -> Bool {
    guard value.hasPrefix("/"), value != "/", !value.hasSuffix("/"),
      !value.contains("//"), !value.contains(":"), value.utf8.count <= 4096
    else { return false }
    return value.split(separator: "/").allSatisfy { $0 != "." && $0 != ".." }
  }
}

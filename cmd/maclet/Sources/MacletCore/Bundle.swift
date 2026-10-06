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

import Darwin
import Foundation
import Virtualization

public struct MacletError: Error, CustomStringConvertible {
  public let description: String
  public init(_ description: String) { self.description = description }
}

public struct LumeConfig: Codable, Sendable {
  public var os: String
  public var cpuCount: Int
  public var memorySize: UInt64
  public var hardwareModel: Data
  public var machineIdentifier: Data
  public var macAddress: String
  public var networkMode: String?

  public func validate() throws {
    guard os == "macOS", networkMode == nil || networkMode == "nat" else {
      throw MacletError("Only macOS images with NAT networking are supported")
    }
    guard
      (VZVirtualMachineConfiguration
        .minimumAllowedCPUCount...VZVirtualMachineConfiguration.maximumAllowedCPUCount).contains(
          cpuCount),
      (VZVirtualMachineConfiguration
        .minimumAllowedMemorySize...VZVirtualMachineConfiguration.maximumAllowedMemorySize)
        .contains(memorySize)
    else { throw MacletError("CPU or memory outside host limits") }
    guard let model = VZMacHardwareModel(dataRepresentation: hardwareModel), model.isSupported,
      VZMacMachineIdentifier(dataRepresentation: machineIdentifier) != nil,
      VZMACAddress(string: macAddress) != nil
    else { throw MacletError("Invalid or unsupported hardware model, machine identifier, or MAC") }
  }

  public func personalized(machineIdentifier: Data, macAddress: String) throws -> Self {
    try validate()
    guard machineIdentifier != self.machineIdentifier,
      let mac = VZMACAddress(string: macAddress), mac.isLocallyAdministeredAddress,
      !mac.isMulticastAddress,
      mac.string.lowercased() != VZMACAddress(string: self.macAddress)?.string.lowercased()
    else {
      throw MacletError(
        "Clone requires a new machine identifier and locally administered unicast MAC")
    }
    var result = self
    result.machineIdentifier = machineIdentifier
    result.macAddress = mac.string.lowercased()
    result.networkMode = "nat"
    try result.validate()
    return result
  }
}

public struct ActorBundle: Codable, Sendable {
  public let actorID: String
  public let config: LumeConfig

  public static func read(_ directory: URL) throws -> Self {
    let bundle = try JSONDecoder().decode(
      Self.self, from: Data(contentsOf: directory.appendingPathComponent("actor.json")))
    try bundle.config.validate()
    for name in ["disk.img", "nvram.bin"] {
      try requireRegularFile(directory.appendingPathComponent(name))
    }
    return bundle
  }

  public static func create(source: URL, destination: URL, actorID: String, cpuCount: Int? = nil, memorySize: UInt64? = nil) throws -> Self {
    let source = source.resolvingSymlinksInPath().standardizedFileURL
    let destination = destination.resolvingSymlinksInPath().standardizedFileURL
    guard !actorID.isEmpty, actorID.utf8.count <= 128 else {
      throw MacletError("Actor ID must contain 1...128 bytes")
    }
    guard destination.path != source.path, !destination.path.hasPrefix(source.path + "/") else {
      throw MacletError("Destination must be outside the source bundle")
    }
    for name in ["config.json", "disk.img", "nvram.bin"] {
      try requireRegularFile(source.appendingPathComponent(name))
    }
    // Lume does not share our lock. Reject visible open handles; callers must also
    // keep the source powered off throughout cloning (including other users).
    let check = Process()
    check.executableURL = URL(fileURLWithPath: "/usr/sbin/lsof")
    check.arguments = [
      "-t", "--", source.appendingPathComponent("disk.img").path,
      source.appendingPathComponent("nvram.bin").path,
    ]
    let output = Pipe()
    check.standardOutput = output
    try check.run()
    let handles = output.fileHandleForReading.readDataToEndOfFile()
    check.waitUntilExit()
    // lsof returns 1 if either file has no handles, even when the other does.
    guard handles.isEmpty, check.terminationStatus == 1 else {
      throw MacletError("Source has open handles, or lsof could not establish it is offline")
    }
    let base = try JSONDecoder().decode(
      LumeConfig.self, from: Data(contentsOf: source.appendingPathComponent("config.json")))
    var config = try base.personalized(
      machineIdentifier: VZMacMachineIdentifier().dataRepresentation,
      macAddress: VZMACAddress.randomLocallyAdministered().string)
    config.cpuCount = cpuCount ?? config.cpuCount
    config.memorySize = memorySize ?? config.memorySize
    try config.validate()
    let bundle = Self(actorID: actorID, config: config)
    // mkdir is exclusive: never reuse or clean up somebody else's destination.
    guard mkdir(destination.path, 0o700) == 0 else {
      throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
    do {
      for name in ["disk.img", "nvram.bin"] {
        let target = destination.appendingPathComponent(name).path
        guard clonefile(source.appendingPathComponent(name).path, target, 0) == 0 else {
          throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        guard chmod(target, 0o600) == 0 else { throw POSIXError(.EACCES) }
      }
      try writeJSON(bundle, to: destination.appendingPathComponent("actor.json"))
      return bundle
    } catch {
      try? FileManager.default.removeItem(at: destination)
      throw error
    }
  }

  // snapshot publishes only immutable cold-boot state. The bundle must not be owned.
  public static func snapshot(source: URL, destination: URL) throws {
    guard !(try BundleLock.isOwned(source)) else { throw MacletError("Cannot snapshot a running bundle") }
    let bundle = try read(source)
    guard mkdir(destination.path, 0o700) == 0 else {
      throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
    do {
      for name in ["disk.img", "nvram.bin"] {
        let target = destination.appendingPathComponent(name).path
        guard clonefile(source.appendingPathComponent(name).path, target, 0) == 0 else {
          throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        guard chmod(target, 0o600) == 0 else { throw POSIXError(.EACCES) }
      }
      try writeJSON(bundle, to: destination.appendingPathComponent("actor.json"))
    } catch {
      try? FileManager.default.removeItem(at: destination)
      throw error
    }
  }

  public static func restore(snapshot: URL, destination: URL, actorID: String, cpuCount: Int? = nil, memorySize: UInt64? = nil) throws -> Self {
    let old = try read(snapshot)
    var config = try old.config.personalized(
      machineIdentifier: VZMacMachineIdentifier().dataRepresentation,
      macAddress: VZMACAddress.randomLocallyAdministered().string)
    config.cpuCount = cpuCount ?? config.cpuCount
    config.memorySize = memorySize ?? config.memorySize
    try config.validate()
    let bundle = Self(actorID: actorID, config: config)
    guard mkdir(destination.path, 0o700) == 0 else {
      throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }
    do {
      for name in ["disk.img", "nvram.bin"] {
        let target = destination.appendingPathComponent(name).path
        guard clonefile(snapshot.appendingPathComponent(name).path, target, 0) == 0 else {
          throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        guard chmod(target, 0o600) == 0 else { throw POSIXError(.EACCES) }
      }
      try writeJSON(bundle, to: destination.appendingPathComponent("actor.json"))
      return bundle
    } catch {
      try? FileManager.default.removeItem(at: destination)
      throw error
    }
  }
}

public func requireRegularFile(_ url: URL) throws {
  let values = try url.resourceValues(forKeys: [.isRegularFileKey, .isSymbolicLinkKey])
  guard values.isRegularFile == true, values.isSymbolicLink != true else {
    throw MacletError("Expected a regular, non-symlink file: \(url.path)")
  }
}

public func writeJSON<T: Encodable>(_ value: T, to url: URL) throws {
  let encoder = JSONEncoder()
  encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
  try encoder.encode(value).write(to: url, options: .atomic)
}

// The descriptor stays open for the whole VM lifetime; a crash releases ownership.
public final class BundleLock {
  private let descriptor: Int32
  public init(_ directory: URL) throws {
    let descriptor = open(
      directory.appendingPathComponent("owner.lock").path,
      O_CREAT | O_RDWR | O_NOFOLLOW | O_CLOEXEC, 0o600)
    guard descriptor >= 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
    guard flock(descriptor, LOCK_EX | LOCK_NB) == 0 else {
      let code = errno
      close(descriptor)
      throw POSIXError(POSIXErrorCode(rawValue: code) ?? .EIO)
    }
    self.descriptor = descriptor
  }
  deinit {
    flock(descriptor, LOCK_UN)
    close(descriptor)
  }

  public static func isOwned(_ directory: URL) throws -> Bool {
    do {
      let lock = try BundleLock(directory)
      withExtendedLifetime(lock) {}
      return false
    } catch let error as POSIXError where error.code == .EWOULDBLOCK {
      return true
    }
  }
}

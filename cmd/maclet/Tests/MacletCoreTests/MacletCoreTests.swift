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
import Testing
import Virtualization

@testable import MacletCore

private func baseConfig() -> LumeConfig {
  // Virtual Mac hardware-model fixture, not a per-machine identity.
  let model = Data(
    base64Encoded:
      "YnBsaXN0MDDTAQIDBAQFXxAZRGF0YVJlcHJlc2VudGF0aW9uVmVyc2lvbl8QD1BsYXRmb3JtVmVyc2lvbl8QEk1pbmltdW1TdXBwb3J0ZWRPUxACowYHBxANEAAIDys9UlRYWgAAAAAAAAEBAAAAAAAAAAgAAAAAAAAAAAAAAAAAAABc"
  )!
  return LumeConfig(
    os: "macOS", cpuCount: 3, memorySize: 3 * 1024 * 1024 * 1024,
    hardwareModel: model, machineIdentifier: VZMacMachineIdentifier().dataRepresentation,
    macAddress: "02:13:24:35:46:57", networkMode: "nat")
}

private func temporaryDirectory() throws -> URL {
  let url = FileManager.default.temporaryDirectory.appendingPathComponent(
    "maclet-test-\(UUID().uuidString)")
  try FileManager.default.createDirectory(at: url, withIntermediateDirectories: false)
  return url
}

private func sourceBundle(in root: URL) throws -> URL {
  let source = root.appendingPathComponent("source")
  try FileManager.default.createDirectory(at: source, withIntermediateDirectories: false)
  try writeJSON(baseConfig(), to: source.appendingPathComponent("config.json"))
  try Data("disk-original".utf8).write(to: source.appendingPathComponent("disk.img"))
  try Data("nvram-original".utf8).write(to: source.appendingPathComponent("nvram.bin"))
  return source
}

@Test func durableVolumeConfigurationRequiresRealUniqueShares() throws {
  let root = try temporaryDirectory()
  defer { try? FileManager.default.removeItem(at: root) }
  let share = root.appendingPathComponent("share", isDirectory: true)
  try FileManager.default.createDirectory(at: share, withIntermediateDirectories: false)
  let config = root.appendingPathComponent("macletd-volumes.json")
  let volume = VMVolume(name: "data", mountPath: "/workspace", tag: "ate-data", hostPath: share.path)
  try writeJSON([volume], to: config)
  #expect(try VMVolume.read(from: root) == [volume])

  let duplicate = VMVolume(name: "cache", mountPath: "/cache", tag: "ate-data", hostPath: share.path)
  try writeJSON([volume, duplicate], to: config)
  #expect(throws: (any Error).self) { try VMVolume.read(from: root) }

  try FileManager.default.removeItem(at: config)
  #expect(try VMVolume.read(from: root).isEmpty)
}

@Test func coldSnapshotRestoreIsIndependentAndPersonalized() throws {
  let root = try temporaryDirectory()
  defer { try? FileManager.default.removeItem(at: root) }
  let original = try ActorBundle.create(
    source: sourceBundle(in: root), destination: root.appendingPathComponent("actor"),
    actorID: "original")
  let snapshot = root.appendingPathComponent("snapshot")
  try ActorBundle.snapshot(source: root.appendingPathComponent("actor"), destination: snapshot)
  let restored = try ActorBundle.restore(
    snapshot: snapshot, destination: root.appendingPathComponent("restored"), actorID: "clone")
  #expect(restored.actorID == "clone")
  #expect(restored.config.machineIdentifier != original.config.machineIdentifier)
  #expect(restored.config.macAddress != original.config.macAddress)
  try Data("changed".utf8).write(to: root.appendingPathComponent("restored/disk.img"))
  #expect(try Data(contentsOf: snapshot.appendingPathComponent("disk.img")) == Data("disk-original".utf8))
}

@Test func snapshotRejectsSymlinkAndExistingDestination() throws {
  let root = try temporaryDirectory()
  defer { try? FileManager.default.removeItem(at: root) }
  let actor = root.appendingPathComponent("actor")
  _ = try ActorBundle.create(source: sourceBundle(in: root), destination: actor, actorID: "actor")
  let destination = root.appendingPathComponent("snapshot")
  try FileManager.default.createDirectory(at: destination, withIntermediateDirectories: false)
  #expect(throws: (any Error).self) { try ActorBundle.snapshot(source: actor, destination: destination) }
  try FileManager.default.removeItem(at: destination)
  try FileManager.default.removeItem(at: actor.appendingPathComponent("disk.img"))
  try FileManager.default.createSymbolicLink(
    at: actor.appendingPathComponent("disk.img"), withDestinationURL: actor.appendingPathComponent("nvram.bin"))
  #expect(throws: (any Error).self) { try ActorBundle.snapshot(source: actor, destination: destination) }
}

@Test func personalizedConfigPreservesBootCompatibilityAndResources() throws {
  let base = baseConfig()
  let identity = VZMacMachineIdentifier().dataRepresentation
  let result = try base.personalized(machineIdentifier: identity, macAddress: "6A:09:08:07:06:05")
  #expect(result.hardwareModel == base.hardwareModel)
  #expect(result.cpuCount == 3)
  #expect(result.memorySize == 3_221_225_472)
  #expect(result.machineIdentifier == identity)
  #expect(result.macAddress == "6a:09:08:07:06:05")
  let decoded = try JSONDecoder().decode(LumeConfig.self, from: JSONEncoder().encode(result))
  #expect(decoded.machineIdentifier == identity)
  #expect(decoded.hardwareModel == base.hardwareModel)
  #expect(throws: (any Error).self) {
    try base.personalized(
      machineIdentifier: base.machineIdentifier, macAddress: "6a:09:08:07:06:05")
  }
  for mac in [base.macAddress, "03:09:08:07:06:05", "00:09:08:07:06:05", "invalid"] {
    #expect(throws: (any Error).self) {
      try base.personalized(machineIdentifier: identity, macAddress: mac)
    }
  }
  #expect(throws: (any Error).self) {
    try base.personalized(machineIdentifier: Data([1, 2, 3]), macAddress: "6a:09:08:07:06:05")
  }
}

@Test func rejectsInvalidConfig() throws {
  let base = baseConfig()
  let mutations: [(inout LumeConfig) -> Void] = [
    { $0.os = "linux" }, { $0.networkMode = "bridged" }, { $0.cpuCount = 0 },
    { $0.cpuCount = VZVirtualMachineConfiguration.maximumAllowedCPUCount + 1 },
    { $0.memorySize = 0 },
    { $0.memorySize = VZVirtualMachineConfiguration.maximumAllowedMemorySize + 1 },
    { $0.hardwareModel = Data() }, { $0.machineIdentifier = Data() }, { $0.macAddress = "bad" },
  ]
  for mutate in mutations {
    var config = base
    mutate(&config)
    #expect(throws: (any Error).self) { try config.validate() }
  }
}

@Test func cloneOwnsIndependentDiskNVRAMAndIdentity() throws {
  let root = try temporaryDirectory()
  defer { try? FileManager.default.removeItem(at: root) }
  let source = try sourceBundle(in: root)
  let destination = root.appendingPathComponent("actor")
  let base = try JSONDecoder().decode(
    LumeConfig.self, from: Data(contentsOf: source.appendingPathComponent("config.json")))
  let bundle = try ActorBundle.create(source: source, destination: destination, actorID: "actor-17")
  #expect(bundle.actorID == "actor-17")
  #expect(bundle.config.machineIdentifier != base.machineIdentifier)
  #expect(bundle.config.macAddress != base.macAddress)
  #expect(
    try ActorBundle.read(destination).config.machineIdentifier == bundle.config.machineIdentifier)
  for name in ["disk.img", "nvram.bin"] {
    let original = try Data(contentsOf: source.appendingPathComponent(name))
    #expect(try Data(contentsOf: destination.appendingPathComponent(name)) == original)
    let file = try FileHandle(forWritingTo: destination.appendingPathComponent(name))
    try file.write(contentsOf: Data("CHANGED".utf8))
    try file.close()
    #expect(try Data(contentsOf: source.appendingPathComponent(name)) == original)
  }
  #expect(throws: (any Error).self) {
    try ActorBundle.create(source: source, destination: destination, actorID: "replacement")
  }
  #expect(try ActorBundle.read(destination).actorID == "actor-17")
  #expect(throws: (any Error).self) {
    try ActorBundle.create(
      source: source, destination: source.appendingPathComponent("nested"), actorID: "bad")
  }
  #expect(!FileManager.default.fileExists(atPath: source.appendingPathComponent("nested").path))
}

@Test func refusesOpenOrSymlinkedSource() throws {
  let root = try temporaryDirectory()
  defer { try? FileManager.default.removeItem(at: root) }
  let source = try sourceBundle(in: root)
  let destination = root.appendingPathComponent("actor")
  let disk = source.appendingPathComponent("disk.img")
  let openDisk = try FileHandle(forReadingFrom: disk)
  #expect(throws: (any Error).self) {
    try ActorBundle.create(source: source, destination: destination, actorID: "live")
  }
  try openDisk.close()
  #expect(!FileManager.default.fileExists(atPath: destination.path))
  try FileManager.default.removeItem(at: disk)
  try FileManager.default.createSymbolicLink(
    at: disk, withDestinationURL: source.appendingPathComponent("nvram.bin"))
  #expect(throws: (any Error).self) {
    try ActorBundle.create(source: source, destination: destination, actorID: "symlink")
  }
}

@Test func lockAndStatusRejectStaleReady() throws {
  let root = try temporaryDirectory()
  defer { try? FileManager.default.removeItem(at: root) }
  let source = try sourceBundle(in: root)
  let destination = root.appendingPathComponent("actor")
  _ = try ActorBundle.create(source: source, destination: destination, actorID: "owned")
  try writeJSON(
    VMStatus(actorID: "owned", phase: "running", ready: true),
    to: destination.appendingPathComponent("status.json"))
  #expect(try !VMStatus.read(destination).ready)
  do {
    let lock = try BundleLock(destination)
    #expect(try BundleLock.isOwned(destination))
    #expect(throws: (any Error).self) { try BundleLock(destination) }
    #expect(try VMStatus.read(destination).ready)
    withExtendedLifetime(lock) {}
  }
  #expect(try !BundleLock.isOwned(destination))
  #expect(try VMStatus.read(destination).phase == "stopped")
  #expect(try !VMStatus.read(destination).ready)
}

@Test func leaseRequiresExactMACUnexpiredLeaseAndValidIP() {
  let leases = """
    {
    hw_address=1,2:a:b:c:d:e
    ip_address=192.168.64.23
    lease=0x1001
    }
    {
    hw_address=1,2:a:b:c:d:f
    ip_address=192.168.64.99
    lease=0x2000
    }
    {
    hw_address=1,2:a:b:c:d:e
    ip_address=192.168.64.24
    lease=0x1000
    }
    """
  let now = Date(timeIntervalSince1970: 4096)
  #expect(DHCPLeases.address(in: leases, mac: "02:0A:0B:0C:0D:0E", now: now) == "192.168.64.23")
  #expect(
    DHCPLeases.address(in: leases, mac: "02:0a:0b:0c:0d:0e", now: now.addingTimeInterval(1)) == nil)
  #expect(DHCPLeases.address(in: leases, mac: "02:0a:0b:0c:0d:00", now: now) == nil)
  #expect(
    DHCPLeases.address(
      in: leases.replacingOccurrences(of: "192.168.64.23", with: "192.168.64.999"),
      mac: "02:0a:0b:0c:0d:0e", now: now) == nil)
}

@Test func probeCannotRedirectAddressThroughPathAndOnlyAccepts2xx() throws {
  let probe = try HTTPProbe(port: 8123, path: "/guest/ready", timeout: 27)
  #expect(probe.url(ip: "192.168.64.23")?.absoluteString == "http://192.168.64.23:8123/guest/ready")
  #expect(probe.url(ip: "example.com") == nil)
  for code in [200, 204, 299] { #expect(HTTPProbe.accepts(statusCode: code)) }
  for code in [0, 199, 300, 302, 404, 503] { #expect(!HTTPProbe.accepts(statusCode: code)) }
  for path in ["", "ready", "/ready?ok=1", "/ready#fragment", "/has space", "/\\elsewhere"] {
    #expect(throws: (any Error).self) { try HTTPProbe(port: 8123, path: path, timeout: 27) }
  }
  for port in [0, 65536] {
    #expect(throws: (any Error).self) { try HTTPProbe(port: port, path: "/", timeout: 27) }
  }
  for timeout in [0.0, 3601.0, .infinity, .nan] {
    #expect(throws: (any Error).self) { try HTTPProbe(port: 8123, path: "/", timeout: timeout) }
  }
}

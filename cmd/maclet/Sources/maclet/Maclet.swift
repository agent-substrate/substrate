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
import MacletCore
import Virtualization

@main
struct Maclet {
  @MainActor
  static func main() async {
    do {
      let args = Array(CommandLine.arguments.dropFirst())
      guard let command = args.first else { throw usage }
      switch command {
      case "create" where args.count == 4 || args.count == 6:
        let resources = try resources(args)
        let bundle = try ActorBundle.create(
          source: directory(args[1]), destination: directory(args[2]), actorID: args[3], cpuCount: resources.0, memorySize: resources.1)
        try emit(bundle)
      case "start" where args.count == 5:
        guard let port = Int(args[2]), let timeout = Double(args[4]) else { throw usage }
        let probe = try HTTPProbe(port: port, path: args[3], timeout: timeout)
        try await HostVM(directory: directory(args[1])).run(probe: probe)
      case "status" where args.count == 2:
        try emit(VMStatus.read(directory(args[1])))
      case "stop" where args.count == 2:
        let directory = directory(args[1])
        let status = try VMStatus.read(directory)
        if status.phase != "stopped" {
          try writeJSON(status.runID, to: directory.appendingPathComponent("stop.json"))
          let deadline = ContinuousClock.now + .seconds(30)
          while try BundleLock.isOwned(directory) {
            guard ContinuousClock.now < deadline else {
              throw MacletError("Stop did not finish within 30 seconds")
            }
            try await Task.sleep(for: .milliseconds(200))
          }
        }
        try emit(VMStatus.read(directory))
      case "snapshot" where args.count == 3:
        try ActorBundle.snapshot(source: directory(args[1]), destination: directory(args[2]))
      case "restore" where args.count == 4 || args.count == 6:
        let resources = try resources(args)
        try emit(ActorBundle.restore(snapshot: directory(args[1]), destination: directory(args[2]), actorID: args[3], cpuCount: resources.0, memorySize: resources.1))
      default: throw usage
      }
    } catch {
      FileHandle.standardError.write(Data("maclet: \(error)\n".utf8))
      exit(1)
    }
  }

  static func directory(_ path: String) -> URL { URL(fileURLWithPath: path, isDirectory: true) }
  static func resources(_ args: [String]) throws -> (Int?, UInt64?) {
    if args.count == 4 { return (nil, nil) }
    guard let cpu = Int(args[4]), let memory = UInt64(args[5]), cpu > 0, memory > 0 else { throw usage }
    return (cpu, memory)
  }
  static var usage: MacletError {
    MacletError(
      "Usage: maclet create SOURCE_LUME_BUNDLE NEW_BUNDLE ACTOR_ID | start BUNDLE HTTP_PORT HTTP_PATH TIMEOUT_SECONDS | status BUNDLE | stop BUNDLE | snapshot BUNDLE SNAPSHOT | restore SNAPSHOT NEW_BUNDLE ACTOR_ID"
    )
  }
}

func emit<T: Encodable>(_ value: T) throws {
  let encoder = JSONEncoder()
  encoder.outputFormatting = [.sortedKeys]
  var data = try encoder.encode(value)
  data.append(0x0a)
  try FileHandle.standardOutput.write(contentsOf: data)
}

// Never follow redirects out of the selected guest's readiness endpoint.
final class NoRedirects: NSObject, URLSessionTaskDelegate {
  func urlSession(
    _ session: URLSession, task: URLSessionTask,
    willPerformHTTPRedirection response: HTTPURLResponse,
    newRequest request: URLRequest, completionHandler: @escaping (URLRequest?) -> Void
  ) {
    completionHandler(nil)
  }
}

// VZ invokes delegates on the VM's queue, which is the main queue here.
@MainActor
final class HostVM: NSObject, @preconcurrency VZVirtualMachineDelegate {
  private let directory: URL
  private let bundle: ActorBundle
  private let ownership: BundleLock
  private let machine: VZVirtualMachine
  private var status: VMStatus
  private var stopRequested = false
  private var guestFailure: String?

  init(directory: URL) throws {
    self.directory = directory
    bundle = try ActorBundle.read(directory)
    ownership = try BundleLock(directory)
    status = VMStatus(actorID: bundle.actorID)
    let config = bundle.config
    let platform = VZMacPlatformConfiguration()
    platform.hardwareModel = VZMacHardwareModel(dataRepresentation: config.hardwareModel)!
    platform.machineIdentifier = VZMacMachineIdentifier(
      dataRepresentation: config.machineIdentifier)!
    // Installed macOS needs its original boot data, copied into private storage.
    platform.auxiliaryStorage = VZMacAuxiliaryStorage(
      url: directory.appendingPathComponent("nvram.bin"))
    let vm = VZVirtualMachineConfiguration()
    vm.platform = platform
    vm.bootLoader = VZMacOSBootLoader()
    vm.cpuCount = config.cpuCount
    vm.memorySize = config.memorySize
    let disk = try VZDiskImageStorageDeviceAttachment(
      url: directory.appendingPathComponent("disk.img"), readOnly: false)
    vm.storageDevices = [VZVirtioBlockDeviceConfiguration(attachment: disk)]
    let network = VZVirtioNetworkDeviceConfiguration()
    network.macAddress = VZMACAddress(string: config.macAddress)!
    network.attachment = VZNATNetworkDeviceAttachment()
    vm.networkDevices = [network]

    let volumes = try VMVolume.read(from: directory)
    var directoryShares: [VZDirectorySharingDeviceConfiguration] = []
    let guestConfig = directory.appendingPathComponent("macletd-guest-config", isDirectory: true)
    if FileManager.default.fileExists(atPath: guestConfig.path) {
      let configShare = VZVirtioFileSystemDeviceConfiguration(tag: "ate-config")
      configShare.share = VZSingleDirectoryShare(
        directory: VZSharedDirectory(url: guestConfig, readOnly: true))
      directoryShares.append(configShare)
    }
    for volume in volumes {
      let share = VZVirtioFileSystemDeviceConfiguration(tag: volume.tag)
      share.share = VZSingleDirectoryShare(
        directory: VZSharedDirectory(
          url: URL(fileURLWithPath: volume.hostPath, isDirectory: true), readOnly: false))
      directoryShares.append(share)
    }
    vm.directorySharingDevices = directoryShares

    let graphics = VZMacGraphicsDeviceConfiguration()
    graphics.displays = [
      VZMacGraphicsDisplayConfiguration(widthInPixels: 1024, heightInPixels: 768, pixelsPerInch: 80)
    ]
    vm.graphicsDevices = [graphics]
    vm.keyboards = [VZUSBKeyboardConfiguration()]
    vm.pointingDevices = [VZUSBScreenCoordinatePointingDeviceConfiguration()]
    vm.entropyDevices = [VZVirtioEntropyDeviceConfiguration()]
    try vm.validate()
    machine = VZVirtualMachine(configuration: vm)
    super.init()
    machine.delegate = self
  }

  func guestDidStop(_ virtualMachine: VZVirtualMachine) { guestFailure = "Guest shut down" }
  func virtualMachine(_ virtualMachine: VZVirtualMachine, didStopWithError error: Error) {
    guestFailure = error.localizedDescription
  }

  func run(probe: HTTPProbe) async throws {
    let signals = [SIGINT, SIGTERM].map { number in
      signal(number, SIG_IGN)
      let source = DispatchSource.makeSignalSource(signal: number, queue: .main)
      source.setEventHandler { [weak self] in
        Task { @MainActor in self?.stopRequested = true }
      }
      source.resume()
      return source
    }
    defer {
      for source in signals { source.cancel() }
      withExtendedLifetime(ownership) {}
    }
    let sessionConfig = URLSessionConfiguration.ephemeral
    sessionConfig.connectionProxyDictionary = [:]
    sessionConfig.timeoutIntervalForRequest = 2
    sessionConfig.timeoutIntervalForResource = 3
    let session = URLSession(
      configuration: sessionConfig, delegate: NoRedirects(), delegateQueue: nil)
    defer { session.invalidateAndCancel() }
    do {
      try publish()
      try await machine.start()
      status.phase = "running"
      try publish()
      let deadline = ContinuousClock.now + .seconds(probe.timeout)
      while true {
        if let guestFailure { throw MacletError("Guest stopped: \(guestFailure)") }
        if stopRequested || requestedStop() { break }
        if !status.ready {
          guard ContinuousClock.now < deadline else {
            throw MacletError("Readiness timeout: \(status.detail)")
          }
          let leases = (try? String(contentsOfFile: "/var/db/dhcpd_leases", encoding: .utf8)) ?? ""
          status.ipAddress = DHCPLeases.address(in: leases, mac: bundle.config.macAddress)
          if let ip = status.ipAddress, let url = probe.url(ip: ip) {
            do {
              let (bytes, response) = try await session.bytes(from: url)
              bytes.task.cancel()
              let code = (response as? HTTPURLResponse)?.statusCode ?? 0
              status.ready = HTTPProbe.accepts(statusCode: code) && ContinuousClock.now < deadline
              status.detail = "GET \(url.absoluteString): HTTP \(code)"
            } catch { status.detail = "HTTP probe at \(ip) failed: \(error.localizedDescription)" }
          } else {
            status.detail = "Waiting for an unexpired DHCP lease for \(bundle.config.macAddress)"
          }
          try publish()
        }
        try await Task.sleep(for: .seconds(1))
      }
      status.ready = false
      status.phase = "stopping"
      try publish()
      let forced = try await shutdown()
      status.phase = "stopped"
      status.detail =
        forced ? "Forced stop after 15-second graceful shutdown deadline" : "Guest stopped"
      try publish()
    } catch {
      status.ready = false
      status.phase = "failed"
      status.detail = String(describing: error)
      do { _ = try await shutdown() } catch { status.detail += "; stop failed: \(error)" }
      try? publish()
      throw MacletError(status.detail)
    }
  }

  private func requestedStop() -> Bool {
    guard let data = try? Data(contentsOf: directory.appendingPathComponent("stop.json")),
      let runID = try? JSONDecoder().decode(UUID.self, from: data)
    else { return false }
    return runID == status.runID
  }

  private func shutdown() async throws -> Bool {
    if machine.state == .stopped { return false }
    if machine.canRequestStop { try? machine.requestStop() }
    let deadline = ContinuousClock.now + .seconds(15)
    while machine.state != .stopped && ContinuousClock.now < deadline {
      try await Task.sleep(for: .milliseconds(200))
    }
    if machine.state != .stopped {
      try await machine.stop()
      return true
    }
    return false
  }

  private func publish() throws {
    try writeJSON(status, to: directory.appendingPathComponent("status.json"))
    try emit(status)
  }
}

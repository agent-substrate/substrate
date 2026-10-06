// swift-tools-version: 6.0
// Copyright 2026 Google LLC
// SPDX-License-Identifier: Apache-2.0

import PackageDescription

let package = Package(
  name: "maclet",
  platforms: [.macOS(.v14)],
  products: [.executable(name: "maclet", targets: ["maclet"])],
  targets: [
    .target(name: "MacletCore"),
    .executableTarget(name: "maclet", dependencies: ["MacletCore"]),
    .testTarget(name: "MacletCoreTests", dependencies: ["MacletCore"]),
  ]
)

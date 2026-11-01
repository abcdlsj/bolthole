// swift-tools-version:5.9
import PackageDescription

// Root manifest so the repository can be added as a SwiftPM dependency.
// The sources live in swift/; swift/Package.swift is the same package for
// working inside that directory.
let package = Package(
    name: "Bolthole",
    platforms: [.iOS(.v15), .macOS(.v12), .tvOS(.v15), .watchOS(.v8), .visionOS(.v1)],
    products: [
        .library(name: "Bolthole", targets: ["Bolthole"]),
    ],
    dependencies: [
        // Only used off Apple platforms; iOS/macOS use CryptoKit.
        .package(url: "https://github.com/apple/swift-crypto.git", "3.0.0" ..< "5.0.0"),
    ],
    targets: [
        .target(
            name: "Bolthole",
            dependencies: [
                .product(name: "Crypto", package: "swift-crypto", condition: .when(platforms: [.linux, .windows, .android])),
            ],
            path: "swift/Sources/Bolthole"
        ),
        .testTarget(
            name: "BoltholeTests",
            dependencies: ["Bolthole"],
            path: "swift/Tests/BoltholeTests"
        ),
    ]
)

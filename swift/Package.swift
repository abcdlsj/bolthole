// swift-tools-version:5.9
import PackageDescription

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
            ]
        ),
        .testTarget(
            name: "BoltholeTests",
            dependencies: ["Bolthole"]
        ),
    ]
)

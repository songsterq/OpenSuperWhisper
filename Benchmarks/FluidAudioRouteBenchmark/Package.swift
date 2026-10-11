// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "FluidAudioRouteBenchmark",
    platforms: [.macOS(.v14)],
    dependencies: [
        .package(url: "https://github.com/FluidInference/FluidAudio.git", exact: "0.15.6")
    ],
    targets: [
        .executableTarget(
            name: "FluidAudioRouteBenchmark",
            dependencies: [.product(name: "FluidAudio", package: "FluidAudio")]
        )
    ]
)

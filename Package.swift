// swift-tools-version:5.3

import PackageDescription

let package = Package(
    name: "parse-connector",
    platforms: [
        .macOS(.v10_15),
    ],
    products: [
        .library(name: "ParseConnector", targets: ["ParseConnector"]),
    ],
    dependencies: [
        .package(url: "https://github.com/mongodb/mongo-swift-driver.git", from: "1.0.0"),
        .package(url: "https://github.com/SusanDoggie/DoggieDB.git", from: "0.0.1"),
    ],
    targets: [
        .target(
            name: "ParseConnector",
            dependencies: [
                .product(name: "MongoSwift", package: "mongo-swift-driver"),
                .product(name: "DoggieDB", package: "DoggieDB"),
            ]
        ),
    ]
)

import Foundation
import XCTest
@testable import Bolthole

/// Runs against a live Go/JS relay and agent. Set BOLTHOLE_CONN to an agent
/// started with `bolthole agent --http <upstream> --direct 127.0.0.1:0`
/// whose upstream answers GET /hello and echoes WebSocket messages
/// (js/test/interop.test.js has one). Skipped otherwise.
final class InteropTests: XCTestCase {
    var conn: String!

    override func setUpWithError() throws {
        guard let c = ProcessInfo.processInfo.environment["BOLTHOLE_CONN"] else {
            throw XCTSkip("BOLTHOLE_CONN not set")
        }
        conn = c
    }

    /// Linux libcurl usually lacks WebSocket support, so tests there use a
    /// small socket-based transport instead of URLSessionWebSocketTask.
    func options() -> BoltholeClient.Options {
        var o = BoltholeClient.Options()
        #if os(Linux)
        o.makeTransport = { POSIXWebSocketTransport(url: $0) }
        #endif
        return o
    }

    func testHTTPOverRelay() async throws {
        var o = options()
        o.mode = .relayOnly
        let c = try BoltholeClient(conn, options: o)
        let res = try await c.request("GET", "/hello")
        XCTAssertEqual(res.status, 200)
        let body = try await res.data()
        XCTAssertEqual(String(decoding: body, as: UTF8.self), "hello from node upstream")
        let path = await c.path
        XCTAssertEqual(path, "relay")

        let big = Data((0..<(1 << 20)).map { UInt8(truncatingIfNeeded: $0 * 7) })
        let echo = try await c.request("POST", "/echo", body: big)
        let back = try await echo.data()
        XCTAssertEqual(back, big)
        await c.close()
    }

    func testWebSocketThroughAgent() async throws {
        let c = try BoltholeClient(conn, options: options())
        let ws = try await c.webSocket("/live")
        try await ws.send(.text("hi"))
        let m = try await ws.receive()
        XCTAssertEqual(m, .text("echo:hi"))
        await ws.close()
        await c.close()
    }

    func testLearnsDirectPathAndUpgrades() async throws {
        let c = try BoltholeClient(conn, options: options())
        _ = try await c.request("GET", "/hello").data() // first connection: relay, learns direct candidates
        let cands = await c.directCandidates
        XCTAssertFalse(cands.isEmpty, "agent advertised no direct candidates")
        let upgraded = await c.tryUpgrade()
        XCTAssertTrue(upgraded)
        let path = await c.path
        XCTAssertTrue(path.hasPrefix("direct "), path)
        let res = try await c.request("GET", "/hello")
        XCTAssertEqual(res.status, 200)
        await c.close()
    }
}

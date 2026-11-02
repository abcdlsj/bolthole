import Foundation
import XCTest
@testable import Bolthole

/// Two connected in-memory transports.
final class PipeTransport: MessageTransport, @unchecked Sendable {
    let inbox: AsyncStream<Data>
    let inCont: AsyncStream<Data>.Continuation
    weak var peer: PipeTransport?
    var iterator: AsyncStream<Data>.Iterator

    init() {
        (inbox, inCont) = AsyncStream.makeStream(of: Data.self)
        iterator = inbox.makeAsyncIterator()
    }

    static func pair() -> (PipeTransport, PipeTransport) {
        let a = PipeTransport(), b = PipeTransport()
        a.peer = b
        b.peer = a
        return (a, b)
    }

    func read() async throws -> Data {
        guard let d = await iterator.next() else { throw BoltholeError.streamClosed }
        return d
    }

    func write(_ data: Data) async throws {
        guard let peer else { throw BoltholeError.streamClosed }
        peer.inCont.yield(data)
    }

    func close() {
        inCont.finish()
        peer?.inCont.finish()
    }
}

final class MuxTests: XCTestCase {
    func sessions() -> (MuxSession, MuxSession) {
        let (a, b) = PipeTransport.pair()
        return (MuxSession(a, config: MuxConfig(dialer: true)), MuxSession(b, config: MuxConfig(dialer: false)))
    }

    func testLargeEchoRespectsFlowControl() async throws {
        let (cli, srv) = sessions()
        Task {
            for await st in srv.incoming {
                try await st.accept(["hello": "world"])
                while let c = try await st.read() { try await st.write(c) }
                try await st.closeWrite()
            }
        }
        let st = try await cli.open(kind: "echo")
        let meta = try await st.accepted()
        XCTAssertEqual(String(decoding: meta, as: UTF8.self), #"{"hello":"world"}"#)
        var data = Data(count: 3 << 20)
        for i in 0..<data.count { data[i] = UInt8(truncatingIfNeeded: i * 31) }
        let payload = data
        Task {
            try await st.write(payload)
            try await st.closeWrite()
        }
        let got = try await st.readAll()
        XCTAssertEqual(got, data)
    }

    /// Streams the peer doesn't read share one session window; writers resume
    /// once it reads.
    func testSessionWindowCapsBufferedData() async throws {
        let (cli, srv) = sessions()
        let n = 24 // 24 × 4 MiB per-stream windows = 96 MiB > sessionWindow
        var outs: [MuxStream] = []
        for _ in 0..<n { outs.append(try await cli.open(kind: "sink")) }
        var ins: [MuxStream] = []
        for await st in srv.incoming {
            ins.append(st)
            if ins.count == n { break }
        }
        let data = Data(count: initialWindow)
        let writers = outs.map { st in
            Task {
                try await st.write(data)
                try await st.closeWrite()
            }
        }
        try await Task.sleep(nanoseconds: 500_000_000)
        let left = await cli.sendCredit
        XCTAssertEqual(left, 0, "writers should stall once the session window is used up")

        let readers = ins.map { st in Task { try await st.readAll().count } }
        for w in writers { try await w.value }
        for r in readers {
            let count = try await r.value
            XCTAssertEqual(count, data.count)
        }
    }

    func testResetPropagates() async throws {
        let (cli, srv) = sessions()
        Task { for await st in srv.incoming { await st.reset("not_found", "nope") } }
        let st = try await cli.open(kind: "x")
        do {
            try await st.accepted()
            XCTFail("expected reset")
        } catch BoltholeError.reset(let code, _) {
            XCTAssertEqual(code, "not_found")
        }
    }

    func testWebSocketRecords() async throws {
        let (cli, srv) = sessions()
        Task {
            for await st in srv.incoming {
                try await st.accept(meta: nil)
                let ws = WebSocketStream(st)
                while let m = try await ws.receive() { try await ws.send(m) }
                await ws.close()
            }
        }
        let st = try await cli.open(kind: "ws", meta: ["path": "/x"])
        try await st.accepted()
        let ws = WebSocketStream(st)
        try await ws.send(.text("hi"))
        try await ws.send(.data(Data([1, 2, 3])))
        let m1 = try await ws.receive()
        let m2 = try await ws.receive()
        XCTAssertEqual(m1, .text("hi"))
        XCTAssertEqual(m2, .data(Data([1, 2, 3])))
        await ws.close(code: 1000)
        let end = try await ws.receive()
        XCTAssertNil(end)
    }

    func testE2EOverPipe() async throws {
        let (a, b) = PipeTransport.pair()
        let psk = ConnectCode.random().derive().psk
        async let agent = agentHandshake(b, psk: psk) { _ in AgentHello(proto: protoMux, direct: ["ws://lan/.bolthole/e2e"]) }
        let (ch, hello) = try await clientHandshake(a, psk: psk, hello: ClientHello(name: "t"))
        let (ach, ahello) = try await agent
        XCTAssertEqual(hello.direct, ["ws://lan/.bolthole/e2e"])
        XCTAssertEqual(ahello.name, "t")
        var cfg = MuxConfig(dialer: false)
        cfg.maxFrame = maxRecord
        let srv = MuxSession(ach, config: cfg)
        Task {
            for await st in srv.incoming {
                try await st.accept(meta: nil)
                try await st.write(try await st.readAll())
                try await st.closeWrite()
            }
        }
        var ccfg = MuxConfig(dialer: true)
        ccfg.maxFrame = maxRecord
        let cli = MuxSession(ch, config: ccfg)
        let st = try await cli.open(kind: "echo")
        let msg = Data(repeating: 7, count: 200_000)
        try await st.write(msg)
        try await st.closeWrite()
        let got = try await st.readAll()
        XCTAssertEqual(got, msg)
    }
}

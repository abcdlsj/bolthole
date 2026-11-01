import Foundation
import XCTest
@testable import Bolthole
#if canImport(CryptoKit)
import CryptoKit
#else
import Crypto
#endif

/// Checks the Swift implementation byte for byte against spec/test-vectors.json,
/// which the Go implementation generates.
final class VectorTests: XCTestCase {
    struct Vectors: Decodable {
        struct Handshake: Decodable {
            var client_ephemeral_private, agent_ephemeral_private, client_hello, agent_hello: String
            var msg1, msg2, client_record_0, agent_record_0: String
        }
        struct F: Decodable { var hex: String; var type: UInt8; var stream_id: UInt32; var payload_hex: String }
        var code, routing_token, token_hash, psk, conn_string: String
        var handshake: Handshake
        var frames: [F]
    }

    static let vectors: Vectors = {
        let url = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
            .appendingPathComponent("../../../spec/test-vectors.json").standardized
        return try! JSONDecoder().decode(Vectors.self, from: Data(contentsOf: url))
    }()

    func testDerivation() throws {
        let v = Self.vectors
        let k = try ConnectCode(v.code).derive()
        XCTAssertEqual(k.routingToken, v.routing_token)
        XCTAssertEqual(k.tokenHash, v.token_hash)
        XCTAssertEqual(k.psk.hexString, v.psk)
        XCTAssertEqual(ConnString.format(relay: URL(string: "https://relay.example.com")!, code: try ConnectCode(v.code)), v.conn_string)
        let cs = try ConnString(v.conn_string)
        XCTAssertEqual(cs.relay.absoluteString, "https://relay.example.com")
        XCTAssertEqual(cs.code.string, v.code)
    }

    func testFrames() throws {
        for f in Self.vectors.frames {
            let enc = Frame(type: f.type, streamID: f.stream_id, payload: Data(hex: f.payload_hex)!).encode()
            XCTAssertEqual(enc.hexString, f.hex)
            let d = try Frame.decode(Data(hex: f.hex)!)
            XCTAssertEqual(d.type, f.type)
            XCTAssertEqual(d.streamID, f.stream_id)
        }
    }

    func testClientHandshake() async throws {
        let h = Self.vectors.handshake
        let psk = try ConnectCode(Self.vectors.code).derive().psk
        let t = ScriptedTransport(reads: [Data(hex: h.msg2)!])
        let eph = try Curve25519.KeyAgreement.PrivateKey(rawRepresentation: Data(hex: h.client_ephemeral_private)!)
        let (channel, hello) = try await clientHandshake(t, psk: psk, hello: ClientHello(protos: ["mux/1"], name: "vector-client"), ephemeral: eph)
        XCTAssertEqual(t.writes[0].hexString, h.msg1)
        XCTAssertEqual(hello.name, "vector-agent")
        try await channel.write(Data("ping".utf8))
        XCTAssertEqual(t.writes[1].hexString, h.client_record_0)
    }

    func testAgentHandshake() async throws {
        let h = Self.vectors.handshake
        let psk = try ConnectCode(Self.vectors.code).derive().psk
        let t = ScriptedTransport(reads: [Data(hex: h.msg1)!, Data(hex: h.client_record_0)!])
        let eph = try Curve25519.KeyAgreement.PrivateKey(rawRepresentation: Data(hex: h.agent_ephemeral_private)!)
        let (channel, hello) = try await agentHandshake(t, psk: psk, ephemeral: eph) { _ in AgentHello(proto: "mux/1", name: "vector-agent") }
        XCTAssertEqual(hello.name, "vector-client")
        XCTAssertEqual(t.writes[0].hexString, h.msg2)
        let ping = try await channel.read()
        XCTAssertEqual(String(decoding: ping, as: UTF8.self), "ping")
        try await channel.write(Data("pong".utf8))
        XCTAssertEqual(t.writes[1].hexString, h.agent_record_0)
    }

    func testWrongPSKRejected() async throws {
        let h = Self.vectors.handshake
        var psk = try ConnectCode(Self.vectors.code).derive().psk
        psk[psk.startIndex] ^= 1
        let t = ScriptedTransport(reads: [Data(hex: h.msg2)!])
        do {
            _ = try await clientHandshake(t, psk: psk, hello: ClientHello(protos: ["mux/1"], name: "vector-client"))
            XCTFail("handshake succeeded with a wrong PSK")
        } catch {}
    }
}

final class ScriptedTransport: MessageTransport, @unchecked Sendable {
    var reads: [Data]
    var writes: [Data] = []
    init(reads: [Data]) { self.reads = reads }
    func read() async throws -> Data {
        guard !reads.isEmpty else { throw BoltholeError.streamClosed }
        return reads.removeFirst()
    }
    func write(_ data: Data) async throws { writes.append(data) }
    func close() {}
}

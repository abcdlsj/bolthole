import Foundation
#if canImport(CryptoKit)
import CryptoKit
#else
import Crypto
#endif

/// A reliable, ordered message transport: a WebSocket, a `ws` stream, or
/// an encrypted channel.
public protocol MessageTransport: AnyObject, Sendable {
    func read() async throws -> Data
    func write(_ data: Data) async throws
    func close()
}

/// The application protocol that runs a mux session inside the channel.
public let protoMux = "mux/1"

/// Largest plaintext in one record (Noise caps messages at 65535 bytes).
public let maxRecord = 65535 - 16

private let protocolName = Data("Noise_NNpsk0_25519_AESGCM_SHA256".utf8)
private let prologue = Data("bolthole/v1".utf8)

/// Encrypted payload of handshake message 1.
public struct ClientHello: Codable, Sendable {
    public var protos: [String]
    public var name: String?
    public init(protos: [String] = [protoMux], name: String? = nil) {
        self.protos = protos
        self.name = name
    }
}

/// JSON string literal escaped the way Go's encoding/json does, so hellos
/// (and therefore handshake transcripts) are byte-identical across SDKs.
func jsonString(_ s: String) -> String {
    var out = "\""
    for u in s.unicodeScalars {
        switch u {
        case "\"": out += "\\\""
        case "\\": out += "\\\\"
        case "\n": out += "\\n"
        case "\r": out += "\\r"
        case "\t": out += "\\t"
        case "<", ">", "&", "\u{2028}", "\u{2029}":
            out += String(format: "\\u%04x", u.value)
        default:
            if u.value < 0x20 { out += String(format: "\\u%04x", u.value) } else { out.unicodeScalars.append(u) }
        }
    }
    return out + "\""
}

private func jsonArray(_ a: [String]) -> String { "[" + a.map(jsonString).joined(separator: ",") + "]" }

extension ClientHello {
    /// Canonical encoding: fields in declaration order, nil fields omitted.
    func canonicalJSON() -> Data {
        var parts = ["\"protos\":" + jsonArray(protos)]
        if let name { parts.append("\"name\":" + jsonString(name)) }
        return Data(("{" + parts.joined(separator: ",") + "}").utf8)
    }
}

extension AgentHello {
    func canonicalJSON() -> Data {
        var parts = ["\"proto\":" + jsonString(proto)]
        if let name { parts.append("\"name\":" + jsonString(name)) }
        if let direct, !direct.isEmpty { parts.append("\"direct\":" + jsonArray(direct)) }
        return Data(("{" + parts.joined(separator: ",") + "}").utf8)
    }
}

/// Encrypted payload of handshake message 2.
public struct AgentHello: Codable, Sendable {
    public var proto: String
    public var name: String?
    /// URLs where the agent can be reached without the relay.
    public var direct: [String]?
    public init(proto: String, name: String? = nil, direct: [String]? = nil) {
        self.proto = proto
        self.name = name
        self.direct = direct
    }
}

final class CipherState {
    let key: SymmetricKey?
    var n: UInt64 = 0

    init(_ key: Data?) { self.key = key.map { SymmetricKey(data: $0) } }

    private func nonce() throws -> AES.GCM.Nonce {
        var b = Data(count: 4)
        withUnsafeBytes(of: n.bigEndian) { b.append(contentsOf: $0) }
        n += 1
        return try AES.GCM.Nonce(data: b)
    }

    func encrypt(ad: Data, _ pt: Data) throws -> Data {
        guard let key else { return pt }
        let box = try AES.GCM.seal(pt, using: key, nonce: try nonce(), authenticating: ad)
        return box.ciphertext + box.tag
    }

    func decrypt(ad: Data, _ ct: Data) throws -> Data {
        guard let key else { return ct }
        guard ct.count >= 16 else { throw BoltholeError.decryptFailed }
        let box = try AES.GCM.SealedBox(nonce: try nonce(), ciphertext: ct.prefix(ct.count - 16), tag: ct.suffix(16))
        return try AES.GCM.open(box, using: key, authenticating: ad)
    }
}

private func hmac(_ key: Data, _ data: Data) -> Data {
    Data(HMAC<SHA256>.authenticationCode(for: data, using: SymmetricKey(data: key)))
}

private func noiseHKDF(_ ck: Data, _ ikm: Data, _ n: Int) -> [Data] {
    let temp = hmac(ck, ikm)
    var out: [Data] = []
    var prev = Data()
    for i in 1...n {
        prev = hmac(temp, prev + Data([UInt8(i)]))
        out.append(prev)
    }
    return out
}

final class SymmetricState {
    var h: Data
    var ck: Data
    var cs = CipherState(nil)

    init() {
        h = protocolName.count <= 32 ? protocolName + Data(count: 32 - protocolName.count) : Data(SHA256.hash(data: protocolName))
        ck = h
        mixHash(prologue)
    }

    func mixHash(_ d: Data) { h = Data(SHA256.hash(data: h + d)) }

    func mixKey(_ ikm: Data) {
        let o = noiseHKDF(ck, ikm, 2)
        ck = o[0]
        cs = CipherState(o[1])
    }

    func mixKeyAndHash(_ ikm: Data) {
        let o = noiseHKDF(ck, ikm, 3)
        ck = o[0]
        mixHash(o[1])
        cs = CipherState(o[2])
    }

    func encryptAndHash(_ pt: Data) throws -> Data {
        let ct = try cs.encrypt(ad: h, pt)
        mixHash(ct)
        return ct
    }

    func decryptAndHash(_ ct: Data) throws -> Data {
        let pt = try cs.decrypt(ad: h, ct)
        mixHash(ct)
        return pt
    }

    func split() -> (CipherState, CipherState) {
        let o = noiseHKDF(ck, Data(), 2)
        return (CipherState(o[0]), CipherState(o[1]))
    }
}

private func dh(_ priv: Curve25519.KeyAgreement.PrivateKey, _ pub: Data) throws -> Data {
    let p = try Curve25519.KeyAgreement.PublicKey(rawRepresentation: pub)
    return try priv.sharedSecretFromKeyAgreement(with: p).withUnsafeBytes { Data($0) }
}

/// Runs Noise_NNpsk0_25519_AESGCM_SHA256 as initiator (spec §5.2).
/// `ephemeral` is for tests only.
public func clientHandshake(
    _ t: MessageTransport, psk: Data, hello: ClientHello = ClientHello(),
    ephemeral: Curve25519.KeyAgreement.PrivateKey = .init()
) async throws -> (Channel, AgentHello) {
    let ss = SymmetricState()
    ss.mixKeyAndHash(psk)
    let e = ephemeral.publicKey.rawRepresentation
    ss.mixHash(e)
    ss.mixKey(e)
    try await t.write(e + ss.encryptAndHash(hello.canonicalJSON()))

    let msg2 = try await t.read()
    guard msg2.count >= 48 else { throw BoltholeError.handshakeFailed }
    let re = Data(msg2.prefix(32))
    let agentHello: AgentHello
    do {
        ss.mixHash(re)
        ss.mixKey(re)
        ss.mixKey(try dh(ephemeral, re))
        agentHello = try JSONDecoder().decode(AgentHello.self, from: try ss.decryptAndHash(Data(msg2.dropFirst(32))))
    } catch {
        throw BoltholeError.handshakeFailed
    }
    guard hello.protos.contains(agentHello.proto) else { throw BoltholeError.handshakeFailed }
    let (send, recv) = ss.split()
    return (Channel(t, send: send, recv: recv), agentHello)
}

/// Runs the responder side. `choose` returns the agent hello or throws to refuse.
public func agentHandshake(
    _ t: MessageTransport, psk: Data, ephemeral: Curve25519.KeyAgreement.PrivateKey = .init(),
    choose: (ClientHello) throws -> AgentHello
) async throws -> (Channel, ClientHello) {
    let ss = SymmetricState()
    let msg1 = try await t.read()
    guard msg1.count >= 48 else { throw BoltholeError.handshakeFailed }
    let re = Data(msg1.prefix(32))
    let hello: ClientHello
    do {
        ss.mixKeyAndHash(psk)
        ss.mixHash(re)
        ss.mixKey(re)
        hello = try JSONDecoder().decode(ClientHello.self, from: try ss.decryptAndHash(Data(msg1.dropFirst(32))))
    } catch {
        throw BoltholeError.handshakeFailed
    }
    let reply = try choose(hello)
    let e = ephemeral.publicKey.rawRepresentation
    ss.mixHash(e)
    ss.mixKey(e)
    ss.mixKey(try dh(ephemeral, re))
    try await t.write(e + ss.encryptAndHash(reply.canonicalJSON()))
    let (recv, send) = ss.split()
    return (Channel(t, send: send, recv: recv), hello)
}

/// The encrypted message channel: one Noise transport record per message.
/// Any record that fails to decrypt closes it.
public final class Channel: MessageTransport, @unchecked Sendable {
    private let inner: MessageTransport
    private let send: CipherState
    private let recv: CipherState
    private let lock = NSLock()

    init(_ inner: MessageTransport, send: CipherState, recv: CipherState) {
        self.inner = inner
        self.send = send
        self.recv = recv
    }

    public func read() async throws -> Data {
        let ct = try await inner.read()
        do {
            return try lock.withLock { try recv.decrypt(ad: Data(), ct) }
        } catch {
            inner.close()
            throw BoltholeError.decryptFailed
        }
    }

    /// Callers must not write concurrently (the mux serialises writes), so
    /// counters and transport order always match.
    public func write(_ data: Data) async throws {
        guard data.count <= maxRecord else { throw BoltholeError.recordTooLarge }
        let ct = try lock.withLock { try send.encrypt(ad: Data(), data) }
        try await inner.write(ct)
    }

    public func close() { inner.close() }
}

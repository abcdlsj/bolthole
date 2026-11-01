import Foundation
#if canImport(CryptoKit)
import CryptoKit
#else
import Crypto
#endif

/// Errors raised by this package.
public enum BoltholeError: Error, Equatable, CustomStringConvertible {
    case invalidCode
    case invalidConnString(String)
    case handshakeFailed
    case decryptFailed
    case recordTooLarge
    case protocolError(String)
    case reset(code: String, message: String)
    case sessionClosed(String)
    case goingAway
    case streamClosed
    case noPath
    case unreachable([String])

    public var description: String {
        switch self {
        case .invalidCode: return "bolthole: a connect code is 43 base64url characters"
        case .invalidConnString(let s): return "bolthole: bad connection string: \(s)"
        case .handshakeFailed: return "bolthole: e2e handshake failed"
        case .decryptFailed: return "bolthole: record failed authentication"
        case .recordTooLarge: return "bolthole: record too large"
        case .protocolError(let s): return "bolthole: protocol error: \(s)"
        case .reset(let code, let message): return message.isEmpty ? "stream reset: \(code)" : "stream reset: \(code): \(message)"
        case .sessionClosed(let s): return "bolthole: session closed: \(s)"
        case .goingAway: return "bolthole: peer is going away"
        case .streamClosed: return "bolthole: stream closed"
        case .noPath: return "bolthole: no path to the agent"
        case .unreachable(let errs): return "bolthole: could not reach the agent: " + errs.joined(separator: "; ")
        }
    }
}

/// A connect code: the one secret that grants access to an agent.
public struct ConnectCode: Equatable, Sendable {
    public let bytes: Data

    public init(bytes: Data) throws {
        guard bytes.count == 32 else { throw BoltholeError.invalidCode }
        self.bytes = Data(bytes)
    }

    public init(_ string: String) throws {
        guard let b = Data(base64URL: string.trimmingCharacters(in: .whitespacesAndNewlines)), b.count == 32 else {
            throw BoltholeError.invalidCode
        }
        bytes = b
    }

    /// A fresh random code.
    public static func random() -> ConnectCode {
        var g = SystemRandomNumberGenerator()
        return try! ConnectCode(bytes: Data((0..<32).map { _ in UInt8.random(in: 0...255, using: &g) }))
    }

    public var string: String { bytes.base64URL }

    /// Routing token, token hash and PSK (spec §1.1).
    public func derive() -> DerivedKeys {
        let ikm = SymmetricKey(data: bytes)
        let salt = Data("bolthole/v1".utf8)
        func expand(_ info: String, _ n: Int) -> Data {
            HKDF<SHA256>.deriveKey(inputKeyMaterial: ikm, salt: salt, info: Data(info.utf8), outputByteCount: n)
                .withUnsafeBytes { Data($0) }
        }
        let token = expand("routing-token", 24).base64URL
        return DerivedKeys(routingToken: token, tokenHash: DerivedKeys.hash(token: token), psk: expand("e2e-psk", 32))
    }
}

/// Everything derived from a code.
public struct DerivedKeys: Sendable {
    public let routingToken: String
    public let tokenHash: String
    public let psk: Data

    public static func hash(token: String) -> String {
        Data(SHA256.hash(data: Data(token.utf8))).hexString
    }
}

/// A connection string: "https://relay.example.com/#k=<code>[&name=…]".
public struct ConnString: Sendable {
    /// The relay base URL, without fragment or trailing slash.
    public let relay: URL
    public let code: ConnectCode
    public let name: String?

    public init(_ string: String) throws {
        guard var comps = URLComponents(string: string.trimmingCharacters(in: .whitespacesAndNewlines)),
              comps.scheme == "http" || comps.scheme == "https"
        else { throw BoltholeError.invalidConnString("must start with http(s)://") }
        var params: [String: String] = [:]
        for pair in (comps.fragment ?? "").split(separator: "&") {
            let kv = pair.split(separator: "=", maxSplits: 1).map(String.init)
            if kv.count == 2 { params[kv[0]] = kv[1].removingPercentEncoding ?? kv[1] }
        }
        guard let k = params["k"] else { throw BoltholeError.invalidConnString("missing #k=") }
        code = try ConnectCode(k)
        name = params["name"]
        comps.fragment = nil
        comps.query = nil
        while comps.path.hasSuffix("/") { comps.path.removeLast() }
        guard let u = comps.url else { throw BoltholeError.invalidConnString("bad URL") }
        relay = u
    }

    /// Renders relay + code as a connection string.
    public static func format(relay: URL, code: ConnectCode) -> String {
        var s = relay.absoluteString
        if let i = s.firstIndex(of: "#") { s = String(s[..<i]) }
        if !s.hasSuffix("/") { s += "/" }
        return s + "#k=" + code.string
    }
}

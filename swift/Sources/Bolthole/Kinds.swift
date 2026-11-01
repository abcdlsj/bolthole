import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

// MARK: - ws kind (spec §4.2)

/// A WebSocket message.
public enum WSMessage: Equatable, Sendable {
    case text(String)
    case data(Data)
}

/// WebSocket messages carried as records on a `ws` stream.
public actor WebSocketStream {
    public nonisolated let stream: MuxStream
    /// The subprotocol the agent selected.
    public nonisolated let `protocol`: String
    private var pending = Data()
    public private(set) var closeCode: Int?
    public private(set) var closeReason: String?

    public init(_ stream: MuxStream, protocol: String = "") {
        self.stream = stream
        self.protocol = `protocol`
    }

    private func need(_ n: Int) async throws -> Bool {
        while pending.count < n {
            guard let c = try await stream.read() else { return false }
            pending.append(c)
        }
        return true
    }

    /// The next message, or nil after the peer closed.
    public func receive() async throws -> WSMessage? {
        while true {
            guard try await need(5) else { return nil }
            let op = pending[pending.startIndex]
            let n = Int(pending.readU32(at: 1))
            guard n <= 16 << 20 else {
                await stream.reset("protocol_error", "ws record too large")
                throw BoltholeError.protocolError("ws record too large")
            }
            guard try await need(5 + n) else { return nil }
            let payload = Data(pending.dropFirst(5).prefix(n))
            pending = Data(pending.dropFirst(5 + n))
            switch op {
            case 1: return .text(String(decoding: payload, as: UTF8.self))
            case 2: return .data(payload)
            case 8:
                closeCode = payload.count >= 2 ? Int(payload[payload.startIndex]) << 8 | Int(payload[payload.startIndex + 1]) : 1005
                closeReason = String(decoding: payload.dropFirst(2), as: UTF8.self)
                return nil
            default: continue // unknown opcodes are skipped
            }
        }
    }

    public func send(_ m: WSMessage) async throws {
        let (op, p): (UInt8, Data) = {
            switch m {
            case .text(let s): return (1, Data(s.utf8))
            case .data(let d): return (2, d)
            }
        }()
        try await stream.write(Data([op]) + Data.u32(UInt32(p.count)) + p)
    }

    public func close(code: Int = 1000, reason: String = "") async {
        let r = Data(reason.utf8.prefix(123))
        let p = Data([UInt8(code >> 8), UInt8(code & 0xff)]) + r
        try? await stream.write(Data([8]) + Data.u32(UInt32(p.count)) + p)
        try? await stream.closeWrite()
    }
}

// MARK: - http kind (spec §4.1)

struct RequestMeta: Codable {
    var method: String
    var path: String
    var headers: [String: [String]]?
}

struct ResponseMeta: Codable {
    var status: Int
    var headers: [String: [String]]?
}

private let hopByHop: Set<String> = ["connection", "keep-alive", "transfer-encoding", "upgrade", "te", "trailer",
                                     "proxy-connection", "proxy-authenticate", "proxy-authorization", "host"]

/// An HTTP response whose body streams from the agent.
public struct StreamedResponse: Sendable {
    public let status: Int
    /// Lowercase names, all values.
    public let headers: [String: [String]]
    public let body: MuxStream

    public func header(_ name: String) -> String? { headers[name.lowercased()]?.first }

    /// Reads the whole body.
    public func data() async throws -> Data { try await body.readAll() }

    /// Lines of the body as they arrive (handy for Server-Sent Events).
    public func lines() -> AsyncThrowingStream<String, Error> {
        AsyncThrowingStream { cont in
            let task = Task {
                var buf = Data()
                do {
                    while let chunk = try await body.read() {
                        buf.append(chunk)
                        while let i = buf.firstIndex(of: 0x0A) {
                            cont.yield(String(decoding: buf[buf.startIndex..<i], as: UTF8.self))
                            buf = Data(buf[(i + 1)...])
                        }
                    }
                    if !buf.isEmpty { cont.yield(String(decoding: buf, as: UTF8.self)) }
                    cont.finish()
                } catch {
                    cont.finish(throwing: error)
                }
            }
            cont.onTermination = { _ in task.cancel(); Task { await body.close() } }
        }
    }
}

/// Performs one HTTP exchange over a new `http` stream.
func httpRequest(_ s: MuxSession, method: String, path: String, headers: [String: String], body: Data?) async throws -> StreamedResponse {
    var h: [String: [String]] = [:]
    for (k, v) in headers where !hopByHop.contains(k.lowercased()) { h[k.lowercased(), default: []].append(v) }
    if let body, !body.isEmpty { h["content-length"] = [String(body.count)] }
    let st = try await s.open(kind: "http", meta: RequestMeta(method: method, path: path, headers: h))
    let upload = Task {
        do {
            if let body, !body.isEmpty { try await st.write(body) }
            try await st.closeWrite()
        } catch {
            await st.reset("cancel")
        }
    }
    do {
        let raw = try await withTaskCancellationHandler {
            try await st.accepted()
        } onCancel: {
            Task { await st.reset("cancel") }
        }
        let meta = try JSONDecoder().decode(ResponseMeta.self, from: raw)
        return StreamedResponse(status: meta.status, headers: meta.headers ?? [:], body: st)
    } catch {
        upload.cancel()
        await st.reset("cancel")
        throw error
    }
}

// MARK: - URLSession WebSocket transport

/// A MessageTransport over URLSessionWebSocketTask.
public final class URLSessionWebSocketTransport: MessageTransport, @unchecked Sendable {
    let task: URLSessionWebSocketTask

    public init(url: URL, session: URLSession = .shared) {
        task = session.webSocketTask(with: url)
        task.maximumMessageSize = 2 << 20
        task.resume()
    }

    public func read() async throws -> Data {
        switch try await task.receive() {
        case .data(let d): return d
        case .string(let s): return Data(s.utf8)
        @unknown default: throw BoltholeError.protocolError("unknown websocket message")
        }
    }

    public func write(_ data: Data) async throws { try await task.send(.data(data)) }

    public func close() { task.cancel(with: .normalClosure, reason: nil) }
}

/// Adapts a `ws` stream to MessageTransport (to run E2E on top of it).
final class WSStreamTransport: MessageTransport, @unchecked Sendable {
    let ws: WebSocketStream
    init(_ ws: WebSocketStream) { self.ws = ws }
    func read() async throws -> Data {
        switch try await ws.receive() {
        case .data(let d): return d
        case .text(let s): return Data(s.utf8)
        case nil: throw BoltholeError.streamClosed
        }
    }
    func write(_ data: Data) async throws { try await ws.send(.data(data)) }
    func close() { Task { await ws.close(); await ws.stream.reset("cancel") } }
}

import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

/// Reaches an agent through a relay, or directly when it can, over an
/// end-to-end encrypted session.
///
///     let client = try BoltholeClient("https://relay.example.com/#k=…")
///     let res = try await client.request("GET", "/api/status")
///     print(res.status, String(decoding: try await res.data(), as: UTF8.self))
public actor BoltholeClient {
    /// Path selection.
    public enum Mode: Sendable { case auto, relayOnly, directOnly }

    public struct Options: Sendable {
        public var name: String?
        public var mode: Mode = .auto
        /// Extra direct candidates (ws://host:port/.bolthole/e2e).
        public var direct: [String] = []
        /// Head start for direct candidates over the relay.
        public var relayDelay: TimeInterval = 0.25
        public var directTimeout: TimeInterval = 2
        public var relayTimeout: TimeInterval = 30
        /// Makes the transport for a ws(s) URL. Defaults to URLSessionWebSocketTask.
        public var makeTransport: @Sendable (URL) -> MessageTransport = { URLSessionWebSocketTransport(url: $0) }
        /// Called when the active path changes ("relay" or "direct <url>").
        public var onPath: (@Sendable (String) -> Void)?
        /// Streams the agent opens toward this client.
        public var onStream: (@Sendable (MuxStream) async -> Void)?
        public init() {}
    }

    public static let e2ePath = "/.bolthole/e2e"

    public nonisolated let connString: ConnString
    private let keys: DerivedKeys
    private var options: Options
    private var current: (session: MuxSession, hello: AgentHello, path: String)?
    private var dialing: Task<(session: MuxSession, hello: AgentHello, path: String), Error>?
    /// Direct candidates: configured plus those the agent advertised.
    public private(set) var directCandidates: [String]
    /// The active path, or "" when idle.
    public private(set) var path = ""

    public init(_ connString: String, options: Options = Options()) throws {
        self.connString = try ConnString(connString)
        keys = self.connString.code.derive()
        self.options = options
        directCandidates = options.direct
    }

    /// The live session, dialing (or redialing) if needed.
    public func session() async throws -> MuxSession {
        if let c = current, await !c.session.isClosed, await !c.session.remoteGoAway { return c.session }
        if let d = dialing { return try await d.value.session }
        let cands = candidates()
        let t = Task { try await self.race(cands) }
        dialing = t
        defer { dialing = nil }
        let c = try await t.value
        install(c)
        return c.session
    }

    struct Candidate: Sendable {
        var path: String
        var url: URL
        var delay: TimeInterval
        var timeout: TimeInterval
    }

    private func candidates() -> [Candidate] {
        var out: [Candidate] = []
        if options.mode != .relayOnly {
            for u in directCandidates { if let url = URL(string: u) { out.append(Candidate(path: "direct " + u, url: url, delay: 0, timeout: options.directTimeout)) } }
        }
        if options.mode != .directOnly {
            var comps = URLComponents(url: connString.relay, resolvingAgainstBaseURL: false)!
            comps.scheme = comps.scheme == "https" ? "wss" : "ws"
            comps.path += "/c/\(keys.routingToken)\(Self.e2ePath)"
            out.append(Candidate(path: "relay", url: comps.url!, delay: out.isEmpty ? 0 : options.relayDelay, timeout: options.relayTimeout))
        }
        return out
    }

    private nonisolated func race(_ cands: [Candidate]) async throws -> (session: MuxSession, hello: AgentHello, path: String) {
        guard !cands.isEmpty else { throw BoltholeError.noPath }
        let opts = await options
        let psk = keys.psk
        return try await withThrowingTaskGroup(of: (session: MuxSession, hello: AgentHello, path: String)?.self) { group in
            var errors: [String] = []
            for c in cands {
                group.addTask {
                    if c.delay > 0 { try? await Task.sleep(nanoseconds: UInt64(c.delay * 1e9)) }
                    if Task.isCancelled { return nil }
                    do {
                        return try await Self.dial(c, psk: psk, opts: opts)
                    } catch {
                        throw RaceError(path: c.path, error: error)
                    }
                }
            }
            var winner: (session: MuxSession, hello: AgentHello, path: String)?
            while let next = await Result(catching: { try await group.next() }).asOptional() {
                switch next {
                case .success(let r?):
                    if winner == nil {
                        winner = r
                        group.cancelAll()
                    } else {
                        await r.session.close()
                    }
                case .success(nil):
                    break
                case .failure(let e):
                    errors.append((e as? RaceError).map { "\($0.path): \($0.error)" } ?? "\(e)")
                }
            }
            if let winner { return winner }
            throw BoltholeError.unreachable(errors)
        }
    }

    private static func dial(_ c: Candidate, psk: Data, opts: Options) async throws -> (session: MuxSession, hello: AgentHello, path: String) {
        let raw = opts.makeTransport(c.url)
        let timeout = Task {
            try await Task.sleep(nanoseconds: UInt64(c.timeout * 1e9))
            raw.close()
        }
        defer { timeout.cancel() }
        do {
            let (channel, hello) = try await withTaskCancellationHandler {
                try await clientHandshake(raw, psk: psk, hello: ClientHello(protos: [protoMux], name: opts.name))
            } onCancel: {
                raw.close()
            }
            var cfg = MuxConfig(dialer: true)
            cfg.maxFrame = maxRecord
            let session = MuxSession(channel, config: cfg)
            if let onStream = opts.onStream {
                Task {
                    for await st in session.incoming { Task { await onStream(st) } }
                }
            } else {
                Task {
                    for await st in session.incoming { await st.reset("unsupported_kind") }
                }
            }
            return (session, hello, c.path)
        } catch {
            raw.close()
            throw error
        }
    }

    private func install(_ c: (session: MuxSession, hello: AgentHello, path: String)) {
        let old = current
        current = c
        path = c.path
        for u in c.hello.direct ?? [] where !directCandidates.contains(u) { directCandidates.append(u) }
        if let old {
            Task {
                await old.session.goAway(code: "going_away", message: "client switched path")
                for _ in 0..<240 { // let in-flight streams finish (up to 2 minutes)
                    if await old.session.streamCount == 0 { break }
                    try? await Task.sleep(nanoseconds: 500_000_000)
                }
                await old.session.close()
            }
        }
        options.onPath?(c.path)
    }

    /// Tries direct candidates now and switches to one if it answers.
    /// Call it when the network changes (e.g. from NWPathMonitor).
    @discardableResult
    public func tryUpgrade() async -> Bool {
        guard path == "relay", !directCandidates.isEmpty, options.mode != .relayOnly else { return false }
        let cands = directCandidates.compactMap { u in URL(string: u).map { Candidate(path: "direct " + u, url: $0, delay: 0, timeout: options.directTimeout) } }
        guard let c = try? await race(cands) else { return false }
        install(c)
        return true
    }

    /// Forces the next request to re-race all paths (e.g. after leaving Wi-Fi).
    public func reconnect() async {
        if let c = current { await c.session.close() }
        current = nil
    }

    /// Pins or unpins the path.
    public func setMode(_ mode: Mode) async {
        options.mode = mode
        await reconnect()
    }

    public func close() async {
        if let c = current { await c.session.close() }
        current = nil
    }

    // MARK: Conveniences

    /// One HTTP request to the agent; the body streams.
    public func request(_ method: String, _ path: String, headers: [String: String] = [:], body: Data? = nil) async throws -> StreamedResponse {
        try await httpRequest(try await session(), method: method, path: path, headers: headers, body: body)
    }

    /// URLRequest flavour: only the URL's path and query are used.
    public func data(for req: URLRequest) async throws -> (Data, HTTPURLResponse) {
        let u = req.url!
        var p = u.path.isEmpty ? "/" : u.path
        if let q = u.query { p += "?" + q }
        let res = try await request(req.httpMethod ?? "GET", p, headers: req.allHTTPHeaderFields ?? [:], body: req.httpBody)
        let body = try await res.data()
        var flat: [String: String] = [:]
        for (k, v) in res.headers { flat[k] = v.joined(separator: ", ") }
        return (body, HTTPURLResponse(url: u, statusCode: res.status, httpVersion: "HTTP/1.1", headerFields: flat)!)
    }

    /// Opens a WebSocket to `path` on the agent.
    public func webSocket(_ path: String, protocols: [String] = []) async throws -> WebSocketStream {
        struct Meta: Encodable { var path: String; var protocols: [String] }
        struct Accept: Decodable { var `protocol`: String? }
        let st = try await session().open(kind: "ws", meta: Meta(path: path, protocols: protocols))
        let raw = try await st.accepted()
        let p = (try? JSONDecoder().decode(Accept.self, from: raw))?.protocol ?? ""
        return WebSocketStream(st, protocol: p)
    }

    /// Opens a raw byte stream to a named TCP target on the agent.
    public func tcp(_ target: String = "") async throws -> MuxStream {
        let st = try await session().open(kind: "tcp", meta: ["target": target])
        try await st.accepted()
        return st
    }

    /// Opens a stream of your own kind and waits for the agent to accept.
    public func open<M: Encodable>(kind: String, meta: M) async throws -> (MuxStream, Data) {
        let st = try await session().open(kind: kind, meta: meta)
        let acc = try await st.accepted()
        return (st, acc)
    }
}

struct RaceError: Error {
    var path: String
    var error: Error
}

extension Result where Failure == Error {
    init(catching body: () async throws -> Success) async {
        do { self = .success(try await body()) } catch { self = .failure(error) }
    }
}

extension Result where Success: OptionalProtocol {
    /// nil when the group is exhausted.
    func asOptional() -> Result<Success.Wrapped, Failure>? {
        switch self {
        case .success(let v): return v.wrapped.map { .success($0) }
        case .failure(let e): return .failure(e)
        }
    }
}

protocol OptionalProtocol { associatedtype Wrapped; var wrapped: Wrapped? { get } }
extension Optional: OptionalProtocol { var wrapped: Wrapped? { self } }

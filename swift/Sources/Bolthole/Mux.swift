import Foundation

/// Frame types (spec §3.1).
public enum FrameType: UInt8, Sendable {
    case open = 1, accept, data, fin, reset, window, ping, pong, goAway
}

public struct Frame: Sendable {
    public var type: UInt8
    public var streamID: UInt32
    public var payload: Data

    public init(type: UInt8, streamID: UInt32, payload: Data = Data()) {
        self.type = type
        self.streamID = streamID
        self.payload = payload
    }

    public func encode() -> Data {
        var d = Data([type])
        d.append(Data.u32(streamID))
        d.append(payload)
        return d
    }

    public static func decode(_ d: Data) throws -> Frame {
        guard d.count >= 5 else { throw BoltholeError.protocolError("short frame") }
        return Frame(type: d[d.startIndex], streamID: d.readU32(at: 1), payload: Data(d.dropFirst(5)))
    }
}

let initialWindow = 4 * 1024 * 1024
let maxChunk = 32 * 1024

struct ResetPayload: Codable {
    var code: String
    var message: String?
}

/// Session configuration.
public struct MuxConfig: Sendable {
    /// True for the side that dialed the transport (odd stream ids).
    public var dialer: Bool
    public var heartbeat: TimeInterval = 20
    public var maxStreams = 256
    public var maxFrame = (1 << 20) + 5
    public init(dialer: Bool) { self.dialer = dialer }
}

/// A multiplexed session over a MessageTransport (spec §3).
public actor MuxSession {
    let transport: MessageTransport
    let config: MuxConfig
    private var streams: [UInt32: MuxStream] = [:]
    private var nextID: UInt32
    public private(set) var isClosed = false
    public private(set) var closeError: Error?
    public private(set) var remoteGoAway = false
    private var localGoAway = false
    private var lastRecv = Date()
    private var lastSend = Date()
    private var doneWaiters: [CheckedContinuation<Void, Never>] = []

    /// Streams the peer opens.
    public nonisolated let incoming: AsyncStream<MuxStream>
    private let incomingCont: AsyncStream<MuxStream>.Continuation

    // Writes go through one task so frames hit the transport in call order.
    private let writes: AsyncStream<(Data, CheckedContinuation<Void, Error>?)>.Continuation
    private var tasks: [Task<Void, Never>] = []

    public init(_ transport: MessageTransport, config: MuxConfig) {
        self.transport = transport
        self.config = config
        nextID = config.dialer ? 1 : 2
        (incoming, incomingCont) = AsyncStream.makeStream(of: MuxStream.self)
        let (ws, wc) = AsyncStream.makeStream(of: (Data, CheckedContinuation<Void, Error>?).self)
        writes = wc
        Task { await self.start(ws) }
    }

    private func start(_ ws: AsyncStream<(Data, CheckedContinuation<Void, Error>?)>) {
        let t = transport
        tasks.append(Task { [weak self] in
            for await (frame, cont) in ws {
                do {
                    try await t.write(frame)
                    cont?.resume()
                    await self?.noteSend()
                } catch {
                    cont?.resume(throwing: error)
                    await self?.close(error)
                }
            }
        })
        tasks.append(Task { [weak self] in
            while true {
                do {
                    let msg = try await t.read()
                    guard let self else { return }
                    try await self.receive(msg)
                } catch {
                    await self?.close(error)
                    return
                }
            }
        })
        if config.heartbeat > 0 {
            let hb = config.heartbeat
            tasks.append(Task { [weak self] in
                while !Task.isCancelled {
                    try? await Task.sleep(nanoseconds: UInt64(hb / 4 * 1e9))
                    guard let self, await self.tick() else { return }
                }
            })
        }
    }

    private func noteSend() { lastSend = Date() }

    private func tick() -> Bool {
        if isClosed { return false }
        if Date().timeIntervalSince(lastRecv) > 3 * config.heartbeat {
            close(BoltholeError.sessionClosed("peer silent for too long"))
            return false
        }
        if Date().timeIntervalSince(lastSend) >= config.heartbeat {
            enqueue(Frame(type: FrameType.ping.rawValue, streamID: 0, payload: Data(count: 8)))
        }
        return true
    }

    /// Fire-and-forget write (control frames).
    func enqueue(_ f: Frame) {
        guard !isClosed else { return }
        writes.yield((f.encode(), nil))
    }

    /// Write and wait until it reached the transport.
    func send(_ f: Frame) async throws {
        if isClosed { throw closeError ?? BoltholeError.sessionClosed("closed") }
        let data = f.encode()
        try await withCheckedThrowingContinuation { (c: CheckedContinuation<Void, Error>) in
            writes.yield((data, c))
        }
    }

    /// Ends the session; every stream fails.
    public func close(_ error: Error = BoltholeError.sessionClosed("closed")) {
        guard !isClosed else { return }
        isClosed = true
        closeError = error
        transport.close()
        writes.finish()
        incomingCont.finish()
        for t in tasks { t.cancel() }
        let all = streams.values
        streams.removeAll()
        for st in all { Task { await st.terminate(error) } }
        for w in doneWaiters { w.resume() }
        doneWaiters.removeAll()
    }

    /// Waits until the session ends.
    public func done() async {
        if isClosed { return }
        await withCheckedContinuation { doneWaiters.append($0) }
    }

    public var streamCount: Int { streams.count }

    /// Asks the peer to open no new streams.
    public func goAway(code: String = "going_away", message: String = "") {
        guard !localGoAway else { return }
        localGoAway = true
        enqueue(Frame(type: FrameType.goAway.rawValue, streamID: 0, payload: (try? JSONEncoder().encode(ResetPayload(code: code, message: message))) ?? Data()))
    }

    /// Opens a stream. It does not wait for ACCEPT; use `stream.accepted()`.
    public func open(kind: String, rawMeta meta: Data? = nil) throws -> MuxStream {
        if isClosed { throw closeError ?? BoltholeError.sessionClosed("closed") }
        if remoteGoAway { throw BoltholeError.goingAway }
        let id = nextID
        nextID += 2
        var payload = Data("{\"kind\":".utf8) + (try JSONEncoder().encode(kind))
        if let meta { payload += Data(",\"meta\":".utf8) + meta }
        payload += Data("}".utf8)
        guard payload.count + 5 <= config.maxFrame else { throw BoltholeError.protocolError("open meta too large") }
        let st = MuxStream(session: self, id: id, kind: kind, meta: meta, outbound: true)
        streams[id] = st
        enqueue(Frame(type: FrameType.open.rawValue, streamID: id, payload: payload))
        return st
    }

    /// Opens a stream with Encodable meta.
    public func open<M: Encodable>(kind: String, meta: M) throws -> MuxStream {
        try open(kind: kind, rawMeta: try JSONEncoder().encode(meta))
    }

    func remove(_ id: UInt32) { streams[id] = nil }

    private func isPeerID(_ id: UInt32) -> Bool { id != 0 && ((id % 2 == 1) != config.dialer) }

    private func receive(_ msg: Data) async throws {
        lastRecv = Date()
        guard msg.count <= config.maxFrame else { throw BoltholeError.protocolError("frame too large") }
        let f = try Frame.decode(msg)
        guard let type = FrameType(rawValue: f.type) else { return } // unknown types are ignored
        switch type {
        case .ping:
            enqueue(Frame(type: FrameType.pong.rawValue, streamID: 0, payload: f.payload))
        case .pong:
            break
        case .goAway:
            remoteGoAway = true
        case .open:
            guard isPeerID(f.streamID), streams[f.streamID] == nil else { throw BoltholeError.protocolError("bad open") }
            struct Open: Decodable { var kind: String }
            guard let op = try? JSONDecoder().decode(Open.self, from: f.payload), !op.kind.isEmpty else {
                enqueue(resetFrame(f.streamID, "protocol_error"))
                return
            }
            if localGoAway { enqueue(resetFrame(f.streamID, "going_away")); return }
            if streams.count >= config.maxStreams { enqueue(resetFrame(f.streamID, "refused")); return }
            let st = MuxStream(session: self, id: f.streamID, kind: op.kind, meta: rawMeta(f.payload), outbound: false)
            streams[f.streamID] = st
            incomingCont.yield(st)
        case .accept, .data, .fin, .reset, .window:
            guard let st = streams[f.streamID] else {
                if type == .accept || type == .data { enqueue(resetFrame(f.streamID, "unknown_stream")) }
                return
            }
            await st.handle(type, f.payload)
        }
    }

    func resetFrame(_ id: UInt32, _ code: String, _ message: String = "") -> Frame {
        Frame(type: FrameType.reset.rawValue, streamID: id, payload: (try? JSONEncoder().encode(ResetPayload(code: code, message: message))) ?? Data())
    }
}

/// Extracts the raw "meta" JSON from an OPEN payload.
private func rawMeta(_ payload: Data) -> Data? {
    guard let obj = try? JSONSerialization.jsonObject(with: payload) as? [String: Any], let m = obj["meta"] else { return nil }
    return try? JSONSerialization.data(withJSONObject: m, options: [.fragmentsAllowed])
}

/// One bidirectional byte stream.
public actor MuxStream {
    public nonisolated let id: UInt32
    public nonisolated let kind: String
    /// Raw OPEN meta (JSON), if any.
    public nonisolated let meta: Data?
    let outbound: Bool
    let session: MuxSession

    private var buf: [Data] = []
    private var recvFin = false
    private var recvCredit = initialWindow
    private var unacked = 0
    private var sendCredit = initialWindow
    private var localFin = false
    private var isAccepted = false
    private var acceptMeta: Data?
    private var failure: Error?
    private var finished = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    init(session: MuxSession, id: UInt32, kind: String, meta: Data?, outbound: Bool) {
        self.session = session
        self.id = id
        self.kind = kind
        self.meta = meta
        self.outbound = outbound
    }

    /// Decodes the OPEN meta.
    public nonisolated func decodeMeta<T: Decodable>(_ type: T.Type) throws -> T {
        try JSONDecoder().decode(T.self, from: meta ?? Data("{}".utf8))
    }

    private func wake() {
        let w = waiters
        waiters.removeAll()
        for c in w { c.resume() }
    }

    private func waitForChange() async {
        await withCheckedContinuation { waiters.append($0) }
    }

    /// Answers an inbound OPEN.
    public func accept(meta: Data? = nil) async throws {
        guard !outbound, !isAccepted else { return }
        if let failure { throw failure }
        isAccepted = true
        try await session.send(Frame(type: FrameType.accept.rawValue, streamID: id, payload: meta ?? Data()))
    }

    public func accept<M: Encodable>(_ meta: M) async throws {
        try await accept(meta: try JSONEncoder().encode(meta))
    }

    /// Waits for the peer's ACCEPT and returns its raw meta.
    @discardableResult
    public func accepted() async throws -> Data {
        while true {
            if isAccepted { return acceptMeta ?? Data() }
            if let failure { throw failure }
            await waitForChange()
        }
    }

    /// Next chunk, or nil at end of stream.
    public func read() async throws -> Data? {
        while true {
            if !buf.isEmpty {
                let c = buf.removeFirst()
                unacked += c.count
                if unacked >= initialWindow / 2, !recvFin, failure == nil {
                    let credit = unacked
                    unacked = 0
                    recvCredit += credit
                    await session.enqueue(Frame(type: FrameType.window.rawValue, streamID: id, payload: .u32(UInt32(credit))))
                }
                return c
            }
            if let failure { throw failure }
            if recvFin { return nil }
            await waitForChange()
        }
    }

    /// Everything until the peer's FIN.
    public func readAll() async throws -> Data {
        var out = Data()
        while let c = try await read() { out.append(c) }
        return out
    }

    /// Writes, waiting for flow-control credit as needed.
    public func write(_ data: Data) async throws {
        if !outbound, !isAccepted { try await accept() }
        var off = data.startIndex
        while off < data.endIndex {
            if let failure { throw failure }
            if localFin { throw BoltholeError.streamClosed }
            if sendCredit <= 0 { await waitForChange(); continue }
            let n = min(data.endIndex - off, maxChunk, sendCredit)
            sendCredit -= n
            try await session.send(Frame(type: FrameType.data.rawValue, streamID: id, payload: Data(data[off..<off + n])))
            off += n
        }
    }

    /// Half-close: no more data from this side.
    public func closeWrite() async throws {
        if localFin || failure != nil { return }
        if !outbound, !isAccepted { try await accept() }
        localFin = true
        wake()
        try await session.send(Frame(type: FrameType.fin.rawValue, streamID: id))
        await maybeFinish()
    }

    /// Done with the stream; resets it if the peer is still sending.
    public func close() async {
        if !recvFin, failure == nil { await reset("cancel"); return }
        try? await closeWrite()
    }

    /// Aborts the stream both ways.
    public func reset(_ code: String = "cancel", _ message: String = "") async {
        if failure != nil || finished { return }
        await session.enqueue(session.resetFrame(id, code, message))
        await terminate(BoltholeError.reset(code: code, message: message))
    }

    func terminate(_ error: Error) async {
        if failure == nil, !finished { failure = error }
        finished = true
        wake()
        await session.remove(id)
    }

    private func maybeFinish() async {
        if localFin, recvFin, !finished {
            finished = true
            wake()
            await session.remove(id)
        }
    }

    func handle(_ type: FrameType, _ payload: Data) async {
        switch type {
        case .accept:
            if outbound, !isAccepted {
                isAccepted = true
                acceptMeta = payload
                wake()
            }
        case .data:
            if recvFin || failure != nil { return }
            if payload.count > recvCredit { await reset("flow_control", "window exceeded"); return }
            recvCredit -= payload.count
            buf.append(payload)
            wake()
        case .fin:
            recvFin = true
            wake()
            await maybeFinish()
        case .reset:
            let r = try? JSONDecoder().decode(ResetPayload.self, from: payload)
            await terminate(BoltholeError.reset(code: r?.code ?? "internal", message: r?.message ?? ""))
        case .window:
            guard payload.count == 4 else { await reset("protocol_error", "bad window"); return }
            sendCredit += Int(payload.readU32(at: 0))
            wake()
        default:
            break
        }
    }
}

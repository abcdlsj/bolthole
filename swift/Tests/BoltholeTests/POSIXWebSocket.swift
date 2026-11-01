#if os(Linux)
import Foundation
import Glibc
@testable import Bolthole

/// A minimal blocking ws:// client on POSIX sockets, for running the
/// interop tests on Linux, where URLSessionWebSocketTask needs a libcurl
/// built with WebSocket support. Apps use URLSessionWebSocketTransport.
final class POSIXWebSocketTransport: MessageTransport, @unchecked Sendable {
    private var fd: Int32 = -1
    private let url: URL
    private let readQueue = DispatchQueue(label: "ws.read")
    private let writeQueue = DispatchQueue(label: "ws.write")
    private var opened = false
    private let lock = NSLock()
    private var buffer = Data()

    init(url: URL) { self.url = url }

    private func ensureOpen() throws {
        lock.lock()
        defer { lock.unlock() }
        if opened { return }
        opened = true
        let host = url.host ?? "127.0.0.1"
        let port = url.port ?? 80
        var hints = addrinfo(ai_flags: 0, ai_family: AF_INET, ai_socktype: Int32(SOCK_STREAM.rawValue), ai_protocol: 0,
                             ai_addrlen: 0, ai_addr: nil, ai_canonname: nil, ai_next: nil)
        var res: UnsafeMutablePointer<addrinfo>?
        guard getaddrinfo(host, String(port), &hints, &res) == 0, let ai = res else { throw BoltholeError.noPath }
        defer { freeaddrinfo(res) }
        fd = socket(ai.pointee.ai_family, ai.pointee.ai_socktype, ai.pointee.ai_protocol)
        guard fd >= 0, connect(fd, ai.pointee.ai_addr, ai.pointee.ai_addrlen) == 0 else { throw BoltholeError.noPath }
        let key = Data((0..<16).map { _ in UInt8.random(in: 0...255) }).base64EncodedString()
        var path = url.path.isEmpty ? "/" : url.path
        if let q = url.query { path += "?" + q }
        let req = "GET \(path) HTTP/1.1\r\nHost: \(host):\(port)\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
            "Sec-WebSocket-Key: \(key)\r\nSec-WebSocket-Version: 13\r\n\r\n"
        try sendAll(Data(req.utf8))
        while true {
            if let r = buffer.range(of: Data("\r\n\r\n".utf8)) {
                let head = String(decoding: buffer[..<r.lowerBound], as: UTF8.self)
                buffer = Data(buffer[r.upperBound...])
                guard head.hasPrefix("HTTP/1.1 101") else { throw BoltholeError.protocolError(head.components(separatedBy: "\r\n").first ?? "") }
                return
            }
            try fill()
        }
    }

    private func sendAll(_ d: Data) throws {
        try d.withUnsafeBytes { (p: UnsafeRawBufferPointer) in
            var off = 0
            while off < d.count {
                let n = Glibc.send(fd, p.baseAddress! + off, d.count - off, Int32(MSG_NOSIGNAL))
                if n <= 0 { throw BoltholeError.streamClosed }
                off += n
            }
        }
    }

    private func fill() throws {
        var tmp = [UInt8](repeating: 0, count: 65536)
        let n = recv(fd, &tmp, tmp.count, 0)
        if n <= 0 { throw BoltholeError.streamClosed }
        buffer.append(contentsOf: tmp[0..<n])
    }

    private func need(_ n: Int) throws { while buffer.count < n { try fill() } }

    private func readFrameBlocking() throws -> Data {
        try ensureOpen()
        var message = Data()
        while true {
            try need(2)
            let b0 = buffer[buffer.startIndex], b1 = buffer[buffer.startIndex + 1]
            var len = Int(b1 & 0x7f)
            var off = 2
            if len == 126 {
                try need(4)
                len = Int(buffer[buffer.startIndex + 2]) << 8 | Int(buffer[buffer.startIndex + 3])
                off = 4
            } else if len == 127 {
                try need(10)
                len = 0
                for i in 2..<10 { len = len << 8 | Int(buffer[buffer.startIndex + i]) }
                off = 10
            }
            try need(off + len)
            let payload = Data(buffer[(buffer.startIndex + off)..<(buffer.startIndex + off + len)])
            buffer = Data(buffer[(buffer.startIndex + off + len)...])
            let opcode = b0 & 0x0f
            if opcode == 8 { throw BoltholeError.streamClosed }
            if opcode == 9 { try writeFrameBlocking(payload, opcode: 10); continue }
            if opcode == 10 { continue }
            message.append(payload)
            if b0 & 0x80 != 0 { return message }
        }
    }

    private func writeFrameBlocking(_ d: Data, opcode: UInt8 = 2) throws {
        try ensureOpen()
        var f = Data([0x80 | opcode])
        if d.count < 126 {
            f.append(0x80 | UInt8(d.count))
        } else if d.count < 65536 {
            f.append(0x80 | 126)
            f.append(contentsOf: [UInt8(d.count >> 8), UInt8(d.count & 0xff)])
        } else {
            f.append(0x80 | 127)
            for i in (0..<8).reversed() { f.append(UInt8((d.count >> (i * 8)) & 0xff)) }
        }
        let mask = (0..<4).map { _ in UInt8.random(in: 0...255) }
        f.append(contentsOf: mask)
        f.append(Data(d.enumerated().map { $0.element ^ mask[$0.offset % 4] }))
        try sendAll(f)
    }

    func read() async throws -> Data {
        try await withCheckedThrowingContinuation { c in
            readQueue.async { c.resume(with: Result { try self.readFrameBlocking() }) }
        }
    }

    func write(_ data: Data) async throws {
        try await withCheckedThrowingContinuation { (c: CheckedContinuation<Void, Error>) in
            writeQueue.async { c.resume(with: Result { try self.writeFrameBlocking(data) }) }
        }
    }

    func close() {
        if fd >= 0 { shutdown(fd, Int32(SHUT_RDWR)) }
    }
}
#endif

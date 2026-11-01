# Bolthole

[English](README.md) | 简体中文

**让任何服务都能从任何地方访问，而中间的 relay 读不到你的数据。**

你的应用跑在笔记本、树莓派或者 NAT 后面的服务器上。Bolthole 让手机、浏览器或另一个程序从任何地方访问它：不用端口转发，不用 VPN，也不用信任 relay 的运营者。

```
 你的服务 ── agent ──主动外连 WSS──▶ relay ◀──HTTPS/WSS── client（手机、浏览器、CLI、服务器）
                 └────────────── 端到端加密（Noise）──────────────┘
                 └────────── 同一网络时直接连接，不经过 relay ──────────┘
```

- **任何应用、任何语言都能用。** 用 Go 或 JavaScript SDK；或者把 `bolthole` 二进制放在应用旁边当 sidecar，应用继续在 localhost 上讲普通的 HTTP、WebSocket、TCP。
- **不需要自己的服务器。** relay 可以一条命令部署到 Cloudflare Workers 免费套餐，也可以在任何机器上跑单个 Go 二进制。
- **relay 是瞎的。** client 和 agent 用只有双方知道的密钥跑 Noise 握手，relay 只转发密文，不落盘。
- **能直连就直连。** client 同时尝试局域网/VPN 地址和 relay，谁先握手成功用谁；走 relay 时后台会探测直连，可用就自动升级；也可以手动固定路径。
- **稳定性。** 每条流独立流控、心跳检测半开连接、带抖动的指数退避重连、relay 重启时 GOAWAY 平滑切换（先连新再断旧）、限流、内存有界。
- **可扩展。** HTTP、WebSocket、TCP 只是三种内置的流类型（kind）。几行代码就能定义自己的 kind，agent 也能反向给 client 开流，还能在加密通道里协商自己的应用协议。

## 五分钟上手

```bash
go install github.com/abcdlsj/bolthole/cmd/bolthole@latest

# 1. 起一个 relay（或者直接用你部署到 Cloudflare 的那个）
bolthole serve --addr :8787

# 2. 在有服务的机器上
bolthole agent --relay http://localhost:8787 --http http://127.0.0.1:3000 --qr
#    → 连接串：http://localhost:8787/#k=6Xo2…（当密码对待）

# 3. 在任何其他地方
bolthole connect 'http://localhost:8787/#k=6Xo2…' --listen 127.0.0.1:8080
curl http://127.0.0.1:8080/        # 端到端加密地访问到你的服务
```

**连接串**就是 relay 的 URL，`#` 后面带一个 256 位的 code。URL fragment 永远不会发给服务器，所以 relay 看不到它。

## 在代码里用

Go：

```go
code, _ := bolthole.LoadOrCreateCode("~/.config/myapp/bolthole.code")
a := bolthole.Expose("https://relay.example.com", code, myHandler)
go a.Run(ctx)

c, _ := bolthole.Dial(connString)
resp, _ := c.HTTPClient().Get("http://agent/api/status")
```

JavaScript（浏览器、Node 22+、Deno、Bun、Workers，无依赖、无需构建）：

```js
import { Client, Agent } from 'bolthole';
const c = new Client('https://relay.example.com/#k=6Xo2…');
const res = await c.fetch('/api/status');

new Agent({ relay: 'https://relay.example.com', fetch: (req) => new Response('hi') }).run();
```

其他语言：两端各放一个 `bolthole` sidecar，或者按 [协议规范](spec/PROTOCOL.md) 自己实现，规范附带逐字节的 [测试向量](spec/test-vectors.json)。

## 部署 relay

| | |
| --- | --- |
| **Cloudflare Workers**（免费，无服务器） | `npx wrangler deploy`，见 [docs/deploy.md](docs/deploy.md) |
| **Docker + 自动 HTTPS** | `RELAY_DOMAIN=relay.example.com docker compose up -d` |
| **单个二进制** | `bolthole serve --addr :8787`，前面放任意 TLS 反代 |
| **嵌进你的 Go 服务** | `http.Handle("/", relay.New(relay.Config{}))` |

所有版本协议完全一致，同一套测试分别跑在 Go relay 和 Worker 上。设置 `BOLTHOLE_KEYS`（共享 key）或 `BOLTHOLE_JWT_SECRET`（`bolthole token` 签发的每个 agent 的 token）可以防止别人蹭用你的 relay；谁能访问某个 agent 只由它的 code 决定。

## 示例

| | |
| --- | --- |
| [`examples/phone-remote`](examples/phone-remote) | 手机打开一个链接，实时看笔记本 CPU/内存，还能一键在笔记本终端弹消息。浏览器 SDK + 端到端加密的 SSE。 |
| [`examples/ssh-anywhere`](examples/ssh-anywhere) | 一行 `ProxyCommand` 就能在任何地方 `ssh laptop`，同一 Wi-Fi 下自动直连。 |
| [`examples/webhook-inbox`](examples/webhook-inbox) | 一个公开的 webhook 地址直通你的笔记本，美化打印并转发给本地开发服务器。 |
| [`examples/chat`](examples/chat) | 在你机器上开一个终端聊天室，基于自定义流类型。 |
| [`examples/python-sidecar`](examples/python-sidecar) | Python 应用和客户端都不引入任何 SDK，sidecar 包办一切。 |

## 安全提示

- code 就是 agent 的密码。删掉 code 文件并重启 agent 即可让所有人失效。
- 默认只接受端到端加密。`--public` 会额外通过 relay 提供明文 HTTP，适合本来就公开的内容（webhook、落地页），这部分 relay 能看到也能改。
- 通过 relay 下发的网页（比如 `phone-remote`）可能被恶意 relay 篡改。正式使用时请自己托管客户端页面或打包进 App。
- relay 能看到时序、大小和在和哪个 token hash 通信。务必放在 TLS 后面。

## 许可

MIT

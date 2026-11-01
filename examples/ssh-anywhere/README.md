# SSH into your laptop from anywhere

No port forwarding, no VPN, no exposed sshd. The relay only sees encrypted
bytes, and sshd still does its own authentication on top.

On the laptop:

```bash
bolthole agent --relay https://relay.example.com --tcp ssh=127.0.0.1:22 --direct :7777
```

On the other machine, add to `~/.ssh/config`:

```
Host laptop
  User me
  ProxyCommand bolthole tcp 'https://relay.example.com/#k=…' ssh
```

Then `ssh laptop`, `scp file laptop:`, `rsync -e ssh …` and VS Code Remote
SSH all work. When both machines are on the same Wi-Fi the connection goes
direct (the agent advertises its LAN address inside the encrypted
handshake); otherwise it goes through the relay. `--mode relay` or
`--mode direct` on `bolthole tcp` pins the path.

The same trick works for any TCP service: a database (`--tcp pg=127.0.0.1:5432`
with `bolthole connect … --tcp 127.0.0.1:5432=pg`), VNC, Minecraft, a
printer. Only targets you list with `--tcp` are reachable.

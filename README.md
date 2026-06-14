# SIPcord Bridge

This is a slice of the code that powers [SIPcord](https://sipcord.net/) that you can use to self host something similar. It's not the full SIPcord package but rather the core functionality used in SIPcord with ways to build your own backend adapter. SIPcord itself uses this as a component of the full build so the code is the same that runs on the public bridges.

This means you have to build the call routing backend yourself. I am including a `static-router` backend which you can use to map extensions in a TOML file like this
```toml
[extensions]
1000 = { guild = "123456789012345620", channel = "987654321012345620" }
2000 = { guild = "123456789012345620", channel = "111222333444555620" }

[menus.main]
extension = "8000"
timeout_seconds = 10
max_attempts = 3

[phones]
777 = { label = "Shop speakerphone" }
111 = { label = "Desk phone" }
```
but if you want more fancy routing you have to build it. You can easily use sipcord-bridge as a library and provide your own routers by implementing the `Backend` trait.

This was written a mix between myself and claude, sure, some of it's big slop but the parts I care about are not.

### Can you help me set this up?

**No.** I am not providing support for this as my goal is to run [sipcord.net](https://sipcord.net/), not support self hosting. If you want to run this self hosted, feel free to use this code but you are on your own here.

### I have a feature request!

**PR's welcome**. No really, feel free to implement it and contribute.

## Self-host setup notes

These notes cover the static-router Docker setup. The bridge maps inbound SIP
extension digits to Discord voice channels, and can also place outbound calls
from Discord into a PBX extension when outbound SIP target settings are enabled.

### Prerequisites

- A Discord bot with voice permissions. Create one at https://discord.com/developers/applications, enable the **Message Content** intent, and grab the bot token.
- A Docker host reachable from your PBX or SIP clients. SIP uses port 5060 and RTP uses UDP 10000-15000 by default.
- Docker (recommended) or Rust nightly toolchain if building from source.

### 1. Invite the bot to your server

Use this URL format, replacing `YOUR_CLIENT_ID`:
```
https://discord.com/oauth2/authorize?client_id=YOUR_CLIENT_ID&scope=bot&permissions=36700160
```
The bot needs Connect + Speak permissions in voice channels. If you want the
bot nickname to change to the phone/person being called, also grant Change
Nickname.

### 2. Get Discord channel IDs

Enable Developer Mode in Discord (Settings > App Settings > Advanced > Developer Mode). Right-click a voice channel and click "Copy Channel ID". Do the same for the server (guild) by right-clicking the server name.

### 3. Create the dialplan

Create a `dialplan.toml` mapping extensions to Discord channels:

```toml
[extensions]
1000 = { guild = "123456789012345678", channel = "987654321012345678" }
2000 = { guild = "123456789012345678", channel = "111222333444555666" }
```

Each extension is what you'll dial from your SIP phone. Pick any numbers you like.

You can also add a dynamic phone menu. A caller dials the menu extension,
Sipcord reads the available Discord servers, the caller picks one with DTMF,
then Sipcord reads that server's voice channels and joins the selected channel:

```toml
[menus.main]
extension = "8000"
timeout_seconds = 10
max_attempts = 3
```

The menu uses `espeak-ng` for local text-to-speech with a female English voice.
Emoji and common Discord channel separators are skipped in spoken names. Press
`#` to repeat the current menu page, `9` for the next page when available, and
`*` for the previous page when available.

You can also add a phone directory for Discord-originated calls. These entries
show up in `/directory` as buttons. Clicking one dials that extension from your
current Discord voice channel:

```toml
[phones]
777 = { label = "Shop speakerphone" }
111 = { label = "Desk phone" }
```

By default, the TOML key is the extension to dial. If you want the key and
dialed extension to differ, set `extension` explicitly:

```toml
[phones]
shop = { label = "Shop speakerphone", extension = "777" }
```

### 4a. Run with Docker (recommended)

Create a directory for your deployment:

```bash
mkdir sipcord && cd sipcord
```

Create a `.env` file:

```env
DISCORD_BOT_TOKEN=your_bot_token_here
SIP_PUBLIC_HOST=192.168.0.100
RTP_PUBLIC_IP=192.168.0.100
DISCORD_OUTBOUND_SIP_HOST=192.168.0.25
DISCORD_OUTBOUND_SIP_PORT=5060
DISCORD_OUTBOUND_SIP_TRANSPORT=udp
```

Set both IPs to the address other SIP devices use to reach the bridge. For
example, if FreePBX is `192.168.0.25` and this container runs on an OMV host at
`192.168.0.100`, use `192.168.0.100`. Do not use `0.0.0.0` here; this value is
advertised in SIP Contact/SDP headers, and callers must be able to route back to
it.

Set `DISCORD_OUTBOUND_SIP_HOST` to the PBX or SIP server that should receive
Discord-originated extension calls. For a FreePBX box at `192.168.0.25`, that
means `DISCORD_OUTBOUND_SIP_HOST=192.168.0.25`.

Create a `docker-compose.yml`:

```yaml
services:
  sipcord-bridge:
    image: ghcr.io/legop3/sipcord-bridge:latest
    container_name: sipcord-bridge
    restart: always
    network_mode: host
    env_file:
      - .env
    volumes:
      - ./dialplan.toml:/app/dialplan.toml:ro
      # Uncomment to persist data across restarts:
      # - ./data:/var/lib/sipcord
```

Place your `dialplan.toml` in the same directory, then:

```bash
docker compose up -d
docker logs -f sipcord-bridge
```

You should see it load the dialplan and start listening.

For a LAN deployment on an OMV host at `192.168.0.100`, startup should include
lines like:

```text
Static router running on 192.168.0.100:5060
Public host Contact rewriting enabled: 192.168.0.100:5060
Account RTP config: ... public_addr=192.168.0.100
```

Images are published by GitHub Actions to `ghcr.io/legop3/sipcord-bridge`
on pushes to `master`, version tags like `v2.1.2`, and manual workflow runs.
If the package is private, make it public in the GitHub package settings or
log in to GHCR from your OMV host before pulling.

### 4b. FreePBX trunk example

Create a PJSIP trunk that points at the Docker host running the bridge. For
example, if FreePBX is `192.168.0.25` and the bridge container is on
`192.168.0.100`, the trunk should point at `192.168.0.100`.

PJSIP trunk, General:

```text
Trunk Name: sipcord
SIP Server: 192.168.0.100
SIP Server Port: 5060
Authentication: Outbound
Registration: None
Username: sipcord
Secret: any-random-string
```

PJSIP trunk, Advanced:

```text
Client URI: sip:sipcord@192.168.0.100:5060
Server URI: sip:192.168.0.100:5060
From Domain: 192.168.0.100
Contact User: sipcord
Transport: UDP
Direct Media: No
RTP Symmetric: Yes
Force rport: Yes
Rewrite Contact: Yes
```

The bridge challenges inbound SIP requests, but the static router does not make
authorization decisions from the username/password. Configure outbound
credentials in FreePBX so it can answer the SIP digest challenge; the bridge
routes by the dialed extension in `dialplan.toml`.

Create an outbound route such as:

```text
Route Name: sipcord
Trunk Sequence: sipcord
Dial pattern prefix: 8
Dial pattern match: 1101
```

With that route, dialing `81101` from a FreePBX extension sends `1101` to the
bridge, which matches:

```toml
[extensions]
1101 = { guild = "668249361339383808", channel = "931737080176979968" }
```

To debug routing from FreePBX:

```bash
asterisk -rvvv
pjsip set logger host 192.168.0.100
```

You should see an `INVITE sip:1101@192.168.0.100:5060`, followed by the digest
challenge, a second INVITE with auth, a `200 OK`, and an `ACK`. If the call ends
after about 32 seconds, check that `SIP_PUBLIC_HOST` and `RTP_PUBLIC_IP` are set
to the bridge host address, not the FreePBX address and not `0.0.0.0`.

For a menu extension, route the menu number the same way. For example, dialing
`88000` can strip the `8` prefix and send `8000` to Sipcord, where
`[menus.main] extension = "8000"` answers and collects DTMF.

### 4c. Discord -> extension calling

If `DISCORD_OUTBOUND_SIP_HOST` is set, the bot registers `/call` and `/hangup`
slash commands in each guild it is connected to.

Usage:

```text
/call extension:1101
/directory
/hangup
```

Behavior:
- The user running `/call` must already be in a Discord voice channel.
- The bot uses that voice channel as the bridge destination.
- The bridge dials the requested extension through the configured PBX target.
- It dials the requested extension directly, for example
  `sip:1101@192.168.0.25:5060;transport=udp`.
- When the SIP side answers, the phone call is connected to the Discord voice
  channel where the command was run.
- `/hangup` ends active SIP calls connected to the voice channel where the
  command was run.
- `/directory` opens the configured phone directory as Discord buttons. Clicking
  a phone button behaves like `/call` for that extension.
- When a Discord-originated call starts, the bot tries to set its server
  nickname to the matching phone directory label. If the extension is not in the
  directory, it uses the dialed extension.

Current scope:
- `/call` is implemented for the static self-host backend.
- It dials a configured PBX/SIP host by extension.
- `/hangup` ends calls already connected to the current Discord voice channel.
- Rich status updates back into Discord after the initial slash command reply
  are not implemented yet.

### 4d. Build from source

Requires Rust nightly (for `portable_simd`) and system dependencies for pjproject (OpenSSL, Opus, libtiff, etc). See the `Dockerfile` for the full list.

```bash
cargo run --release -p sipcord-bridge
```

The binary reads `config.toml` from the working directory (or `CONFIG_PATH`), the dialplan from `./dialplan.toml` (or `DIALPLAN_PATH`), and sound files from `./wav/` (or `SOUNDS_DIR`).

### 5. Configure a direct SIP phone

Point your SIP client at the bridge host on port 5060. The static router routes
by dialed extension after the SIP digest handshake.

Example Oink (or any softphone) setup:
- **SIP Server:** `192.168.0.100`
- **Port:** `5060`
- **Transport:** `UDP`
- **Username/Password:** anything

Dial `1000` (or whatever you put in `dialplan.toml`) and you should hear the bot join the Discord voice channel.

### Environment variables reference

| Variable | Default | Description |
|----------|---------|-------------|
| `DISCORD_BOT_TOKEN` | *(required)* | Discord bot token |
| `SIP_PUBLIC_HOST` | *(required)* | Routable IP/hostname advertised in SIP Contact headers |
| `SIP_PORT` | `5060` | SIP listening port |
| `RTP_PORT_START` | `10000` | Start of RTP port range |
| `RTP_PORT_END` | `15000` | End of RTP port range |
| `RTP_PUBLIC_IP` | *(local address if unset)* | Routable IP advertised in SDP for RTP media |
| `DISCORD_OUTBOUND_SIP_HOST` | *(disabled if unset)* | PBX/SIP host used by Discord `/call` |
| `DISCORD_OUTBOUND_SIP_PORT` | `5060` | Port for Discord-originated outbound SIP calls |
| `DISCORD_OUTBOUND_SIP_TRANSPORT` | `udp` | Transport for Discord-originated outbound SIP calls: `udp`, `tcp`, or `tls` |
| `CONFIG_PATH` | `./config.toml` | Path to config.toml |
| `DIALPLAN_PATH` | `./dialplan.toml` | Path to dialplan.toml |
| `SOUNDS_DIR` | `./wav` | Path to sound files directory |
| `DATA_DIR` | `/var/lib/sipcord` | Persistent data directory |
| `DEV_MODE` | `false` | Enable dev mode logging |
| `RUST_LOG` | `sipcord_bridge=info,pjsip=warn` | Log level filter |

### NAT / Firewall notes

`SIP_PUBLIC_HOST` is not a bind-all setting. It is written into SIP headers, so
it must be the address peers should call back. On a LAN, use the Docker host's
LAN IP. Across NAT, use the public IP or hostname.

If your server is behind NAT, you need to:
- Forward UDP port 5060 (SIP signaling)
- Forward UDP ports 10000-15000 (RTP media)
- Set `SIP_PUBLIC_HOST` to your *public* IP
- Set `RTP_PUBLIC_IP` to the public RTP address

For servers with both a public and private interface (e.g. behind a load balancer), you can set `SIP_LOCAL_HOST` and `SIP_LOCAL_CIDR` so local clients get the private IP in Contact headers:

```env
SIP_LOCAL_HOST=192.168.1.100
SIP_LOCAL_CIDR=192.168.1.0/24
```

### Fax support

The bridge can receive faxes (both G.711 passthrough and T.38 UDPTL). Received faxes are demodulated via SpanDSP and posted as PNG images to a Discord text channel. To set up fax, add a mapping with a text channel ID in your dialplan — the bridge routes faxes to text channels and voice calls to voice channels automatically.

### Acknowledgements

- Thanks to [dusthillguy](https://dusthillguy-music-blog1.tumblr.com/) for letting me use the song [*"Joona Kouvolalainen buttermilk"*](https://www.youtube.com/watch?v=IK1ydvw3xkU) as hold music.
- Thanks to [wberg](https://wberg.com/) for hosting `bridge-eu1`
- Thanks to [chrischrome](https://litenet.tel/) for hosting `bridge-use1`

### License

Code is AGPLv3

Dusthillguy track is whatever dusthillguy wishes

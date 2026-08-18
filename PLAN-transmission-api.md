# Plan: Transmission-Compatible API for TorrentDam Operator

## Goal

Implement a Transmission RPC-compatible HTTP API in the operator so Radarr can use TorrentDam as a download client.

## Architecture

- `transmission.scala` — all Transmission RPC API logic (HTTP routes, session ID, JSON, magnet parsing, torrent mapping)
- `operator.scala` — starts the HTTP server, injects operation callbacks (create/delete/list torrents) into the server, runs controller loop concurrently
- The server receives callbacks for K8s operations, keeping it decoupled from K8s client logic

## Transmission RPC Methods (Radarr subset)

| Method | Implementation |
|--------|----------------|
| `session-get` | Static config response |
| `torrent-get` | List Torrent CRs via callback, map to Transmission format |
| `torrent-add` | Parse magnet link, create Torrent CR via callback |
| `torrent-remove` | Delete Torrent CR via callback |
| `torrent-set` | No-op, return success |
| `queue-move-top` | No-op, return success |

## Key Details

- **No auth** — cluster-internal only
- **Magnet links only** for torrent-add (no .torrent file parsing)
- **X-Transmission-Session-Id** handshake (409 Conflict on missing header)
- **Port 9091** exposed via Service
- **Env vars**: `PVC_NAME`, `DHT_NODE` for torrent CR creation defaults

## Torrent CR ↔ Transmission Mapping

| Transmission field | Source |
|---|---|
| `id` | `hashString.hashCode` |
| `hashString` | `torrent.spec.infoHash` |
| `name` | `torrent.metadata.name` |
| `downloadDir` | `/data/<downloadPath>` |
| `status` | Pod phase → Transmission code (Pending→3, Running→4, Succeeded→0, Failed→0) |
| `isFinished` | `phase == "Succeeded"` |
| `labels` | `["radarr"]` |
| Other fields | Defaults/zeros |

## Files to Change

1. **`transmission.scala`** (new) — RPC handler, HTTP routes, magnet parsing, JSON mapping
2. **`operator.scala`** — add http4s-server deps, start server with callbacks, add `create`/`delete` RBAC
3. **`deployment.yaml`** — add Service, container port, env vars, RBAC verbs
4. **`Dockerfile`** — copy `transmission.scala`

## Radarr Configuration

- Host: `torrentdam-operator.media-server.svc.cluster.local`
- Port: `9091`
- Url Base: `/transmission/`
- No auth
- Movie Category: `radarr`
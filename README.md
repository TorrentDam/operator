# TorrentDam Operator

Single-file Scala 3 Kubernetes operator for managing torrent downloads.

Watches `Torrent` custom resources and creates/deletes pods accordingly. Exposes a JSON-based HTTP API inspired by [Transmission's RPC spec](https://github.com/transmission/transmission/blob/main/docs/rpc-spec.md) for torrent management.

Goal: Support all methods required by [Radarr](https://github.com/Radarr/Radarr) and [Sonarr](https://github.com/Sonarr/Sonarr).
Operator
========

Manages torrents in k8s cluster.

Concept
-------

There is a custom resource called `Torrent`.
_Operator_ watches for changes to `Torrent` and creates/deletes pods accordingly.
_Operator_ exposes JSON-based API for clients to manage torrents. The API is copies [Transmission's RPC](https://github.com/transmission/transmission/blob/main/docs/rpc-spec.md). Not all methods are implemented. The goal is to support all methods required by [Radarr](https://github.com/Radarr/Radarr) and [Sonarr](https://github.com/Sonarr/Sonarr).
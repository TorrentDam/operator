# TorrentDam Operator

Single-file Scala 3 Kubernetes operator for managing torrent downloads.

Watches `Torrent` custom resources and creates/deletes pods accordingly. Exposes a JSON-based HTTP API inspired by [Transmission's RPC spec](https://github.com/transmission/transmission/blob/main/docs/rpc-spec.md) for torrent management.

Goal: Support all methods required by [Radarr](https://github.com/Radarr/Radarr) and [Sonarr](https://github.com/Sonarr/Sonarr).

## Installation

The operator can be installed directly or referenced from another kustomize configuration.

### Direct

```sh
kubectl apply -k .
```

### From another kustomize configuration

Add this to your `kustomization.yaml`:

```yaml
resources:
  - https://github.com/TorrentDam/operator//?ref=main
```

To pin a specific image tag, override the `images` entry in your consuming `kustomization.yaml`:

```yaml
images:
  - name: ghcr.io/torrentdam/operator
    newTag: <sha-or-tag>
```

> **Note:** By default the operator watches the namespace it is deployed in (via the downward API `WATCH_NAMESPACE` env var). To watch cluster-wide, remove the env var from the Deployment.

### Creating a torrent

```sh
kubectl apply -f examples/pvc.yaml      # one-time: storage for downloads
kubectl apply -f examples/torrent.yaml  # a Torrent resource
```

A pod running the torrent client will be created and bound to the PVC named in `spec.pvcName`.

### Uninstall

```sh
kubectl delete -k .
```
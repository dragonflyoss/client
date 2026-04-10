# Examples of Request

Examples of downloading a file and preheating an OCI image via the Dragonfly P2P network using the `request` module.

## Prerequisites

1. A running Dragonfly scheduler service.
2. At least one seed peer registered with the scheduler.

## Run GET Example

Downloads a file via the Dragonfly P2P network using the `get` method.

```shell
export DRAGONFLY_SCHEDULER_ENDPOINT="http://127.0.0.1:8002"
cargo run -p request --bin get
```

## Run Preheat Example

Preheats an OCI image (`dragonflyoss/scheduler:v2.4.3`) via the Dragonfly P2P network
using the `preheat` method. All blobs (config and layers) are downloaded through the
Dragonfly seed peer proxy and cached in the P2P network.

```shell
export DRAGONFLY_SCHEDULER_ENDPOINT="http://127.0.0.1:8002"
cargo run -p request --bin preheat
```

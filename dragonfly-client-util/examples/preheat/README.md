# Example of Preheat Request

An example of preheating an OCI image (`dragonflyoss/scheduler:v2.4.3`) via the Dragonfly P2P network
using the `preheat` method. All blobs (config and layers) are downloaded through the Dragonfly
seed peer proxy and cached in the P2P network.

## Prerequisites

1. A running Dragonfly scheduler service.
2. At least one seed peer registered with the scheduler.

## Run Example

```shell
export DRAGONFLY_SCHEDULER_ENDPOINT="http://127.0.0.1:8002"
cargo run -p preheat
```

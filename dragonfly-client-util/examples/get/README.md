# Example of GET Request

An example of downloading a file via the Dragonfly P2P network using the `get` method.

## Prerequisites

1. A running Dragonfly scheduler service.
2. At least one seed peer registered with the scheduler.

## Run Example

```shell
export DRAGONFLY_SCHEDULER_ENDPOINT="http://127.0.0.1:8002"
cargo run -p get
```

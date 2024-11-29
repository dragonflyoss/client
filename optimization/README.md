# Performance Optimization Guidance

> This is a reference benchmark process document designed to
> assist in performance analysis and optimization for **client**.
> This document provides as general a testing framework as possible,
> allowing developers with needs to adjust it
> according to their specific circumstances across various platform.

## Flow

![architecture](../image/client_bencmark.png)

## Preparation

### 1. Set up Dragonfly cluster

- Please refer to [official doc](https://d7y.io/docs/next/getting-started/installation/helm-charts/).

### 2. Start a file server

- start with docker:

```bash
export FILE_SERVER_PORT=12345
docker run -d  --rm -p ${FILE_SERVER_PORT}:80 --name dragonfly-fs dragonflyoss/file-server:latest
```

- check

```bash
# return success if ready
curl -s -o /dev/null \
     -w "%{http_code}" \
     http://localhost:12345/nano \
| grep -q "200" \
&& echo "Success" \
|| echo "Failed"
```

- optional:

> you can build your own image, take a reference from [**Dockerfile**](https://github.com/dragonflyoss/perf-tests/blob/main/tools/file-server/Dockerfile).

### 3. Prepare related tools

- Request Generator: [**oha**](https://github.com/hatoo/oha)

```bash
brew install oha
```

- Profiling: [**flamegraph**](https://github.com/flamegraph-rs/flamegraph)

```bash
cargo install flamegraph
```

### 4. start the target client

- compile the target binary

```bash
cargo build --release   --bin dfdaemon
```

- connect to dragonfly cluster

```bash
# prepare client.yaml by yourself.
./target/release/dfdaemon --config client.yaml -l info --verbose
```

## FlameGraph

Now, let's start benchmark with the following params:

- $FILE_SERVER_ADDRESS
- $CLIENT_PROXY_ADDRESS

### collect flamegraph

- capture the flamegraph:

```bash
## stop after all requests done.
sudo flamegraph -o my_flamegraph.svg --pid 3442
```

- make the request:

```bash
oha -c 1000 \
    -n 100 \
    --rand-regex-url $FILE_SERVER_ADDRESS/\(nano\|micro\|small\|medium\|large\) \
    -x $CLIENT_PROXY_ADDRESS
```

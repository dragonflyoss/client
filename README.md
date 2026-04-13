# Dragonfly Client

[![GitHub release](https://img.shields.io/github/release/dragonflyoss/client.svg)](https://github.com/dragonflyoss/client/releases)
[![CI](https://github.com/dragonflyoss/client/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/dragonflyoss/client/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/dragonflyoss/client/branch/main/graph/badge.svg)](https://codecov.io/gh/dragonflyoss/dfdaemon)
[![Open Source Helpers](https://www.codetriage.com/dragonflyoss/client/badges/users.svg)](https://www.codetriage.com/dragonflyoss/client)
[![Discussions](https://img.shields.io/badge/discussions-on%20github-blue?style=flat-square)](https://github.com/dragonflyoss/dragonfly/discussions)
[![Twitter](https://img.shields.io/twitter/url?style=social&url=https%3A%2F%2Ftwitter.com%2Fdragonfly_oss)](https://twitter.com/dragonfly_oss)
[![LICENSE](https://img.shields.io/github/license/dragonflyoss/dragonfly.svg?style=flat-square)](https://github.com/dragonflyoss/dragonfly/blob/main/LICENSE)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fdragonflyoss%2Fclient.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fdragonflyoss%2Fclient?ref=badge_shield)

Dragonfly client written in Rust. It can serve as both a peer and a seed peer.

## Documentation

You can find the full documentation on the [d7y.io](https://d7y.io).

## S3-Aware Proxy Mode

`dfdaemon` can run as an HTTPS proxy for authenticated S3 reads so existing AWS SDK and CLI
clients can use Dragonfly with `HTTPS_PROXY` plus a trusted proxy CA, without changing
application code.

V1 behavior:

- `GetObject` uses Dragonfly P2P.
- Ranged `GetObject` uses Dragonfly P2P. If the caller's SigV4 signature explicitly covers the
  `Range` header, `dfdaemon` preserves that original signed `Range` on the source request instead
  of rewriting it.
- `HeadObject` and `ListObjectsV2` stay direct passthrough.
- Other S3 APIs stay passthrough and are not accelerated.
- The proxy preserves caller-provided SigV4 headers or presigned URLs. `dfdaemon` does not
  discover AWS credentials or re-sign origin requests.

Example client environment:

```shell
export HTTPS_PROXY=http://127.0.0.1:4001
export HTTP_PROXY=http://127.0.0.1:4001
export AWS_CA_BUNDLE=/etc/ssl/certs/dragonfly-proxy-ca.pem
```

See the sample `dfdaemon` service in [`ci/dfdaemon.service`](./ci/dfdaemon.service) and the
docker-compose client config template in the
[dragonfly repository](https://github.com/dragonflyoss/dragonfly/blob/main/deploy/docker-compose/template/client.template.yaml).

## Community

Join the conversation and help the community grow. Here are the ways to get involved:

- **Slack Channel**: [#dragonfly](https://cloud-native.slack.com/messages/dragonfly/) on [CNCF Slack](https://slack.cncf.io/)
- **Github Discussions**: [Dragonfly Discussion Forum](https://github.com/dragonflyoss/dragonfly/discussions)
- **Developer Group**: <dragonfly-developers@googlegroups.com>
- **Mailing Lists**:
  - **Developers**: <dragonfly-developers@googlegroups.com>
  - **Maintainers**: <dragonfly-maintainers@googlegroups.com>
- **Twitter**: [@dragonfly_oss](https://twitter.com/dragonfly_oss)
- **DingTalk Group**: `22880028764`

## Contributing

You should check out our
[CONTRIBUTING](./CONTRIBUTING.md) and develop the project together.

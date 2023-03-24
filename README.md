# Dfdaemon

![alt][logo-linear]

[![GitHub release](https://img.shields.io/github/release/dragonflyoss/dfdaemon.svg)](https://github.com/dragonflyoss/dfdaemon/releases)
[![CI](https://github.com/dragonflyoss/dfdaemon/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/dragonflyoss/dfdaemon/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/dragonflyoss/dfdaemon/branch/main/graph/badge.svg)](https://codecov.io/gh/dragonflyoss/dfdameon)
[![Open Source Helpers](https://www.codetriage.com/dragonflyoss/dfdaemon/badges/users.svg)](https://www.codetriage.com/dragonflyoss/dfdaemon)
[![TODOs](https://badgen.net/https/api.tickgit.com/badgen/github.com/dragonflyoss/Dragonfly2/main)](https://www.tickgit.com/browse?repo=github.com/dragonflyoss/Dragonfly2&branch=main)
[![Discussions](https://img.shields.io/badge/discussions-on%20github-blue?style=flat-square)](https://github.com/dragonflyoss/Dragonfly2/discussions)
[![Twitter](https://img.shields.io/twitter/url?style=social&url=https%3A%2F%2Ftwitter.com%2Fdragonfly_oss)](https://twitter.com/dragonfly_oss)
[![GoDoc](https://godoc.org/github.com/dragonflyoss/Dragonfly2?status.svg)](https://godoc.org/github.com/dragonflyoss/Dragonfly2)
[![OpenSSF Best Practices](https://bestpractices.coreinfrastructure.org/projects/7103/badge)](https://bestpractices.coreinfrastructure.org/projects/7103)
[![LICENSE](https://img.shields.io/github/license/dragonflyoss/Dragonfly2.svg?style=flat-square)](https://github.com/dragonflyoss/Dragonfly2/blob/main/LICENSE)

Provide efficient, stable and secure file distribution and image acceleration
based on p2p technology to be the best practice and
standard solution in cloud native architectures.

## Introduction

Dragonfly is an open source P2P-based file distribution and
image acceleration system. It is hosted by the
Cloud Native Computing Foundation ([CNCF](https://cncf.io/)) as
an Incubating Level Project.
Its goal is to tackle all distribution problems in cloud native architectures.
Currently Dragonfly focuses on being:

- **Simple**: Well-defined user-facing API (HTTP), non-invasive to all container engines;
- **Efficient**: Seed peer support, P2P based file distribution to save enterprise bandwidth;
- **Intelligent**: Host-level speed limit, intelligent flow control due to host detection;
- **Secure**: Block transmission encryption, HTTPS connection support.

## Architecture

![alt][arch]

**Manager:** Maintain the relationship between each P2P cluster, dynamic configuration management and RBAC.
It also includes a front-end console, which is convenient for users to visually operate the cluster.

**Scheduler:** Select the optimal download parent peer for the download peer. Exceptions control Dfdaemon's back-to-source.

**Seed Peer**: Dfdaemon turns on the Seed Peer mode can be used as
a back-to-source download peer in a P2P cluster,
which is the root peer for download in the entire cluster.

**Peer**: Deploy with dfdaemon, based on the C/S architecture, it provides the `dfget` command download tool,
and the `dfget daemon` running daemon to provide task download capabilities.

## Documentation

You can find the full documentation on the [d7y.io][d7y.io].

## Community

Welcome developers to actively participate in community discussions
and contribute code to Dragonfly. We will remain
concerned about the issues discussed in the community and respond quickly.

- **Slack Channel**: [#dragonfly](https://cloud-native.slack.com/messages/dragonfly/) on [CNCF Slack](https://slack.cncf.io/)
- **Discussion Group**: <dragonfly-discuss@googlegroups.com>
- **Developer Group**: <dragonfly-developers@googlegroups.com>
- **Github Discussions**: [Dragonfly Discussion Forum][discussion]
- **Twitter**: [@dragonfly_oss](https://twitter.com/dragonfly_oss)
- **DingTalk**: [23304666](https://qr.dingtalk.com/action/joingroup?code=v1,k1,3wEdP6zHKQbOzBRwOvv8wyIbxDEU0kXMrxphaOcPz6I=&_dt_no_comment=1&origin=11)

<!-- markdownlint-disable -->
<div align="center">
  <img src="docs/images/community/dingtalk-group.jpeg" width="300" title="dingtalk">
</div>
<!-- markdownlint-restore -->

## Contributing

You should check out our
[CONTRIBUTING][contributing] and develop the project together.

## Code of Conduct

Please refer to our [Code of Conduct][codeconduct].

[arch]: docs/images/arch.png
[logo-linear]: docs/images/logo/dragonfly-linear.svg
[website]: https://d7y.io
[discussion]: https://github.com/dragonflyoss/Dragonfly2/discussions
[contributing]: CONTRIBUTING.md
[codeconduct]: CODE_OF_CONDUCT.md
[d7y.io]: https://d7y.io/
[dingtalk]: docs/images/community/dingtalk-group.jpeg

Dragonfly client written in Rust

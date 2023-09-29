<h1 align="center">
  Knative + Apache Kafka
</h1>

<p>
  <strong><a href="https://github.com/knative/community/tree/main/mechanics/MATURITY-LEVELS.md">
    These components are BETA
  </a></strong>
</p>

<p align="center">
  <a href="https://goreportcard.com/report/knative-extensions/eventing-kafka-broker">
    <img src="https://goreportcard.com/badge/knative-extensions/eventing-kafka-broker" alt="Go-Report">
  </a>
  <a href="https://github.com/knative-extensions/eventing-kafka-broker/releases">
    <img src="https://img.shields.io/github/release-pre/knative-extensions/eventing-kafka-broker.svg">
  </a>
  <a href="https://github.com/knative-sanbox/eventing-kafka-broker/blob/master/LICENSE">
      <img src="https://img.shields.io/github/license/knative-extensions/eventing-kafka-broker.svg">
  </a>
  <a href="https://slack.cncf.io/">
      <img src="https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social">
  </a>
  <a href="https://cloud-native.slack.com/archives/C04LMU33V1S">
      <img src="https://img.shields.io/badge/%23eventing-white.svg?logo=slack&color=522a5e">
  </a>
</p>

<p align="center">
  <a href="https://img.shields.io/github/downloads/knative-extensions/eventing-kafka-broker/total">
    <img src="https://img.shields.io/github/downloads/knative-extensions/eventing-kafka-broker/total" alt="Downloads">
  </a>
  <a href="https://codecov.io/gh/knative-extensions/eventing-kafka-broker">
    <img src="https://codecov.io/gh/knative-extensions/eventing-kafka-broker/branch/master/graph/badge.svg" alt="Go-Report">
  </a>
  <a href="https://testgrid.knative.dev/eventing-kafka-broker">
    <img src="https://img.shields.io/badge/testgrid-eventing-informational?logo=data:image/x-icon;base64,AAABAAEAICAAAAEACACoCAAAFgAAACgAAAAgAAAAQAAAAAEACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAALAAAAFQAAACoAAw0AAAAAVQAGGgAAAABgAAAAgAAKJgAAAACqAA0zAAATTAAAGmYAAB1zAAAmmQAAM8wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACgoKCgoKCAsQEBAQEBANDRAQEBAQEAsPEBAQEBAQAAAKCgoKCgoICxAQEBAQEA0NEBAQEBAQCw8QEBAQEBAAAAoKCgoKCggLEBAQEBAQDQ0QEBAQEBALDxAQEBAQEAAACgoKCgoKCAsQEBAQEBANDRAQEBAQEAsPEBAQEBAQAAAKCgoKCgoICxAQEBAQEA0NEBAQEBAQCw8QEBAQEBAAAAoKCgoKCggLEBAQEBAQDQ0QEBAQEBALDxAQEBAQEAAACAgICAgIBwkPDw8PDw8MDA8PDw8PDwkODw8PDw8PAAALCwsLCwsJBAsLCwsLCwYCAwMDAwMDAQkLCwsLCwsAABAQEBAQEA8LEBAQEBAQDQUKCgoKCgoDDxAQEBAQEAAAEBAQEBAQDwsQEBAQEBANBQoKCgoKCgMPEBAQEBAQAAAQEBAQEBAPCxAQEBAQEA0FCgoKCgoKAw8QEBAQEBAAABAQEBAQEA8LEBAQEBAQDQUKCgoKCgoDDxAQEBAQEAAAEBAQEBAQDwsQEBAQEBANBQoKCgoKCgMPEBAQEBAQAAAQEBAQEBAPCxAQEBAQEA0FCgoKCgoKAw8QEBAQEBAAAA0NDQ0NDQwGDQ0NDQ0NCwMFBQUFBQUCDA0NDQ0NDQAADQ0NDQ0NDAIFBQUFBQUDCw0NDQ0NDQYMDQ0NDQ0NAAAQEBAQEBAPAwoKCgoKCgUNEBAQEBAQCw8QEBAQEBAAABAQEBAQEA8DCgoKCgoKBQ0QEBAQEBALDxAQEBAQEAAAEBAQEBAQDwMKCgoKCgoFDRAQEBAQEAsPEBAQEBAQAAAQEBAQEBAPAwoKCgoKCgUNEBAQEBAQCw8QEBAQEBAAABAQEBAQEA8DCgoKCgoKBQ0QEBAQEBALDxAQEBAQEAAAEBAQEBAQDwMKCgoKCgoFDRAQEBAQEAsPEBAQEBAQAAALCwsLCwsJAQMDAwMDAwIGCwsLCwsLBAkLCwsLCwsAAA8PDw8PDw4JDw8PDw8PDAwPDw8PDw8JBwgICAgICAAAEBAQEBAQDwsQEBAQEBANDRAQEBAQEAsICgoKCgoKAAAQEBAQEBAPCxAQEBAQEA0NEBAQEBAQCwgKCgoKCgoAABAQEBAQEA8LEBAQEBAQDQ0QEBAQEBALCAoKCgoKCgAAEBAQEBAQDwsQEBAQEBANDRAQEBAQEAsICgoKCgoKAAAQEBAQEBAPCxAQEBAQEA0NEBAQEBAQCwgKCgoKCgoAABAQEBAQEA8LEBAQEBAQDQ0QEBAQEBALCAoKCgoKCgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA">
  </a>
  <a href="https://github.com/issues?q=is%3Aopen+is%3Aissue+archived%3Afalse+org%3Aknative-extensions+repo%3Aeventing-kafka-broker+label%3A%22help+wanted%22+%22+">
      <img src="https://img.shields.io/github/issues/knative-extensions/eventing-kafka-broker/help%20wanted.svg">
  </a>
</p>

<h5><p align="center"><b><i>If you’re using Knative or if you like the project, please <a href="https://github.com/knative-extensions/eventing-kafka-broker/stargazers">★</a> this repository to show your support! 🤩</i></b></p></h5>

<p align="center">
  <a href="https://knative.dev/docs/eventing/broker/kafka-broker/">Eventing Kafka Broker Docs</a> •
  <a href="https://knative.dev/docs/eventing/sink/kafka-sink/">Eventing Kafka Sink Docs</a> •
  <a href="docs/channel/README.md">Eventing Kafka Channel Docs</a> •
  <a href="https://github.com/knative-extensions/eventing-kafka-broker/blob/master/CONTRIBUTING.md">Contributing</a> •
  <a href="https://github.com/issues?q=is%3Aopen+is%3Aissue+archived%3Afalse+org%3Aknative-extensions+repo%3Aeventing-kafka-broker+label%3A%22help+wanted%22+%22+">Help Wanted Issues</a> •
  <a href="https://clotributor.dev/search?project=knative&ts_query_web=knative">Open Knative Issues to Work On</a>

</p>

## Working Group

| Sponsoring WG                                                                                                      |
| ------------------------------------------------------------------------------------------------------------------ |
| [Eventing](https://github.com/knative/community/blob/main/working-groups/WORKING-GROUPS.md#eventing)               |

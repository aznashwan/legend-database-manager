# FINOS Legend Database Manager Operator

## Description

The Legend Operators package the core [FINOS Legend](https://legend.finos.org)
components for quick and easy deployment of a Legend stack.

This repository contains a [Juju](https://juju.is/) Charm for
deploying a service which exposes a MongoDB database to other Legend Charms.

The full Legend solution can be installed with the dedicated
[Legend bundle](https://charmhub.io/finos-legend-bundle).


## Usage

The Legend Database Operator can be deployed by running:

```sh
$ juju deploy finos-legend-db-k8s --channel=edge
```


## Relations

The standalone DBM will initially be blocked, and will require being
related to the [MongoDB Operator](https://github.com/canonical/mongodb-operator).

```sh
$ juju deploy mongodb-k8s --channel=edge
$ juju relate finos-legend-db-k8s mongodb-k8s
# Relate to the legend services:
$ juju relate finos-legend-db-k8s finos-legend-sdlc-k8s
$ juju relate finos-legend-db-k8s finos-legend-engine-k8s
$ juju relate finos-legend-db-k8s finos-legend-studio-k8s
```

## OCI Images

This charm has no actual workload container, but deploys a shell container
based on the [Ubuntu Xenial](https://hub.docker.com/_/ubuntu) image.

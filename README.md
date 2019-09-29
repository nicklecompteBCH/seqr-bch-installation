# seqr-bch-installation

## Introduction

This is the port of [seqr](https://github.com/macarthur-lab/seqr) currently running at Boston Children's Hospital. As such it is free software, and distributed without warranty. Please see [LICENSE.md](https://github.com/nicklecompeBCH/seqr-bch-installation/LICENSE.md) for details.

There have been a number of changes to the deployment scripts, infrastructure, the hail-elasticsearch-pipelines, and Seqr itself compared to the upstream source. At a high level:

- gcloud provisioning / DAL code has been replaced with code that is more AWS-friendly
- there is no dependency on kubernetes
- All code is running on Python 3.6 + and dependencies have been upgraded accordingly
- hail 0.2 is built from source and a modified v02 hail-elasticsearch-pipelines is used to load VEP results into ES
- I am still not sure exactly what is going on, but it appears that the Perl dependencies in VEP [sometimes don't play very nicely with hail / Spark on AWS Linux 2](https://discuss.hail.is/t/vep-annotation-ioexception-error-13-permission-denied/837/11). I have used a custom vep87.json which provides at least a workaround for this issue.

This repository also contains a custom, extremely alpha Python 3 module for managing infrastructure. It is essentially Terraform for people who are bad at reading Terraform :) But we have emphasized cluster-level and application-level abstractions, and also kept a close eye on cost.

## `common` module

This is the equivalent of a Prelude in a Haskell-like language. Grab-back of `functools`-style helpers, typecheckers, and so on. Primarily used in the `architecture` module but we would like to move more of Seqr into a typechecked environment.

## `architecture` module

See [architecture/README.md](https://github.com/nicklecompeBCH/seqr-bch-installation/architecture/README.md) for a more detailed technical overview (assuming I have written it).

The architecture module provides structred Python types and functions for managing deployments, local and cloud. It is written in a way that may be extensible to a physical datacenter.

## `app`

This contains Seqr and hail-elasticsearch-pipelines.

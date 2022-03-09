DaggyR
======

Description
===========

DaggyR is a work orchestration framework for running workflows modeled as
directed, acyclic graphs (DAGs). These are quite useful for all kinds of
work flows, especially Fetch, Extract, Transform, Load (FETL) workloads.

Below is an example workflow where data are pulled from three sources
(A, B, C), some work is done on them, and a report is generated.

Each step depends on the success of its upstream dependencies, e.g.
`Derive_Data_AB` can't run until `Transform_A` and `Transform_B` have
completed successfully.

```mermaid
graph LR
  Pull_A-->Transform_A;
  Pull_B-->Transform_B;
  Pull_C-->Transform_C;

  Transform_A-->Derive_Data_AB;
  Transform_B-->Derive_Data_AB;
  Derive_Data_AB-->Derive_Data_ABC;
  Transform_C-->Derive_Data_ABC;

  Derive_Data_ABC-->Report;
```

Individual tasks (vertices) are queued as soon as their upstream tasks have
completed successfully, and run via a task executor back end as soon as
that back end has capacity.

Executors
---------

Daggy supports multiple executor back ends:

- Local executor (via fork)
- [Slurm](https://slurm.schedmd.com/overview.html)
- SSH (run tasks on SSH remotes)
- Remote agent -- An agent you can run on a host and submit jobs to
- [kubernetes](https://kubernetes.io/) (planned)

Trackers
--------

Run state is maintained via trackers. Currently daggy supports an
in-memory state manager. Future plans include supporting SQL
[postgres](https://postgresql.org).

Running the Server
==================

```bash
# By default will use a memory tracker and local executor with
# min(1, # of cores -2) workers / task parallelism
cargo run --bin server
```

More detailed configurations and exmaples are in the `examples` directory.

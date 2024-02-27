---
layout: default
title: VIP-4 Store Lifecycle Hooks
parent: Proposals
permalink: /docs/proposals/vip-4
---

# VIP-4: Store Lifecycle Hooks

* **Status**: _Under Discussion_
* **Author(s)**: _Felix GV_
* **Pull Request**: _N/A_
* **Release**: _N/A_

## Introduction

The [Venice Push Job](../user_guide/write_api/push_job.md) takes data from a grid and pushes it to a store in all 
regions. This works fine in many cases, but there are some use cases where we would like to have greater control over 
the steps of the process. This proposal is to add new configs and hooks which can be used both to monitor and control 
the push job in a finer-grained manner than is possible today. In particular, this proposal focuses on the way that each 
individual region is handled.

Currently, the sequence of steps happening within a push job is as follows:

![Push Job Steps](../assets/images/push_job_steps.drawio.svg)

## Problem Statement 

Pushing to all regions in parallel makes the push faster, but it also means that if the push causes an issue, the impact
is going to be global (affecting all regions). It would be desirable to have certain checks and balances that reduce the
blast radius in cases where the content of the push causes issues. Examples of such issues include:

* Data quality issues:
  * Incomplete data (due to issues in data generation logic, or in upstream datasets).
  * Change in semantics (e.g. some embeddings trained by a new ML model / weights / params / etc. are incompatible with 
    that used at inference-time).
* Schema-related issues:
  * Some optional fields which used to always be populated get deleted (or get populated with null) and the reading app 
    fails due to lack of null checking. This kind of app-side bug can happen even though a schema evolution is fully 
    compatible.
* Infra issues:
  * Larger payloads take more resources, resulting in lack of capacity and thus latency degradation.
  * Certain types of yet-unknown infra bugs are somehow triggered by a data push.

Tighter control of how the new version of the dataset is deployed to each region could allow us to catch issues while
only one region is affected, and abort deploying to other regions. See Scope, below, for specific examples of flow 
control strategies.

In addition, we would like to make it easier to integrate the push job into proprietary monitoring systems such that 
each region getting data deployed to it results in events getting emitted or other observability actions.

## Scope

This proposal is about full push jobs. Incremental pushes and nearline writes are out of scope. At the time of 
submitting this proposal, it is undetermined whether this work will apply to stream reprocessing jobs. In terms of 
priority, we care mostly about supporting full pushes from offline grids, and it may be fine to leave stream 
reprocessing out of scope, although depending on the design details we choose, we may be able to support stream 
reprocessing "for free" as well.

## Project Justification

The cost of having global impact in the case of issues mentioned above is too high, and we would like to provide 
first-class options to reduce the blast radius. Building this within Venice itself will make it easier to automate these
methodologies, thus reducing toil for users.

## Functional specification

The proposed API for this described in code here:

- StoreLifecycleHooks, which is the main part of this proposal.
- StoreLifecycleHooksFactory.
- StoreLifecycleEventOutcome.
- StoreVersionLifecycleEventOutcome.

## Proposed Design


## Development Milestones


## Test Plan


## References 


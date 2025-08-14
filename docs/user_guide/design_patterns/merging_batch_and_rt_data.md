---
layout: default
title: Merging Batch & Real-Time Data
parent: Design Patterns
grand_parent: User Guides
permalink: /docs/user_guide/design_patterns/merging_batch_and_rt_data
---

# Merging Batch & Real-Time Data

Venice being a derived data platform, an important category of use cases is to merge batch data sources and real-time
data sources. This is a field where the industry has come up with multiple patterns over the past decade and a half.
This page provides an overview of all these patterns, and how they can be implemented in Venice. The patterns are 
presented in the order they were published over the years.

While it is useful to understand the history of how things were done in the past and how they have evolved over time, in 
practice, most Venice users choose the [Hybrid Store](#hybrid-store) design pattern. Impatient readers may choose to go 
directly to that section if they wish to skip the historical context.

## Lambda Architecture

The Lambda Architecture was proposed by [Nathan Marz](https://github.com/nathanmarz) in 2011, in a blog post oddly 
titled [How to beat the CAP theorem](http://nathanmarz.com/blog/how-to-beat-the-cap-theorem.html). While the post 
describes what has come to be known as the "lambda architecture", it does not actually mention it by that name. Some
have also commented that it is questionable whether the CAP theorem is "beaten" at all, but these details matter little 
to the subject at hand.

In a nutshell, the idea is to have two parallel pipelines, one for batch, and one for data. Both of these pipelines are
going to perform both processing and serving of their respective data, each using specialized technology for the job.
Only at the periphery are the two pipelines merged together, presumably within the user's own application.

## Kappa Architecture

The Kappa Architecture was proposed by [Jay Kreps](https://github.com/jkreps) in 2014, in a blog post titled 
[Questioning the Lambda Architecture](https://www.oreilly.com/radar/questioning-the-lambda-architecture/).

## Hybrid Store


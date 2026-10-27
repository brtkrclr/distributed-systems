---
title: Distributed Systems Internals
description: From one machine to many regions, a connected, from-scratch tour of how distributed systems really work.
permalink: /
---

# Distributed Systems Internals

Notes on how systems made of many machines actually work, from a single request's latency budget up to a multi-region design with a bill of materials. They pick up where [System Internals](https://brtkrclr.github.io/system-internals/) leaves off: one Linux machine is no longer enough. Each part builds on the previous ones. Look for the **🔗 Connections** boxes: they show where a concept from one part shows up again in another. The **💡 Interview tip** boxes are the things worth saying out loud in a system design / NALSD interview. The running example is a URL shortener.

**Contents**

0. [The Big Picture: How Everything Connects](#0-the-big-picture-how-everything-connects)
1. [Numbers and Estimation](#1-numbers-and-estimation)
2. [Fundamentals](#2-fundamentals)
3. [Time, Clocks and Ordering](#3-time-clocks-and-ordering)
4. [Consistency Models](#4-consistency-models)
5. [Replication](#5-replication)
6. [Consensus and Coordination](#6-consensus-and-coordination)
7. [Partitioning (Sharding)](#7-partitioning-sharding)
8. [Caching](#8-caching)
9. [Storage Engines and Transactions](#9-storage-engines-and-transactions)
10. [Messaging and Streams](#10-messaging-and-streams)
11. [Scalability Building Blocks](#11-scalability-building-blocks)
12. [Reliability, Resilience and Observability](#12-reliability-resilience-and-observability)
13. [Probabilistic and Distributed Data Structures](#13-probabilistic-and-distributed-data-structures)
14. [Putting It All Together: Designing a URL Shortener, Every Layer (NALSD style)](#14-putting-it-all-together-designing-a-url-shortener-every-layer-nalsd-style)
15. [Questions and Answers](#15-questions-and-answers)

---

# 0. The Big Picture: How Everything Connects

A distributed system is many machines pretending to be one. This part shows the whole picture on one page, names the three illusions every distributed system tries to keep up, and traces one URL shortener redirect through every layer, with the part number behind each step. Come back here whenever a later part feels disconnected from the rest.

Here is a production URL shortener running in two regions. Almost every part of these notes is a box or an arrow in this picture:

```
                        users in the US, EU, ...
                                  |
          DNS / GeoDNS / anycast (11.3): "send me to a nearby, healthy region"
                                  |
                CDN / edge PoP (11.4), HTTP caching (8.9), TLS (11.9)
                                  |
          +-----------------------+--------------------------+
          v                                                  v
+------------- REGION us-east -------------+  +------ REGION eu-west -----+
| L7 load balancer (11.2)                  |  | the same stack, with a    |
|   health checks (12.6), rate limit (11.7)|  | FULL copy of the data     |
|        |                                 |  |                           |
| app servers: stateless (11.1)            |  | serves EU clicks locally: |
|   sized with Little's Law (2.4, 1.7)     |  | p99 < 100 ms needs that   |
|   timeouts, retries (2.7)                |  | (1.1, 2.3)                |
|   breakers, shedding (12.4, 12.5)        |  |                           |
|        |                  \              |  | on a local miss: ask the  |
|        v                   ID ranges     |  | home region (5.4, 14.8)   |
| cache cluster (Part 8)     (11.6, 6.8)   |  |                           |
|        | miss                            |  |                           |
|        v                                 |  |                           |
| database, sharded (Part 7)               |  |                           |
|   replicas in AZ-a | AZ-b | AZ-c (5.8)   |  |                           |
|   quorums / consensus per shard (Part 6) |  |                           |
|   B-tree or LSM on each disk (Part 9)    |  |                           |
|        | change data capture (10.6)      |  |                           |
|        v                                 |  |                           |
| Kafka: click events, cache invalidations |  |                           |
|   (Part 10)                              |  |                           |
+------------------------------------------+  +---------------------------+
          ^    async cross-region replication (5.3, 5.8)     ^
          +---------- lag = seconds, not zero (5.4) ---------+

 underneath everything:
   numbers and failure math ........ Parts 1, 2   (how big, how often)
   clocks and ordering ............. Part 3       (who wrote last?)
   consistency models, CAP ......... Part 4       (what may a read return?)
   SLOs, failure domains, tracing .. Part 12      (how we know it works)
```

## 0.1 What Makes a System "Distributed"

A system is **distributed** when its parts run on separate machines that cooperate **only by sending messages over a network**. Three things follow from that one sentence, and they are the source of every hard problem in these notes:

1. **No shared memory.** Machine A can't look at machine B's RAM. It can only ask, and the answer is already old when it arrives (2.1).
2. **No global clock.** Each machine has its own clock, and they drift apart. "Which write happened first?" has no free answer (Part 3).
3. **Independent, partial failure.** One machine, one disk, one switch, one availability zone can die while the rest keeps running. Worse, from the outside a dead machine, a slow machine and a lost message look the same: silence (2.2).

Leslie Lamport's definition is the one to remember: *"A distributed system is one in which the failure of a computer you didn't even know existed can render your own computer unusable."*

### Why Distribute at All?

Distribution is expensive, so you need a reason. There are only three:

| Reason | What one machine can't do | URL shortener example | Where |
| :---- | :---- | :---- | :---- |
| **Scale** | Hold all the data or serve all the requests. | 3.6 TB, 12K QPS at peak. (Honestly: one big box could almost do it, see 14.3.) | Parts 1, 7, 11 |
| **Availability** | Stay up when it dies, reboots or gets patched. | 99.99% means ≤ ~4.3 minutes of downtime per month. One machine can't promise that. | 1.3, Parts 5, 12 |
| **Latency** | Be close to users on every continent. | A US-only server adds ~100–150 ms RTT for EU users, which blows the 100 ms p99. | 1.1, 2.3, 5.8 |

> 💡 **Interview tip:** Say which of the three you're buying before you add machines. For the URL shortener, the honest answer is "availability and latency, more than throughput". That one sentence shows the interviewer you are designing, not reciting.

### You Already Know Most of This

If you read the Linux notes, almost every idea here is a familiar one, stretched across a network where messages are slow and can get lost:

| On one machine (Linux notes) | Across machines (these notes) | Where |
| :---- | :---- | :---- |
| CPU caches, MESI invalidation | Distributed caches, cache invalidation | Part 8 |
| Write-back page cache, `fsync()` | Async vs sync replication | 5.3 |
| Journal / write-ahead log | WAL, replication log, Raft log | 9.4, 6.4 |
| Mutex, atomic compare-and-swap | Leases, fencing tokens, consensus | 6.6, 6.7 |
| Scheduler, cgroups | Load balancing, rate limiting, Borg/Kubernetes | 11.2, 11.7, 14.11 |
| Process crash, `SIGKILL` | Node crash, failover | 5.5, 12.2 |
| Monotonic vs wall clock | Same problem, plus clocks on *different* machines | 3.2 |

The big difference: on one machine, the hardware gives you coherence and a single clock for free. Across machines you have to build them yourself, and you pay for them in round trips.

> 🔗 **Connections:** The three properties above map onto the three illusions in 0.2. "No shared memory" is why we replicate and cache (Parts 5, 8). "No global clock" is why we need logical clocks and consensus (Parts 3, 6). "Partial failure" is why timeouts, retries and idempotency (2.6, 2.7) appear in every design.

## 0.2 The Three Illusions

A well-built distributed system gives its clients three illusions, on top of hardware that is spread out, slow to talk to and constantly failing:

| Illusion | What the client believes | Reality | How the system fakes it | Where |
| :---- | :---- | :---- | :---- | :---- |
| **1. One machine** | "There's one database, with one copy of my data, in one place." | Data is split across many shards, each copied 3+ times across zones and regions. | Partitioning, request routing, replication, unique IDs. | Parts 5, 7, 11 (11.6) |
| **2. Always up** | "It never fails." | Disks, servers, racks, zones and whole regions fail. Networks drop and delay packets. | Redundancy across failure domains, failover, consensus, retries, health checks, load shedding. | Parts 2, 5, 6, 12 |
| **3. Instant and consistent** | "Every read sees the latest write, immediately." | A round trip across the Atlantic takes ~70–80 ms, limited by the speed of light in fiber. Replicas lag. Clocks disagree. | Caches and local replicas for speed, consistency models and clocks to define what "latest" means. | Parts 1, 3, 4, 8 |

The three pull against each other. Illusion 3 (consistent) wants every read to check with one authoritative copy. Illusion 2 (always up) wants any copy to answer even when the others are unreachable. That tension is exactly what CAP and PACELC describe (4.6, 4.7), and most of the design choices in these notes are about picking which illusion to weaken, where, and by how much.

When the illusions break, you get the classic bugs:

| Broken illusion | Symptom | Where |
| :---- | :---- | :---- |
| One machine | Two leaders both accept writes after a network partition: **split brain**. | 5.5, 6.6 |
| One machine | A double-clicked "Shorten" button creates two different short codes. | 2.6 |
| Always up | A slow database makes every app thread wait; the whole fleet stops answering. | 2.4, 12.3 |
| Always up | Clients retry a struggling service and turn a blip into an outage. | 2.7, 12.3 |
| Instant and consistent | A link created in the US returns 404 when clicked in the EU two seconds later. | 5.4, 14.8 |
| Instant and consistent | A user changes a link's destination; some clicks still go to the old one. | 8.5, 8.9, 14.7 |

> 🔗 **Connections:** Part 14 is built around these failures. Every "don't know" in the mock interview (server sizing, multi-region replication, replication lag, cache invalidation) is one of these illusions breaking, and 14.4, 14.5, 14.7 and 14.8 show how to keep each one up.

## 0.3 The One Request to Remember: a Redirect's Journey

If you remember one thing, remember this request. A user in Paris clicks `https://sho.rt/aB3xK9p`, a link someone created in New York yesterday.

```
 TIME ------------------------------------------------------------------->

 browser: DNS ---> TCP+TLS ---> GET /aB3xK9p ................. 302 ---> dest
          |           |              |                          ^
          |           |              v                          |
          |           |       edge/CDN: cached? --no--> EU LB --+
          |           |                                 |       |
          |           |                       app: cache GET ---+ hit (~90%)
          |           |                                 | miss
          |           |                       EU DB replica ----+ found
          |           |                                 | miss (new link)
          |           |                       US home region ---+ (~100 ms)
          |           |
          |           +-- 1 RTT to a nearby PoP (~10-20 ms), TLS 1.3 adds 1
          +-- usually cached by the OS/resolver; GeoDNS picks EU (11.3)

 later, off the critical path:  click event ---> Kafka ---> analytics (10.2)
```

Step by step, with the part behind each one:

1. **DNS.** The browser resolves `sho.rt`. **GeoDNS** or **anycast** points it at the closest healthy region, here `eu-west` (11.3). This is the first place region failover happens (14.9).
2. **Connection.** TCP and TLS handshakes to a nearby edge. Each costs a round trip, which is why edges exist (11.4, 1.1) and why connection reuse matters (11.2).
3. **Edge / CDN.** If the redirect was served with `Cache-Control: max-age=...`, the CDN can answer without touching our servers. Whether that's allowed depends on 301 vs 302 and how fast edits must take effect (8.9, 14.7).
4. **Load balancer.** An L7 load balancer picks a healthy app server, using readiness checks to skip broken ones (11.2, 12.6). Rate limiting stops someone scanning random codes (11.7).
5. **App server.** Stateless, so any server can answer (11.1). It has a timeout budget for each downstream call, and retries at most once, with jitter (2.7). There are enough servers because of Little's Law (2.4) and failure-domain headroom (1.7).
6. **Cache.** `GET url:aB3xK9p` on a Redis/Memcached cluster, sharded by consistent hashing (7.4, Part 8). ~90% of redirects end here in ~1 ms (8.2).
7. **Database replica.** On a miss, read the local replica of the shard that owns `aB3xK9p` (7.3, 7.7). Fill the cache (cache-aside, 8.3).
8. **Replication lag.** The link was created in the US. If it's brand new, the EU replica might not have it yet (5.4). Instead of returning a wrong 404, the app asks the **home region** directly, and only negative-caches a 404 the home region confirms (8.8, 14.8).
9. **Response.** A `302 Found` with `Location: https://destination.example/...`. The browser then talks to the destination, not to us.
10. **After the response.** The app emits a click event to Kafka for analytics (10.1, 10.2). Counting happens asynchronously so it never adds latency to the redirect.

The latency budget for the p99 < 100 ms target looks like this (server-side, measured at the load balancer):

| Step | Typical | Notes |
| :---- | :---- | :---- |
| LB + app processing | ~1–2 ms | Mostly TLS termination and parsing. |
| Cache hit | ~0.5–1 ms | Same-AZ round trip. |
| Cache miss → local DB | +2–10 ms | Index lookup, maybe one SSD read (9.2, 9.3). |
| Local miss → home region | +~80–150 ms | Only for very new links clicked far away (5.4, 14.8). |

The lesson: the common path is a few milliseconds, and the expensive cross-region path is reserved for the rare case. That's the whole trick of a multi-region read path, and it's why the design meets a p99 target that one cross-ocean round trip alone would break.

The **write path** (creating a link) is the mirror image: rare (~40/s on average), allowed to be slower, and it must never lose data. It gets an ID from a pre-allocated range (11.6), writes to a quorum of replicas across three AZs (5.3, 6.2), uses an idempotency key so a retry doesn't create a second link (2.6), and replicates asynchronously to the other regions (5.8).

> 🔗 **Connections:** Part 14 repeats this journey with full numbers, then breaks it on purpose: lose a server, an AZ and a region (14.9). Many Q&As in Part 15 are "what happens at step N when X fails?"

## 0.4 Reading Order

- **Parts 1–2** are the vocabulary and the math: latency numbers, estimation, availability nines, Little's Law, tail latency, retries. Read them first; every later design decision is justified with these numbers.
- **Parts 3–4** are the theory of "when" and "what may a read return": clocks, ordering, consistency models, CAP.
- **Parts 5–7** are how data lives on many machines: replication, consensus, partitioning. This is the core of most interviews.
- **Parts 8–10** are the performance and plumbing layers: caching, storage engines and transactions, messaging and streams.
- **Parts 11–13** are the building-block catalog: load balancers, DNS, IDs, rate limiters, reliability patterns, observability, and the probabilistic data structures (Bloom filters, HyperLogLog, ...) that keep showing up.
- **Part 14** designs a URL shortener end to end, NALSD style, and answers every question from the mock interview.
- **Part 15** is a set of questions that only make sense once you see the connections.

If your interview is soon, a fast path: 1.7–1.8, 2.4, 2.7, 4.6, 5.3–5.5, 5.8, 6.2, 7.4, 8.5, then all of Part 14.

---

# 1. Numbers and Estimation

Every system design answer rests on a few numbers: how many requests, how many bytes, how many machines, and how long each step takes. This part gives you the reference numbers to memorize and a repeatable method for back-of-the-envelope math, ending with the full URL shortener envelope that the rest of these notes build on (Part 14).

## 1.1 Latency Numbers

You can't design a system without knowing what is cheap and what is expensive. The key fact: **each step outward from the CPU costs roughly 10× to 1,000× more than the step before it.** A design that does one cross-region call per request lives in a different world from one that does one memory lookup.

Approximate, order-of-magnitude values (memorize the order of magnitude, not the digits):

| Operation | Time | Scaled: if 1 ns were 1 second |
| :---- | :---- | :---- |
| L1 cache hit | ~1 ns | 1 s |
| Uncontended mutex lock/unlock | ~20 ns | 20 s |
| Main memory (DRAM) access | ~100 ns | ~2 min |
| Compress 1 KB (Snappy/LZ4-class) | ~2 µs | ~30 min |
| Send 1 KB over a 10 Gbps link (serialization only) | ~1 µs | ~15 min |
| Read 1 MB sequentially from memory | ~50–100 µs | ~1 day |
| NVMe SSD random read (4 KB) | ~20–100 µs | ~0.25–1 day |
| Round trip within one datacenter / AZ | ~0.1–0.5 ms | ~1–6 days |
| Redis/Memcached GET over the network | ~0.2–1 ms | ~2–12 days |
| Read 1 MB sequentially from SSD | ~0.3–1 ms | ~4–12 days |
| Round trip between AZs in one region | ~1–2 ms | ~1.5–3 weeks |
| Indexed point query in Postgres/MySQL (warm) | ~1 ms | ~12 days |
| HDD seek | ~5–10 ms | ~2–4 months |
| Read 1 MB sequentially from HDD | ~5–10 ms | ~2–4 months |
| Round trip US East ↔ US West | ~60–70 ms | ~2 years |
| Round trip US East ↔ Western Europe | ~70–100 ms | ~3 years |
| Round trip US ↔ Asia / Australia | ~150–250 ms | ~5–8 years |

Where the cross-region numbers come from: light in fiber travels at ~200,000 km/s, which is **~5 µs per km, one way**. New York to London is ~5,600 km in a straight line, so the physics floor is ~28 ms one way, ~56 ms round trip. Real fiber paths are not straight and add routers, so you see ~70+ ms. **No amount of engineering removes this.** You can only avoid crossing the ocean.

```
  where a request's time goes (one redirect, cache hit, same AZ)

  client --- ~20-80 ms internet RTT ---> LB ---> app ---> Redis
                                              <0.5 ms    <1 ms
  server-side work: ~1-2 ms          network to the user: ~10s of ms

  same redirect, but the data lives in another region:
  app (EU) ------ ~90 ms RTT ------> DB (US)
  server-side work: ~90+ ms   <- the cross-region hop dominates everything
```

Things to take away:

- **Memory is ~1,000× faster than a network round trip.** A local in-process cache (8.1) beats a remote Redis by ~1,000×. A remote Redis beats a cross-region DB read by ~100×.
- **Sequential beats random.** That is the whole reason LSM trees (9.3) and append-only logs (10.2) exist.
- **A TLS handshake costs 1–2 extra round trips** (TLS 1.3 / 1.2). For a user 100 ms away, a fresh connection costs ~200–300 ms before the first byte. This is why connections are kept alive and why CDNs terminate TLS close to users (11.4).
- **Within one region, the network is cheap compared to disk on HDD, but not compared to memory.** Inside an AZ you can afford a few hops per request. Across regions you can afford **zero** hops on the hot path if your latency target is under ~100 ms.

> 💡 **Interview tip:** When the interviewer says "p99 redirect latency under 100 ms globally", immediately say: "A US↔EU round trip is ~80–100 ms, so a redirect for an EU user can't touch a US database on the hot path. Data must be readable in every region." That one sentence motivates multi-region replication (5.8) and the replication-lag problem (5.4, 14.8).

> 🔗 **Connections:** These numbers drive tail latency (2.3), timeout values (2.7), cache placement (8.1), why consensus across regions is slow (6.4: every commit is at least one cross-region round trip), and why Spanner's commit wait is acceptable (3.6: ~ms of waiting is small next to the network).

## 1.2 Powers of Two and Time Conversions

Envelope math is mostly about getting the **exponent** right. Keep everything in powers of ten and round aggressively.

### Sizes

| Power of 2 | Exact | ≈ Power of 10 | Name |
| :---- | :---- | :---- | :---- |
| 2^10 | 1,024 | 10^3 | KB (thousand) |
| 2^20 | 1,048,576 | 10^6 | MB (million) |
| 2^30 | ~1.07 × 10^9 | 10^9 | GB (billion) |
| 2^40 | ~1.1 × 10^12 | 10^12 | TB (trillion) |
| 2^50 | ~1.13 × 10^15 | 10^15 | PB |
| 2^32 | ~4.3 × 10^9 | 4 × 10^9 | max uint32, IPv4 space |
| 2^64 | ~1.8 × 10^19 | 2 × 10^19 | max uint64 |

The ~2–12% gap between 2^n and 10^n doesn't matter in an interview. Use decimal (10^3) and say so.

Handy key-space sizes for short codes (11.6):

```
base62 alphabet: [a-z A-Z 0-9] = 62 symbols

62^6 ~ 5.7 x 10^10   (57 billion)
62^7 ~ 3.5 x 10^12   (3.5 trillion)
62^8 ~ 2.2 x 10^14

100M URLs/month x 12 x 10 years = 1.2 x 10^10   -> 7 chars is plenty
```

### Time

| Period | Exact seconds | Use this |
| :---- | :---- | :---- |
| 1 day | 86,400 | **~10^5** (or 8.64 × 10^4 when precision matters) |
| 1 month | ~2.6 × 10^6 | **~2.5 × 10^6** |
| 1 year | ~3.15 × 10^7 | **~3 × 10^7** |

```
QPS shortcuts (per day -> per second, using 10^5 s/day):

  1 M/day   ->    ~12 /s        ("a million a day is about 10 a second")
  100 M/day ->  ~1,200 /s
  1 B/day   -> ~12,000 /s

per month -> per second (2.5 x 10^6 s/month):
  100 M/month -> 100 x 10^6 / 2.5 x 10^6 = 40 /s
```

> 💡 **Interview tip:** Say your rounding out loud: "I'll use 10^5 seconds per day; that's ~15% high, which is fine for sizing." Interviewers care that you're consistent and aware, not that you multiply 86,400 in your head.

> 🔗 **Connections:** These conversions feed every estimate in 1.4–1.8 and the capacity plan in 14.2. Key-space math decides short-code length (11.6, 14.6).

## 1.3 Availability Nines and Downtime

**Availability** is the fraction of time (or of requests) a system serves correctly. It's quoted in "nines". Memorize the yearly and monthly downtime budgets:

| Availability | Downtime / year | Downtime / month | Downtime / day |
| :---- | :---- | :---- | :---- |
| 99% (two nines) | ~3.65 days | ~7.3 h | ~14.4 min |
| 99.9% (three nines) | ~8.8 h | ~43.8 min | ~1.4 min |
| 99.95% | ~4.4 h | ~21.9 min | ~43 s |
| 99.99% (four nines) | ~53 min | ~4.4 min | ~8.6 s |
| 99.999% (five nines) | ~5.3 min | ~26 s | ~0.86 s |

The useful intuition: **at four nines, a human can't even get paged and log in before the monthly budget is gone.** Four nines and above require automated failover (5.5) and automated rollback (12.7), not people.

### Serial availability: dependencies multiply

If a request needs A **and** B **and** C to all work, the availabilities multiply:

```
A_total = A1 x A2 x A3

LB 99.99%  ->  app 99.95%  ->  DB 99.9%
0.9999 x 0.9995 x 0.999 = ~0.9984   -> ~99.84%, worse than any single part
```

Every hard dependency you add on the hot path **lowers** your ceiling. A service can't be more available than the product of its hard dependencies. That's why a redirect path should depend on as few things as possible (cache, then DB fallback), and why analytics should be asynchronous (10.1) rather than a synchronous call that can fail the redirect.

### Parallel availability: redundancy multiplies the failure probability

If you have redundant copies and need **any one** of them:

```
A_total = 1 - (1 - A)^n

two replicas at 99%:    1 - 0.01^2 = 0.9999        -> 99.99%
three replicas at 99%:  1 - 0.01^3 = 0.999999      -> 99.9999%
```

This formula assumes **independent** failures. Real failures are correlated: replicas in the same rack share a switch, replicas in the same AZ share power, every replica runs the same buggy binary. That's why you spread replicas across **failure domains** (12.2) and roll out changes gradually (12.7). In practice, redundancy gets you from ~99% to ~99.9–99.99%, not to the six nines the math promises.

> 💡 **Interview tip:** When you add a component, say what it does to availability: "Adding Redis as a hard dependency would cap us at Redis's availability, so on a cache error we fall back to the DB." That shows you think in serial vs parallel paths.

> 🔗 **Connections:** Nines become SLOs and error budgets (12.1). The independence assumption is exactly what failure domains (12.2) and multi-AZ placement (5.8) try to make true. Quorum systems (6.2) are a parallel-availability trick: any majority will do.

## 1.4 QPS Estimation

**QPS** (queries per second) is the number you derive first; everything else (bandwidth, servers, cache size) multiplies off it.

### The method

```
average QPS = (users x actions per user per day) / 86,400
peak QPS    = average QPS x peak factor
```

1. Start from a given number (DAU, writes per month, events per day).
2. Split **reads and writes**. They're usually very different (a URL shortener is ~100:1 read-heavy), and they scale differently: reads scale with caches and replicas (Part 8, 5.2), writes scale with partitioning (Part 7).
3. Convert to **average** per second.
4. Multiply by a **peak factor**.

### Peak factor

Traffic is not flat. Most consumer services have a daily cycle where the busiest hour is ~2–3× the daily average. Events (a viral link, a product launch, a sports final) can spike far higher, briefly.

| Traffic shape | Typical peak ÷ average |
| :---- | :---- |
| Global service, users spread across time zones | ~1.5–2× |
| Regional consumer service (daily cycle) | ~2–3× |
| Event-driven spikes (launch, viral link, flash sale) | ~10× or more, short-lived |

Use 2–3× unless told otherwise, and say that viral spikes are handled with caching, autoscaling and load shedding (12.5), not by provisioning 10× all year.

### Example

```
"10 M daily active users, each does 20 reads and 1 write per day"

reads:  10^7 x 20 / 10^5 = 2,000 QPS avg -> x3 peak = 6,000 QPS
writes: 10^7 x 1  / 10^5 =   100 QPS avg -> x3 peak =   300 QPS
```

> 🔗 **Connections:** Peak QPS sizes servers (1.7) via Little's Law (2.4). Read QPS × miss rate is your DB load behind a cache (8.2). Write QPS decides whether a single leader is enough (5.2) or you must shard (7.1).

## 1.5 Storage Estimation

### The method

```
logical bytes = items x bytes per item
raw bytes     = logical x replication factor x overhead
```

- **Bytes per item:** add up the fields. Round up.
- **Time horizon:** state it (1, 5 or 10 years). Storage keeps growing; QPS doesn't, necessarily.
- **Replication factor:** typically 3 for a replicated database (one leader + two followers, or N=3 in Cassandra/DynamoDB-style systems, 5.7). Object stores like S3 use erasure coding, which costs ~1.2–1.5× instead of 3× (9.9).
- **Overhead:** indexes, per-row headers, B-tree page fill factor, LSM space amplification during compaction (9.3), and free-space headroom so disks never fill (you want disks below ~70–80% full). A **1.5–2×** multiplier is a reasonable default.

```
items x size x replicas x overhead
  |      |        |         |
  |      |        |         `- indexes, compaction, free space: ~1.5-2x
  |      |        `----------- 3 for RF=3 (or ~1.5 for erasure coding)
  |      `-------------------- sum of fields, rounded up
  `--------------------------- per time unit x horizon
```

### Example

```
1 B photos/year metadata, 1 KB each, 5 years, RF=3, 2x overhead

logical: 10^9 x 10^3 B x 5   = 5 x 10^12 B  = 5 TB
raw:     5 TB x 3 x 2        = 30 TB
```

Then ask: does it fit on one machine? A single server holds ~a few to tens of TB of SSD. 30 TB raw means ~5 TB logical per replica set member: possible on one big box, but you'd likely shard to keep each shard small enough to rebuild quickly (7.1, 7.6).

> 🔗 **Connections:** The total decides whether you shard (7.1) and how many partitions you need. The working set (the hot fraction) decides cache size (8.2). Replication factor comes from 5.1 and 6.2.

## 1.6 Bandwidth Estimation

```
bandwidth (bytes/s) = QPS x bytes per request (or response)
bandwidth (bits/s)  = bytes/s x 8
```

**Watch the units.** Storage and payload sizes are quoted in **bytes** (B). Network links are quoted in **bits** per second (bps). A "1 Gbps" NIC moves **125 MB/s**, not 1 GB/s.

| Link | ≈ Bytes/s |
| :---- | :---- |
| 1 Gbps | 125 MB/s |
| 10 Gbps | 1.25 GB/s |
| 25 Gbps (common cloud NIC) | ~3 GB/s |
| 100 Gbps | 12.5 GB/s |

Separate **ingress** (writes coming in) from **egress** (responses going out). Cloud providers usually charge for egress to the internet and across regions, so egress is also a cost line (14.10).

```
example: 6,000 QPS of reads, 50 KB average response

6,000 x 50 KB = 300 MB/s  x 8 = 2.4 Gbps egress at peak
```

2.4 Gbps saturates a couple of 1 Gbps links but is easy on a 25 Gbps NIC. For large media, egress is usually served by a CDN (11.4), not your servers.

> 💡 **Interview tip:** Always write "B" vs "b" carefully and convert once, at the end. "12 MB/s, which is ~100 Mbps" sounds confident and avoids the classic 8× error.

> 🔗 **Connections:** Bandwidth decides whether you need a CDN (11.4), how much cross-region replication traffic costs (5.8), and NIC sizing in the bill of materials (14.10).

## 1.7 Server Count and Failure-Domain Headroom

### Step 1: servers needed to carry peak

```
servers = peak QPS / sustainable QPS per server
```

If the interviewer doesn't give you QPS per server, **derive it with Little's Law (2.4)** from CPU time per request and core count. Don't say "I don't know": say "Let me assume 2 ms of CPU per request on 8 cores."

### Step 2: add headroom for failures and maintenance

The number from step 1 is the fleet you need **while everything is healthy**. Real fleets lose machines all the time: crashes, deploys (12.7), kernel updates, a whole AZ outage (12.2).

| Rule | Meaning | When |
| :---- | :---- | :---- |
| **N+1** | Survive losing any one server | Minimum for anything that matters |
| **N+2** | Survive one failure **while** one is down for maintenance/deploy | Common production default |
| **Survive an AZ** | Remaining AZs must carry full peak | Anything with an availability SLO above ~99.9% |
| **Survive a region** | Remaining regions must carry full peak | Multi-region active-active (5.8) |

### Surviving the loss of a failure domain

If you spread servers across **k** equal failure domains and want to survive losing one, the remaining fraction is (k−1)/k, and **that** must carry peak:

```
total servers = N / remaining fraction = N x k / (k - 1)

N = 6 servers needed at peak

2 AZs: 6 / (1/2) = 12   (each AZ alone must carry all 6)  -> 100% extra
3 AZs: 6 / (2/3) =  9   (3 per AZ; lose one, 6 remain)   ->  50% extra
4 AZs: 6 / (3/4) =  8   (2 per AZ; lose one, 6 remain)   ->  33% extra
```

This is why **three AZs is the sweet spot**: more domains means less spare capacity, and three is also the minimum for a majority quorum that survives one AZ loss (6.2).

```
normal:     AZ-a [###]   AZ-b [###]   AZ-c [###]     9 servers, 6 needed
AZ-b down:  AZ-a [###]   AZ-b [xxx]   AZ-c [###]     6 left: exactly enough
```

Then add N+1/N+2 on top, within each AZ, so a deploy or a bad host during an AZ outage doesn't push you over the edge.

> 💡 **Interview tip:** Say the formula explicitly: "I need 6 at peak. Spread over 3 AZs, I must survive losing one, so 6 ÷ ⅔ = 9, then one spare per AZ for deploys: 12." It's a short sentence that shows you understand failure domains.

> 🔗 **Connections:** Failure domains are defined in 12.2. The same "remaining capacity must carry the load" logic applies to caches (losing a cache node shifts load to the DB: 8.7, 12.3) and to regions (5.8, 14.9). Without headroom, losing one AZ overloads the others and you get a cascading failure (12.3).

## 1.8 Worked Example: URL Shortener Envelope

This is the envelope for the running example. Part 14 builds the full design on these numbers (14.2).

### Assumptions (given)

| Input | Value |
| :---- | :---- |
| New URLs | 100 M / month |
| Size per URL | 500 B long URL + 100 B metadata = **600 B** |
| Reads | 100 redirects per URL (read:write = 100:1) |
| Peak factor | 3× |
| Redirect response size | 1 KB (headers + small body) |
| Sustainable QPS per app server | 2,000 (derived in 2.4) |
| Availability zones | 3 |

### Writes (creating short URLs)

```
100 M / month / 2.5 x 10^6 s/month = 40 writes/s average
peak: 40 x 3                       = 120 writes/s
```

120 writes/s is tiny. One Postgres leader handles this easily. Writes are **not** the scaling problem here; reads are.

### Reads (redirects)

```
100 M x 100 = 10^10 redirects / month
10^10 / 2.5 x 10^6 = 4,000 QPS average
peak: 4,000 x 3    = 12,000 QPS
```

### Storage

```
per month: 10^8 x 600 B          = 6 x 10^10 B = 60 GB / month
per year:  60 GB x 12            = 720 GB / year
5 years:   720 GB x 5            = 3.6 TB logical
10 years:  720 GB x 10           = 7.2 TB logical

with RF=3:           3.6 TB x 3  = ~11 TB    (5-year)
with ~1.5-2x overhead (indexes, headroom): ~16-22 TB raw
```

3.6 TB logical fits on one large SSD server, but we'll still shard (14.5) so that rebuilding a replica takes hours, not days, and so writes and reads spread out.

Total URLs after 10 years: 10^8 × 120 = 1.2 × 10^10. With base62, 62^7 ≈ 3.5 × 10^12, so **7-character codes** give ~300× headroom (11.6, 14.6).

### Cache size (preview of 8.2)

Redirect traffic is heavily skewed: a small fraction of links gets most clicks. If we cache the URLs created in the last month plus the popular old ones, a rough working set:

```
assume hot set = 20% of last ~3 months of URLs
3 x 10^8 URLs x 0.2 x 600 B = 3.6 x 10^10 B = 36 GB
```

36 GB fits in RAM on a couple of Redis nodes (plus replicas). A high hit ratio here is what keeps the DB load small: at 95% hits, the DB sees 12,000 × 0.05 = 600 QPS at peak.

### Bandwidth

```
egress (redirects): 12,000 QPS x 1 KB = 12 MB/s  x 8 = ~96 Mbps   (~100 Mbps)
ingress (creates):     120 QPS x 600 B = 72 KB/s                  (negligible)
```

100 Mbps peak is small; a single modern NIC is 10–25 Gbps. Bandwidth is not a constraint for this service.

### Servers

```
needed at peak:       12,000 / 2,000 per server   = 6 app servers
survive 1 of 3 AZs:   6 / (2/3)                   = 9   (3 per AZ)
+1 spare per AZ for deploys / bad hosts           = 12  (4 per AZ)
```

### Summary

```
+------------------+---------------------+---------------------------+
| Quantity         | Value               | So what                   |
+------------------+---------------------+---------------------------+
| write QPS (peak) | ~120 /s             | one DB leader is enough   |
| read QPS (peak)  | ~12,000 /s          | cache + stateless app     |
| storage, 5 years | ~3.6 TB logical     | shard for ops, not size   |
|                  | ~11 TB with RF=3    |                           |
| cache working set| ~36 GB              | a few Redis nodes         |
| egress (peak)    | ~100 Mbps           | not a constraint          |
| app servers      | 6 needed, 9 for AZ  | 12 with deploy headroom   |
|                  | loss                |                           |
| code length      | 7 chars base62      | 3.5 x 10^12 codes         |
+------------------+---------------------+---------------------------+
```

> 💡 **Interview tip:** End every envelope with a "so what" column. The numbers are only useful if they change the design: here they say "reads dominate, writes are tiny, data is small, bandwidth is irrelevant, so focus on caching, read replicas in every region, and the replication-lag problem."

> 🔗 **Connections:** Recapped and used in 14.2. Read QPS drives caching (8.2, 14.7). The "reads in every region" conclusion leads to multi-region replication (5.8, 14.8) and replication lag (5.4). Server count comes from Little's Law (2.4) and AZ headroom (1.7).

---

# 2. Fundamentals

A distributed system is a set of machines that talk over a network that can drop, delay or duplicate messages, where any machine can fail independently. This part covers the laws and habits that follow from that: what can go wrong, how to measure latency honestly, how to size things with Little's Law, why some parts never scale, and how to make retries safe.

## 2.1 Fallacies of Distributed Computing

In the 1990s, L. Peter Deutsch and colleagues at Sun listed eight assumptions that programmers new to distributed systems make, all false. Almost **every design decision in a distributed system is a defense against one of them.**

| # | Fallacy | Reality | Design defense |
| :---- | :---- | :---- | :---- |
| 1 | The network is reliable | Packets drop, connections reset, switches fail, replies get lost | Replicas (5.1), retries with idempotency (2.6, 2.7), timeouts |
| 2 | Latency is zero | 0.5 ms in an AZ, ~100 ms across an ocean (1.1) | Caches (Part 8), local reads in each region (5.8), batching |
| 3 | Bandwidth is infinite | Links saturate; cross-region links cost money | Compression, CDNs (11.4), sending deltas, backpressure (10.5) |
| 4 | The network is secure | Anyone on the path may read or alter traffic | TLS/mTLS, authN/authZ (11.9) |
| 5 | Topology doesn't change | Servers come and go, IPs change, autoscaling | Service discovery (11.5), health checks (12.6), DNS (11.3) |
| 6 | There is one administrator | Many teams, many configs, many providers | Clear ownership, API contracts, versioned config, safe rollouts (12.7) |
| 7 | Transport cost is zero | Serialization CPU, egress bills | Efficient encodings (protobuf), co-location, egress-aware design |
| 8 | The network is homogeneous | Mixed hardware, OS versions, protocols, clients | Standard protocols, compatibility testing, defensive parsing |

### The URL shortener version of fallacy #1

Your app server calls the database to look up `abc123`. Three things can happen, and from the app's point of view the last two can look identical:

```
app                       network                       DB
 |  SELECT ... 'abc123'                                  |
 |------------------------------------------------------>|  1. success:
 |<------------------------------------------------------|     reply arrives
 |
 |------------------------------------------------------>|  2. fails/hangs:
 |                        (no reply, ever)               X     DB crashed or
 |                                                             request lost
 |
 |------------------------------------------------------>|  3. succeeds, but
 |                                           X<----------|     the REPLY is
 |                                                             lost
```

For a **read** this is harmless: retry. For a **write** like "create short URL", case 3 means the row exists but the client thinks it failed. If it retries blindly, you get a duplicate (2.6).

> 💡 **Interview tip:** When the interviewer asks "what happens if X fails?", name the fallacy and the defense: "The network isn't reliable, so this call can time out after the write succeeded. That's why the create endpoint takes an idempotency key."

> 🔗 **Connections:** #1 drives replication (Part 5), consensus (Part 6) and delivery semantics (10.3). #2 drives caching (Part 8) and multi-region design (5.8, 14.8). #5 drives membership and failure detection (6.9).

## 2.2 Failure Models and Partial Failure

### Partial failure

On a single machine, things usually fail **completely**: the process crashes, and you know it. In a distributed system, **some parts fail while others keep running**, and the parts that are up often **can't tell** what happened to the parts that are down. This is **partial failure**, and it's the defining difficulty of distributed systems.

The root problem: **a timeout tells you almost nothing.** When a request times out, one of three things happened:

```
timeout fired. which one?

 (a) never got it        req  ----X                        nothing happened
 (b) processed, reply    req  ---------> [done] ---X       side effect DID
     lost                                                  happen
 (c) just slow           req  ---------> [ still working... ]  ---> reply
                                                           arrives later
                         |<---- timeout ---->|
```

You can't distinguish (a), (b) and (c) from the caller's side. Everything in Parts 5 and 6 (failover, leases, fencing, consensus) is about acting correctly despite that ambiguity. For example, a follower that stops hearing from the leader can't know whether the leader is dead (a) or merely slow (c), which is how split brain happens (5.5).

### Failure models

A **failure model** is the set of things you assume a faulty node can do. Algorithms are proven correct under a specific model; choose the weakest model that's realistic for your environment.

| Model | What a faulty node does | Realistic? | Who designs for it |
| :---- | :---- | :---- | :---- |
| **Crash-stop** | Halts and never comes back | Simplified; useful for proofs | Textbook algorithms |
| **Crash-recovery** | Halts, may restart later with what it wrote to **durable** storage; loses memory | **Yes, the normal datacenter model** | Raft, Paxos, databases (WAL: 9.4) |
| **Omission** | Stays up but drops some messages (sending or receiving) | Yes: overloaded NICs, full queues, firewalls | Retries, acks, timeouts |
| **Timing / performance** | Responds, but too slowly (GC pause, swap, noisy neighbor) | Yes, common | Timeouts, hedging (2.3), leases (6.6) |
| **Byzantine** | Arbitrary behavior: lies, corrupts data, sends conflicting messages | Rare inside one company; real in blockchains, untrusted parties | BFT protocols (need 3f+1 nodes) |

Most datacenter systems assume **crash-recovery with omission and timing failures**, and handle a narrow slice of Byzantine behavior (corrupted bits) with checksums rather than full BFT.

There's also **gray failure**: a node that passes health checks but fails real requests (e.g. disk is slow, one dependency is broken). Health checks that only test "is the process up" miss it (12.6).

> 💡 **Interview tip:** If you say "if a node fails, we fail over", the follow-up will be "how do you know it failed?" The honest answer: "We don't; we suspect it after a timeout. So failover must be safe even if the old leader is actually alive: that's what fencing tokens are for (6.6)."

> 🔗 **Connections:** The "is it dead or slow?" problem drives failure detectors and heartbeats (6.9), leases and fencing (6.6), and split brain (5.5). FLP (6.1) is the formal version: with even one possible crash and no timing bounds, deterministic consensus can't be guaranteed. 2f+1 vs 3f+1 sizing is in 6.2.

## 2.3 Latency, Throughput and Tail Latency

### Latency vs throughput

- **Latency:** how long one request takes, from the caller's point of view (includes queueing).
- **Throughput:** how many requests complete per second.

They're related but different. A highway can have high throughput (many cars per hour) and high latency (each car takes an hour). Increasing throughput by batching often **raises** latency. Little's Law (2.4) connects them.

### Percentiles, not averages

Latency distributions have long tails. The average hides the slow requests that users notice.

- **p50 (median):** half of requests are faster.
- **p99:** 99% are faster; 1 in 100 is slower.
- **p99.9:** 1 in 1,000 is slower.

```
latency distribution of a typical service

count
  |  ####
  |  ######
  |  ########
  |  ##########
  |  ############ ###
  |  ##################  ##    #        #              <- the tail
  +--+-------+----------+-----------+---------------> latency
     p50     avg        p99         p99.9
     5 ms    9 ms       80 ms       400 ms
```

Causes of tail latency: GC pauses, a cache miss going to disk, a TCP retransmit (~200 ms minimum RTO on Linux), lock contention, a noisy neighbor, a background compaction (9.3), a request queued behind a slow one.

1% sounds small, but the **heaviest users** hit the tail most: they make the most requests, so they almost always see it.

### Fan-out amplification

When one user request fans out to many backends in parallel and must wait for **all** of them, the user's latency is the **slowest** of them. The tail of the parts becomes the median of the whole:

```
P(at least one of n calls is slower than its p99) = 1 - 0.99^n

n = 1     ->  1%
n = 10    ->  1 - 0.99^10  = ~10%
n = 100   ->  1 - 0.99^100 = ~63%
```

With 100-way fan-out, **63% of user requests** see at least one p99-slow backend. Search engines, feeds and scatter-gather queries across shards (7.9) live with this.

### Hedged requests

From Dean and Barroso's *The Tail at Scale*: if a request hasn't returned after roughly the **p95** latency, send a duplicate to another replica and use whichever answers first.

```
t=0     send to replica A
t=p95   no answer yet -> send same request to replica B
        first reply wins; cancel the other

extra load: only ~5% of requests get a second copy  -> ~5% more load
benefit:    the long tail is cut sharply
```

Requirements: the request must be **idempotent** (2.6), and replicas must be independent (not the same overloaded box). A variant, **tied requests**, sends to both immediately and lets the servers cancel each other when one starts.

### The URL shortener's latency budget

Suppose the requirement is **p99 redirect < 100 ms** for users worldwide.

```
budget for an EU user:
  internet RTT to nearest edge/region: ~10-40 ms
  LB + app + cache:                    ~2-5 ms
  DB on cache miss (same region):      ~1-5 ms
                                       --------
                                       well under 100 ms   OK

but if the data only lives in us-east:
  EU app -> US DB round trip:          ~80-100 ms
  + the rest                           -> p99 > 100 ms     FAIL
```

So the requirement forces a copy of the data **in each region** (5.8). That in turn creates the "created in US, clicked in EU a second later" problem: the EU copy may not have the new row yet (5.4, 14.8).

> 💡 **Interview tip:** Always state latency targets as percentiles ("p99 < 100 ms"), and when you add fan-out, mention the 1 − 0.99^n effect and a mitigation (hedging, fewer shards per query, or tolerating partial results).

> 🔗 **Connections:** Tail latency sets timeouts (2.7) and SLOs (12.1). Hedging needs idempotency (2.6). Fan-out across shards is in 7.9. Monitoring percentiles (not averages) is in 12.9 and 12.10.

## 2.4 Little's Law

This is the tool for sizing servers, thread pools and connection pools when nobody gives you a "QPS per server" number.

### The law

```
L = λ x W

L = average number of requests IN the system (in flight, concurrency)
λ = arrival rate (throughput, requests/second)
W = average time each request spends in the system (latency, seconds)
```

It holds for any stable system (arrivals ≈ departures over time), regardless of the distribution of arrivals or service times. It's just counting: if 100 people per minute enter a shop and each stays 5 minutes, there are on average 500 people inside.

```
       arrivals                                  departures
  λ = 12,000/s  --->  [  L = 120 requests  ]  --->  12,000/s
                      each spends W = 10 ms
```

### Use 1: how many requests are in flight

```
12,000 QPS x 10 ms = 12,000 x 0.010 = 120 requests in flight
```

So across the whole fleet, about 120 requests are being processed at any moment. That's surprisingly few, and it tells you something about concurrency limits.

### Use 2: server capacity from CPU time (the interview move)

Apply the law to the **CPU cores** of one server. A core is a "slot" that holds one request while it uses CPU:

```
given:  8 cores per server
        2 ms of CPU time per request (measure it, or assume it)

max throughput per server = cores / CPU time per request
                          = 8 / 0.002 s
                          = 4,000 QPS   (at 100% CPU)
```

Never plan to run at 100%. Queueing delay explodes as utilization approaches 1. For a simple queue (M/M/1), average response time is `service time / (1 − utilization)`:

```
utilization   response time vs. service time
   50%           2x
   70%          ~3.3x
   90%          10x
   95%          20x
```

So plan at **~50–70% utilization**. Using 50%:

```
sustainable per server = 4,000 x 0.5 = 2,000 QPS
servers for peak       = 12,000 / 2,000 = 6
then failure headroom (1.7): 6 / (2/3) = 9, +1 per AZ = 12
```

Note the I/O time (waiting for Redis or the DB) doesn't consume cores if the server uses async I/O or enough threads. That's why the CPU time per request (2 ms), not the total latency (10 ms), sets core capacity.

### Use 3: sizing thread and connection pools

Little's Law also gives the **concurrency** a server needs:

```
per server: 2,000 QPS x 10 ms total latency = 20 requests in flight
-> a thread-per-request server needs at least ~20 threads (plus headroom
   for slow tails: plan ~50-100)

DB connection pool:
  cache miss rate 5% -> 2,000 x 0.05 = 100 DB queries/s per server
  each query holds a connection ~5 ms
  100 x 0.005 = 0.5 connections busy on average -> pool of ~5-10 is plenty

across 12 servers x 10 connections = 120 connections at the DB: fine
(Postgres gets unhappy at thousands; use a pooler like PgBouncer then)
```

If a pool is too small, requests queue for a connection and W grows. If it's too large, you overload the downstream. Little's Law tells you the right order of magnitude.

### Use 4: diagnosing overload

If W rises (the DB slows from 5 ms to 500 ms) while λ stays the same, L rises 100×. Threads and connections fill up, and the server stops accepting work. That's how one slow dependency takes down its callers (12.3), and it's why timeouts (2.7) and bulkheads (12.4) cap W and L.

> 💡 **Interview tip:** When asked "how many servers?" without a per-server number, say: "Let me assume ~2 ms of CPU per redirect and 8 cores. That's 4,000 QPS at full CPU; I'll run at 50%, so 2,000 QPS per server. 12,000 peak ÷ 2,000 = 6, then 9 to survive an AZ loss." Then offer: "In reality I'd load-test one server to get this number."

> 🔗 **Connections:** Feeds server count (1.7, 1.8, 14.4). Explains cascading failures (12.3: W grows, L explodes), why bulkheads cap concurrency (12.4), consumer lag in queues (10.5: lag = arrival rate × wait), and connection pooling in front of databases (14.5).

## 2.5 Amdahl's Law and Serial Bottlenecks

If a fraction **s** of the work is serial (can't be parallelized), then with **N** workers the speedup is:

```
speedup(N) = 1 / (s + (1 - s) / N)

as N -> infinity:  speedup -> 1 / s

s = 5%:   N = 10   -> 1 / (0.05 + 0.095)  = ~6.9x
          N = 100  -> 1 / (0.05 + 0.0095) = ~16.8x
          N = inf  -> 1 / 0.05            = 20x   (hard ceiling)
```

**5% serial work caps you at 20×, no matter how many machines you buy.** Adding servers past a point just adds cost.

### What the "serial part" looks like in real systems

| Serial part | Why it serializes | Fix |
| :---- | :---- | :---- |
| **Single leader database** for writes | All writes go through one node | Partition writes (Part 7), or accept it if write QPS is low |
| **Global lock** (one distributed lock for all requests) | Only one holder at a time | Finer-grained locks, per-key locks, avoid locks (6.7) |
| **One ID counter** (`SELECT nextval`, one Redis `INCR`) | Every create waits on one sequence | Pre-allocated ID ranges per server, Snowflake IDs (11.6) |
| **Single coordinator** (one scheduler, one master) | All decisions through one node | Shard the coordinator, or keep it off the data path (GFS master: 14.11) |
| **One hot key / hot partition** | All traffic for that key hits one node | Key splitting, caching (7.5) |

For the URL shortener, a single counter for short codes is the classic trap: every create in every region would wait on one node, often cross-region. Giving each app server a block of 1,000 IDs at a time turns 1,000 round trips into one (11.6, 14.6).

There's an extension, the **Universal Scalability Law** (Gunther), which adds a **coherence** cost: workers must coordinate with each other (cache invalidation, consensus, lock handoff), which grows ~N². Past some point, adding nodes makes throughput go **down**. You see this when a cluster gets slower after scaling out because of cross-node chatter.

> 💡 **Interview tip:** When you scale a tier horizontally, look for the thing all requests still share and name it: "The app tier scales linearly, but every create still hits one ID sequence. I'll hand out ID ranges so that's not serial anymore."

> 🔗 **Connections:** Single-leader limits (5.2) and the reasons to partition (7.1). ID generation (11.6). Hot keys (7.5). Consensus is inherently a serial point per group, which is why systems run many Raft groups (6.4, 7.6).

## 2.6 Idempotency

An operation is **idempotent** if doing it twice has the same effect as doing it once. This matters because, after a timeout (2.2), the only safe thing to do is retry, and **retries are only safe for idempotent operations**.

| Operation | Idempotent? | Why |
| :---- | :---- | :---- |
| `SET x = 5` | ✅ Yes | Second run leaves x = 5 |
| `x = x + 1` (increment) | ❌ No | Second run makes it +2 |
| `DELETE /urls/abc123` | ✅ Yes (effect) | Second call finds nothing to delete (may return 404; the state is the same) |
| `PUT /urls/abc123 {target}` | ✅ Yes | Full replacement with the same value |
| `POST /shorten {long_url}` | ❌ No (by default) | Each call creates a new short code |
| `GET /abc123` (redirect) | ✅ Yes | Read-only (but see click counting below) |
| "charge card $10" | ❌ No | Two charges |
| "append event to log" | ❌ No | Two copies of the event |

Note "same effect" is about **state**, not about the response. `DELETE` returning 200 then 404 is still idempotent.

### The double-click problem

A user clicks "Shorten" twice (or the browser retries after a timeout). With a non-idempotent `POST /shorten`, you get two different codes for the same long URL. Mostly harmless for a shortener, but the same bug in payments double-charges people.

### Idempotency keys

The standard fix (popularized by Stripe's API): the **client** generates a unique key per logical operation and sends it with every retry of that operation.

```
client                                   server
  | POST /shorten                          |
  | Idempotency-Key: 7f3c-...              |
  |--------------------------------------->| key unseen:
  |                                        |   create abc123
  |                                        |   store (7f3c -> abc123), TTL 24h
  |            X<--------------------------| reply lost
  | (timeout, retry with SAME key)         |
  |--------------------------------------->| key seen:
  |<---------------------------------------|   return stored abc123
  |  201 abc123                            |   (no second create)
```

Implementation details that matter:

- **Store the key atomically with the effect.** Insert the key and the new row in **one transaction**, with a unique constraint on the key. Otherwise a crash between "create row" and "store key" breaks it.
- **Handle concurrent duplicates.** Two retries can arrive at the same time. The unique constraint makes one insert win; the loser reads the winner's result (or gets "in progress, retry later").
- **Expire keys** after a window longer than any client's retry period (e.g. 24 hours).
- **Scope keys** per user/client so one client can't collide with another.

A domain-specific alternative for the shortener: deduplicate on **(user, long URL)** with a unique index, and return the existing code. That makes "shorten" naturally idempotent, though it changes product behavior (same long URL always gets the same code per user).

### Turning non-idempotent operations into idempotent ones

- **Increments → set of unique events.** Instead of `clicks = clicks + 1`, record click events with unique IDs and count distinct IDs; duplicates collapse. (Analytics pipelines do this with event IDs: 10.3.)
- **Conditional writes.** "Set target = B **if version = 7**" (compare-and-set). A retry after success fails the condition harmlessly.
- **Sequence numbers.** The receiver remembers the last sequence applied per sender and ignores older or equal ones. Kafka's idempotent producer works this way (10.3).

> 💡 **Interview tip:** Whenever you write a retry, say "this is safe because the operation is idempotent", or "this needs an idempotency key". Interviewers listen for this, especially on create and payment endpoints.

> 🔗 **Connections:** Idempotent consumers are how at-least-once delivery becomes effectively-once (10.3). The transactional outbox (10.6) relies on idempotent consumers. Sagas need idempotent steps and compensations (9.8). Hedged requests require idempotency (2.3).

## 2.7 Timeouts, Retries, Backoff, Jitter and Retry Budgets

### Timeouts

**Every network call needs a timeout.** Without one, a hung dependency holds a thread forever, and by Little's Law (2.4) your in-flight count grows until the server runs out of threads.

How to pick the value:

- Base it on the **downstream's measured latency**, typically a bit above its **p99 or p99.9**. If the DB's p99 is 20 ms, a 50–100 ms timeout is reasonable; 30 seconds is not.
- **Too short:** you time out healthy-but-slow requests, retry them, and add load exactly when the system is slow.
- **Too long:** stuck requests pile up and exhaust threads.
- **Use deadlines, not just per-hop timeouts.** The caller passes "this request must finish by T" down the chain (gRPC deadlines do this). A backend shouldn't keep working on a request whose user gave up 2 seconds ago.

```
deadline propagation (total budget 100 ms):

client --(100 ms)--> app --(remaining ~95 ms)--> cache
                      |
                      `--(remaining ~90 ms)--> DB
each hop subtracts what it has used; if remaining <= 0, fail fast
```

### What to retry

Retry only when **both** are true:

1. The operation is **idempotent** (2.6), or carries an idempotency key.
2. The error is **transient**.

| Retry | Don't retry |
| :---- | :---- |
| Timeout, connection reset/refused | 400 Bad Request, 404, validation errors |
| 503 Service Unavailable, 429 (after the `Retry-After`) | 401/403 auth errors |
| Leader changed / "not leader" redirects | Errors that will fail the same way again |

### Exponential backoff

Don't retry immediately. Wait, and double the wait each time, up to a cap:

```
sleep = min(cap, base x 2^attempt)

base = 100 ms, cap = 5 s:
attempt 0: 100 ms
attempt 1: 200 ms
attempt 2: 400 ms
attempt 3: 800 ms
...       capped at 5 s
```

This gives a struggling service room to recover instead of hammering it.

### Jitter

Backoff alone isn't enough. If 10,000 clients all failed at the same moment (a DB blip), they all retry at exactly +100 ms, +200 ms, +400 ms: synchronized waves. This is the **thundering herd**.

```
no jitter: synchronized spikes          full jitter: spread out

load                                    load
 |  #     #        #                     |
 |  #     #        #                     | . : . : : . : . . : . .
 |  #     #        #                     | : : : : : : : : : : : :
 +--+-----+--------+-----> time          +-----------------------> time
   100   200      400 ms
```

Add randomness. The common "full jitter" form:

```
sleep = random(0, min(cap, base x 2^attempt))
```

### Retry budgets

Even with backoff, retries **add load** during an outage, which is exactly when you can't afford it. A **retry budget** caps retries as a fraction of normal traffic, e.g. "retries may be at most **~10%** of requests". When the budget is exhausted, fail fast instead of retrying.

- Envoy supports retry budgets (a percentage of active requests).
- gRPC's retry throttling uses a token bucket per client: successes add tokens, failures remove them, and retries stop when tokens run low.

```
normal:   1,000 req/s, 1% fail -> 10 retries/s     (within 10% budget)
outage:   1,000 req/s, 80% fail -> budget allows only ~100 retries/s,
          not 800+; the dependency gets ~1,100 req/s instead of ~3,000
```

### Retry amplification across layers

Retries at several layers **multiply**:

```
client -> API gateway -> app -> DB
each layer makes up to 3 attempts

attempts reaching the DB for ONE user request:
3 (gateway) x 3 (app) x 3 (DB client) = 27x
```

A DB that's slow because it's overloaded now receives up to 27× the load. This is one of the most common causes of **cascading failures** (12.3): the system can't recover because the retries keep it overloaded (a "retry storm").

The fix: **retry at one layer only** (usually the one closest to the failing dependency, or the outermost client), and have other layers fail fast. Combine with retry budgets and circuit breakers (12.4).

### Putting it together

```
for attempt in 0 .. max_attempts-1:
    if deadline passed: fail
    result = call(timeout = min(per_try_timeout, time_left))
    if success: return result
    if not (idempotent and transient(result)): fail
    if not retry_budget.allow(): fail
    sleep(random(0, min(cap, base * 2^attempt)))
fail
```

> 💡 **Interview tip:** Say the four together: "timeouts based on downstream p99, retries only for idempotent calls on transient errors, exponential backoff with jitter, and a retry budget, with retries at only one layer to avoid 27× amplification." That's the full answer most candidates only half-give.

> 🔗 **Connections:** Retries without idempotency create duplicates (2.6). Retry storms cause cascading failures (12.3), countered by circuit breakers and load shedding (12.4, 12.5). Jitter fights the same herd effect as in cache stampedes (8.7). Leader-election timeouts in Raft are randomized for the same reason (6.3).

---

# 3. Time, Clocks and Ordering

Many distributed bugs are really time bugs: "which write came last?", "is this lease still valid?", "did the user create this URL before or after it was deleted?" This part explains why machines can't agree on the time, which clock to use for what, and how logical clocks (Lamport, vector, HLC) and bounded-uncertainty clocks (TrueTime) give you an ordering anyway.

## 3.1 Why There Is No Global Clock

Each machine has its own clock, driven by a **quartz crystal oscillator**. They don't agree, and they can't be made to agree exactly.

### Drift

Quartz crystals tick at slightly different rates depending on manufacturing and temperature. Typical drift is **tens of parts per million (ppm)**:

```
20 ppm drift = 20 x 10^-6 x 86,400 s/day = ~1.7 s per day

two servers drifting in opposite directions at 20 ppm:
  after 1 hour without sync: up to ~144 ms apart
```

So without correction, clocks wander apart by seconds per day.

### NTP

**NTP (Network Time Protocol)** periodically asks time servers for the time and adjusts the local clock. It estimates network delay from the round trip and assumes the delay is symmetric:

```
client                          server
 t1 |--------- request -------->| t2
    |                           |
 t4 |<-------- response --------| t3

round-trip delay = (t4 - t1) - (t3 - t2)
clock offset     = ((t2 - t1) + (t3 - t4)) / 2   <- assumes equal paths
```

If the paths are asymmetric or a packet is queued, the estimate is off. Typical accuracy: **~ms over the public internet, sub-ms within a datacenter**. Cloud providers run local time services (AWS Time Sync, Google's internal NTP) that do much better (~µs-level claims with special hardware). **PTP (Precision Time Protocol)** with hardware timestamping reaches sub-µs on a LAN.

Accuracy is never zero, and **the error bound is usually unknown to the application**. You get a number, not "a number ± how much".

### Clock jumps

NTP corrects a clock in one of two ways:

- **Slew:** speed up or slow down the clock slightly (bounded, e.g. 500 ppm max in Linux) until it converges. Time keeps moving forward, smoothly.
- **Step:** if the offset is large, set the clock to the new value **instantly**. Time can jump forward, or **backward**.

Other causes of time jumping from a process's view:

- **VM pauses and live migration:** the guest freezes for seconds; on resume, its clock jumps.
- **Long GC pauses / SIGSTOP:** the clock is fine, but the process missed seconds. A lease it thought it held may have expired (6.6).
- **Manual changes** and misconfigured NTP servers.

### Leap seconds

UTC occasionally inserts a **leap second** (23:59:60) to stay aligned with Earth's rotation. Software that assumes every minute has 60 seconds breaks:

- The 2012 leap second triggered a Linux kernel bug that caused livelocked CPU spinning in Java and other software at several large websites.
- On 1 January 2017, Cloudflare's DNS had an outage because a leap second made a computed time difference **negative**, and code that assumed time only moves forward failed.

Large operators now **smear** leap seconds: Google and AWS spread the extra second over ~24 hours by running clocks slightly slow, so no clock ever shows 23:59:60. (Mixing smeared and unsmeared time sources gives up to ~0.5–1 s disagreement during the smear.) The international time community has voted to stop adding leap seconds by 2035, but existing systems still need to handle them.

### The consequence

```
node A clock: 10:00:00.000   writes x = "old"
node B clock: 09:59:59.950   writes x = "new"   (50 ms later in real time)

last-write-wins by timestamp: "old" wins   <- the NEWER write is lost
```

**You can't order events on different machines by comparing their wall-clock timestamps** unless you know the clocks' error bound and it's smaller than the gap between the events. This is the motivation for everything else in this part.

> 💡 **Interview tip:** If you propose last-write-wins (LWW) with timestamps, say the cost: "LWW silently drops writes when clocks are skewed by more than the gap between them. It's fine for a URL's target being edited rarely by one owner, not for counters or carts."

> 🔗 **Connections:** Clock skew breaks LWW conflict resolution (5.6, 5.7), lease safety (6.6), Redlock's assumptions (6.7), Snowflake ID ordering when the clock goes backward (11.6), and TTL-based cache expiry across nodes (8.5). The fixes are logical clocks (3.4–3.5), HLC (3.6) and fencing tokens (6.6).

## 3.2 Wall Clock vs Monotonic Clock

Operating systems expose two different kinds of clock. Using the wrong one is a classic bug.

| | Wall clock (time-of-day) | Monotonic clock |
| :---- | :---- | :---- |
| **Linux** | `CLOCK_REALTIME` | `CLOCK_MONOTONIC` |
| **Java** | `System.currentTimeMillis()`, `Instant.now()` | `System.nanoTime()` |
| **Python** | `time.time()` | `time.monotonic()` |
| **Go** | `time.Now()` wall part | `time.Now()` monotonic part, used by `time.Since` (Go ≥ 1.9) |
| **Meaning** | "What time is it?" (seconds since 1970 UTC) | "How much time has passed?" since an arbitrary point (often boot) |
| **Can jump backward?** | **Yes** (NTP step, manual change, VM resume) | **No**, never goes backward |
| **Comparable across machines?** | Roughly (to within NTP error) | **No**, the zero point is per machine |
| **Use for** | Timestamps shown to humans, log lines, "created_at", expiry dates stored in data | Timeouts, durations, latency measurement, local lease/heartbeat timers, rate limiter windows |

### The bug

```python
start = time.time()          # wall clock
do_work()
elapsed = time.time() - start   # NTP stepped the clock back 2 s during work
# elapsed = -1.95 s  -> negative latency, broken timeouts, crashes
```

Use `time.monotonic()` for `elapsed`. (Go fixed this class of bug in 1.9 by embedding a monotonic reading in `time.Now()`, after incidents like the Cloudflare leap second outage in 3.1.)

### Two subtle points

- **Monotonic doesn't mean accurate.** NTP may still slew its rate slightly. It only guarantees no backward jumps. It's perfect for "has 10 seconds passed?" on one machine.
- **A monotonic clock doesn't help across machines.** If node A holds a lease "for 10 seconds" (measured on A's monotonic clock) and node B measures "10 seconds" on its own, the two can disagree if one process was paused. Lease designs therefore build in a safety margin and use fencing tokens (6.6).

> 🔗 **Connections:** Timeouts and backoff (2.7) must use the monotonic clock. Leases and failure detectors (6.6, 6.9) measure durations locally, so monotonic; but their safety across machines needs bounded drift or fencing. Stored timestamps (`created_at`, TTL expiry in 8.5) are wall-clock values.

## 3.3 Happened-Before and Causality

If physical time can't order events across machines, what can? Leslie Lamport's 1978 paper *Time, Clocks, and the Ordering of Events in a Distributed System* answered: **order events by what could have influenced what**.

### The happened-before relation (→)

Event **a → b** ("a happened before b") if any of these is true:

1. **Same process:** a and b happen in the same process, and a comes first.
2. **Message:** a is the **sending** of a message, and b is the **receipt** of that message.
3. **Transitivity:** a → c and c → b.

If neither a → b nor b → a, the events are **concurrent** (written a ‖ b). Concurrent doesn't mean "at the same instant". It means **neither could have known about the other**.

### A space-time diagram

Time runs left to right; each line is one process; arrows are messages:

```
P1  --a1--------a2----------------------a3------->
             \                          ^
              \ m1                      | m3
               v                        |
P2  ------------b1-------b2-------------b3-------->
                          \
                           \ m2
                            v
P3  ---c1--------------------c2------------------->

a1 -> a2 (same process)          a2 -> b1 (m1 send/receive)
b2 -> c2 (m2)                    a1 -> c2 (a1 -> a2 -> b1 -> b2 -> c2)
c1 || a1, c1 || b1, c1 || b2     (no chain of messages connects them)
b3 -> a3 (m3)
```

### Why "potential causality" is the right notion

If a → b, then a **could** have influenced b (information could have flowed from a to b). If a ‖ b, it's **impossible** for either to have influenced the other. That's what matters for correctness:

- A reply must not be visible before the question (4.3 causal consistency).
- A delete should beat the create it saw, but two concurrent edits are a **conflict** that needs resolution (5.6), not something to order arbitrarily without realizing it.

### URL shortener example

```
US region:  user creates abc123 -> long URL  (event a)
            user posts the link on social media (event b, a -> b)
EU region:  friend clicks abc123             (event c, b -> c, so a -> c)
```

The click **causally depends** on the create: the friend could only know the code because the create happened. If EU's replica hasn't received the create yet, it returns 404 for a URL that "exists". That's a **causality violation** in the user's eyes. Fixes (fall back to the origin region on a miss, or carry a version in the link) are covered in 5.4 and 14.8.

> 🔗 **Connections:** Happened-before is the formal basis of causal consistency (4.3) and session guarantees like read-your-writes and consistent prefix (4.4). Concurrent writes are what multi-leader and leaderless systems must detect (5.6, 5.7), which is what vector clocks do (3.5).

## 3.4 Lamport Clocks

A **Lamport clock** is a single integer counter per process that's consistent with happened-before.

### Rules

```
each process keeps a counter C, starting at 0

1. before each local event:           C = C + 1
2. when sending a message:            C = C + 1; attach C to the message
3. when receiving a message with T:   C = max(C, T) + 1
```

### Example

```
P1  [1]a1 -----[2]a2(send)---------------------------[6]a3(recv)-->
                   \                                   ^
                    \ T=2                              | T=5
                     v                                 |
P2  -------------[3]b1(recv)---[4]b2(send)----[5]b3(send)--------->
                                   \
                                    \ T=4
                                     v
P3  [1]c1 ---------------------[5]c2(recv)------------------------>

b1: max(0, 2) + 1 = 3      c2: max(1, 4) + 1 = 5      a3: max(2, 5) + 1 = 6
```

### What it guarantees, and what it doesn't

- **Guaranteed:** if a → b then **L(a) < L(b)**.
- **Not guaranteed:** L(a) < L(b) does **not** imply a → b. In the example, c1 has L=1 and b1 has L=3, but they're concurrent. A Lamport timestamp **can't detect concurrency**.

### Total order

Break ties with the process ID: order events by **(L, process_id)**. This gives a **total order** that every node computes identically and that respects causality. Lamport used this to build mutual exclusion: everyone agrees on the order of lock requests without a central server.

Total order is useful, but it's **arbitrary for concurrent events**: if two users concurrently claim the same custom alias, (L, node_id) picks a winner deterministically, but it's not "who clicked first in real time".

### Where you see the idea

- Raft **terms** and Paxos **ballot numbers** are Lamport-style counters: a node that sees a higher term adopts it (6.3, 6.5).
- **Fencing tokens** are monotonically increasing numbers that order lock holders (6.6).
- Kafka's partition **offsets** give a total order within one partition (10.4).

> 💡 **Interview tip:** The one-liner: "Lamport clocks give an order consistent with causality, but can't tell you whether two events were concurrent. If you need to detect conflicts, you need vector clocks."

> 🔗 **Connections:** Raft terms (6.3), Paxos ballots (6.5), fencing tokens (6.6), log offsets (10.4). Detecting concurrency needs vector clocks (3.5). HLC (3.6) is a Lamport clock that also tracks physical time.

## 3.5 Vector Clocks

A **vector clock** keeps **one counter per process**. It captures happened-before **exactly**: you can tell whether two events are ordered or concurrent.

### Rules

```
each process i keeps a vector V of N counters, all 0

1. local event at i:        V[i] = V[i] + 1
2. send at i:               V[i] = V[i] + 1; attach V
3. receive V_m at i:        V[j] = max(V[j], V_m[j]) for every j;
                            then V[i] = V[i] + 1
```

### Comparing two vectors

```
V(a) <= V(b)   if V(a)[k] <= V(b)[k] for every k
a -> b         if V(a) <= V(b) and V(a) != V(b)
a || b         if neither V(a) <= V(b) nor V(b) <= V(a)
```

### Example: concurrent edits in a multi-leader setup

Suppose the shortener lets owners edit a custom alias's destination, and both US and EU accept writes (multi-leader, 5.6). Vectors are [US, EU]:

```
              US                                  EU
start:   x = "a.com"  [0,0]                 x = "a.com"  [0,0]
                |                                  |
owner edits in US:                    teammate edits in EU:
         x = "b.com"  [1,0]                 x = "c.com"  [0,1]
                |                                  |
                 \--------- replicate ---------->  |
                  <-------- replicate ----------/  |

compare [1,0] vs [0,1]:
  US > EU in slot 0, EU > US in slot 1  -> CONCURRENT -> conflict
```

With last-write-wins on wall-clock timestamps, one edit would silently disappear. With vector clocks, the system **knows** it's a conflict and can keep both versions ("siblings") for the application or user to resolve, or apply a deterministic rule while logging the conflict.

Contrast with a causal sequence:

```
US writes x = "b.com" [1,0], replicates to EU
EU (having seen [1,0]) writes x = "c.com" -> [1,1]

compare [1,0] vs [1,1]: [1,0] <= [1,1]  -> EU's write is NEWER, no conflict
```

### Version vectors

In storage systems you usually track versions of a **data item**, not every event in every process. A **version vector** has one entry per **replica that wrote the item**. Amazon's Dynamo paper used them to detect conflicting versions of a shopping cart and return all siblings to the client to merge. Riak used them too (later with **dotted version vectors** to fix sibling explosion).

### Costs

- **Size grows with the number of writers.** With thousands of clients as writers, vectors get large. Systems keep one entry per **server replica** instead of per client, and **prune** old entries (which can cause false conflicts).
- **Conflicts still need a resolution policy.** Detecting concurrency is half the job; merging is the other half (app merge, or CRDTs that merge automatically: 13.6).
- Cassandra deliberately chose **per-cell LWW timestamps** instead of vector clocks: simpler and faster, at the cost of silently dropping concurrent writes.

> 💡 **Interview tip:** "Lamport: causality-consistent order, can't detect concurrency. Vector clock: detects concurrency exactly, costs O(N) space. LWW: cheapest, loses concurrent writes." That three-way comparison answers most clock questions.

> 🔗 **Connections:** Conflict detection in multi-leader (5.6) and leaderless replication (5.7). Siblings and merging lead to CRDTs (13.6). Causal consistency (4.3) can be implemented by tracking dependencies with vectors.

## 3.6 Hybrid Logical Clocks and TrueTime

Lamport and vector clocks capture causality but have **no relation to real time**. You can't ask "give me the database as of 10:00:00" with them. Physical clocks relate to real time but break causality under skew. Two practical answers combine both.

### Hybrid Logical Clocks (HLC)

An **HLC** timestamp is a pair **(l, c)**:

- **l**: the largest physical time this node has seen (from its own clock or from any message).
- **c**: a logical counter to order events that share the same l.

```
pt = local physical clock

local or send event:
  l_old = l
  l = max(l_old, pt)
  c = (l == l_old) ? c + 1 : 0
  attach (l, c) to outgoing messages

receive message with (l_m, c_m):
  l_old = l
  l = max(l_old, l_m, pt)
  if   l == l_old and l == l_m: c = max(c, c_m) + 1
  elif l == l_old:              c = c + 1
  elif l == l_m:                c = c_m + 1
  else:                         c = 0
```

Properties:

- **Respects causality** like a Lamport clock: a → b ⇒ HLC(a) < HLC(b), compared as (l, then c).
- **Stays close to physical time:** l never drifts further from real time than the clock skew between nodes.
- **Fits in 64 bits** (e.g. 48 bits of ms time + 16 bits counter), so it's a drop-in for a timestamp column.
- **Tolerates clocks going backward:** l never decreases, because it takes the max.

```
node A (clock fast):  pt=105 -> event (105, 0) --msg-->
node B (clock slow):                                pt=100, receives (105,0)
                                                    l = max(100, 105) = 105
                                                    c = 0 + 1 = 1 -> (105, 1)

B's event is ordered AFTER A's, even though B's own clock says 100.
```

HLC doesn't give you **real-time ordering** of unrelated events across nodes (if two events are concurrent and clocks are skewed, the order can still be "wrong" by up to the skew). Systems add an **uncertainty interval** for that. **CockroachDB** uses HLC with a configured **maximum clock offset** (500 ms by default): when a read sees a value with a timestamp inside its uncertainty window, it may restart at a higher timestamp, and a node whose clock drifts beyond the limit shuts itself down. **YugabyteDB** and **MongoDB** (cluster time) also use HLC-style timestamps.

### TrueTime (Google Spanner)

Google took the opposite approach: instead of hiding clock error, **measure it and expose it**. **TrueTime** puts GPS receivers and atomic clocks in every datacenter, and its API returns an interval:

```
TT.now()  ->  [earliest, latest]

guarantee: the true absolute time is somewhere inside the interval
uncertainty epsilon: typically ~1-7 ms (half-width), per the Spanner paper
```

### Commit wait

Spanner uses TrueTime to give **external consistency** (linearizability for transactions, 4.2): if T1 commits before T2 starts in real time, T1's timestamp is smaller than T2's, across the whole planet.

```
transaction T1 ready to commit:
  s = TT.now().latest                 choose commit timestamp
  wait until TT.now().earliest > s    "commit wait": ~2 x epsilon
  then release locks, reply "committed"

timeline (real time ->)
  |---- uncertainty ----|
  [earliest      latest=s]
                         |--- wait ---|
                                       ^ now even the earliest possible
                                         true time is past s; any later
                                         transaction anywhere will get a
                                         timestamp > s
```

Cost: every read-write commit waits **~2ε, a few ms**. That's small next to the cross-region consensus round trip Spanner already pays (6.4), which is why it's acceptable. **The tighter the clock bound, the shorter the wait**, which is why Google invested in the hardware.

Benefits: consistent snapshot reads at any timestamp across all shards **without locking**, and globally ordered transactions without a central timestamp server.

Outside Google: AWS offers **ClockBound** with its Time Sync Service, exposing an error-bounded time similar in spirit. Without bounded clocks, alternatives are a **central timestamp oracle** (TiDB's PD hands out timestamps: a serial point, 2.5) or HLC with uncertainty intervals (CockroachDB).

### Summary

| Clock | Captures causality | Detects concurrency | Relates to real time | Size | Used by |
| :---- | :---- | :---- | :---- | :---- | :---- |
| Wall clock | ❌ (under skew) | ❌ | ✅ (± unknown error) | 64 bits | LWW in Cassandra, logs |
| Lamport | ✅ | ❌ | ❌ | 1 integer | Raft terms, fencing tokens |
| Vector / version vector | ✅ | ✅ | ❌ | N integers | Dynamo, Riak |
| HLC | ✅ | ❌ | ✅ (within skew) | 64 bits | CockroachDB, YugabyteDB, MongoDB |
| TrueTime | ✅ (via commit wait) | ❌ | ✅ (bounded ε) | interval | Spanner |

> 💡 **Interview tip:** If asked "how does Spanner get strong consistency globally?", answer: "TrueTime bounds clock uncertainty to a few ms using GPS and atomic clocks, and every commit waits out that uncertainty before acknowledging, so timestamp order matches real-time order." Then add: "Without that hardware, CockroachDB uses hybrid logical clocks plus an uncertainty window and occasional read restarts."

> 🔗 **Connections:** External consistency is linearizability (4.2) applied to transactions. Commit wait and Paxos groups combine in Spanner (6.5, 14.11). Snapshot reads at a timestamp are MVCC snapshot isolation (9.6). Timestamp-based ID schemes like Snowflake inherit clock problems (11.6). Bounded clocks make leases safer (6.6).

---

# 4. Consistency Models

Once data lives on more than one machine, "what does a read return?" stops having an obvious answer. A consistency model is the contract between a data store and its clients about which values a read is allowed to see. This part names the contracts, from strongest to weakest, so that in an interview you can say exactly which one your design gives and what it costs.

```
  STRONGER (more coordination, more latency, less available under partition)
     ^
     |   Linearizable            "one copy, real-time order"           4.2
     |        |
     |   Sequential              "one copy, some order, not real time"  4.3
     |        |
     |   Causal                  "cause before effect, everywhere"      4.3
     |        |
     |   Session guarantees      "you see your own writes, in order"    4.4
     |   (RYW, monotonic reads, consistent prefix)
     |        |
     |   Eventual                "if writes stop, replicas converge"    4.5
     v
  WEAKER (local reads, low latency, available under partition)
```

## 4.1 What "Consistency" Means (vs ACID's C)

The word "consistency" is overloaded. In an interview, say which one you mean.

| Term | Meaning | Where |
| :---- | :---- | :---- |
| **ACID C** | The database moves from one valid state to another: constraints, foreign keys, invariants ("balance ≥ 0") hold after each transaction. Mostly the *application's* job. | 9.5 |
| **Consistency model** (this part) | What a read may return when there are several copies of the data. | 4.2–4.5 |
| **CAP C** | Specifically **linearizability**. Nothing weaker. | 4.6 |
| **Cache consistency** | Whether a cache can serve a value the source of truth has already changed. | 8.5 |
| **Isolation** (ACID I) | What concurrent *transactions* can see of each other on one logical database. Sometimes confused with consistency. | 9.6 |

A useful way to keep them apart: **isolation** is about concurrent transactions touching multiple objects; **consistency models** are about the replicas of an object and the order in which operations appear to happen. Linearizability is a single-object, real-time guarantee. Serializability is a multi-object, transaction guarantee with no real-time promise. **Strict serializability** (Spanner) is both.

> 💡 **Interview tip:** When someone asks "is it consistent?", answer with a model, not yes/no: "Redirect reads are eventually consistent across regions, with read-your-writes for the creator. Short-code uniqueness is linearizable because it goes through a single conditional write."

> 🔗 **Connections:** ACID and isolation levels are in Part 9 (9.5, 9.6). The CAP "C" is 4.6. Cache staleness is its own problem in 8.5.

## 4.2 Linearizability

**Linearizability** (a.k.a. atomic consistency, strong consistency) means the system behaves as if there were **one copy** of the data, and every operation takes effect **atomically at some instant between its start and its end**. Once any read has returned a new value, every later read (in real, wall-clock time) must also return it or something newer.

```
time ---------------------------------------------------------------->

Client A:  [ write x = 1 ...................... ok ]
Client B:         [ read x -> 0 ]                       allowed: overlaps
Client C:                     [ read x -> 1 ]           the write, so 0 or 1
Client D:                                  [ read x -> 0 ]   VIOLATION:
                                                         C already saw 1 and
                                                         D started after C ended
```

The rule that catches people: an operation that **overlaps** a write may see the old or the new value. But once *any* client has seen the new value, the "switch" has happened, and no later read may go back.

### What breaks linearizability

- Reading from an **asynchronous follower** (5.3): the follower may be behind the leader.
- A **stale leader** that doesn't know it was replaced and serves reads from its local state (6.4, 6.6).
- **Caches** in front of the database (8.5).
- **Leaderless quorums** with sloppy quorums or concurrent writes (5.7).

### How you get it

- A single leader that serves all reads and writes, **and** knows it is still the leader (lease or quorum check, 6.4).
- A consensus protocol (Raft, Paxos: 6.3–6.5) where reads go through the log or a ReadIndex check.
- Synchronized clocks with bounded error plus commit-wait (Spanner TrueTime, 3.6).

### Why it costs

Every linearizable operation needs to talk to a **majority** (or to a leader that recently talked to a majority). If the replicas are in different regions, that is a cross-region round trip on the critical path:

```
linearizable write, replicas in us-east, eu-west, ap-south:
  leader in us-east needs 1 more ack -> nearest other region (eu-west)
  ~80 ms RTT  -> every write >= ~80 ms

eventually consistent local read in eu-west: ~1 ms
```

### When you actually need it

| Need | Why linearizability | Example |
| :---- | :---- | :---- |
| Uniqueness constraints | Two clients must not both "win" the same name | Custom short alias `/my-link`; usernames |
| Leader election, locks | Exactly one holder | etcd/ZooKeeper (6.8) |
| Cross-channel timing | A message tells someone to go read the data | Upload image, then enqueue "resize image X": the worker must see it |
| Money, inventory | No double spending, no overselling | Last seat on a flight |

Everything else (redirect lookups, view counts, timelines) is usually fine with something weaker.

> 💡 **Interview tip:** "Linearizable" is a per-object, real-time guarantee. "Serializable" is a per-transaction, any-order guarantee. If you mix them up, a good interviewer will notice. Spanner gives both ("strict serializability" / "external consistency").

> 🔗 **Connections:** Linearizable stores are built on consensus (6.1–6.5). Locks need it plus fencing (6.6, 6.7). CAP's C is linearizability (4.6). Spanner gets it with TrueTime (3.6).

## 4.3 Sequential and Causal Consistency

### Sequential consistency

**Sequential consistency** (Lamport, 1979): all clients see all operations in **one total order**, and that order respects **each client's own program order**. Unlike linearizability, the order does **not** have to match real time. A read is allowed to return a value that is "old" in wall-clock terms, as long as everyone agrees on the same story.

```
real time ---------------------------------------------->
A:  write x=1                  write x=2
B:                read x -> 1             read x -> 1  (still 1!)

Linearizable?  No: B's 2nd read started after write x=2 finished.
Sequential?    Yes: the order  w(1), r(1), r(1), w(2)  is a valid single
               order that keeps A's order and B's order.
```

It's the model CPU memory models are compared against (the Linux notes' MESI section is the hardware that makes caches behave this way). In distributed stores it's rarely offered by name: ZooKeeper's guarantee is close to it (writes linearizable, reads sequential per client, 6.8).

### Causal consistency

**Causal consistency**: if operation A **happened before** B (3.3), every node sees A before B. Operations that are **concurrent** (neither caused the other) may be seen in different orders on different nodes.

```
Alice (US):  post P: "I lost my job"
Alice (US):  post Q: "Just kidding, got promoted!"   (Q depends on P)
Bob   (EU):  reply R to Q: "Congrats!"               (R depends on Q)

Causal store: anyone who sees R also sees Q and P before it.
Non-causal:   Carol in APAC may see R ("Congrats!") next to P only.
```

Implementation: each write carries its **dependencies** (a vector clock 3.5, or "the version I last read"), and a replica delays making a write visible until its dependencies are visible.

Why it matters: causal consistency is the **strongest model that stays available during a network partition** (in CAP's sense, 4.6). That's why it's attractive for geo-replicated systems: each region can keep accepting reads and writes locally and still never show an effect before its cause. MongoDB offers causally consistent sessions; COPS and Eiger are the classic research systems.

> 🔗 **Connections:** Causality is defined by happened-before (3.3) and tracked with Lamport or vector clocks (3.4, 3.5). Multi-leader conflict detection uses the same vectors (5.6).

## 4.4 Session Guarantees: Read-Your-Writes, Monotonic Reads, Consistent Prefix

Session guarantees are small, cheap promises scoped to **one client session**. They fix the most visible replication-lag bugs (5.4) without paying for linearizability.

### Read-your-writes (read-after-write)

After you write something, **your** later reads see it. Other users may still see the old value.

```
           write                      read
User U  ---> leader (US) ok           ---> follower F (lagging 2 s)
                 \                              |
                  \---- replication ---> F      v
                         (arrives at t=2s)    "404 Not Found"  <- violation
t:       0                           0.3 s
```

URL shortener: user creates `sho.rt/aZ3k9`, gets the link, and pastes it into a new tab 300 ms later. If the redirect hits a lagging replica, the user's own brand-new link is "broken".

### Monotonic reads

A client never sees time go **backwards**: once it has seen version v, later reads return v or newer. Violated when consecutive reads go to different replicas with different lag.

```
User U: read #1 -> replica F1 (lag 0.1 s) -> sees comment C
User U: read #2 -> replica F2 (lag 5 s)   -> comment C is gone
```

Fix: pin a user's reads to one replica (e.g. `hash(user_id) % replicas`), or carry the last-seen version and refuse to read from a replica that's behind it.

### Consistent prefix

If writes happen in order W1, W2, a reader sees them in that order: never W2 without W1. This breaks in **partitioned** systems where W1 and W2 went to different shards with different lag.

```
Shard 1 (lag 5 s): W1 = question "What's 2+2?"
Shard 2 (lag 0 s): W2 = answer   "4"
Reader sees: "4" ... then 5 s later "What's 2+2?"   <- answer before question
```

Fix: put causally related writes on the same shard, or track causal dependencies (4.3).

### The other two

- **Monotonic writes:** a client's writes are applied in the order it issued them.
- **Writes-follow-reads:** if you read X and then write Y, Y is ordered after X everywhere (a reply comes after the post it replies to).

| Guarantee | Protects against | Cheap fix |
| :---- | :---- | :---- |
| Read-your-writes | "My edit disappeared" | Read from leader for N s after a write; or version token |
| Monotonic reads | "The comment flickered away" | Sticky replica per user |
| Consistent prefix | "Answer before question" | Same partition for related writes |
| Monotonic writes | "My second edit applied first" | Single leader per key; sequence numbers |
| Writes-follow-reads | "Reply visible before post" | Causal metadata (4.3) |

> 💡 **Interview tip:** Session guarantees are about *one* user. The "created in US, clicked in EU" problem is usually about a *different* user (the person who receives the link). Read-your-writes alone does not fix it. See 5.4 for the fixes that do.

> 🔗 **Connections:** The concrete fixes (leader reads, LSN tokens, sticky routing) are in 5.4. Sticky routing is a load balancer feature (11.2).

## 4.5 Eventual Consistency

**Eventual consistency**: if no new writes arrive, all replicas **eventually converge** to the same value. That's all it promises. It says nothing about *when*, and nothing about what you might read in the meantime (old values, values going backwards, writes out of order).

In practice "eventually" is usually **milliseconds to seconds**, but there is **no bound**: a replica that was partitioned for an hour is an hour behind.

```
t=0      write x=5 at replica A
         A: 5     B: 3     C: 3
t=50ms   A -> B replicated
         A: 5     B: 5     C: 3     <- reads at C still return 3
t=900ms  A -> C replicated (C was slow)
         A: 5     B: 5     C: 5     <- converged
```

### Two questions eventual consistency leaves open

1. **How do replicas converge if they received concurrent writes?** You need a deterministic conflict rule: last-writer-wins, CRDT merge, or app-level merge (5.6). **Strong eventual consistency** means any two replicas that have received the same set of updates are in the same state, with no rollbacks; CRDTs give this (13.6).
2. **How do missed updates get delivered?** Read repair, hinted handoff and anti-entropy with Merkle trees (5.7).

### Examples

- **DNS**: changes spread as TTLs expire (11.3).
- **Cassandra / DynamoDB** default reads (5.7). DynamoDB's eventually consistent read is half the cost of a strongly consistent one.
- **CDN and caches** (8.5, 11.4).
- **Amazon S3** was eventually consistent for overwrites until Dec 2020; it now gives strong read-after-write consistency. Good reminder that these properties are design choices, not laws.

### Measure it

Eventual consistency is fine **if you know the lag**. Track replica lag as an SLI (12.1): Postgres `pg_stat_replication.replay_lag`, MySQL `Seconds_Behind_Source`, Kafka consumer lag. Alert when p99 lag exceeds what your product can tolerate.

> 💡 **Interview tip:** Don't say "it's eventually consistent, so it's fine." Say what the window is (~100 ms – 1 s cross-region in normal operation), what the user sees during it, and how you handle the cases where that window hurts (5.4).

> 🔗 **Connections:** The repair mechanisms are in 5.7 and 13.4. Convergence without coordination is the point of CRDTs (13.6). Cache TTLs are a form of bounded staleness (8.5).

## 4.6 CAP Theorem (and what it does NOT say)

**CAP** (Brewer's conjecture 2000, proved by Gilbert and Lynch 2002): when a **network partition** separates replicas, a system must choose between:

- **C**onsistency: every read sees the latest write (strictly: **linearizability**), or
- **A**vailability: every request to a non-failed node gets a non-error response.

```
          normal operation                 partition
   +--------+        +--------+     +--------+   X X X   +--------+
   |  US    |<------>|  EU    |     |  US    |  link cut |  EU    |
   | x = 1  |        | x = 1  |     | x = 2  |   X X X   | x = 1  |
   +--------+        +--------+     +--------+           +--------+
                                       ^                     ^
                                  write x=2            read x?  EU must:

     CP choice: refuse (error / timeout)  -> consistent, not available
     AP choice: answer "x = 1" (stale)    -> available, not consistent
```

That's the whole theorem. You can't keep both properties **for requests that reach the minority side of a partition**.

### What CAP does NOT say

- **Not "pick 2 of 3".** Partitions are not optional (fallacy #1, 2.1). The real choice is only C vs A, and only **while partitioned**. When there's no partition you can have both.
- **Not about latency.** CAP is silent on the normal-case trade-off, which is what you actually feel every day. That's PACELC (4.7).
- **Narrow definitions.** "C" is only linearizability; "A" means *every* non-failed node answers. A system with Raft (CP) is still highly available in the everyday sense: the majority side keeps working.
- **Not a per-system label.** One system can make different choices per operation: Cassandra at `CL=ONE` is AP; at `CL=QUORUM` on both reads and writes it behaves much more like CP (5.7). DynamoDB offers both read modes.

### Typical labels (behavior during a partition)

| System | Choice | What the minority side does |
| :---- | :---- | :---- |
| etcd, ZooKeeper, Consul | CP | Minority rejects writes (and linearizable reads) |
| Spanner, CockroachDB | CP | Minority ranges unavailable |
| Cassandra, DynamoDB (default), Riak | AP | Both sides accept; reconcile later (5.6, 5.7) |
| Postgres primary + async replicas | Neither cleanly | Replicas serve stale reads; writes only where the primary is |

> 💡 **Interview tip:** For each operation, state the choice and its user-visible effect. "During a US-EU partition, EU keeps serving redirects from its replica (AP: a few new links may 404 there). Creating a new link with a custom alias fails in EU, because uniqueness needs the leader (CP)."

> 🔗 **Connections:** Partitions are failure model #1 (2.1, 2.2). The majority side is the side that has a quorum (6.2). The minority side of a Raft cluster can't elect a leader (6.3).

## 4.7 PACELC

**PACELC** (Abadi, 2012) extends CAP to the normal case:

```
if Partition:  choose  Availability  or  Consistency
Else:          choose  Latency       or  Consistency
```

The "Else" half matters more day to day. Even with a perfectly healthy network, a strongly consistent read or write must wait for coordination (a quorum, a leader in another region, a clock uncertainty window). A weaker read can answer from the nearest copy.

```
EL (latency):     read from local replica              ~1 ms
EC (consistency): read via leader/quorum in other region   ~80-150 ms
```

| System | PACELC | Notes |
| :---- | :---- | :---- |
| Cassandra, Riak, DynamoDB (default) | PA/EL | Tunable per request (5.7) |
| Spanner, CockroachDB | PC/EC | Pay cross-region quorum on writes |
| etcd, ZooKeeper | PC/EC | Small metadata, consistency first |
| MongoDB (default config) | PA/EC | Abadi's classification; depends heavily on read/write concern settings |
| Postgres + async read replicas | PC/EL-ish | Writes go to one primary; replica reads trade consistency for latency |

These labels are approximations: most modern systems are tunable per query, so describe the knob, not the label.

> 💡 **Interview tip:** PACELC is the honest framing for multi-region. "A partition is rare. Every request pays the Else choice. For redirects I choose L: serve from the local region. For short-code creation I choose C: route to the leader region and pay ~80 ms, since creates are ~1% of traffic."

> 🔗 **Connections:** The numbers behind "L" are in 1.1. Multi-region read-local / write-global designs are 5.8. Spanner's EC choice is made cheaper by TrueTime (3.6).

---

# 5. Replication

Replication means keeping a copy of the same data on several machines. It is how you survive losing a disk, a server, a zone or a region, and how you serve reads close to users. All the difficulty comes from one thing: **the data changes**, and the copies can't all change at the same instant.

## 5.1 Why Replicate

| Goal | How replication helps | URL shortener |
| :---- | :---- | :---- |
| **Durability** | Data survives a lost disk or machine | A created link is never lost |
| **Availability** | Another copy can serve when one is down | Redirects keep working when a DB server dies |
| **Read throughput** | Spread reads over many copies | 100:1 read/write ratio: add read replicas |
| **Latency** | Put a copy near the user | EU users read from an EU replica: ~1 ms instead of ~80 ms |

Replication is different from **partitioning** (Part 7): partitioning splits data so each node holds *part* of it; replication copies the *same* data. Real systems do both: each partition is replicated 3×.

```
          partition 1         partition 2         partition 3
         (codes a..h)        (codes i..q)        (codes r..z)
  AZ-a:   [P1 leader]         [P2 follower]       [P3 follower]
  AZ-b:   [P1 follower]       [P2 leader]         [P3 follower]
  AZ-c:   [P1 follower]       [P2 follower]       [P3 leader]
```

The three families, which the rest of this part covers:

| Family | Who accepts writes | Examples | Section |
| :---- | :---- | :---- | :---- |
| **Single-leader** | One leader per partition | Postgres, MySQL, MongoDB, Kafka, Raft groups | 5.2 |
| **Multi-leader** | Several leaders (often one per region) | MySQL/Postgres with BDR, CouchDB, Google Docs-style editing | 5.6 |
| **Leaderless** | Any replica | Dynamo, Cassandra, Riak, ScyllaDB | 5.7 |

> 🔗 **Connections:** Storage math multiplies by the replica count (1.5). Partitioning is Part 7. Consensus (Part 6) is replication with a correctness proof for choosing the leader.

## 5.2 Single-Leader Replication

One replica is the **leader** (primary, master). All writes go to it. It writes the change to its local log and sends the change stream to the **followers** (replicas, standbys, secondaries), which apply it in the same order. Reads can go to the leader (fresh) or to followers (possibly stale).

```
                   writes                     reads (fresh)
   clients ------------------+     +---------------------- clients
                             v     v
                       +-------------+
                       |   LEADER    |  WAL / binlog:  ... 1041 1042 1043
                       +------+------+
              replication     |     replication stream (ordered)
              +---------------+----------------+
              v                                v
       +-------------+                  +-------------+
       | FOLLOWER 1  | applied to 1043  | FOLLOWER 2  | applied to 1039
       +-------------+                  +-------------+
              ^                                ^
              +---------- reads (maybe stale) -+---------- clients
```

Because one node decides the order of all writes, there are **no write conflicts**. That's the big reason this is the default.

### What is in the replication stream?

| Method | What is shipped | Pros / cons | Examples |
| :---- | :---- | :---- | :---- |
| **Statement-based** | The SQL text (`INSERT ... NOW()`) | Small, but nondeterministic functions (`NOW()`, `RAND()`, auto-increment races) diverge | Old MySQL default |
| **Physical (WAL shipping)** | Byte-level page changes from the WAL (9.4) | Exact copy; tied to storage format, same major version on both sides | Postgres streaming replication |
| **Logical (row-based)** | "Row with key K now has these column values" | Version-independent, readable by other systems (CDC 10.6) | MySQL row-based binlog, Postgres logical replication |

### Adding a new follower

You can't just copy files while writes are happening. The standard procedure:

```
1. take a consistent snapshot of the leader, note its log position (LSN 5000)
2. copy the snapshot to the new follower                (minutes to hours)
3. follower requests every change since LSN 5000
4. follower applies the backlog, then streams live      ("caught up")
```

This is also how a follower recovers after a restart: it remembers its last applied position and asks for everything after it. The leader has to **retain** enough log for this (Postgres replication slots, `wal_keep_size`; Kafka retention).

### Where it hurts

- **Write throughput** is capped by one machine per partition (Amdahl's serial part, 2.5). The fix is partitioning (Part 7), not more followers.
- **Leader failure** needs failover (5.5).
- **Followers lag** (5.3, 5.4).

> 💡 **Interview tip:** For a read-heavy system like a URL shortener, "single leader per shard + read replicas + cache" is the default answer. Say so, then spend your time on lag (5.4) and failover (5.5), which is what the interviewer is probing.

> 🔗 **Connections:** The replication stream is the write-ahead log (9.4). Logical replication is the basis of change data capture (10.6). Raft is single-leader replication where leader choice is made safe by consensus (6.3, 6.4).

## 5.3 Synchronous vs Asynchronous Replication

The key question: **when does the leader tell the client "ok"?**

### Synchronous

The leader waits until the follower confirms it has the write (on disk) before acknowledging.

```
client          leader                    follower
  |  write x=5    |                           |
  |-------------->|  append to WAL, fsync      |
  |               |-------- x=5 -------------->|
  |               |                           | append, fsync
  |               |<------- ack --------------|
  |<----- ok -----|                           |
  |               |                           |
  latency = local write + 1 RTT to follower + follower fsync
```

- **Pro:** if the leader dies right after "ok", the follower has the write. **RPO = 0** (5.9).
- **Con:** every write pays the round trip. And if the follower is **down or slow, writes stall**. One slow replica makes the whole system slow (tail latency, 2.3).

### Asynchronous

The leader acknowledges as soon as the write is durable locally, and ships it in the background.

```
client          leader                    follower
  |  write x=5    |                           |
  |-------------->|  append to WAL, fsync      |
  |<----- ok -----|                           |
  |               |-------- x=5 -------------->|  (some ms .. s later)
  |               |                           | append
  |               |<------- ack --------------|
  latency = local write only
```

- **Pro:** fast; a slow or dead follower doesn't affect writes.
- **Con:** if the leader dies before shipping, **acknowledged writes are lost** on failover. And followers serve **stale** data (5.4).

### The window of loss, on a timeline

```
time ------------------------------------------------------------>
leader:    w1   w2   w3   w4   w5   X (leader crashes)
              \    \    \
follower:      w1   w2   w3                <- w4, w5 were acknowledged
                                              to clients but never arrived
           |<------- replication lag ------->|
           new leader = follower: w4 and w5 are GONE
```

At 1,000 writes/s and 200 ms of lag, a crash loses ~200 acknowledged writes.

### The middle ground: semi-synchronous and quorum commit

Waiting for *all* followers is fragile; waiting for *none* loses data. Real systems wait for **some**:

| Mode | Wait for | Examples |
| :---- | :---- | :---- |
| **Semi-synchronous** | At least 1 follower to *receive* (not apply) the write | MySQL semi-sync; Postgres `synchronous_standby_names = 'ANY 1 (s1, s2)'` |
| **Quorum commit** | A majority of replicas (counting the leader) | Raft/Paxos (6.4), Kafka `acks=all` with `min.insync.replicas=2` |
| **Fully synchronous** | Every follower | Rare; availability = product of all replicas (1.3) |

With 1 leader + 2 followers and "wait for ANY 1", the write latency is the **faster** of the two follower round trips, and one follower can die without stalling writes.

### Where you can afford synchronous

The cost is one RTT to the replica you wait for:

```
                         RTT (~)        sync write adds    at 1,000 writes/s
same AZ                  ~0.1-0.5 ms    negligible         fine
other AZ, same region    ~1-2 ms        ~1-2 ms            fine
other region (US-EU)     ~70-90 ms      ~80 ms             each client waits
other region (US-Asia)   ~150-200 ms    ~170 ms            ~170 ms per write
```

So the common production shape is:

```
   sync (or quorum) across AZs in the home region   -> RPO 0 for an AZ loss
   async across regions                             -> RPO = seconds for a
                                                       region loss, but fast
```

> 💡 **Interview tip:** Say "synchronous to a second AZ, asynchronous to other regions," and then name the consequence of the async part: on a whole-region failure you can lose the last ~seconds of writes (5.9), and remote readers can see stale data (5.4). Interviewers want the trade-off, not just the term.

> 🔗 **Connections:** Raft's majority commit is quorum replication (6.2, 6.4). Kafka's ISR is the same idea for logs (10.2). The loss window is the RPO (5.9). Serial availability math for "wait for all" is in 1.3.

## 5.4 Replication Lag and Its Symptoms

**Replication lag** is how far a follower is behind the leader, measured in time ("2.1 s behind") or log position ("40 MB of WAL behind"). Under normal load it's small:

```
same region, healthy:           ~1-10 ms
cross-region, healthy:          ~RTT/2 + apply time ~ 50-200 ms
under load / big transaction:   seconds
follower recovering / overload: minutes or hours  (no upper bound!)
```

Small most of the time is exactly why it bites: everything works in testing, and the bug only appears under load or during a failure.

### Symptoms (each one breaks a session guarantee from 4.4)

| Symptom | What the user sees | Broken guarantee |
| :---- | :---- | :---- |
| Read stale data right after writing | "I saved it and it's gone" | Read-your-writes |
| Data appears then disappears on refresh | Flickering comments | Monotonic reads |
| Effects before causes | Answer before question | Consistent prefix |
| Another user can't see new data yet | **New short link 404s in another region** | None of the session guarantees cover this: it's cross-user |

### The "created in US, clicked in EU" problem

Setup: the leader DB is in us-east. eu-west has an **async** read replica and a cache. A US user creates a link and immediately sends it to a colleague in Europe, who clicks within seconds.

```
     US user      US app+leader        EU replica      EU app+cache    EU user
        |               |                   |                |            |
 0s     |-POST /shorten>|                   |                |            |
        |               | INSERT aZ3k9, ok  |                |            |
        |<-sho.rt/aZ3k9-|                   |                |            |
        |               |\ async ship       |                |            |
        | link sent over chat ...........................................>|
 0.3s   |               | \ (in flight)     |                |            |
        |               |  \                |                |<-------GET-|
        |               |   \               |<--------SELECT-|            |
        |               |    \              |-no row-------->|            |
        |               |     \             |                |-404------->|
        |               |      \            |                | caches the |
        |               |       \           |                | 404 (5 min)|
 0.9s   |               |        `--------->| row arrives    |            |
        |               |                   |                |            |
 5s     |               |                   |                |<-----retry-|
        |               |                   |                |-404------->|
        |               |                   | never asked;   | from neg.  |
        |               |                   | row is here    | cache      |
```

Two separate problems:

1. **The lag window itself** (~0.1–1 s, sometimes much more): the EU replica genuinely doesn't have the row yet.
2. **Negative caching makes it sticky.** If EU caches "aZ3k9 not found" for 5 minutes (a reasonable defense against scanners, 8.8), the link stays broken long after the row arrives.

And note: the clicker is a **different user**. Read-your-writes, sticky sessions and "read from leader after your own write" don't help them. Fixes have to work for *any* reader.

### Fix 1: fall back to the origin on a miss (the main answer)

A miss in the local region is not proof that the code doesn't exist. Before returning 404, ask the leader region.

```
EU app: GET /aZ3k9
   |
   +--> EU cache  ---- hit --------------------------------> 301/302  (~1 ms)
   |      miss
   +--> EU replica --- found -------------------------------> redirect (~2 ms)
   |      not found
   +--> US leader (cross-region, ~80 ms)
          |--- found  ---> redirect, and fill EU cache        (~85 ms, rare)
          `--- not found -> 404, negative-cache for a SHORT TTL
```

Why it's cheap: real codes are almost always in the replica already. Misses that reach the US are only (a) links created in the last ~second and (b) invalid codes. The ~80 ms penalty applies to a tiny fraction of traffic.

Protect the fallback path from abuse: a flood of random codes would all fall through to the US leader. Use a rate limit on fallbacks (11.7), a **Bloom filter** of existing codes per region (8.8, 13.1) (a "definitely not present" answer skips the fallback, but a Bloom filter built from the lagging replica has the same lag, so only trust it for codes older than the lag), and a negative cache with a TTL of seconds, not minutes.

### Fix 2: make the code tell you where it was born

Embed the origin region in the code, e.g. the first base62 character, or a region bit in a Snowflake-style ID (11.6).

```
  a Z 3 k 9
  ^
  region prefix 'a' = us-east   -> EU knows exactly where to fall back
```

It also lets every region **create** codes locally without coordination (each region owns its own code space), which removes the need to forward writes to one leader.

### Fix 3: replicate creates synchronously (pay on write, not on read)

Writes are rare: at 100 redirects per URL, creates are ~1% of requests. So you can afford to make the create wait until the row is in every region (or a majority of regions) before returning the short link:

```
POST /shorten latency:
   async:                          ~5 ms
   wait for all regions (US/EU/AP): ~150-200 ms  (the slowest region's RTT)
   wait for majority of 3 regions:  ~80 ms       (the nearest other region)
```

A 200 ms create is fine for a human clicking "Shorten". After it returns, any region can serve the redirect. This is the PACELC "EC on writes, EL on reads" choice (4.7). With "majority of regions" you still need Fix 1 for the region that wasn't in the majority.

### Fix 4: push to the edge on create

On create, also write the mapping into each region's cache (or a CDN key-value store) directly, asynchronously but in parallel with DB replication. Mostly shortens the window; doesn't close it alone.

### Fixes for the creator's own reads (read-your-writes)

These help when the **same** user reads after writing, e.g. the creator opening their dashboard:

| Technique | How | Cost |
| :---- | :---- | :---- |
| **Read from leader after write** | For N seconds after a user's write (N > p99 lag), route their reads to the leader. Store "last write time" in a cookie. | Extra leader load; cross-region RTT if leader is remote |
| **Read from leader for own data** | "My links" page always reads from leader; public redirects from replicas | Simple, scoped |
| **Version (LSN) token** | Write returns its commit position (LSN 1043). Client sends it on reads. Replica serves only if its applied LSN ≥ 1043; else waits briefly or forwards to the leader. | Precise; needs plumbing. Postgres `pg_last_wal_replay_lsn()`; MongoDB causal sessions use cluster time |
| **Sticky sessions** | Pin a user to one replica (or one region) | Gives monotonic reads; not RYW by itself |

```
LSN token, step by step:
  1. client --write--> leader      leader commits at LSN 1043
  2. leader --"ok, lsn=1043"--> client  (client stores it in a cookie)
  3. client --read, min_lsn=1043--> replica R
  4. R.applied_lsn = 1039 < 1043  -> wait up to 50 ms, else forward to leader
     R.applied_lsn = 1050 >= 1043 -> serve locally, guaranteed fresh enough
```

### Monitor lag and act on it

- Export lag per replica as a metric and alert on p99 (12.10).
- Have load balancers stop routing to a replica whose lag exceeds a threshold (like a readiness check, 12.6). MySQL ProxySQL and many Postgres poolers support "max replication lag".
- Watch for long transactions and schema migrations: a 10-minute `ALTER TABLE` on the leader can mean 10 minutes of lag downstream.

> 💡 **Interview tip:** The crisp answer to "created in US, clicked in EU": "EU reads from an async replica, so for ~a second the new code might not be there. I treat a local miss as 'unknown', not 'not found': fall back to the origin region (the code's prefix says which one), fill the local cache, and only negative-cache 404s briefly. Creates are 1% of traffic, so optionally I make creates wait for cross-region replication and pay ~100–200 ms once per link instead of risking a broken first click."

> 🔗 **Connections:** The guarantees being broken are in 4.4. Negative caching and Bloom filters are 8.8 and 13.1. Region-aware IDs are 11.6. Cache invalidation across regions (when a destination changes) is 8.5. The full multi-region design is 5.8 and 14.8.

## 5.5 Failover (and split brain)

When a follower dies, it reconnects and catches up from its last log position (5.2). Easy. When the **leader** dies, someone must promote a follower: **failover**.

### The three steps

```
1. DETECT     leader misses heartbeats for T seconds (e.g. T = 10-30 s)
2. ELECT      pick the follower with the most up-to-date log
              (by coordinator, or by vote: Raft 6.3)
3. RECONFIGURE  clients / proxies / DNS point to the new leader;
              old leader, if it comes back, must become a follower
```

Each step has a trap.

### Trap 1: choosing the timeout

| Timeout | Effect |
| :---- | :---- |
| Too short (1–2 s) | GC pauses, network blips and load spikes trigger needless failovers. A failover under load adds more load: cascading failure (12.3). |
| Too long (60 s+) | Long write outage every time the leader really dies. |

A timeout can't distinguish "dead" from "slow" (2.2). Pick from observed heartbeat latency distribution (6.9). Managed services land around ~30 s to a few minutes of total failover time (e.g. AWS RDS Multi-AZ documents ~1–2 min).

### Trap 2: lost writes with async replication

The new leader may be missing the old leader's last writes (5.3). Those writes are usually **discarded**, which silently breaks durability. Worse, anything *outside* the DB that already saw them is now inconsistent: for example, if auto-increment IDs are reused by the new leader, a cache or another system keyed by those IDs may now attach old data to new rows. DDIA describes a GitHub incident of exactly this shape.

### Trap 3: split brain

The old leader wasn't dead, just partitioned or paused. It comes back still believing it's leader. Now **two leaders accept writes**.

```
time ------------------------------------------------------------------>
node A (leader):  ok ok ok [ GC pause / network cut ..... ] ok: "I'm leader!"
                                                            writes x=1
                                 |                              |
monitor:          heartbeats ... no heartbeats for 10 s         |
                                 v                              |
node B:           follower  -> PROMOTED to leader               |
                                 writes x=2                     v
                                                     TWO LEADERS, diverging
```

Defenses, from weakest to strongest:

- **STONITH** ("shoot the other node in the head"): power off or fence the old leader via the hypervisor/PDU before promoting. Crude but effective.
- **Leader leases** (6.6): the leader may only act while holding a time-bounded lease; the new one isn't promoted until the old lease has surely expired.
- **Fencing tokens** (6.6): each leader gets a higher epoch number; storage and downstream systems reject writes from older epochs.
- **Consensus** (6.3): a leader needs a majority to commit anything. An old leader on the minority side simply can't commit, so it can't do damage.

### Tools

Postgres: **Patroni** (stores leader key in etcd/Consul/ZooKeeper with a TTL), pg_auto_failover. MySQL: Orchestrator, Group Replication. Managed: RDS/Aurora, Cloud SQL. Systems built on Raft (etcd, CockroachDB, Kafka KRaft) do it internally.

> 💡 **Interview tip:** When you say "fail over to the replica", immediately add the three costs: detection time (RTO), lost async writes (RPO), and split-brain prevention (fencing/consensus). That turns a buzzword into an answer.

> 🔗 **Connections:** Leader election done safely is Raft (6.3). Leases and fencing are 6.6. Failure detection is 6.9. Recovery time is RTO (5.9). DNS-based reconfiguration is limited by TTLs (11.3).

## 5.6 Multi-Leader Replication and Conflict Resolution

**Multi-leader** (multi-master, active-active writes): more than one node accepts writes, and each leader replicates to the others asynchronously. Usually one leader **per region**.

```
      us-east                         eu-west
   +-----------+   async, both ways  +-----------+
   | leader US |<------------------->| leader EU |
   +-----+-----+                     +-----+-----+
         |  sync/async in-region           |
    [followers]                       [followers]
   US writes: ~5 ms local            EU writes: ~5 ms local
```

### When it's worth it

- **Multi-region writes** with low latency, and each region keeps writing when the inter-region link is down.
- **Offline clients**: each phone/laptop is a "leader" for its local copy (calendar apps, CouchDB/PouchDB).
- **Collaborative editing** (Google Docs, Figma): each user's edits apply locally first.

### The price: write conflicts

Two leaders can accept conflicting writes to the same key at the same time. Neither knows about the other until replication arrives.

```
time ---------------------------------------------------------------->
US leader:  SET alias "launch" -> https://a.com       replicate --->
                                         \          /
                                          \        /   (both arrive ~80 ms
                                           X          later; who wins?)
                                          /        \
EU leader:  SET alias "launch" -> https://b.com       replicate --->
```

URL shortener: two people claim the custom alias `sho.rt/launch` in two regions within the same second. Both got "ok".

### Resolution strategies

| Strategy | How | Trade-off | Where |
| :---- | :---- | :---- | :---- |
| **Avoid conflicts** | Route all writes for a given key to one "home" leader (by user, by code prefix). | Best option when possible; failover of a home region re-introduces conflicts briefly | 5.8 |
| **Last-writer-wins (LWW)** | Attach a timestamp; highest wins | Simple. **Silently drops** the other write. Wall clocks skew (3.1), so the "last" may not be the latest. | Cassandra, DynamoDB global tables |
| **CRDTs** | Data types whose merge is commutative, associative, idempotent (counters, sets, text) | Automatic, no data loss; only for data that fits a CRDT | Riak, Redis Enterprise active-active (13.6) |
| **Application merge** | Keep both versions ("siblings") and let the app or user resolve | Most correct; most work | Amazon's Dynamo shopping cart (union of items); CouchDB |
| **Detect, don't resolve** | Use version vectors (3.5) to tell "concurrent" from "newer" | Required before any merge that isn't LWW | Riak, Dynamo |

### Topologies

```
 all-to-all           ring                     star
  A <--> B           A --> B                    B
  ^ \  / ^           ^     |                    ^
  |  \/  |           |     v               C <- A -> D
  |  /\  |           D <-- C                    v
  v /  \ v                                      E
  C <--> D      one node down breaks it   hub is a single point
  writes can arrive out of causal order (need causal tracking, 4.3)
```

> 💡 **Interview tip:** Prefer "avoid conflicts" over "resolve conflicts". For uniqueness (custom aliases), multi-leader is the wrong tool: that check must be linearizable (4.2), so route it to one home leader or use a consensus-backed conditional write (6.7). Use multi-leader only for data where merging is natural.

> 🔗 **Connections:** LWW depends on clocks (3.1, 3.6). Detecting concurrency uses vector clocks (3.5). CRDTs are 13.6. Multi-region trade-offs are summarized in 5.8.

## 5.7 Leaderless Replication

In **leaderless** (Dynamo-style) replication, the client (or a coordinator node acting for it) sends each write to **all N replicas** of the key in parallel and waits for **W** acks. Reads go to replicas in parallel and wait for **R** answers, taking the value with the newest version. Amazon's Dynamo paper (2007) popularized it; Cassandra, Riak and ScyllaDB use it.

### The quorum condition

```
N = replicas per key     W = write acks needed     R = read replies needed

If  R + W > N,  every read set overlaps every write set in >= 1 node,
so a read sees at least one copy of the latest successful write.

N = 3, W = 2, R = 2:

  write v2 ->  [A: v2]  [B: v2]  [C: v1]   (C was slow; write still ok, W=2)
  read     ->  [A: v2]           [C: v1]   (R=2 replies)
                  ^ overlap guaranteed: pick the newest -> v2
```

| N, W, R | Tolerates | Use |
| :---- | :---- | :---- |
| 3, 2, 2 | 1 node down for reads and writes | Balanced default (Cassandra `QUORUM`) |
| 3, 3, 1 | Fast reads; any 1 node down blocks writes | Read-heavy, rare writes |
| 3, 1, 3 | Fast writes; any 1 node down blocks reads | Rare |
| 3, 1, 1 | Fast and available; **no overlap**, stale reads | Cassandra `ONE`: eventual consistency |

Latency bonus: with N=3, R=2, a read waits for the **second-fastest** of three replicas, so one slow replica doesn't hurt the tail (like a built-in hedged request, 2.3).

### R + W > N is not linearizability

Even with a strict quorum, edge cases remain: concurrent writes resolved by LWW drop one; a write that reached fewer than W nodes "fails" but isn't rolled back, so later reads may or may not see it; a read concurrent with a write may see it on one read and not the next. Cassandra adds lightweight transactions (Paxos) when you need a real compare-and-set.

### Keeping replicas in sync

Replicas miss writes (down, slow, dropped). Three mechanisms repair them:

```
1. READ REPAIR (on the read path)
   read: A=v2, C=v1  -> client returns v2, and writes v2 back to C
   Fixes keys that are read. Rarely-read keys stay stale.

2. HINTED HANDOFF (on the write path)
   home replica C is down -> write goes to D with a hint "this belongs to C"
   when C returns, D forwards the write and deletes the hint

3. ANTI-ENTROPY (background)
   replicas compare Merkle trees (13.4) of their key ranges:
            root hash differs?
           /                \
     left half same      right half differs -> recurse
                                ...
                         leaf: keys 812-819 differ -> sync only those
   Compares billions of keys by exchanging ~log(n) hashes.
```

### Sloppy quorum

If some of a key's N home nodes are unreachable, a **sloppy quorum** accepts writes on *other* healthy nodes (with hints) so the write still gets W acks. That increases write availability, but **R + W > N no longer guarantees overlap**: the W writes may have landed on nodes the R reads don't consult. It's a durability guarantee, not a consistency one. Dynamo and Riak use it; Cassandra uses hinted handoff but doesn't count hints toward the consistency level.

### Multi-datacenter quorums (Cassandra)

| Level | Waits for | Cross-region RTT on the request? |
| :---- | :---- | :---- |
| `ONE` / `LOCAL_ONE` | 1 replica (local DC) | No |
| `LOCAL_QUORUM` | Majority in the local DC | No (typical choice) |
| `QUORUM` | Majority of all replicas, all DCs | Yes |
| `EACH_QUORUM` | Majority in every DC (writes) | Yes |
| `ALL` | Every replica | Yes, and any replica down fails it |

`LOCAL_QUORUM` for both reads and writes gives strong-ish consistency **within** a region and async replication **between** regions: the same shape as 5.3's recommendation.

> 💡 **Interview tip:** Write the inequality and plug in numbers: "N=3, W=2, R=2 → 2+2 > 3, so reads overlap the last write and one node can be down." Then add the caveat that sloppy quorums and LWW break the guarantee. That shows you know the formula is necessary, not sufficient.

> 🔗 **Connections:** Majority quorums reappear in consensus (6.2). Merkle trees are 13.4. Placement of N replicas on the ring is consistent hashing (7.4). Version conflicts use vector clocks (3.5) or LWW (5.6).

## 5.8 Multi-AZ vs Multi-Region; Active-Active vs Active-Passive

### The failure domains

```
+---------------------------- REGION us-east -----------------------------+
|  +------- AZ a -------+  +------- AZ b -------+  +------- AZ c -------+  |
|  | separate power,    |  |                    |  |                    |  |
|  | cooling, building  |  |                    |  |                    |  |
|  +--------------------+  +--------------------+  +--------------------+  |
|         <------ ~1-2 ms RTT between AZs ------>                          |
+--------------------------------------------------------------------------+
                 ^
                 | ~70-90 ms RTT (US east - EU west)
                 v
+---------------------------- REGION eu-west -----------------------------+
|  ... 3 more AZs ...                                                     |
+--------------------------------------------------------------------------+
```

| | Multi-AZ | Multi-Region |
| :---- | :---- | :---- |
| Protects against | Server, rack, building/power failure | Whole-region outage, regional network or control-plane failure, natural disaster |
| RTT between copies | ~1–2 ms | ~30–200 ms |
| Replication | **Synchronous / quorum** is affordable | Usually **asynchronous** |
| Data loss on failure | RPO 0 with sync | RPO = replication lag (seconds) |
| Latency for far-away users | No help | Serve users from nearby: the big win |
| Complexity | Low; most managed DBs do it with one flag | High: lag (5.4), conflicts (5.6), routing (11.3), data residency rules |
| Cost | ~1.5–3× one AZ | ~2–3× one region, plus cross-region egress |

Rule of thumb: **every production system should be multi-AZ. Go multi-region only for a reason**: global users with a latency SLO (p99 < 100 ms is impossible across an ocean, see 2.3), an availability target above what one region gives, or regulation.

### Active-passive

One region serves all traffic. The other is a **warm standby** with an async replica; it takes over only when the primary fails.

```
                users everywhere
                      |
                   GeoDNS / global LB
                      | 100% traffic
          +-----------v-----------+            +-----------------------+
          |   us-east (ACTIVE)    |  async     |  eu-west (PASSIVE)    |
          |  app + cache + leader |----------->|  replica (+ idle app) |
          +-----------------------+            +-----------------------+

failover:  1. detect region down     2. promote EU replica (lose last ~s)
           3. flip DNS (wait for TTL) 4. scale up EU app tier (cold caches!)
           RTO: minutes to an hour     RPO: replication lag
```

- Simple: one writer, no conflicts.
- EU users pay ~80 ms per request all the time.
- The standby is unproven until you fail over. Test it (12.8). Its caches are cold: expect a load spike on the DB right after failover (8.7).

### Active-active

Every region serves live traffic. There are several flavors, and **the interviewer usually wants you to pick one and justify it**:

| Flavor | Reads | Writes | Conflicts | Fits |
| :---- | :---- | :---- | :---- | :---- |
| **A. Read-local, write-global** | Local replica | Forwarded to one leader region | None | Read-heavy: **URL shortener** |
| **B. Home region per record** | Local (fallback to home) | To the record's home region | None (if routing is right) | User-owned data (each user homed) |
| **C. Multi-leader** | Local | Local | Yes: LWW/CRDT/merge (5.6) | Counters, carts, collaborative data |
| **D. Global consensus** | Local (stale) or via leader | Majority of regions (~1 RTT) | None | Spanner, CockroachDB: money, inventory |

### URL shortener: flavor A, step by step

```
                         GeoDNS / anycast (11.3): user -> nearest region
             +-----------------------+------------------------+
             v                       v                        v
     +---------------+       +---------------+        +---------------+
     |   us-east     |       |   eu-west     |        |   ap-south    |
     | app (stateless)       | app           |        | app           |
     | cache         |       | cache         |        | cache         |
     | DB LEADER     |=async>| DB replica    |        | DB replica    |
     |  (sync to 2   |=======================async====>|               |
     |   other AZs)  |       |               |        |               |
     +-------^-------+       +-------+-------+        +-------+-------+
             |  creates forwarded (+~80 ms)  |                |
             +-------------------------------+----------------+
             |  redirect misses fall back (5.4 Fix 1)         |
             +------------------------------------------------+

redirect (99% of requests): local cache -> local replica          ~1-5 ms
create   (1% of requests):  forward to us-east leader             ~80-200 ms
new link clicked abroad:    local miss -> fall back to leader     ~85 ms once
```

What happens when a region fails:

- **eu-west dies:** GeoDNS stops sending users there; they go to us-east or ap-south. Nothing to promote.
- **us-east (leader) dies:** redirects keep working everywhere from replicas (reads are fine). **Creates fail** until you promote a replica in another region (5.5), losing the last ~seconds of creates (5.9). Alternative: flavor A + region-prefixed codes (5.4 Fix 2) turns into flavor B: every region creates its own codes, and losing one region only stops creates *of that region's prefix*, which nobody needs.

### Capacity: surviving the loss of a region

If any region can fail, the remaining ones must absorb its traffic (1.7):

```
3 regions, each normally at utilization u.  Lose one: the other two take
3u / 2 each.  Need 3u/2 <= ~80%  ->  u <= ~53%.

general:  N regions, survive 1 loss:   u <= target × (N-1)/N
   N = 2  ->  each region at <= 40% (of an 80% target):  2x over-provisioned
   N = 3  ->  <= 53%
   N = 5  ->  <= 64%
```

More regions are cheaper per unit of headroom, but more expensive in complexity and cross-region traffic.

> 💡 **Interview tip:** A strong multi-region answer has four parts: (1) routing: GeoDNS/anycast to the nearest healthy region; (2) data: which flavor, and which data is sync vs async; (3) the lag problem and your fix (5.4); (4) region-loss walkthrough with capacity math. Say them in that order.

> 🔗 **Connections:** RTT numbers are in 1.1; headroom math in 1.7. Routing is GeoDNS/anycast (11.3) and CDNs (11.4). Cache invalidation across regions is 8.5. The full worked design is 14.8, and the failure drill is 14.9. Spanner-style flavor D relies on TrueTime (3.6) and Paxos (6.5).

## 5.9 RPO and RTO

Two numbers define a disaster-recovery plan:

- **RPO (Recovery Point Objective):** how much data (measured in time) you can afford to **lose**. "RPO = 5 s" means after a disaster you may lose up to the last 5 seconds of writes.
- **RTO (Recovery Time Objective):** how long until service is **back**.

```
                          disaster
time ---------------------------X-------------------------------------->
         last safe copy         |                         service restored
              |<---- RPO ------>|<---------- RTO ---------------->|
              data written here         outage: detect, decide,
              is LOST                   promote, reroute, warm up
```

| Strategy | RPO | RTO | Cost |
| :---- | :---- | :---- | :---- |
| Nightly backup to another region | up to 24 h | hours (restore TBs) | $ |
| Backups + continuous WAL archiving (PITR) | ~minutes | ~1 h+ | $ |
| Async cross-region replica, manual promotion | seconds (= lag) | ~15–60 min | $$ |
| Async replica, automated failover | seconds | ~1–5 min (+ DNS TTL) | $$ |
| Sync/quorum replication across AZs | 0 for AZ loss | ~30 s–2 min | $$ |
| Quorum across regions (Spanner-style) | 0 for region loss | ~seconds (automatic) | $$$, slower writes |

### Worked example: URL shortener

```
creates: 100M / month ÷ 2.5×10^6 s/month ≈ 40 writes/s avg, ~120/s peak (3×)
async cross-region lag at failover: ~1 s
=> region loss loses ~40-120 newly created links                 (RPO ~1 s)

RTO for creates: detect (~30-60 s) + promote (~30 s) + DNS TTL (60 s)
                 ≈ 2-3 min
RTO for redirects: ~0 (other regions already serving)
```

That's usually acceptable: tell the interviewer that users whose create failed or got lost will retry, and that idempotency keys (2.6) make the retry safe. If it isn't acceptable, make creates wait for a second region (5.4 Fix 3): RPO becomes 0, creates get ~80 ms slower.

### Backups are not replication

Replication copies **mistakes** instantly: `DELETE FROM urls` on the leader is on every replica within a second. You still need point-in-time backups for logical corruption, bad migrations and ransomware, with restores that are **regularly tested** (12.8).

> 💡 **Interview tip:** Give RPO and RTO **per operation**, because they differ: "redirects: RTO ≈ 0 (served from any region); creates: RPO ≈ 1 s of async lag, RTO ≈ 2–3 min." Also mention that backups guard against the errors that replication faithfully copies.

> 🔗 **Connections:** RTO is part of the availability budget (1.3, 12.1). The data-loss window comes from async replication (5.3). Failover steps are 5.5. Backups, DR drills and chaos testing are 12.8.

---

# 6. Consensus and Coordination

Consensus is how a group of machines agrees on one value, or one ordered log, even when some of them crash and messages are delayed. It's the foundation under leader election, locks, configuration stores and every "strongly consistent" database. You rarely implement it, but you constantly design on top of it, so you need to know what it guarantees, what it costs and where it stops.

## 6.1 The Consensus Problem (and FLP, Two Generals briefly)

Several nodes each **propose** a value; they must **decide** one. The requirements:

| Property | Meaning | Type |
| :---- | :---- | :---- |
| **Agreement** | No two nodes decide differently | Safety |
| **Validity** | The decided value was proposed by someone (no "default" made up) | Safety |
| **Integrity** | A node decides at most once | Safety |
| **Termination** | Every non-crashed node eventually decides | Liveness |

**Safety** = nothing bad ever happens. **Liveness** = something good eventually happens. Practical protocols (Raft, Paxos) **never** violate safety, and guarantee liveness only when the network is behaving reasonably.

### What consensus gives you

Many problems are consensus in disguise:

```
leader election          "which node is leader for term 7?"
distributed lock         "who holds lock L?"
atomic commit            "did transaction T commit or abort?" (9.7)
uniqueness               "who got username 'berat' / alias 'launch'?"
total order broadcast    "what is entry #1043 in the log?"  <- Raft's framing
linearizable CAS         compare-and-set on a register (4.2)
```

Replicated log ⇒ replicated state machine: if every node applies the **same commands in the same order** to the same deterministic state machine, they end in the same state. That's what Raft builds (6.4).

### FLP impossibility

**FLP** (Fischer, Lynch, Paterson, 1985): in a fully **asynchronous** system (no bound on message delay, no clocks), **no deterministic algorithm can guarantee consensus terminates** if even **one** node may crash. The reason: you can never tell a crashed node from a slow one (2.2), and there's always some schedule of delays that keeps the protocol undecided.

How real systems live with it: assume **partial synchrony**. Use **timeouts** and **randomization** (Raft's random election timeouts). Safety holds always; progress happens whenever the network is stable long enough to elect a leader. In practice that's almost always.

### Two Generals

Two generals must attack at the same time and can only communicate by messengers who may be captured. General A sends "attack at dawn". Did it arrive? B sends an ack. Did the ack arrive? A sends an ack of the ack... **No finite number of messages makes both sure.**

```
A ---- "attack at dawn" ----> B
A <---------- "ack" --------- B     B: "did A get my ack?"
A ---------- "ack-ack" -----> B     A: "did B get my ack-ack?"
...forever
```

This is the "request succeeded but the reply was lost" problem (2.2) in pure form. The practical lesson: you can't get certainty over an unreliable network, so design for **idempotent retries** (2.6) and accept that consensus needs a majority rather than everyone.

> 🔗 **Connections:** Atomic commit (2PC) is a consensus problem that blocks when the coordinator dies (9.7). Timeouts and partial failure are 2.2 and 2.7. Total order broadcast is also what a Kafka partition provides (10.4).

## 6.2 Quorums and Majorities (why 2f+1)

A **quorum** is the minimum number of nodes that must take part for an operation to count. Consensus uses **majority** quorums: more than half of the cluster.

**Key property: any two majorities overlap in at least one node.** So if a value was accepted by a majority, any future majority contains at least one node that knows about it. That's how a new leader is guaranteed to learn every committed entry.

```
5 nodes:  A  B  C  D  E
quorum 1: A  B  C            (committed entry 1043)
quorum 2:       C  D  E      (elects next leader)
                ^
           C is in both: the new leader's majority includes someone
           who has entry 1043
```

### Why 2f + 1

To tolerate **f** crashed nodes, the survivors must still form a majority: n − f > n/2 ⇒ n ≥ 2f + 1.

| Cluster size n | Majority | Failures tolerated | Note |
| :---- | :---- | :---- | :---- |
| 1 | 1 | 0 | No fault tolerance |
| 2 | 2 | 0 | **Worse than 1**: two things that can fail |
| 3 | 2 | 1 | Common minimum |
| 4 | 3 | 1 | Same tolerance as 3, slower |
| 5 | 3 | 2 | Common for etcd/ZooKeeper in production |
| 7 | 4 | 3 | Rarely worth it: more nodes per write |

Even sizes add cost without adding tolerance. Use **odd** sizes.

### Split-brain proof

In a partition, at most **one** side can hold a majority. The minority side can't elect a leader or commit, so two leaders can't both make progress.

```
5 nodes, partition 3 | 2:
   [A B C]  majority -> elects leader, commits writes
   [D E]    minority -> cannot commit, rejects writes (the "C" in CP, 4.6)
```

### What it costs

- **Latency:** each commit waits for the **(majority − 1)-th fastest follower**. With 5 nodes, the leader + the 2 fastest of 4 followers. A slow node doesn't hurt; losing 3 does.
- **Throughput:** every write goes through one leader and is sent to every follower. More nodes = more leader work. That's why consensus groups stay small (3–5) and you scale by running **many groups** (one per shard: CockroachDB, TiKV, Spanner).
- **Byzantine** faults (lying nodes, 2.2) need **3f + 1** nodes and different protocols (PBFT). Datacenter systems assume crash faults only.

### Placing replicas across failure domains

```
3 nodes in 3 AZs:         lose any AZ -> 2 of 3 remain -> still a majority
5 nodes in 3 regions (2-2-1):
                          lose any region -> >= 3 of 5 remain -> OK
                          commit = nearest other region's RTT
3 nodes in 2 AZs (2-1):   lose the AZ with 2 -> 1 of 3 -> DOWN. Don't.
```

> 💡 **Interview tip:** "Why 3 or 5 nodes and not 4?" Majority of 4 is 3, so 4 tolerates 1 failure, same as 3, but every write waits for more acks. Also say where you put them: one per AZ, so losing an AZ leaves a majority.

> 🔗 **Connections:** Leaderless replication uses a looser form of quorum, R + W > N (5.7). Placing nodes in failure domains is 1.7 and 12.2. Per-shard consensus groups combine this with partitioning (Part 7).

## 6.3 Raft: Leader Election

**Raft** (Ongaro and Ousterhout, 2014) was designed to be understandable. It splits consensus into **leader election** (here), **log replication** (6.4), and safety rules connecting them. Used by etcd, Consul, CockroachDB, TiKV, Kafka's KRaft, MongoDB (a Raft-like protocol) and many more.

### Terms

Time is divided into **terms**, numbered 1, 2, 3, ... Each term begins with an election; at most **one leader per term**. Terms act as a logical clock (3.4): every message carries the sender's term, and any node that sees a **higher** term immediately adopts it and becomes a follower. A message with a **lower** term is rejected as stale.

```
term:   |---- 1 ----|-- 2 --|----------- 3 -----------|-- 4 --|---- 5 ...
        | election  |split  | election   normal       |       |
        | + leader A|vote,  | + leader C operation    | ...   |
        |           |no     |                         |       |
        |           |leader |                         |       |
```

### The state machine

Every node is in one of three states:

```
                     (1) election timeout:
   start             term++, vote for self, send RequestVote
     |
     v
+----------+ ---------------------------------> +-----------+
| FOLLOWER |                                    | CANDIDATE |----+
+----------+ <--------------------------------- +-----------+    |
     ^        (2) sees a valid leader for this        |    ^     | (3) timeout
     |            term, or any higher term            |    |     | (split vote):
     |                                                |    +-----+ term++, retry
     |                                   (4) votes    |
     |                                   from a       |
     |                                   majority     v
     |                                            +--------+
     +------------------------------------------- | LEADER |
       (5) sees a higher term (it was replaced)   +--------+
```

- **Follower:** passive. Responds to the leader's AppendEntries (which double as **heartbeats**) and candidates' RequestVote. If it hears nothing for an **election timeout**, it becomes a candidate.
- **Candidate:** increments its term, votes for itself, sends RequestVote to all others.
- **Leader:** sends heartbeats (e.g. every ~50 ms) to keep followers from timing out; handles all client writes.

### Election, step by step

```
5 nodes. Leader of term 3 (A) has crashed. Election timeouts are random
in [150, 300] ms, so one follower times out first.

time   A(dead)    B              C              D              E
 0ms    X        follower       follower       follower       follower
                 (timer 210)    (timer 160)    (timer 280)    (timer 250)
160ms            .              TIMEOUT:
                                term=4, vote
                                for self
                                RequestVote(term 4, lastLog idx/term) -->all
165ms            "term 4 > 3,   .              votes C        votes C
                 C's log is as
                 up to date as
                 mine" votes C
166ms                           3 votes (self,
                                B, D) = majority
                                of 5 -> LEADER
167ms            <------------- heartbeats (AppendEntries, term 4) -------->
                 timers reset   timers reset on every heartbeat
```

### The rules that make it safe

1. **One vote per term.** Each node votes for at most one candidate per term, persisted to disk before replying (so a restart can't vote twice). With majorities overlapping (6.2), at most one candidate can win a term.
2. **Election restriction:** a node refuses to vote for a candidate whose log is **less up to date** than its own (compare last entry's term first, then log length). Since a committed entry is on a majority, and the winner needs a majority of votes, the winner is guaranteed to have every committed entry. **Leaders never need to fetch missing committed data.**
3. **Randomized timeouts** (paper suggests 150–300 ms) make split votes unlikely; if one happens, everyone times out again at different random times and a new term starts.

### Split vote

```
B and D time out at nearly the same moment, both in term 5:
   B gets votes {B, C}       D gets votes {D, E}       A is down
   neither has 3 of 5  -> no leader in term 5
   new random timeouts -> B times out first in term 6 -> wins
```

### Timing

```
broadcast time  <<  election timeout  <<  mean time between failures
  ~0.5-20 ms          ~150 ms - 1 s+           months
```

Within a region, etcd defaults are a 100 ms heartbeat and a 1,000 ms election timeout; across regions both must be raised (election timeout ≫ cross-region RTT), so failover takes seconds. Pick too short and GC pauses or network blips trigger needless elections (5.5's timeout trap, again).

### Practical additions

- **Pre-vote:** before incrementing its term, a node asks "would you vote for me?". A node partitioned away and timing out repeatedly then can't disrupt a healthy cluster with ever-higher terms when it rejoins (etcd, CockroachDB).
- **Leader transfer:** hand off leadership gracefully before maintenance.

> 💡 **Interview tip:** Three sentences cover Raft elections: "Followers that miss heartbeats for a randomized timeout become candidates in a new term and request votes. A node votes once per term, only for a candidate whose log is at least as up to date as its own. Whoever gets a majority leads that term, so there's at most one leader per term and it already has every committed entry."

> 🔗 **Connections:** Terms are a Lamport-style logical clock (3.4) and work as fencing tokens (6.6). Heartbeat-based failure detection is 6.9. The majority math is 6.2. Raft-based failover replaces the external failover tooling of 5.5.

## 6.4 Raft: Log Replication and Commit

The leader receives commands, appends them to its **log**, replicates them, and once an entry is on a **majority** it is **committed**: it will never be lost or changed. Each node applies committed entries, in order, to its **state machine** (e.g. a key-value map).

### Log structure

Each entry has an **index** (position) and the **term** in which the leader created it:

```
index:    1      2      3      4      5      6
term:   [ 1  ] [ 1  ] [ 1  ] [ 2  ] [ 3  ] [ 3  ]
cmd:    x<-3   y<-1   y<-9   x<-2   x<-0   y<-7
                                    ^             ^
                             commitIndex = 5   lastIndex = 6
```

**Log Matching Property:** if two logs have an entry with the same index **and** term, they hold the same command and **all earlier entries are identical**. That's what lets the leader verify a follower's whole log by checking one entry.

### A write, step by step

```
client       leader L (term 3)        F1             F2           (F3, F4)
  |               |                    |              |               |
1 | SET x=5 ----->|                    |              |               |
  |               | 2. append at idx 7 (uncommitted), fsync           |
  |               |                    |              |               |
  |               | 3. AppendEntries(term=3, prevLogIndex=6,          |
  |               |    prevLogTerm=3, entries=[7:x=5], leaderCommit=6)|
  |               |------------------->|------------->|-------------->|
  |               |                    |              |               |
  |               | 4. each follower: do I have entry 6 with term 3?  |
  |               |    yes -> append 7, fsync, reply success          |
  |               |<---- ok -----------|              |               |
  |               |<---- ok ------------------------- |    (slow)     |
  |               |                    |              |               |
  |               | 5. idx 7 on L, F1, F2 = 3 of 5 = MAJORITY         |
  |               |    commitIndex = 7                                |
  |               | 6. apply x=5 to state machine                     |
  |<---- ok ------|                    |              |               |
  |               | 7. next AppendEntries / heartbeat carries         |
  |               |    leaderCommit=7 -> followers apply idx 7 too    |
  |               |------------------->|------------->|-------------->|

latency ≈ leader fsync + 1 RTT to the 2nd-fastest follower + its fsync
```

The client gets "ok" after **one round trip to a majority**. Followers learn about the commit one message later, which is fine because only the leader answers clients.

### Fixing inconsistent follower logs

After crashes and leader changes, a follower's log can be missing entries or have **extra, uncommitted** entries from an old term. The consistency check finds the last point of agreement, and the leader overwrites everything after it.

```
index:      1   2   3   4   5   6   7
leader  :  [1] [1] [1] [4] [4] [5] [5]      (term 5 leader)
follower:  [1] [1] [1] [2] [2] [2]          (old leader of term 2 wrote these,
                                             never committed)

leader tries nextIndex=8: prev=(7,5)  follower: "no entry 7"     -> reject
leader backs up nextIndex=7: prev=(6,5)  follower has (6,2)      -> reject
         ... nextIndex=5: prev=(4,4)  follower has (4,2)         -> reject
         ... nextIndex=4: prev=(3,1)  follower has (3,1)         -> MATCH
leader sends entries 4..7; follower deletes its 4..6 and appends:
follower:  [1] [1] [1] [4] [4] [5] [5]      identical to leader
```

The discarded term-2 entries were never committed (they never reached a majority), so no acknowledged write is lost. Real implementations back up by a whole term at a time instead of one index, to converge faster.

### The subtle commit rule

A leader only counts replicas to commit entries **from its own current term**. Older-term entries become committed indirectly, when a current-term entry after them commits. Without this rule, an entry could be on a majority, be treated as committed, and still be overwritten by a later leader (Figure 8 in the Raft paper). In practice a new leader commits a **no-op entry** at the start of its term to settle everything before it.

### Reads

Reading the leader's local state is **not** automatically linearizable: a deposed leader that doesn't know it yet (partitioned, paused) would serve stale data. Options:

| Method | How | Cost |
| :---- | :---- | :---- |
| Log read | Put the read through the log like a write | 1 majority RTT + fsync |
| **ReadIndex** | Record commitIndex, confirm still leader with one heartbeat round to a majority, wait until applied, then read | 1 majority RTT, no disk write (etcd default) |
| **Lease read** | Leader assumes no one else can be elected before its lease ends (relies on bounded clock drift) | No RTT; unsafe if clocks misbehave (6.6) |
| Follower read | Serve from a follower | Fast, stale (unless it asks the leader for the ReadIndex first) |

### Keeping the log bounded

- **Snapshots / log compaction:** periodically snapshot the state machine and discard the log prefix. A far-behind follower gets the snapshot via InstallSnapshot (same idea as bootstrapping a follower, 5.2).
- **Membership changes:** add/remove one server at a time (or joint consensus), so old and new majorities always overlap.
- **Performance:** batch many commands per AppendEntries and per fsync, and pipeline without waiting for each ack. etcd handles roughly ~10k+ writes/s on good hardware; the fsync latency of the disk usually dominates, which is why etcd docs insist on fast SSDs.

> 💡 **Interview tip:** The one-liner for commit: "An entry is committed once it's stored on a majority. The leader then applies it and replies. Because the election restriction makes any future leader have every committed entry, a committed write survives any minority of failures." Then add the caveat about reads needing ReadIndex or leases.

> 🔗 **Connections:** The log is a write-ahead log (9.4) replicated with quorum commit (5.3). Raft per shard ("Multi-Raft") is how CockroachDB/TiKV combine consensus with partitioning (Part 7). Lease reads depend on clocks (3.1, 6.6). Kafka KRaft uses a Raft variant for its metadata log (10.2).

## 6.5 Paxos in One Page

**Paxos** (Lamport, written 1989, published 1998) is the original practical consensus algorithm. Raft is essentially Multi-Paxos with a strong leader and a specified log structure. Google's Chubby and Spanner use Paxos; ZooKeeper uses a similar protocol, **ZAB**.

### Roles

- **Proposers** propose values.
- **Acceptors** vote; a majority of them forms a quorum.
- **Learners** find out what was decided.

(A single process usually plays all three.)

### Single-decree Paxos: two phases

Proposal numbers **n** are unique and totally ordered (e.g. `counter.node_id`).

```
Proposer                     Acceptors (majority needed: 3 of 5)
   |                            |
   | Phase 1a  PREPARE(n)       |
   |--------------------------->|  each acceptor: if n > highest promised,
   |                            |    promise to ignore anything < n
   | Phase 1b  PROMISE(n,       |    and report the highest-numbered
   |   acceptedN, acceptedV)    |    proposal it already ACCEPTED (if any)
   |<---------------------------|
   |                            |
   | choose v:                  |
   |   if any promise reported an accepted value, v = the one with the
   |   highest acceptedN (you MUST adopt it); else v = your own value
   |                            |
   | Phase 2a  ACCEPT(n, v)     |
   |--------------------------->|  accept unless it promised a higher n
   | Phase 2b  ACCEPTED(n, v)   |
   |<---------------------------|
   |                            |
   v is CHOSEN once a majority has accepted (n, v)
```

Why it's safe: once a majority accepted v, any later proposer's Phase 1 majority overlaps it (6.2), sees v, and is forced to propose v again. Values can't change once chosen.

Why it may not terminate: two proposers can keep pre-empting each other with ever-higher n (**duelling proposers**). FLP (6.1) in action. Fix: elect a distinguished proposer (a leader), with random backoff.

### Multi-Paxos

Running both phases for every log entry costs 2 RTTs. **Multi-Paxos** elects a stable leader that runs Phase 1 **once** for all future slots, then does only Phase 2 per entry: **1 RTT per commit**, same as Raft.

### Raft vs Paxos

| | Raft | (Multi-)Paxos |
| :---- | :---- | :---- |
| Leader | Required; only the leader appends | Optimization; many variants |
| Log holes | No: log is contiguous | Allowed: slots decided independently |
| New leader's log | Must already be up to date (election restriction) | Learns missing values in Phase 1 |
| Spec | One paper with full pseudocode | Many papers; implementations differ ("Paxos Made Live") |
| Used by | etcd, Consul, CockroachDB, TiKV | Chubby, Spanner, Megastore; Cassandra LWT; ZAB (ZooKeeper) is close |

> 💡 **Interview tip:** You won't be asked to derive Paxos. Know: two phases (prepare/promise, accept/accepted), majority quorums, "a proposer must adopt the highest-numbered already-accepted value", and that Multi-Paxos with a stable leader costs one round trip, same as Raft.

> 🔗 **Connections:** Spanner runs one Paxos group per split and combines it with TrueTime (3.6) and 2PC across groups (9.7). Cassandra's lightweight transactions use Paxos for compare-and-set (5.7).

## 6.6 Leases and Fencing Tokens

### Leases

A **lease** is a lock with an expiry: "node A holds the leader role until 12:00:10". The holder must renew it before it expires; if A crashes, the lease simply runs out and someone else can take it. No one has to decide that A is dead.

```
A:   acquire(10 s) .... renew .... renew .... [crash]
                                                |<--- 10 s --->|
B:                                                             acquire OK
```

Leases are used for leader election (Chubby, etcd leases, Kubernetes `Lease` objects for controller leader election), lease-based reads (6.4) and cache leases.

The catch: leases depend on **time**, and a process doesn't always know how much time has passed.

### The pause problem

```
time ---------------------------------------------------------------->
client 1:  acquire lease (30 s)  [------ stop-the-world GC pause 40 s ------]
                                                                  write to
                                                                  storage
                                                                  "I hold it!"
lock svc:                        lease expires at 30 s
client 2:                                    acquire lease
                                             write to storage
                                                 ^          ^
                                       two writers, both believe they own it
```

Checking "is my lease still valid?" right before writing doesn't help: the pause can happen **between** the check and the write. Also: VM migration, swapping (the Linux notes' page faults), a `SIGSTOP`, a slow disk, a network delay on the write packet itself, or a wall-clock jump (3.1, 3.2: use a monotonic clock for lease timers).

### Fencing tokens

The fix is to have the **resource** reject stale holders. Every time the lock is granted, the lock service hands out a **monotonically increasing token**. The client sends it with every write; storage remembers the highest token it has seen and rejects lower ones.

```
lock service        client 1           client 2            storage
     |-- token 33 ----->|                  |                   |
     |                  | (GC pause...)    |                   |
     |  lease expires   |                  |                   |
     |-- token 34 ------------------------>|                   |
     |                  |                  |-- write(t=34) --->| max=34 OK
     |                  |-- write(t=33) ---------------------->| 33 < 34
     |                  |<------------------------------ REJECTED
```

Where tokens come from: Raft **terms** (6.3), ZooKeeper **zxid** / znode version, etcd **revision**, a database sequence. The storage side needs a compare (`WHERE fencing_token <= :t`, or a conditional write).

> 💡 **Interview tip:** If you say "use a distributed lock", the follow-up is "what if the holder pauses past its lease?" Answer: "Leases alone can't prevent that; the protected resource must check a fencing token, a monotonically increasing number from the lock service, and reject writes with an older one."

> 🔗 **Connections:** Clock drift and monotonic clocks are 3.1–3.2. Fencing is the split-brain defense in failover (5.5). Raft's term is a built-in fencing token (6.3). Kafka uses producer epochs the same way to fence zombie producers (10.3).

## 6.7 Distributed Locks (and why Redlock is controversial)

First, ask **why** you want the lock:

| Purpose | If the lock fails occasionally | What to use |
| :---- | :---- | :---- |
| **Efficiency** (avoid doing the same work twice: two workers regenerating one report, cache stampede 8.7) | Some duplicated work. Annoying, harmless. | A single Redis instance is fine |
| **Correctness** (two holders would corrupt data, double-charge, oversell) | Data corruption | Consensus-backed lock (etcd, ZooKeeper) **plus fencing tokens** (6.6). Or avoid the lock. |

### A single-Redis lock

```
acquire:  SET lock:report-42 <random-id> NX PX 30000
            NX   = only if not exists
            PX   = expire after 30 s (lease, so a crash doesn't hold it forever)
release:  only if the value is still MY random id (atomic, via Lua script):
            if redis.call("GET", KEYS[1]) == ARGV[1] then
              return redis.call("DEL", KEYS[1]) end
```

The random id prevents deleting a lock that expired and was taken by someone else. Weaknesses: the Redis node is a single point of failure, and with async Redis replication a failover can lose the lock key, so two clients hold it.

### Redlock

Redlock (proposed by Redis's author, antirez) uses **N = 5 independent Redis masters** (no replication between them):

```
1. note start time
2. try SET NX PX on all 5, with a short per-node timeout
3. lock acquired if >= 3 of 5 succeeded AND elapsed < lease validity
4. effective validity = lease - elapsed - clock drift allowance
5. on failure, release on all nodes
```

### The controversy

Martin Kleppmann's 2016 critique ("How to do distributed locking"), with antirez's reply:

- **No fencing token.** Redlock doesn't produce a monotonically increasing number, so the resource can't reject a paused former holder (6.6). Any lease-based lock without fencing has the GC-pause problem.
- **Timing assumptions.** Safety depends on bounded network delay, bounded process pauses and bounded clock drift across the Redis nodes. A clock jump on one node (3.1) can make a key expire early, letting a second client get a majority.
- antirez's position: with monotonic time and sane operational limits, the assumptions are reasonable, and the pause issue affects every lease-based lock.

The practical takeaway most engineers accept: **Redlock is more machinery than you need for efficiency locks, and not strong enough for correctness locks.** For correctness, use a consensus system with fencing.

### Often the best lock is no lock

```
custom alias uniqueness for the URL shortener:
  BAD:   lock("alias:launch"); if not exists: insert; unlock
  GOOD:  one atomic conditional write in a linearizable store
         Postgres:  INSERT ... (unique constraint on alias)  -> error if taken
         DynamoDB:  PutItem with ConditionExpression attribute_not_exists(pk)
         etcd:      Txn(If(CreateRevision(key) == 0)).Then(Put(...))
```

Unique constraints, compare-and-set, conditional writes, and idempotency keys (2.6) give the same guarantee with no lease and no pause problem.

> 💡 **Interview tip:** Say "what is the lock protecting?" before choosing one. For correctness, prefer an atomic conditional write in the database you already have. If you truly need a lock across systems, use etcd/ZooKeeper and pass a fencing token to the resource.

> 🔗 **Connections:** Leases and fencing are 6.6. Locks for cache stampedes are 8.7. Coordination services are 6.8. Linearizability is the property behind conditional writes (4.2).

## 6.8 Coordination Services: ZooKeeper, etcd, Chubby

Instead of embedding consensus into every system, put it in one small, highly reliable service that stores **small, critical metadata**: who's the leader, which nodes are alive, which shard lives where, configuration. Typically 3 or 5 nodes (6.2).

| | ZooKeeper | etcd | Chubby |
| :---- | :---- | :---- | :---- |
| Origin | Yahoo, Apache (2008) | CoreOS, CNCF (2013) | Google (2006 paper) |
| Protocol | ZAB (Paxos-like) | Raft | Paxos |
| Data model | Tree of **znodes** (like files) | Flat key-value with **revisions**, range queries | Files and directories |
| Liveness primitive | **Ephemeral** znodes (deleted when the session dies) | **Leases** attached to keys | Sessions, lock files |
| Change notification | **Watches** (one-shot) | **Watch** streams from a revision | Events |
| Ordering primitive | **Sequential** znodes (`lock-0000000017`) | Monotonic revision per key/cluster | Lock sequencers (fencing) |
| Reads | From any server: **may be stale** (call `sync()` first if needed) | Linearizable by default (ReadIndex), or serializable (local) | Via the master, cached at clients |
| Famous users | Kafka (before KRaft), HBase, Hadoop, Solr | **Kubernetes** (all cluster state), Patroni, CoreDNS | Bigtable, GFS master election |

### Recipes

```
LEADER ELECTION (ZooKeeper)
  each candidate creates ephemeral+sequential  /election/n_00000017
  lowest number = leader
  others watch ONLY their predecessor (n_16 watches n_15) -> no herd effect
  leader dies -> its session expires -> znode deleted -> next one is notified

LEADER ELECTION (etcd)
  grant lease (TTL 10 s); Txn: if key "/leader" absent then put with lease
  leader keeps the lease alive; if it dies, key disappears; watchers retry
  the key's revision serves as a fencing token (6.6)

SERVICE DISCOVERY        each instance registers an ephemeral node / leased key
CONFIGURATION            clients watch /config/... and reload on change
SHARD MAP                which shard on which node (7.7 "directory service")
```

### Limits: don't use them as a database

- Everything is in memory / replicated to every node; every write goes through one leader with fsync.
- etcd: request size limit ~1.5 MiB, default storage quota 2 GiB (configurable, ~8 GiB suggested max). ZooKeeper: znode data ~1 MB max by default.
- Throughput: thousands to tens of thousands of writes/s, not millions.

Store the **pointer**, not the data: "shard 7 is on node-12" belongs in etcd; the URLs in shard 7 don't.

> 💡 **Interview tip:** When your design needs a single leader, a shard map or a lock, say "I'll keep that in a coordination service like etcd or ZooKeeper (Raft/ZAB, 5 nodes across 3 AZs)", and mention that it's on the control path, not the data path: redirects never touch it.

> 🔗 **Connections:** The algorithms underneath are 6.3–6.5. Ephemeral nodes and leases are failure detectors (6.9). Shard maps are request routing (7.7). Service discovery is 11.5. Kafka moved from ZooKeeper to KRaft (10.2).

## 6.9 Membership, Heartbeats, Failure Detectors and Gossip

Every distributed system needs to know **who is in the cluster and who is alive**. Load balancers need it (11.2), leaderless stores need it to pick replicas (5.7), Raft needs it to start elections (6.3).

### Heartbeats and timeouts

Each node sends "I'm alive" every interval *i*; a node is suspected dead after timeout *T* without one.

```
node A heartbeats:  |    |    |    |    |    .    .    .    .
                    0    1s   2s   3s   4s   (A dies at ~4.5 s)
monitor:                                     |<--- T = 5 s --->| suspect A
detection time ≈ T  (plus up to one interval)
```

| T | Detection | False positives |
| :---- | :---- | :---- |
| Short (1–2 s) | Fast | GC pauses, packet loss, load spikes mark healthy nodes dead → needless failovers |
| Long (30 s) | Slow: users see errors for 30 s | Rare |

You can't have both: a timeout can't distinguish crashed from slow (2.2, FLP 6.1). Failure detectors are described by **completeness** (every crashed node is eventually suspected) and **accuracy** (no live node is suspected). A perfect detector is impossible in an asynchronous network; practical ones are complete and *mostly* accurate.

### Phi accrual failure detector

Instead of a yes/no timeout, compute a **suspicion level φ** from the history of heartbeat inter-arrival times: φ = −log10(probability that a heartbeat this late would happen if the node were alive). φ = 1 → ~10% chance the node is alive and merely late; φ = 8 → ~10⁻⁸. Adapts automatically to noisy networks. Used by **Cassandra** (`phi_convict_threshold`, default 8) and **Akka**.

### Centralized vs decentralized membership

```
CENTRALIZED                            GOSSIP (decentralized)
  nodes --heartbeat--> etcd/ZK           every node periodically tells a few
  lease/ephemeral node expires           random peers what it knows
  = "dead"                               membership + state spreads like
  simple, consistent view                an epidemic
  coordination service must scale        no central point; eventually
  to N heartbeats                        consistent view
  e.g. Kubernetes node leases            e.g. Cassandra, Consul (Serf),
                                         Redis Cluster, DynamoDB internals
```

### Gossip

Every interval (e.g. 1 s), each node picks **k** random peers (fanout, e.g. 1–3) and exchanges its membership table: `{node, heartbeat counter, status, version}`. Each entry is merged by keeping the higher version. A rumor reaches everyone in **O(log N) rounds**:

```
round:     0    1    2    3    4    ...   ~log2(N) + a bit
informed:  1    2    4    8    16   ...   N

N = 1,000 nodes, fanout 1, 1 s rounds:
   log2(1000) ≈ 10 -> ~10-20 rounds -> everyone knows within ~10-20 s
per-node cost: k messages per round, regardless of N -> scales well
```

**SWIM** (used by HashiCorp memberlist in Consul/Nomad) improves on plain heartbeating: a node pings a random peer; if no reply, it asks **k other nodes to ping it indirectly** before suspecting it (so one bad link doesn't cause a false positive), then marks it "suspect" for a while (it can refute) before declaring it dead, and piggybacks membership updates on the pings.

### Membership changes aren't free

When a node is declared dead, data and traffic move: shards rebalance (7.6), consistent hashing reassigns its ranges (7.4), and if the node was just slow it now gets **more** load when it comes back. That's why most systems separate "temporarily down" (hinted handoff, 5.7) from "permanently removed" (an operator or a much longer timeout).

> 💡 **Interview tip:** If asked "how does the system know a node died?", answer with the trade-off: "Heartbeats with a timeout. Short timeouts detect faster but cause false failovers from GC pauses; I'd use a few seconds plus indirect probing like SWIM, or a phi-accrual detector, and keep permanent removal a separate, slower decision."

> 🔗 **Connections:** Health checks at the load balancer are the same idea (11.2, 12.6). Gossip is how leaderless stores share ring membership (5.7, 7.4). Rebalancing after a membership change is 7.6. Slow-vs-dead ambiguity is 2.2. Leases in coordination services are a centralized failure detector (6.6, 6.8).

---

# 7. Partitioning (Sharding)

Replication (Part 5) gives you more copies of the same data. Partitioning gives you **different data on different machines**, so that no single machine has to hold all of it or absorb all of its writes. Almost every large system does both, and most of the hard interview questions ("what's your shard key?", "what happens with a viral link?", "how do you add a node?") live in this part.

## 7.1 Why Partition

A **partition** (also called a **shard**, a **region** in HBase, a **tablet** in Bigtable, a **vnode** in Cassandra, a **range** in CockroachDB) is a subset of the data that lives on one node (plus its replicas). You partition when one machine runs out of one of three things:

| Limit | Symptom | Does replication help? |
| :---- | :---- | :---- |
| **Storage** | Dataset bigger than one machine's disk (or than RAM, if you need it hot). | No. Every replica holds everything. |
| **Write throughput** | Leader CPU, WAL fsyncs or disk bandwidth saturated. | No. Every write still goes to every replica. |
| **Read throughput** | Too many reads for one node. | Yes, read replicas help, but caching (Part 8) is usually cheaper. |

So replication scales **reads and availability**; partitioning scales **storage and writes**. Run the URL shortener numbers from 1.8 to see which one actually forces you to shard:

```
writes:   100M URLs/month / 2.5x10^6 s  =   40 writes/s avg,  ~120 peak
reads:    100 redirects/URL             = 4,000 reads/s avg, 12,000 peak
storage:  100M x 600 B = 60 GB/month  -> 720 GB/year -> 3.6 TB over 5 years
          x 3 replicas                 -> ~11 TB, before index/headroom overhead
```

120 writes/s is nothing; one Postgres leader handles that. 12,000 reads/s mostly hit cache. The reason to shard this system is **storage growth over years** (and keeping each shard small enough to back up, restore and move quickly), not write load. Saying that out loud in an interview shows you derive the design from the numbers.

Partitioning and replication combine like this: each partition has its own replica set, and the replicas of different partitions are spread so that one node failing hurts many partitions a little rather than one partition a lot.

```
           node 1            node 2            node 3
        +-----------+     +-----------+     +-----------+
        | P1 leader |     | P1 follow |     | P1 follow |
        | P2 follow |     | P2 leader |     | P2 follow |
        | P3 follow |     | P3 follow |     | P3 leader |
        +-----------+     +-----------+     +-----------+
   lose node 1 -> P1 elects a new leader (5.5); P2, P3 just lose a follower
```

The two questions every partitioning scheme must answer: **which partition owns key K?** (7.2–7.4) and **how does a request find that partition?** (7.7).

> 🔗 **Connections:** Partitions are the unit of replication (Part 5) and often the unit of consensus: Spanner, CockroachDB and TiKV run one Raft/Paxos group per partition (6.3–6.5). A single leader per partition is the "serial part" from Amdahl's Law (2.5) that partitioning multiplies away.

## 7.2 Range Partitioning

**Range partitioning** assigns each partition a contiguous range of keys, like volumes of an encyclopedia.

```
key space (sorted):   a ................................................ z

   +-------------+--------------+---------------+-----------------+
   | P1: a - f   |  P2: g - m   |  P3: n - s    |  P4: t - z      |
   +-------------+--------------+---------------+-----------------+
       node A          node B          node C          node D

   lookup "kq7"  -> P2 (g <= k < n)
   scan  "ma".."nb" -> touch P2 and P3 only
```

**Strengths:**
- **Range scans are cheap.** Keys are sorted within and across partitions, so "all events for user 42 between 10:00 and 11:00" reads one contiguous slice.
- Boundaries can **adapt to the data**: split a range when it grows too big, merge small neighbors. Bigtable, HBase, Spanner, CockroachDB and TiKV all do this automatically.

**Weakness: hot spots from monotonic keys.** If the key is a timestamp or a sequential ID, every new write lands in the **last** range. One node takes 100% of the write load while the others idle.

```
key = auto-increment id or created_at timestamp

   P1: old     P2: old     P3: old     P4: [ newest ... ] <- ALL writes here
```

For the URL shortener: if short codes come from a global counter (11.6) and you range-partition by code, all inserts hit one shard. Fixes:
- Partition by a **hash of the key** instead (7.3), giving up range scans you don't need anyway.
- **Prefix the key** with something well-distributed (e.g. `hash(user_id) % 16` + timestamp). Reads for a time range then fan out to 16 prefixes.
- Make codes non-sequential (e.g. shuffle/permute the counter value before base62 encoding).

> 💡 **Interview tip:** When you propose a range-partitioned key, the interviewer is waiting for "and new writes all go to the last partition." Say it first, then say how you avoid it.

> 🔗 **Connections:** Range partitioning is what the storage engines in 9.2 and 9.3 do *inside* one node (sorted keys). Spanner range-partitions and uses TrueTime (3.6) to make cross-range reads consistent. Range boundaries must be looked up somewhere: see the directory approach in 7.7.

## 7.3 Hash Partitioning

**Hash partitioning** runs the key through a hash function (MD5, Murmur3, xxHash, CRC16; it doesn't need to be cryptographic, just uniform) and assigns the partition from the hash. Similar keys (`abc1`, `abc2`) land in unrelated places, so load spreads evenly even for sequential keys.

The naive version is `partition = hash(key) mod N`. It works until you change N:

```
hash(key) mod 4 vs mod 5, for hash values 0..19:

  h:        0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19
  mod 4:    0 1 2 3 0 1 2 3 0 1  2  3  0  1  2  3  0  1  2  3
  mod 5:    0 1 2 3 4 0 1 2 3 4  0  1  2  3  4  0  1  2  3  4
  same?     Y Y Y Y . . . . . .  .  .  .  .  .  .  .  .  .  .

  only 4 of 20 keys stay put -> 80% of all data moves
```

In general, going from N to N+1 nodes keeps only ~1/(N+1) of keys in place: 10 → 11 nodes moves ~91% of your data. That is why nobody uses plain `mod N` for stored data. The fixes are consistent hashing (7.4) or a fixed number of partitions that is much larger than the node count (7.6).

**Cost: range scans are gone.** Adjacent keys are scattered, so "all keys from `a` to `c`" must ask every partition. Systems often split the key into two parts to get both:

| System | Hashed part (picks the partition) | Sorted part (order within the partition) |
| :---- | :---- | :---- |
| Cassandra | partition key | clustering columns |
| DynamoDB | partition key | sort key |
| URL shortener "clicks" table | `short_code` | `clicked_at` |

So "clicks on `abc` in the last hour" is one partition and a contiguous scan, while different codes spread across the cluster.

> 🔗 **Connections:** The same "hash then mod" problem appears in load balancers that hash clients to backends (11.2) and in cache clusters: rehashing a Memcached tier with `mod N` turns almost every key into a miss at once, a self-inflicted stampede (8.7). Consistent hashing (7.4) fixes both.

## 7.4 Consistent Hashing and Virtual Nodes (and rendezvous hashing)

### The ring

**Consistent hashing** maps both **nodes** and **keys** onto the same circular hash space (say 0 to 2^32−1, wrapping around). A key belongs to the **first node clockwise** from its position.

Positions below are shown as 0–100 for readability:

```
                         N1 (pos 0)
                  .---------*---------.        keys walk CLOCKWISE
       k4 (90) o-'                     '-o k1 (15)  -> N2
              /                           \
             /                             \
  N4 (75)   *                               *   N2 (25)
             \                             /
              \                           /
       k3 (60) o-.                     .-o k2 (35)  -> N3
                  '---------*---------'
                         N3 (pos 50)

   k1 (15) -> N2      k2 (35) -> N3      k3 (60) -> N4
   k4 (90) -> wraps past 100/0 -> N1
```

Each node owns the arc **ending** at its position: N2 owns (0, 25], N3 owns (25, 50], and so on.

### Adding and removing a node

Add N5 at position 40. Only keys in (25, 40] change owner, and they all come from **one** node, N3:

```
before:   ...N2(25) ---------------------------- N3(50)...
                    (25 ......... 40 ........ 50]  all owned by N3

after:    ...N2(25) ------------- N5(40) ------- N3(50)...
                    (25 .. 40] -> N5   (40 .. 50] -> N3

   k2 (35): N3 -> N5    (moves)
   k1, k3, k4: unchanged
```

On average, adding the (N+1)-th node moves ~1/(N+1) of the keys, the minimum possible (compare 80% for `mod N` in 7.3). Removing a node hands its arc to its clockwise neighbor.

### Problems with one point per node

1. **Uneven arcs.** With 4 random positions, one node can easily own 40% of the ring and another 10%.
2. **Lopsided handoff.** When N3 dies, N4 alone inherits all of N3's load, and may fall over next (a cascading failure, 12.3).
3. **Heterogeneous hardware.** A machine with twice the disk should own twice the data.

### Virtual nodes

The fix: each physical node gets many positions (**virtual nodes** or **tokens**) on the ring.

```
physical nodes A, B, C, each with 4 vnodes (real systems use 16-256):

   ring:  A1 B1 C1 A2 C2 B2 A3 C3 B3 A4 B4 C4  (positions scattered randomly)

   C dies -> C1's arc goes to A2, C2's to B2, C3's to B3, C4's to A1(wrap)
          -> the load spreads over A and B, not onto one neighbor
```

- More vnodes = smoother balance (the variance of each node's share shrinks roughly like 1/sqrt(vnodes)), but more metadata and more, smaller ranges to repair.
- Give a bigger machine more vnodes to give it more data.
- Cassandra historically defaulted to 256 tokens per node; 4.0 lowered the default to 16 with a smarter token allocator. Dynamo (the 2007 paper) introduced the idea for production key-value stores.

### Replication on the ring

To store N=3 copies, walk clockwise from the key and take the first 3 **distinct physical nodes** (skipping vnodes of a node already chosen). Dynamo calls this list the **preference list**. Rack/AZ-aware placement also skips nodes in a failure domain already used, so the 3 copies land in 3 AZs (12.2).

### Rendezvous hashing (highest random weight)

**Rendezvous hashing** skips the ring. For each key, score every node with `hash(key, node)` and pick the highest score.

```
key "abc":   hash("abc","A") = 71   hash("abc","B") = 93   hash("abc","C") = 12
             -> B wins.  (for 2 replicas, take the top 2: B, A)

node B removed -> "abc" now goes to A (next highest). Keys whose winner was
                  A or C never change: their top score didn't involve B.
```

- Same minimal-movement property as the ring, perfectly even distribution in expectation, and no vnodes to tune.
- Cost: O(N) hashes per lookup. Fine for tens or hundreds of nodes (cache clusters, CDN edge selection, picking a shard among a few dozen); a ring with binary search is O(log V) for huge clusters.
- **Jump consistent hash** (Google, 2014) is another option: O(log N), no memory, but it only supports adding/removing nodes at the end of a numbered list, so it suits numbered shards rather than arbitrary servers.

> 💡 **Interview tip:** Don't stop at "use consistent hashing." Say what it buys (adding a node moves ~1/N of keys, from neighbors only), what it costs (uneven arcs), and the fix (vnodes). That's the full answer.

> 🔗 **Connections:** The data-structure view is recapped in 13.5. Load balancers use consistent hashing for session affinity and cache locality (11.2), Envoy's ring-hash and Maglev load balancing being examples. Membership (who is on the ring) comes from gossip in Cassandra/Dynamo (6.9) or from a coordination service (6.8).

## 7.5 Hot Keys and Skew

Hashing spreads **different keys** evenly. It does nothing for **one key** that is extremely popular. **Skew** means some partitions get far more load than others; a **hot key** is the extreme case.

The URL shortener version: a link is posted by a celebrity and gets 200,000 clicks/s. All 200,000 go to the one shard (and the one cache node) that owns that code.

```
normal:        shard1 ~3k   shard2 ~3k   shard3 ~3k   shard4 ~3k   QPS
viral link:    shard1 ~3k   shard2 203k  shard3 ~3k   shard4 ~3k
                                 ^ one key; adding shards does not help
```

Sources of skew:
- **Popularity:** viral links, celebrity accounts, a top product.
- **A bad partition key:** partitioning by `country` (one country dominates), by date (today is hot), by `customer_id` when one customer is 30% of traffic.
- **Hash collisions on few partitions** when the partition count is too small.

### Fixes for read-hot keys

1. **Cache it** (Part 8). A redirect for a hot code is immutable or rarely changes, so caching is the natural fix.
2. **Cache it closer.** A single Redis node handles very roughly ~100k simple ops/s; a viral key can exceed one node. Put a small **in-process cache** (8.1) in each app server with a short TTL: 50 app servers each serving the hot key from memory is 50× the capacity of one Redis node. Or let the **CDN** (11.4) serve it.
3. **Replicate the hot key across cache nodes:** store copies as `abc#0` … `abc#7` on different nodes and read a random one. Invalidation now has to delete all 8 copies (8.5).

### Fixes for write-hot keys

Counters are the classic case (e.g. a click counter per link).

```
one hot counter:        clicks:abc  += 1    (200k increments/s on one key)

split into 16:          clicks:abc:0 ... clicks:abc:15
   write:  INCR clicks:abc:<random 0..15>
   read:   sum of the 16 sub-counters      (reads get 16x more expensive)
```

Or batch: each app server accumulates counts locally for 1 s and writes one increment per second, turning 200k writes/s into ~50 writes/s (one per server). Exactness is traded for throughput, which is fine for analytics.

### Detecting hot keys

You can't keep an exact counter per key at 12,000 QPS across billions of keys cheaply. Use a **Count-Min Sketch** plus a small top-k heap (13.2), sampled per server, and promote detected keys to the in-process cache automatically.

> 🔗 **Connections:** Hot keys are what make cache stampedes dangerous (8.7): the hotter the key, the more requests pile up when it expires. A hot partition is a serial bottleneck in the Amdahl sense (2.5). Kafka has the same problem with a hot partition key (10.4).

## 7.6 Rebalancing

**Rebalancing** moves partitions between nodes when you add capacity, lose a node, or fix skew. Requirements: load should end up even, the system keeps serving during the move, and you move no more data than necessary.

### Strategies

| Strategy | How it works | Examples |
| :---- | :---- | :---- |
| `hash mod N` | Recompute everything. | Don't (7.3). |
| **Fixed number of partitions** | Create many more partitions than nodes up front (e.g. 1,024 for 10 nodes). Adding a node steals whole partitions from others; the key→partition map never changes. | Redis Cluster (16,384 hash slots, `CRC16(key) mod 16384`), Couchbase vBuckets, Elasticsearch shards (fixed at index creation) |
| **Dynamic splitting** | Range partitions split when they exceed a size (or load) threshold and merge when small. | HBase, Bigtable, MongoDB chunks, CockroachDB, Spanner |
| **Proportional to nodes** | Each node owns a fixed number of vnodes; a new node splits random existing ranges. | Cassandra (7.4) |

The fixed-partition trick is worth understanding because it's simple and common:

```
1,024 partitions, 4 nodes:  each node holds 256 partitions

add node 5:  each old node gives ~51 partitions to node 5
             node 5 ends with ~204; nobody else moved anything
             key -> partition (hash mod 1024) never changes
             partition -> node is a small table (1,024 entries)
```

Choose the count carefully: too few and you can't spread past that many nodes; too many and per-partition overhead (files, Raft groups, metadata) dominates.

### How a partition actually moves

```
1. snapshot copy    stream the partition's data to the new node (throttled)
2. catch up         replay changes made since the snapshot (WAL/log, 9.4)
3. cut over         briefly block writes, apply the last changes,
                    update the routing map (7.7), unblock
4. clean up         old node deletes its copy after clients see the new map
```

Moving data is slow, so do the math:

```
3.6 TB logical / 10 shards = 360 GB per shard
throttled to 100 MB/s so live traffic isn't hurt:
   360 GB / 100 MB/s = 3,600 s = 1 hour per shard move (per replica)
```

### Automatic vs manual

Fully automatic rebalancing sounds nice but is dangerous combined with automatic failure detection: a node is slow because it's overloaded, the detector declares it dead, the system starts moving its partitions, the copying adds more load to everyone, more nodes look dead, and so on. Many systems (Couchbase, Riak in the past, many in-house systems) have an automatic *proposal* with a human or a rate limiter approving the move.

> 💡 **Interview tip:** "We'd pre-split into many more partitions than nodes, say 1,024, so adding a server moves whole partitions and the key mapping never changes." This one sentence answers most "how do you add capacity?" follow-ups.

> 🔗 **Connections:** The copy + catch-up + cut-over steps are the same as adding a new replica (5.2) and as an online schema migration. Throttling rebalancing traffic is a form of load management (12.5). Failure detection that triggers rebalancing is 6.9.

## 7.7 Request Routing (client-aware, proxy, directory service)

Once data is partitioned, a request for key `abc` must reach the node that owns it. There are three places to put that knowledge:

```
(a) any node forwards        (b) routing tier             (c) client-aware
                                 (proxy)

  client                       client                       client
    |                            |                     (has partition map)
    v                            v                            |
 [node 1]--"not mine"-->       [router]                       |
    |                         /   |   \                       |
    v                        v    v    v                      v
 [node 3] owns abc       [n1]  [n2]  [n3]                  [n3] direct

  Cassandra coordinator   Vitess VTGate, mongos,       Redis Cluster clients,
  (any node can route)    twemproxy, Envoy             Cassandra token-aware
                                                       drivers, Kafka clients
```

| Approach | Pros | Cons |
| :---- | :---- | :---- |
| **Forward from any node** | Client stays simple. | Extra hop when the first node isn't the owner. |
| **Routing tier / proxy** | Clients know nothing; easy to change partitioning; central place for metrics. | Extra hop and another tier to scale and keep available. |
| **Client-aware** | No extra hop, lowest latency. | Smart client libraries in every language; clients must refresh stale maps. |

All three still need a source of truth for **the partition map** (which partition lives where, which replica is leader). Options:

- **A coordination service** (6.8): ZooKeeper or etcd holds the map; routers watch it for changes. HBase, Kafka (before KRaft), Vitess (topology server) and many in-house systems do this.
- **A metadata table** stored in the system itself: HBase's `hbase:meta`, Bigtable's METADATA tablets. Clients cache entries.
- **Gossip** (6.9): every Cassandra node learns ring ownership by gossip, so any node can route.

### Stale maps are normal

Clients cache the map and it goes stale after a rebalance. The system must tell them. Redis Cluster does it with redirects:

```
client                      node A                       node B
  | GET abc (slot 7638) ----->|                             |
  |<-- -MOVED 7638 nodeB:6379-|   (slot moved to B)         |
  | update local slot map     |                             |
  | GET abc ------------------------------------------------>|
  |<---------------------------------------------- "https://..."|
```

(During a migration in progress, Redis returns `-ASK` instead, meaning "try B for this one request, but don't update your map yet.")

> 🔗 **Connections:** A proxy routing tier is the same component as the reverse proxy / API gateway in 11.5, and its scaling is a load-balancing problem (11.2). The partition map is a small, critical, strongly consistent piece of state, which is exactly what etcd/ZooKeeper are for (6.8).

## 7.8 Secondary Indexes: Local vs Global

The primary key decides the partition. A **secondary index** lets you look up by something else. For the URL shortener: the primary key is `short_code`, but the "My links" page needs **all links owned by user 42**. User 42's links are scattered across every shard.

### Local index (document-partitioned)

Each shard indexes only its own rows.

```
   shard 1                   shard 2                   shard 3
 rows: abc(u42) k9x(u7)    rows: q1z(u42) m3p(u9)    rows: zz8(u7)
 index u42 -> [abc]        index u42 -> [q1z]        index u42 -> []
       u7  -> [k9x]              u9  -> [m3p]              u7  -> [zz8]

 query "links of u42": ask ALL shards (scatter), merge results (gather)
```

- **Writes are local and atomic:** the row and its index entry live on the same shard, so one local transaction updates both.
- **Reads scatter-gather** to every shard. With 50 shards, each request waits for the slowest of 50, and tail latency compounds (2.3): if each shard is slow 1% of the time, `1 − 0.99^50 ≈ 39.5%` of queries hit at least one slow shard.
- Used by: Elasticsearch, MongoDB, Cassandra secondary indexes, DynamoDB LSIs.

### Global index (term-partitioned)

The index itself is partitioned by the **indexed value**, independently of the data.

```
  data shards (by short_code)          index shards (by user_id)
  +----------------------+             +-------------------------+
  | abc(u42)  k9x(u7)    | --async-->  | idx-A: u7  -> [k9x,zz8] |
  | q1z(u42)  m3p(u9)    | --async-->  | idx-B: u42 -> [abc,q1z] |
  | zz8(u7)              |             |        u9  -> [m3p]     |
  +----------------------+             +-------------------------+

  query "links of u42": read ONE index shard (idx-B), then fetch rows
```

- **Reads hit one index shard.**
- **A write touches two shards** (the data and the index entry), which needs a distributed transaction (7.9, 9.7) to be atomic. Most systems update global indexes **asynchronously**, so the index is eventually consistent: a link you just created may be missing from "My links" for a moment (a read-your-writes problem, 4.4). DynamoDB GSIs work this way.

### The pragmatic option: a second table

Many designs just maintain a separate table keyed by the other attribute, `links_by_user(user_id, created_at, short_code)`, written alongside the main one (via outbox/CDC, 10.6). It's a global index you manage yourself.

> 💡 **Interview tip:** If an interviewer asks "how do you show a user their links?", name the trade-off: local index = cheap writes, scatter-gather reads; global index = one-shard reads, cross-shard (usually async) writes. For a dashboard read occasionally, either works; say which and why.

> 🔗 **Connections:** Scatter-gather is the fan-out problem from 2.3 (hedged requests help). Async index maintenance is CQRS in miniature (10.7). The eventual consistency of global indexes is a session-guarantee question (4.4).

## 7.9 Cross-Shard Queries and Transactions

Everything that touches one partition is easy: one node, one local transaction (9.5). Everything that touches several is hard.

### Cross-shard queries

| Query type | What happens | Cost |
| :---- | :---- | :---- |
| Point lookup by partition key | One shard | Cheap |
| Range scan within a partition | One shard | Cheap |
| Query by non-key attribute | Scatter-gather (7.8) | Tail latency × shard count |
| Aggregation (`COUNT`, top-k) | Each shard computes partial result, coordinator merges | OK if partials are small |
| Join across shards | Ship rows between shards or to a coordinator | Expensive; avoid in the hot path |

The main defense is **choosing the partition key so that things accessed together live together**. If most queries are "everything about user X", partition by `user_id` and a user's links, clicks and settings co-locate. For a redirect service, the hot query is "code → destination", so partition by `short_code` and accept that per-user views scatter or use a second table.

Analytics queries ("clicks per country per day across all links") shouldn't run on the serving shards at all: stream the data to a warehouse or batch system (10.8).

### Cross-shard transactions

When one logical operation must change rows on two shards atomically, you have three options:

```
1. Avoid it            redesign so the operation hits one shard
                       (co-locate, or make one side eventually consistent)

2. Atomic commit       two-phase commit across shards (9.7)
                       Spanner / CockroachDB: each shard is a Raft/Paxos
                       group, so the 2PC participants don't "die" (6.3-6.5)

3. Sagas               sequence of local transactions + compensations (9.8)
                       no isolation, but no blocking either
```

Option 1 is the default answer. In the URL shortener, creating a link is a single-row insert on one shard; the only cross-shard need is "unique custom alias" (handled by making the alias the primary key, so uniqueness is a single-shard check) and "user's link list" (async, 7.8).

> 💡 **Interview tip:** "We pick the partition key so the common operations are single-shard; cross-shard work is either async or rare." Interviewers want to hear that you avoid distributed transactions by design, not that you know how to implement 2PC.

> 🔗 **Connections:** 2PC and why it blocks: 9.7. Sagas: 9.8. Spanner's use of TrueTime to give consistent cross-shard snapshots: 3.6. The full shortener data-layer design: 14.5.

---

# 8. Caching

A cache keeps a copy of data closer to where it's used, trading **freshness** for **speed and load reduction**. For a read-heavy system like a URL shortener (100 reads per write), caching is the single biggest lever on cost and latency, and **invalidation** (8.5) is where interviews dig in: every copy you create is a copy you may have to correct.

## 8.1 Where Caches Live (browser → CDN → proxy → in-process → distributed cache → DB buffer pool)

A redirect for `sho.rt/abc` can be answered at any of these layers. The closer to the user, the faster, and the **harder to invalidate**.

```
  user
   |
   v
+--------------------+  ~0 ms         you CANNOT invalidate it (8.9)
| browser cache      |  HTTP cache, keyed by URL; obeys Cache-Control
+---------+----------+
          | miss
          v
+--------------------+  ~5-30 ms      purge API; seconds to minutes
| CDN edge (11.4)    |  shared HTTP cache in 100s of PoPs
+---------+----------+
          | miss
          v
+--------------------+  ~0.1-1 ms     you control it; often skipped
| reverse proxy (LB) |  nginx/Varnish/Envoy response cache
+---------+----------+
          | miss
          v
+--------------------+  ~100 ns       one copy PER SERVER (50 servers =
| in-process cache   |  hash map in   50 copies to invalidate)
| (app memory)       |  the app: Caffeine, Guava, a Go LRU
+---------+----------+
          | miss
          v
+--------------------+  ~0.5-1 ms     one logical copy per region;
| distributed cache  |  (same-DC RTT) the usual place to invalidate
| Redis / Memcached  |
+---------+----------+
          | miss
          v
+--------------------+  ~1-10 ms      managed by the DB itself;
| database           |  buffer pool   always consistent with the DB
| (Postgres, MySQL)  |  + OS page cache, then disk
+--------------------+
```

Rules of thumb:
- Each layer **absorbs load** for the layers below. A CDN hit costs your servers nothing.
- Each layer is **another copy**. An edit must reach every copy, or be allowed to expire.
- The **DB buffer pool** (Postgres `shared_buffers`, InnoDB buffer pool) is the one cache you never invalidate by hand, because the DB itself owns both the cache and the data.
- Latencies above are approximate; the cross-layer ratios matter more than exact values.

> 🔗 **Connections:** This is the memory hierarchy (registers → L1 → RAM → disk) one level up: small and fast near the consumer, big and slow at the source. The latency numbers come from 1.1. CDNs are covered in 11.4 and HTTP caching rules in 8.9.

## 8.2 Cache Hit Ratio Math

**Hit ratio** `h` = hits / (hits + misses). Two formulas matter.

**Average latency:**

```
avg = h x t_hit + (1 - h) x t_miss

t_hit  = 0.5 ms   (Redis GET, same DC)
t_miss = 0.5 ms + 5 ms = 5.5 ms   (Redis miss, then DB read)

h = 0.90:  0.90 x 0.5 + 0.10 x 5.5 = 0.45 + 0.55  = 1.00 ms
h = 0.99:  0.99 x 0.5 + 0.01 x 5.5 = 0.495 + 0.055 = 0.55 ms
```

**Load on the database** (usually the number that matters more):

```
DB QPS = (1 - h) x total QPS            peak = 12,000 QPS (1.8)

h = 0.90   ->  0.10  x 12,000 = 1,200 QPS to the DB
h = 0.99   ->  0.01  x 12,000 =   120 QPS
h = 0.999  ->  0.001 x 12,000 =    12 QPS
```

Going from 90% to 99% looks like "9% better" but it cuts DB load **10×**. Think in **miss ratio**, not hit ratio.

### Sizing the cache

Access patterns are usually heavily skewed (roughly Zipf-like): a small fraction of links gets most clicks, so a cache holding a small fraction of keys gets a high hit ratio. For a shortener, a reasonable first estimate is "links created in the last month, plus the evergreen popular ones":

```
100M new links/month x ~600 B          = ~60 GB of raw data
x ~1.5 for per-key overhead in Redis   = ~90 GB
-> e.g. 3 cache shards x 32-64 GB RAM, x replicas per region
```

The `1.5×` overhead is a rough figure; measure it with real keys.

### The cache becomes load-bearing

If the DB is sized for 120 QPS because the cache hits 99%, then **losing the cache** (restart, failover, a bad deploy that changes key names) sends 12,000 QPS to a DB built for 120. The cache has stopped being an optimization and become part of your capacity. Plan for warm-up (gradually shift traffic, pre-load hot keys) and protect the DB with load shedding (12.5).

> 💡 **Interview tip:** Quote the DB load at your hit ratio, then say what happens when the cache is cold. "At 99% the DB sees 120 QPS, but a cold cache means 12,000 QPS, so we warm it gradually and the DB has to survive at least partial cache loss."

> 🔗 **Connections:** Cache loss turning into DB overload is a classic cascading failure (12.3). Cold-start stampedes are 8.7. Hit ratio is one of the first metrics to dashboard (12.9).

## 8.3 Read Patterns: Cache-Aside and Read-Through

### Cache-aside (lazy loading)

The **application** talks to both the cache and the DB. The cache knows nothing about the DB.

```
app                          cache (Redis)                 DB
 | GET url:abc --------------->|                            |
 |<-------------- nil (miss) --|                            |
 | SELECT dest WHERE code='abc' --------------------------->|
 |<----------------------------------------- "https://x" ---|
 | SET url:abc "https://x" EX 86400 ->|                     |
 | return 302 Location: https://x     |                     |
```

```python
def resolve(code):
    dest = cache.get(f"url:{code}")
    if dest is None:                          # miss
        dest = db.query("SELECT dest FROM urls WHERE code=%s", code)
        if dest is not None:
            cache.set(f"url:{code}", dest, ex=86400)   # TTL = safety net
    return dest
```

- **Pros:** Only data that's actually read gets cached. If the cache is down, the app still works (slower). Works with any cache and any DB. This is the default pattern (Redis, Memcached).
- **Cons:** First read of every key is a miss. The read-then-set sequence is **racy** against writes, the source of the stale-cache bug in 8.5. Each app must implement the logic correctly.

### Read-through

The **cache** loads from the DB on a miss. The app only ever talks to the cache.

```
app ---- get("abc") ----> [ cache + loader ] --(miss)--> DB
                                 |
                          fills itself, returns value
```

- Examples: Caffeine/Guava `LoadingCache` (in-process), DynamoDB Accelerator (DAX) in front of DynamoDB, a CDN pulling from origin.
- **Pros:** Loader logic lives in one place. The cache can easily **coalesce** concurrent misses for the same key into one DB load (8.7), which Caffeine's `LoadingCache` does per key.
- **Cons:** The cache must know how to read your data source; fewer off-the-shelf options for distributed caches.

| | Cache-aside | Read-through |
| :---- | :---- | :---- |
| Who loads on miss | Application | Cache |
| Cache down | App falls back to DB | Depends; often an outage |
| Miss coalescing | You build it | Often built in |
| Typical | Redis/Memcached + app code | LoadingCache, DAX, CDN |

> 🔗 **Connections:** Both are read patterns; they say nothing about what happens on **writes**, which is 8.4 and 8.5. A CDN is a read-through cache with the origin as its loader (11.4).

## 8.4 Write Patterns: Write-Through, Write-Back, Write-Around

When data changes, what does the cache do?

```
write-through               write-back (write-behind)     write-around
                     
app -> cache -> DB          app -> cache                  app -> DB
       (sync, both)                 |  (ack now)          (cache untouched;
                                    v  later, async        filled on next read,
                                   DB                      or invalidated: 8.5)
```

| Pattern | Write path | Pros | Cons | Use when |
| :---- | :---- | :---- | :---- | :---- |
| **Write-through** | Write cache and DB synchronously before acking. | Cache always has fresh data for keys you write. | Slower writes; caches data that may never be read; still racy if two writers interleave. | Data is read soon after being written. |
| **Write-back** | Write cache, ack, flush to DB later. | Very fast writes; absorbs bursts; can coalesce many writes into one. | **Data loss** if the cache dies before flushing; DB is behind. | Counters, metrics, CPU caches, OS page cache; rarely for primary data. |
| **Write-around** | Write DB only; cache is filled on later reads. | Doesn't pollute the cache with unread writes. | First read after a write is a miss; existing cache entries must be invalidated (8.5). | Writes that are rarely read back soon. |

For the URL shortener:
- **Creating a link:** write-around is fine (the code is new, so no stale entry exists, assuming no negative cache entry, 8.8). Some designs write-through on create because the creator often clicks the link immediately to test it; either is defensible.
- **Click counters:** write-back. Aggregate in memory or Redis, flush to the DB every few seconds. Losing a few seconds of counts on a crash is acceptable; losing a destination URL is not.
- **Editing a destination:** write to the DB, then **invalidate** (delete) the cache entry. Next section.

> 🔗 **Connections:** Write-back vs write-through is exactly the CPU-cache and page-cache choice from the Linux notes, and `fsync` is the "flush now" for the page cache, the same role the WAL plays in 9.4. Write-back counters are the "batch per server" trick from 7.5.

## 8.5 Cache Invalidation (TTL, delete-on-write, the write/fill race, versioned keys, pub/sub/CDC invalidation across regions)

This is the interview question: **"A user changes the destination of `sho.rt/abc` from A to B. How do you make sure people get B?"**

Start by listing every place A might be cached. That list *is* the answer's structure.

```
                  user edits abc: A -> B
                           |
                           v
              +---------------------------+
              | DB leader (home region)   |  B   <- source of truth
              +-------------+-------------+
                            | async replication (5.3)
           +----------------+----------------+
           v                                 v
  US: DB replica  (B soon)          EU: DB replica  (A until lag passes)
  US: Redis       (A!)              EU: Redis       (A!)
  US: 50 app in-process caches (A!) EU: 50 app in-process caches (A!)
                CDN edges, ~hundreds of PoPs (A!)
                users' browsers, if you sent a 301 (A!, unreachable)
```

There are five tools. Real systems combine them.

### Tool 1: TTL (expire it eventually)

Every cache entry gets a **time-to-live**. After the TTL, the entry disappears and the next read fetches fresh data.

| TTL | Max staleness | Hit ratio | DB load |
| :---- | :---- | :---- | :---- |
| 10 s | 10 s | Lower | Higher |
| 1 h | 1 h | High | Low |
| 24 h | 24 h | Higher | Lower |
| none | forever | Highest | Lowest |

TTL alone is an honest design if the product tolerates the staleness ("edits take up to 5 minutes to propagate"). Even when you invalidate explicitly, **always set a TTL** as the safety net for any invalidation that gets lost: bugs, dropped messages, the race below.

### Tool 2: delete on write (not update on write)

When the DB changes, **delete** the cache key. The next read misses and loads the new value. Two ordering rules matter.

**Rule A: delete, don't set.** If writers set the new value into the cache, two concurrent writers can apply in one order to the DB and the opposite order to the cache:

```
time  Writer W1 (dest=B)        Writer W2 (dest=C)        DB    Cache
t1    UPDATE dest=B                                       B     A
t2                              UPDATE dest=C             C     A
t3                              SET abc=C                 C     C
t4    SET abc=B  (W1 was slow)                            C     B  <- wrong
```

Deletes don't have this problem: deleting twice is the same as deleting once (idempotent, 2.6), and nothing wrong gets written.

**Rule B: write the DB first, then delete.** If you delete first, a reader can refill the old value before your DB write lands:

```
time  Writer W                  Reader R                  DB    Cache
t1    DEL abc                                             A     -
t2                              GET abc -> MISS           A     -
t3                              SELECT -> A               A     -
t4                              SET abc=A                 A     A
t5    UPDATE dest=B                                       B     A  <- stale
                                                                 until TTL
```

So: **UPDATE the DB, then DEL the cache key.** This is the standard cache-aside write path. But it still has a race.

### The write/fill race (DB-then-delete still goes wrong)

Cache-aside reads are two separate steps: read the DB, then fill the cache. If a write and its delete happen **between** those two steps, the reader puts the old value back after the delete already ran:

```
time  Reader R (a click)        Writer W (edit A -> B)    DB    Cache
----  ------------------------  ------------------------  ----  ---------
t1    GET abc -> MISS                                     A     -
t2    SELECT dest -> A                                    A     -
      ...R stalls: GC pause,
         slow network, CPU
         contention...
t3                              UPDATE dest=B; COMMIT     B     -
t4                              DEL abc  (nothing there)  B     -
t5    SET abc=A  EX 86400                                 B     A  <- stale
t6    every later click: HIT A                            B     A  for 24h
```

Why it's easy to dismiss and why it still matters:
- It needs a **miss** on that key, and the writer's UPDATE+DEL must fit inside the reader's SELECT→SET gap. Rare per event, but at millions of requests per day, rare things happen daily.
- When it does happen, the damage lasts **the full TTL**, because nothing will delete it again.
- It's much more likely with **replication lag** (next subsection), where the "gap" is not a GC pause but the replica being seconds behind.

Fixes, from simplest to strongest:

1. **Short TTL.** Bounds the damage. Doesn't prevent it.
2. **Delayed double delete.** Delete right after the write, and again after a delay longer than a typical read-and-fill (e.g. 1 s, via a delayed job). Catches most fills that raced the first delete. Cheap; probabilistic, not a guarantee.
3. **Leases** (Facebook's memcache design, described in their 2013 NSDI paper "Scaling Memcache at Facebook"). On a miss, the cache gives the reader a **lease token**. A delete of that key invalidates the token. A `SET` with an invalid token is rejected:

```
time  Reader R                       Writer W                  Cache
t1    GET abc -> MISS, lease L1                                -, L1 valid
t2    SELECT -> A
t3                                   UPDATE B; COMMIT
t4                                   DEL abc                   -, L1 VOID
t5    SET abc=A with L1 -> REJECTED                            -
t6    next reader: MISS, lease L2, SELECT -> B, SET OK         B
```

   Leases also throttle stampedes: the cache hands out one lease per key at a time, and other readers wait briefly or get a slightly stale value (8.7). Stock Redis doesn't have leases, but you can build the same thing with a Lua script.

4. **Versioned values with a tombstone.** Store a version with every value (a row version or `updated_at` from the DB). The writer, instead of plain `DEL`, writes a short-lived **tombstone** `abc = {deleted, v=8}`. Fills use a compare-and-set (a Lua script in Redis): "write only if the key is absent or holds a lower version." The racing reader carries `v=7`, sees the `v=8` tombstone, and its stale fill is refused.

5. **Invalidate from the database's change stream (CDC)**, described below. This is the strongest fix in practice because the invalidation is triggered by the commit itself and can be ordered after replication.

### Replication lag makes the race likely

With read replicas, readers fill the cache from a **replica**, which can be behind the leader. Now the race doesn't need an unlucky GC pause; it happens almost every time someone clicks during the lag window:

```
time  Writer (edit in US)          EU replica    EU Redis   EU reader
t0                                 A             A
t1    UPDATE B on leader (US)      A             A
t2    send "DEL abc" to EU Redis   A             -
      (pub/sub, fast: ~100 ms)
t3                                 A             -          GET -> MISS
t4                                 A             -          SELECT -> A
t5                                 A             A          SET abc=A
t6    replication arrives (~1 s)   B             A  <- stale until TTL
```

The invalidation **outran the data**. Any invalidation sent directly from the writer to a remote region has this problem.

### Tool 3: CDC-driven invalidation, per region

**Change data capture** (10.6) means tailing the database's own replication log (MySQL binlog, Postgres WAL via logical decoding; Debezium is a common tool). If each region runs an invalidator that tails **its local replica's** log, the delete happens only after the local replica already has B:

```
                  US (home region)                    EU
             +----------------------+      +----------------------+
 edit A->B   | DB leader            |      | DB replica           |
 ----------->| commit B, log entry  |=====>| applies B            |
             +----------+-----------+ repl +----------+-----------+
                        | tail log (CDC)              | tail log (CDC)
                        v                             v
             +----------------------+      +----------------------+
             | invalidator          |      | invalidator          |
             |  DEL url:abc (Redis) |      |  DEL url:abc (Redis) |
             |  publish to app      |      |  publish to app      |
             |  servers (in-proc)   |      |  servers (in-proc)   |
             |  CDN purge /abc      |      |                      |
             +----------------------+      +----------------------+

  EU delete happens AFTER the EU replica has B, so a refill reads B.
```

Why this is the strongest option:
- **Ordering:** invalidation is applied after the data it's invalidating, in commit order.
- **Completeness:** every write is caught, including writes from scripts, admin tools and other services that forgot to call `cache.delete()`.
- **Retries:** the log is durable. If the invalidator crashes, it resumes from its saved log position; no invalidation is lost (it may be repeated, which is fine because `DEL` is idempotent).

Facebook's memcache paper describes exactly this: daemons (called mcsqueal) tail the MySQL commit log in each region and issue deletes to that region's memcache.

The remaining race (a reader in the same region doing SELECT before the replica applies B and SET after the invalidator's DEL) is now small, because the DEL waits for the local replica. Close it fully with leases or versions (fixes 3 and 4 above) if your product needs it.

### Tool 4: pub/sub to in-process caches

In-process caches (8.1) exist on every app server, so a DEL in Redis doesn't touch them. Options:
- **Broadcast** invalidations to all servers: Redis pub/sub (fire-and-forget: a server that's disconnected at that moment misses the message) or a Kafka topic (durable, each server resumes from its offset).
- Or just give in-process entries a **very short TTL** (5–30 s) and accept that much staleness. For hot-key protection (7.5) this is often enough.

### Tool 5: versioned keys

Put the version **in the key** instead of the value: `url:abc:v7`. A new version is a new key, so old entries never need deleting; they age out by TTL or LRU.

This only works if the reader already knows the current version. That's true for:
- **Static assets:** `app.3f9a1c.js` with the content hash in the name, cached forever on CDNs and browsers; deploying means referencing a new name ("cache busting").
- **Objects referenced with a version**, e.g. a config blob whose version comes from etcd (6.8).

For a shortener, the click only carries `abc`, so you'd need another lookup `abc → v7`, which itself must be cached and invalidated. Versioned keys help less here than tools 2–4.

### Putting it together: the answer for "user edits a destination"

```
1. Ask: are links editable at all?  If not (most shorteners), entries are
   immutable: cache with long TTLs, and invalidation is only needed for
   deletes and abuse takedowns (which are still mandatory!).

2. Write path:  UPDATE urls SET dest=B, version=version+1 WHERE code='abc'
                on the leader.  Commit.

3. Same region: DEL url:abc in Redis right away (fast path).

4. Every region: a CDC invalidator tails the LOCAL replica's log and, after
   the replica applies the change:
     - DEL url:abc in that region's Redis
     - broadcast to that region's app servers -> drop in-process entry

5. CDN:   purge /abc via the CDN's API (or keep a short s-maxage, 8.9).

6. Browser: serve 302 (or 301 with a short max-age), never a cacheable-
   forever 301, because browser caches can't be invalidated (8.9).

7. Backstop: every cache layer has a TTL (e.g. Redis 24 h, in-process
   30 s, CDN 5 min), so anything missed above still heals.

Staleness bound: replication lag + invalidator delay, typically under a
few seconds; worst case = the TTL of the layer that missed it.
```

> 💡 **Interview tip:** Structure the answer as "where are the copies, how does each one learn about the change, and what's the bound if it doesn't." Mention the write/fill race by name and one fix (leases or versions); that's what separates a strong answer from "just delete the key."

> 🔗 **Connections:** Replication lag and the "created in US, clicked in EU" problem: 5.4 and 14.8. CDC and the transactional outbox: 10.6. Lease tokens are fencing tokens (6.6) applied to cache fills. The full shortener cache design: 14.7.

## 8.6 Eviction Policies (LRU, LFU, TTL)

A cache has limited memory. When it's full, **eviction** decides what to drop. (Expiry is different: TTL removes entries that are too old even if memory is free.)

| Policy | Evicts | Good for | Weakness |
| :---- | :---- | :---- | :---- |
| **LRU** (least recently used) | The entry not touched for the longest time | Recency-driven access (recent links get clicked) | A one-time scan of many keys flushes the hot set |
| **LFU** (least frequently used) | The entry with the fewest hits | Stable popularity (evergreen links) | Old popular items linger; new items start cold. Needs counter decay |
| **FIFO** | Oldest inserted | Simplicity | Ignores usage |
| **Random** | A random entry | Very cheap; surprisingly OK | No locality |
| **TTL-based** (`volatile-ttl` in Redis) | Entries closest to expiring | When TTL reflects value | Only as good as your TTLs |

### LRU in O(1)

A hash map for lookup plus a doubly linked list for order:

```
hash map: key -> list node

  MRU                                                     LRU
  head <-> [abc] <-> [k9x] <-> [q1z] <-> [m3p] <-> [zz8] <-> tail

GET q1z:  unlink q1z, move to head           O(1)
SET new:  insert at head; if full, drop tail (zz8)   O(1)
```

### What real systems do

- **Redis**: `maxmemory` + `maxmemory-policy`. The default policy is `noeviction` (writes fail when full), so for a cache set it to `allkeys-lru` or `allkeys-lfu`. Redis's LRU/LFU are **approximate**: it samples a few keys (5 by default) and evicts the best candidate among them, which avoids the memory cost of a full linked list. Expired keys are removed lazily on access plus by a periodic sampling sweep.
- **Memcached**: LRU per slab class (memory is divided by item size), with a segmented hot/warm/cold LRU in modern versions.
- **Caffeine** (Java in-process): **W-TinyLFU**, which admits a new item only if its estimated frequency (from a count-min sketch, 13.2) beats the item it would evict. It resists scan pollution and has high hit ratios.

> 🔗 **Connections:** The scan-pollution problem and its fixes (segmented LRU, 2Q) are the same ones the Linux page cache solves with active/inactive lists. Count-min sketch: 13.2. DB buffer pools (8.1) use LRU variants too, e.g. InnoDB's midpoint-insertion LRU.

## 8.7 Thundering Herd / Cache Stampede (request coalescing, locks, probabilistic early refresh)

A **cache stampede** happens when a hot key is missing (expired, evicted, invalidated, or the cache restarted) and many requests miss at the same time, all go to the DB, and all recompute the same value.

Little's Law (2.4) tells you how big the herd is: requests arriving during the refill window all miss.

```
viral link: 50,000 QPS on url:abc
refill time (miss + DB read + SET): 10 ms

misses in flight = 50,000/s x 0.010 s = 500 identical DB queries

and it feeds itself: 500 concurrent queries slow the DB to ~100 ms
   -> 50,000 x 0.100 = 5,000 in flight -> DB falls over (12.3)
```

### Fix 1: request coalescing (single-flight)

Only one request per key goes to the DB; the others wait for its result.

```
req1 --miss--> [ in-flight map: abc -> pending ] --DB--> A
req2 --miss--> sees "abc pending", waits    ------------> A
req3 --miss--> sees "abc pending", waits    ------------> A
                                     one DB query, 3 answers
```

- Per process: Go's `singleflight`, Caffeine's `LoadingCache`. With 50 app servers, the DB sees at most 50 queries instead of 500.
- At the CDN: **request collapsing** (most CDNs and Varnish do it), so one origin fetch serves all edge misses in a PoP.

### Fix 2: a distributed lock per key

The first misser takes a short lock (`SET lock:abc <token> NX PX 3000` in Redis); it loads and fills. Others either wait and retry the cache, or serve a stale copy. Keep the lock short with an expiry so a crashed holder doesn't block the key (6.7 explains why such locks are best-effort, which is fine for an efficiency-only lock like this).

### Fix 3: serve stale while revalidating

Keep the old value past its "soft" expiry. The first request after soft expiry triggers a background refresh; everyone else keeps getting the slightly old value. HTTP has this built in: `Cache-Control: max-age=60, stale-while-revalidate=30` (8.9). Facebook's leases (8.5) also allow returning a slightly stale value to the readers who didn't get the lease.

### Fix 4: probabilistic early refresh (XFetch)

Instead of all readers seeing the expiry at the same instant, each read decides randomly to refresh **early**, with probability rising as expiry approaches:

```
refresh early if:   now - delta x beta x ln(rand())  >=  expiry

  delta = how long a recompute takes (e.g. 10 ms)
  beta  = tuning knob, 1.0 by default (> 1 = refresh earlier)
  rand() in (0, 1], so -ln(rand()) >= 0 is a random "head start"
```

Under heavy traffic, one request almost surely refreshes shortly before expiry, so the key never actually goes missing. Low-traffic keys rarely refresh early, which is fine since they can't stampede.

### Also: jitter your TTLs

If you warm 1 million keys at startup all with `TTL = 3600`, they all expire in the same second an hour later: a mass stampede (sometimes called a **cache avalanche**). Use `TTL = 3600 ± random(0..300)`. Same idea as retry jitter (2.7).

> 💡 **Interview tip:** When you say "cache the hot links," add: "and coalesce misses per key so a viral link expiring doesn't send thousands of identical queries to the DB." It's one sentence and it shows you've seen production.

> 🔗 **Connections:** Little's Law: 2.4. Jitter: 2.7. Locks and why they're only advisory: 6.7. Cascading overload when the DB slows: 12.3; protect it with load shedding (12.5).

## 8.8 Cache Penetration and Negative Caching (Bloom filter → 13.1)

**Cache penetration** is load from keys that **don't exist**. They are never in the cache (nothing to cache), so every request reaches the DB. Bots scanning `sho.rt/aaaaaaa`, `sho.rt/aaaaaab` … produce exactly this, and so does a mistyped link going viral.

Three related failure modes, often confused:

| Name | Cause | Fix |
| :---- | :---- | :---- |
| **Penetration** | Requests for keys that don't exist | Negative caching, Bloom filter, rate limiting |
| **Breakdown** (stampede) | One hot key expires | Coalescing, locks, early refresh (8.7) |
| **Avalanche** | Many keys expire together, or the cache cluster goes down | TTL jitter, replicas, warm-up, load shedding (8.7, 12.5) |

### Negative caching

Cache the **absence**: `url:zzzzzzz = NOT_FOUND`, with a short TTL (e.g. 60 s). Repeated lookups of the same bad code hit the cache.

Pitfalls:
- Random scans use **different** keys each time, so negative caching doesn't help them, and fills the cache with junk. Give negative entries short TTLs, and consider a separate memory budget.
- **Creation must clear negative entries.** If someone requests `abc` (gets a cached NOT_FOUND) and then a user creates custom alias `abc`, the alias is "missing" for up to the negative TTL. Delete `url:abc` on create.

### Bloom filter in front of the DB

A **Bloom filter** (13.1) answers "is this code possibly in the set?" with no false negatives and a tunable false-positive rate. If it says **no**, return 404 without touching the cache or DB.

```
request abc -> [ Bloom filter ] --"definitely not present"--> 404, done
                     |
                "maybe present"  (true, or a ~1% false positive)
                     v
               cache -> DB

sizing for 1% false positives: ~9.6 bits/element, ~7 hash functions
   6 billion codes (5 years at 100M/month) x 9.6 bits
   = 5.8 x 10^10 bits  ~ 7.2 GB
   -> split per shard, or keep in a dedicated service
```

Caveats: a plain Bloom filter can't delete (use a counting Bloom filter or cuckoo filter if links get deleted), and it must be updated on every create, or new links get 404s. That is a consistency problem of its own, so many teams use negative caching plus rate limiting first and add a Bloom filter only when scans actually hurt.

### Rate limit the scanners

Most penetration traffic comes from a few clients. Per-IP or per-ASN rate limiting (11.7) plus abuse detection (11.9) often removes it before caching matters.

> 🔗 **Connections:** Bloom filters: 13.1. LSM trees use the same trick to skip SSTables that can't contain a key (9.3). Rate limiting: 11.7. Short codes that are hard to enumerate (random rather than sequential, 11.6) make scanning less productive in the first place.

## 8.9 HTTP Caching and Redirects (Cache-Control, ETag, 301 vs 302/307)

The browser and CDN layers (8.1) are controlled by HTTP headers your server sends. You can't call `DEL` on someone's browser, so **the headers you send are the only invalidation strategy you get for those layers**: decide them up front.

### Cache-Control directives

| Directive | Meaning |
| :---- | :---- |
| `max-age=N` | Fresh for N seconds in any cache (browser and shared). |
| `s-maxage=N` | Overrides `max-age` for **shared** caches (CDNs, proxies) only. |
| `public` | Shared caches may store it (even for authenticated requests). |
| `private` | Only the end user's browser may store it; CDNs must not. |
| `no-cache` | May be stored, but must be **revalidated** with the origin before every use. (Not "don't cache"!) |
| `no-store` | Don't store it anywhere. This is the real "don't cache". |
| `must-revalidate` | Once stale, never serve without revalidating (no serving stale on errors). |
| `stale-while-revalidate=N` | May serve stale for N seconds while refreshing in the background (8.7). |
| `immutable` | Won't change while fresh; don't even revalidate on reload. For hashed asset names (8.5, tool 5). |

### Validation: ETag and 304

When a cached response goes stale, the cache doesn't have to re-download it; it can ask "has it changed?":

```
browser                                      server
  | GET /logo.png                              |
  |<-- 200, ETag: "v7", max-age=60, [body] ----|
  ...60 s later, stale...
  | GET /logo.png, If-None-Match: "v7" ------->|
  |<-- 304 Not Modified (no body) -------------|   still "v7": reuse cache
```

`Last-Modified` / `If-Modified-Since` works the same way with timestamps. Revalidation still costs a round trip, but saves bandwidth.

### Redirect status codes

| Code | Name | Meaning | Method on follow | Cached by default? |
| :---- | :---- | :---- | :---- | :---- |
| **301** | Moved Permanently | This URL has moved for good. | May change POST to GET (historical) | **Yes** (heuristically cacheable) |
| **302** | Found | Temporarily elsewhere. | May change POST to GET (historical) | No, unless Cache-Control/Expires says so |
| **303** | See Other | Go GET this other URL (e.g. after a form POST). | Always GET | No |
| **307** | Temporary Redirect | Like 302, but method and body must be kept. | Same method | No, unless headers say so |
| **308** | Permanent Redirect | Like 301, but method and body must be kept. | Same method | **Yes** |

The HTTP caching spec (RFC 9110/9111) lists 301 and 308 among the responses that are **heuristically cacheable**: a cache may store and reuse them even if you sent no caching headers. 302 and 307 are only cached when you explicitly allow it.

### Why browsers cache 301s

"Moved Permanently" is a promise: this short URL will **always** go to that destination. If that's true, asking the server again is pure waste, so browsers store the redirect in their HTTP cache and, on the next click, **follow it locally without contacting you**. With no `Cache-Control` header, major browsers keep a 301 for a long time, in practice until the cache entry is evicted or the user clears their cache. That's correct behavior given the promise; the problem is when you break the promise.

```
First click (301 and 302 look the same):

  browser                      sho.rt                      dest A
    | GET /abc ---------------->|                            |
    |<-- 301 Location: A -------|                            |
    | GET A ---------------------------------------------->  |

You then change abc -> B.

Second click after a 301:

  browser                      sho.rt                      dest A
    | (local cache: /abc -> A, permanent)                    |
    | GET A ---------------------------------------------->  |
    |  sho.rt never sees this click:
    |   - user still goes to A (edit has no effect)
    |   - no analytics for this click
    |   - an abuse takedown of abc doesn't reach this user

Second click after a 302:

  browser                      sho.rt                      dest B
    | GET /abc ---------------->|  look up abc -> B, log click
    |<-- 302 Location: B -------|                            |
    | GET B ---------------------------------------------->  |
```

### What this means for a URL shortener

| Goal | Choose |
| :---- | :---- |
| Minimum load, links never change, analytics don't matter | `301` (cached by browsers; SEO "link equity" passes to the target) |
| Editable destinations, click analytics, ability to take down abuse | `302` (or `307`) with explicit caching headers |
| Middle ground | `301` with `Cache-Control: max-age=3600` (staleness bounded to 1 h in browsers) |

Many shorteners (Bitly, for example) have historically used 301; others choose 302 specifically to keep analytics and control. In an interview, either is fine **if you state the trade-off**.

A good concrete choice for an editable shortener that still offloads traffic to the CDN:

```
HTTP/1.1 302 Found
Location: https://example.com/landing
Cache-Control: public, max-age=0, s-maxage=300

  browser: max-age=0 -> asks again on every click (edits and takedowns
           take effect; each click reaches the CDN)
  CDN:     s-maxage=300 -> serves it for up to 5 min without hitting
           origin; on edit, purge /abc (8.5) for immediate effect
  analytics: count clicks from CDN logs, since origin no longer sees all
```

Some CDNs also support provider-specific headers (`Surrogate-Control`, `CDN-Cache-Control`) to set edge TTLs separately from what browsers see.

> 💡 **Interview tip:** "We return 302, not 301, because browsers cache 301s and we couldn't change the destination, count clicks, or take down a malicious link for users who already clicked it. The CDN can still cache the 302 for a few minutes via `s-maxage`, and we purge it on edit." This is exactly the answer for "how do you invalidate when a destination changes?" at the browser layer.

> 🔗 **Connections:** CDNs and their purge APIs: 11.4. The full layered invalidation plan: 8.5 and 14.7. Abuse takedowns: 11.9. Redirect latency is a big part of the request journey in 0.3.

---

# 9. Storage Engines and Transactions

Underneath every database in the earlier parts sits a storage engine that turns "put this key" into bytes on disk, and a transaction layer that decides what concurrent operations see. Knowing B-trees vs LSM trees, what the WAL is for, and which isolation anomalies your database allows lets you justify a database choice instead of naming one.

## 9.1 Data Models: Relational, Key-Value, Wide-Column, Document, Graph, Time-Series

| Model | Shape | Strengths | Weaknesses | Examples |
| :---- | :---- | :---- | :---- | :---- |
| **Relational** | Tables, rows, fixed schema, joins | Flexible queries, joins, constraints, transactions | Harder to shard (joins, cross-shard txns, 7.9) | Postgres, MySQL, Spanner, CockroachDB |
| **Key-value** | `key → opaque value` | Simple, fast, trivially partitioned (7.3) | Only lookup by key | DynamoDB, Redis, etcd, RocksDB |
| **Wide-column** | Partition key → sorted rows of columns | Huge write throughput, range scans within a partition | Query patterns must be designed up front | Bigtable, Cassandra, HBase, ScyllaDB |
| **Document** | JSON-like nested documents | Schema flexibility, data locality for one entity | Weak joins; many-to-many is awkward | MongoDB, Couchbase, Firestore |
| **Graph** | Nodes and edges | Multi-hop traversals ("friends of friends") | Hard to partition | Neo4j, Amazon Neptune |
| **Time-series** | `(series, timestamp) → value` | Compression, downsampling, time-range queries | Narrow use case | Prometheus, InfluxDB, TimescaleDB |

The URL shortener's hot path is a pure key-value lookup: `short_code → destination`. Any of these could serve it. Reasonable choices:
- **Postgres/MySQL** (sharded or not, 7.1): unique constraint on custom aliases, transactions for the user tables, well understood. At ~3.6 TB over 5 years, a handful of shards.
- **DynamoDB / Cassandra:** partitioning and multi-AZ replication built in, scales storage without manual sharding; eventual consistency to think about (Part 4).

> 💡 **Interview tip:** Choose a data store from the **access patterns**: "the hot path is a single-key lookup, writes are ~100/s, data grows ~720 GB/year, so a KV store or a sharded relational DB both work; I'd pick X because of Y." Don't pick a database by brand.

> 🔗 **Connections:** The models map onto engines: relational and document stores usually use B-trees (9.2); wide-column and many KV stores use LSM trees (9.3). Partitioning by key: Part 7.

## 9.2 B-Trees

The **B-tree** (in practice the **B+tree**) is the default index structure of relational databases: Postgres, MySQL InnoDB, SQLite, SQL Server, Oracle. Also bbolt (used by etcd) and LMDB.

### Structure

Data is stored in fixed-size **pages** (Postgres 8 KB, InnoDB 16 KB). Internal pages hold keys and child pointers; **leaf pages** hold the keys with the rows (or pointers to them), and leaves are linked in order for range scans.

```
                         root page (always in RAM)
                   +-------------------------------+
                   |   ptr  [ g ]  ptr  [ p ]  ptr |
                   +----|-----------|---------|----+
             key < g    |  g<=key<p |         |  key >= p
          +-------------+           |         +------------------+
          v                         v                            v
  +---------------+       +---------------+            +---------------+
  | a  c  e  f    |<----->| g  j  k  m    |<---------->| p  r  t  x    |
  | rows / ptrs   |       | rows / ptrs   |            | rows / ptrs   |
  +---------------+       +---------------+            +---------------+
       leaf pages: sorted, doubly linked -> range scans walk sideways

  lookup "k":  root -> middle leaf -> found     (one page per level)
  scan "e".."k": find "e", then follow leaf links right until > "k"
```

### Why it's shallow

The **branching factor** (children per page) is large: an 8 KB page of ~16-byte (key + pointer) entries holds ~500.

```
levels:   1 root  ->  500  ->  250,000  ->  125,000,000 leaf pages
keys:     500^4 ~ 6 x 10^10 entries reachable in 4 levels

URL shortener: 6 x 10^9 codes needs ~4 levels.

internal pages: 1 + 500 + 250,000 = ~250,000 pages x 8 KB = ~2 GB
  -> all internal levels fit in RAM (buffer pool, 8.1)
  -> a point lookup costs ~1 disk read: the leaf
```

### Writes: update in place, and page splits

A B-tree **overwrites the page** that holds the key. When a leaf is full, it **splits** into two half-full pages, and a new separator key goes into the parent (which may split too, up to the root, which is how the tree grows taller).

```
before (leaf full, insert "h"):

  parent: [ ... g ... p ... ]
              |
              v
          [ g  j  k  m ]   full

after split:

  parent: [ ... g ... k ... p ... ]
              |      |
              v      v
          [ g  h  j ]  [ k  m ]
```

Costs:
- **Write amplification:** changing a 100-byte row rewrites a whole 8–16 KB page (plus a WAL record, plus in Postgres a full-page image in the WAL after each checkpoint, 9.4).
- **Random writes:** inserts with random keys touch random pages. Sequential keys (auto-increment) are append-friendly but create a hot page at the right edge (7.2's problem, inside one node).
- A crash mid-split could leave a broken tree, which is why every change is logged in the **WAL** before the page is modified (9.4).

Concurrency inside the tree uses short-lived **latches** on pages (lighter than transaction locks).

> 🔗 **Connections:** Pages are cached in the DB's buffer pool, a cache layer you don't invalidate (8.1). The WAL that makes in-place updates safe: 9.4. B-trees vs LSM trees side by side: end of 9.3.

## 9.3 LSM Trees (memtable, WAL, SSTables, compaction, Bloom filters)

The **log-structured merge tree** never updates data in place. It buffers writes in memory and writes them to disk as **sorted, immutable files**, merging files in the background. Used by RocksDB, LevelDB, Cassandra, ScyllaDB, HBase, Bigtable, CockroachDB (Pebble), TiKV (RocksDB).

### Write path

```
  put(abc, "https://x")
          |
          +------------------------> WAL (append-only, on disk)    (1)
          |                          for crash recovery (9.4)
          v
  +-------------------+
  | memtable (RAM)    |  sorted structure, e.g. a skip list (13.8)   (2)
  | abc, k9x, q1z ... |  ack the client now
  +---------+---------+
            | full (~64 MB, RocksDB's default write buffer size)
            v
  +-------------------+
  | immutable memtable|  flushed in the background                   (3)
  +---------+---------+
            | sequential write
            v
  disk:  L0: [SST] [SST] [SST]    each file sorted; files may overlap  (4)
            |  compaction: merge-sort, drop overwritten + deleted keys
            v
         L1: [SST a-f][SST g-m][SST n-z]          non-overlapping
            |
            v
         L2: [ ... ~10x bigger than L1, non-overlapping ... ]
```

Every disk write is **sequential**: appending to the WAL, writing a whole SSTable, writing compaction output. That's why LSM trees absorb write-heavy workloads well, especially on HDDs, and wear SSDs less per logical write than random page rewrites (though compaction adds its own writes).

**SSTable** (sorted string table): an immutable file of sorted key-value pairs, with a sparse index (every Nth key → offset) and a **Bloom filter** (13.1) of its keys.

**Deletes** write a **tombstone** (a marker saying "abc is deleted"). The data is physically removed only when compaction merges the tombstone with the older value, and the tombstone itself can be dropped only once no older copy can exist below it. (In Cassandra, tombstones must also outlive `gc_grace_seconds` so that repairs don't resurrect deleted data.)

### Read path

```
get(abc):
   1. memtable              hit? return
   2. immutable memtable    hit? return
   3. L0 files, newest first  (may overlap, so check each)
   4. L1: one file whose key range covers abc
   5. L2: one file ... and so on

   for each SSTable:  Bloom filter says "definitely not here"?  skip it
                      else binary-search its index, read one block

newest version wins; a tombstone means "not found"
```

Without Bloom filters, a lookup for a missing key would read a block from every level. With a ~1% false-positive filter per file, almost all of those reads are skipped. This is the same "don't go to disk for keys that don't exist" problem as cache penetration (8.8).

### Compaction strategies

| Strategy | How | Write amp | Space amp | Read amp |
| :---- | :---- | :---- | :---- | :---- |
| **Size-tiered** (STCS, Cassandra's long-time default) | Merge several similar-sized SSTables into one bigger one | Lower | Higher (needs free space for merges, many versions linger) | Higher (more overlapping files) |
| **Leveled** (RocksDB default, LevelDB) | Each level ~10× the previous, non-overlapping files; push down level by level | Higher (~10–30× in practice, workload-dependent) | Lower (~1.1×) | Lower (≈ one file per level) |

The **RUM trade-off**: you can't minimize **R**ead, **U**pdate (write) and **M**emory/space amplification all at once. Compaction strategy chooses where to pay.

Compaction is background work that competes with foreground traffic: if writes arrive faster than compaction can merge, L0 files pile up, reads slow down, and engines like RocksDB eventually **stall writes** on purpose. That's backpressure (10.5) inside a storage engine.

### B-tree vs LSM

| | B-tree | LSM tree |
| :---- | :---- | :---- |
| Write pattern | Random in-place page writes | Sequential appends + background compaction |
| Write throughput | Good | Better, especially for write-heavy loads |
| Point reads | ~1 disk read (internal levels cached) | Memtable + one candidate per level; Bloom filters keep it near 1 read |
| Range scans | Excellent (linked leaves) | Good, but must merge across levels |
| Space | Pages partly empty after splits (~50–70% full is common) | Compact after compaction, but obsolete versions take space until then |
| Latency predictability | Steady | Compaction can cause latency spikes and write stalls |
| Typical | Postgres, MySQL, SQLite | Cassandra, RocksDB, HBase, Bigtable |

For the URL shortener (40 writes/s, 12,000 reads/s mostly cached), either engine is fine; write throughput isn't the constraint. LSM engines shine for the **click events** table if you store every click (200k+ writes/s at viral peaks).

> 💡 **Interview tip:** "B-trees update in place and are read-optimized; LSM trees append and merge, so they're write-optimized, and use Bloom filters to keep reads cheap." Then tie it to your workload's read/write ratio.

> 🔗 **Connections:** Memtables are often skip lists (13.8); SSTable Bloom filters are 13.1. The WAL is 9.4. Write stalls are backpressure (10.5). Merkle trees over SSTable ranges are how Cassandra repairs replicas (13.4, 5.7).

## 9.4 Write-Ahead Log, Checkpoints, Snapshots

### The write-ahead rule

A **write-ahead log (WAL)** is an append-only file of every change. The rule: **a change must be durably in the log (fsynced) before the transaction is acknowledged, and before the modified data page may be written to disk.** Then a crash can never lose an acknowledged change, because recovery replays the log.

```
COMMIT path:
  1. append "set abc.dest = B (txid 901)" to WAL buffer
  2. append "commit 901"
  3. fsync(WAL)                      <- durability point
  4. ack the client
  5. data page with abc updated in buffer pool (dirty)
  ... later ...
  6. background writer flushes dirty page to the data file
```

Why not just fsync the data pages? A transaction may touch pages all over the disk (random writes); the WAL turns that into one sequential append. Writing the page later lets many changes to the same hot page be written once.

### Group commit

fsync is the expensive part. If an fsync costs ~1 ms on your disk, committing one transaction per fsync caps you at ~1,000 commits/s. **Group commit** waits a moment and fsyncs many transactions' records together:

```
1 txn per fsync:      1,000 fsync/s x 1  = 1,000 commits/s
50 txns per fsync:    1,000 fsync/s x 50 = 50,000 commits/s
                      (each waits a little longer; throughput goes way up)
```

(Fsync latency varies a lot: enterprise NVMe drives with power-loss protection can be far faster than ~1 ms; consumer disks and network storage can be slower.)

### Checkpoints

Without help, the WAL grows forever and recovery would replay it from the beginning. A **checkpoint** flushes all dirty pages to the data files and records "everything before log position X is on disk." Recovery starts from the last checkpoint, and older WAL segments can be recycled or archived.

```
WAL:  ...|--- seg 41 ---|--- seg 42 ---|--- seg 43 ---|--- seg 44 ---|
                        ^                             ^
                 checkpoint (LSN X)                  crash
                        |---- replay this on recovery ----|
          segments before X: recycle, or archive for PITR
```

Trade-off: frequent checkpoints = faster recovery but more I/O; rare checkpoints = cheaper but longer recovery (RTO, 5.9).

### The WAL has three other jobs

1. **Replication:** Postgres streaming replication ships WAL records to replicas, which replay them (5.2). MySQL does the same with its binlog.
2. **Change data capture:** tailing the log gives every change in commit order, which is how the cache invalidator in 8.5 and the outbox pattern in 10.6 work.
3. **Point-in-time recovery (PITR):** a base backup plus the archived WAL lets you restore to any moment, e.g. "just before someone ran `DELETE FROM urls` without a WHERE" (12.8).

### Snapshots

A **snapshot** is a consistent copy of the full state at a point in time. Uses:
- **Log compaction in consensus systems:** Raft nodes snapshot their state machine and truncate the log before that point; a slow follower that's too far behind receives the snapshot instead of the whole log (6.4). etcd does exactly this.
- **Backups and new replicas:** copy a snapshot, then catch up from the log (the same steps as rebalancing, 7.6).
- Taking one without stopping writes uses copy-on-write (filesystem/EBS snapshots) or MVCC (a long-running transaction that sees a frozen view, 9.6).

> 🔗 **Connections:** The LSM memtable is protected by exactly this WAL (9.3). The log as the source of truth also underlies Kafka (10.2) and event sourcing (10.7). Fsync and the page cache are the OS-level pieces underneath. Backups and restore drills: 12.8.

## 9.5 ACID vs BASE

**ACID** describes what a database transaction guarantees:

| Letter | Means | Really means | Mechanism |
| :---- | :---- | :---- | :---- |
| **A**tomicity | All or nothing | On error, the transaction can be **aborted** and every partial change is undone | WAL + undo/MVCC |
| **C**onsistency | Data satisfies invariants | **Your application's** invariants (unique aliases, non-negative balances), helped by constraints. Not the "C" of CAP (4.1) | Constraints + correct app logic |
| **I**solation | Concurrent transactions don't interfere | Depends on the isolation level, often much weaker than "as if serial" (9.6) | Locks, MVCC, SSI |
| **D**urability | Committed data survives crashes | Survives a crash *of what*? One disk (fsync), one node (replication), one region (cross-region replication) | WAL fsync (9.4), replication (Part 5) |

**BASE** is the loose opposite used to describe many NoSQL systems of the late 2000s:
- **B**asically **A**vailable: answer even when parts are down (possibly with stale or partial data).
- **S**oft state: state may change without new input, as replicas converge.
- **E**ventually consistent: replicas agree if writes stop (4.5).

It's a spectrum, not a binary. DynamoDB offers ACID transactions across items; Cassandra offers lightweight transactions (Paxos-based compare-and-set) for single partitions; MongoDB has multi-document transactions; Spanner and CockroachDB give serializable distributed transactions. Ask "**which** guarantees, at **what** scope (row, partition, cluster, region), at **what** cost?"

For the shortener: creating a link with a custom alias needs atomic "insert if not exists" on one key (an ACID single-row operation or a conditional put). Click counts can be BASE.

> 💡 **Interview tip:** If you say "ACID," be ready for "which isolation level?" If you say "eventually consistent," be ready for "what does the user see during the inconsistency window?" Both are follow-ups designed to check you know the words mean something specific.

> 🔗 **Connections:** The C in ACID vs the C in CAP: 4.1 and 4.6. Durability across machines and regions: Part 5, RPO in 5.9. Eventual consistency: 4.5.

## 9.6 Isolation Levels and Anomalies (dirty read, lost update, write skew, phantom; snapshot isolation; serializable)

**Isolation** is about what one transaction sees of others running at the same time. Weaker isolation = more concurrency, more anomalies. The anomalies first:

### Dirty read

Reading another transaction's **uncommitted** write. If that transaction aborts, you acted on data that never existed.

### Non-repeatable read (read skew)

Reading the same thing twice in one transaction and getting different answers because someone committed in between. A backup or report that reads table A then table B can see an inconsistent mix.

### Lost update

Two read-modify-write cycles interleave; one overwrites the other.

```
click counter for abc, starts at 100

time  T1                         T2                          DB value
t1    SELECT clicks -> 100                                   100
t2                               SELECT clicks -> 100        100
t3    UPDATE clicks = 101                                    101
t4    COMMIT                                                 101
t5                               UPDATE clicks = 101         101
t6                               COMMIT                      101  <- lost
                                                             (should be 102)
```

Fixes: an **atomic update** (`UPDATE urls SET clicks = clicks + 1`), explicit locking (`SELECT ... FOR UPDATE`), **compare-and-set** (`UPDATE ... WHERE clicks = 100`, retry if 0 rows), or an isolation level that detects it.

### Write skew

Two transactions each read the same data, each check a condition, and each write **different** rows. Each write is valid alone; together they break the invariant. No row is written twice, so lost-update detection doesn't catch it.

```
invariant: at most 1 link per user may be marked "featured"

time  T1 (feature link abc)            T2 (feature link q1z)
t1    SELECT count(*) WHERE featured   SELECT count(*) WHERE featured
      AND user=42  -> 0                AND user=42  -> 0
t2    UPDATE abc SET featured=true     UPDATE q1z SET featured=true
t3    COMMIT                           COMMIT
      -> user 42 now has 2 featured links; both txns saw 0
```

### Phantom

A write in one transaction changes the **result set of a search** in another. Write skew is usually caused by a phantom: T1's `count(*)` query matched rows that T2 was about to change. A classic URL shortener phantom:

```
two users claim custom alias "sale" at the same time:

time  T1                                   T2
t1    SELECT 1 FROM urls                   SELECT 1 FROM urls
      WHERE code='sale' -> none            WHERE code='sale' -> none
t2    INSERT code='sale', dest=X           INSERT code='sale', dest=Y
t3    COMMIT                               COMMIT
      -> without a unique constraint: two rows for "sale", or one
         user's link silently points to the other user's destination
```

There's no row to lock at check time; the conflict is over a row that doesn't exist yet. **Fix: a unique constraint (or primary key) on `code`.** The second insert fails, the app returns "alias taken". This is the database enforcing the invariant instead of the application checking-then-acting.

### The levels

| Level | Dirty read | Non-repeatable read | Lost update | Phantom | Write skew |
| :---- | :---- | :---- | :---- | :---- | :---- |
| Read uncommitted | possible | possible | possible | possible | possible |
| **Read committed** | prevented | possible | possible | possible | possible |
| **Snapshot isolation** (repeatable read in Postgres) | prevented | prevented | prevented in Postgres (2nd writer aborts) | prevented for reads | **possible** |
| **Serializable** | prevented | prevented | prevented | prevented | prevented |

Defaults and naming quirks you should know:
- **Postgres** defaults to Read Committed. Its "Repeatable Read" is snapshot isolation. Its "Serializable" is **SSI** (serializable snapshot isolation), which detects dangerous read/write patterns and aborts one transaction.
- **MySQL InnoDB** defaults to Repeatable Read, which uses snapshots for plain reads but does **not** abort on lost updates in read-modify-write; use `SELECT ... FOR UPDATE` or atomic updates.
- **Oracle**'s "serializable" is actually snapshot isolation.
- **Spanner** and **CockroachDB** default to serializable (Spanner provides external consistency, i.e. strict serializability, 4.2).

### How it's implemented

- **MVCC (multi-version concurrency control):** writers create new row versions; each transaction reads from a snapshot as of its start (or statement start, for Read Committed). Readers never block writers. Used by Postgres, InnoDB, Oracle, CockroachDB, Spanner.
- **Two-phase locking (2PL):** take shared locks to read and exclusive locks to write, hold them until commit. Serializable, but readers and writers block each other and deadlocks need detection. (Not to be confused with two-phase *commit*, 9.7.)
- **SSI:** snapshot isolation plus tracking of read/write dependencies; aborts transactions that could form a non-serializable cycle. Optimistic: great when conflicts are rare; your app must **retry** aborted transactions (which must be safe to retry, 2.6).

> 💡 **Interview tip:** Prefer making the database enforce invariants: unique constraints for aliases, atomic `clicks = clicks + 1` for counters, conditional writes for check-then-act. These work at any isolation level and on most distributed stores (DynamoDB condition expressions, Cassandra LWT).

> 🔗 **Connections:** Isolation is about concurrent transactions on one database; consistency models (Part 4) are about replicas. They combine in "strict serializability" (4.2). Compare-and-set is the same primitive as fencing (6.6) and versioned cache fills (8.5).

## 9.7 Two-Phase Commit (2PC) and Why It Blocks

**Two-phase commit** makes a transaction that spans several nodes (shards, or different databases) commit atomically: either every participant commits, or none does.

```
  coordinator                 shard A (participant)     shard B (participant)
      |                              |                          |
      | ---- PREPARE txn 77 -------->|                          |
      | ---- PREPARE txn 77 ------------------------------------>|
      |                     write changes + "prepared" to WAL   |
      |                     fsync, hold locks                   |
      |<------------- YES -----------|                          |
      |<--------------------------------------------- YES ------|
      |                              |                          |
      | write "COMMIT 77" to own log, fsync   <- commit point   |
      |                              |                          |
      | ---- COMMIT 77 ------------->|                          |
      | ---- COMMIT 77 ----------------------------------------->|
      |                     apply, release locks                 |
      |<------------- ack -----------|<-------------- ack -------|

  phase 1: prepare/vote      phase 2: commit (or abort if any NO / timeout)
```

The key promise: a participant that votes **YES** guarantees it **can** commit, even after a crash (that's why it writes "prepared" to its WAL). After voting YES it is **not allowed** to decide on its own.

### Why it blocks

```
  coordinator               shard A                     shard B
      | -- PREPARE -->        |                            |
      | -- PREPARE ------------------------------------->  |
      |<-- YES ---------------|                            |
      |<-- YES ------------------------------------------- |
      X  coordinator crashes before sending COMMIT/ABORT

  shard A: "I voted YES. Did B vote YES? Did the coordinator decide
            COMMIT or ABORT? I can't tell, and I promised not to choose."
           -> IN DOUBT: holds row locks on txn 77's rows
              until the coordinator recovers
```

- Participants can't unilaterally abort (the coordinator may have decided COMMIT and told the other participant) and can't unilaterally commit (someone may have voted NO).
- Their **locks stay held**, so every other transaction touching those rows waits too. One crashed coordinator can freeze part of your database.
- Costs even without failures: two round trips plus several fsyncs on the critical path, and locks held across network round trips, which caps throughput on contended rows.

**Three-phase commit** tries to avoid blocking but assumes bounded network delays, which real networks don't have (2.1); it's rarely used.

### How real systems make 2PC tolerable

Make the coordinator and each participant **not a single machine**. In Spanner and CockroachDB, every participant is a Raft/Paxos group (6.3–6.5) and the coordinator's decision is itself replicated, so "the coordinator crashed" just means a new leader takes over and finishes the protocol. 2PC still costs latency, but no longer blocks on a single failure.

Elsewhere you'll see 2PC as **XA transactions** (across e.g. a database and a message broker); they work but share all the problems above, which is one reason the outbox pattern (10.6) and sagas (9.8) exist.

> 💡 **Interview tip:** "2PC is atomic but blocking: if the coordinator dies after the participants vote yes, they hold locks until it comes back. Spanner fixes that by making every participant a Paxos group. In most designs I'd avoid cross-shard transactions by choosing the partition key (7.9), or use a saga."

> 🔗 **Connections:** 2PC is an atomic-commitment protocol, not consensus: it needs **all** participants to agree, consensus needs a **majority** (6.1–6.2). Two Generals is why the coordinator can never be sure its messages arrived (6.1). Don't confuse it with two-phase locking (9.6).

## 9.8 Sagas and Compensating Actions

A **saga** replaces one distributed transaction with a **sequence of local transactions**, each in one service or shard. Each step T*i* has a **compensating action** C*i* that semantically undoes it. If step T*k* fails, run C*k−1* … C*1* in reverse.

Example: a premium "branded link" feature in the shortener touches three services.

```
  T1: reserve alias "sale" in links DB      C1: release alias "sale"
  T2: charge 10 credits in billing          C2: refund 10 credits
  T3: register sho.rt/sale in CDN config    C3: remove CDN config

success:   T1 ---> T2 ---> T3 ---> done

T3 fails:  T1 ---> T2 ---> T3 (X)
                            |
                            v
                   C2 (refund) ---> C1 (release alias) ---> "failed" to user
```

### Orchestration vs choreography

```
orchestration                          choreography

      [ saga orchestrator ]            links --"AliasReserved"--> billing
       /        |        \             billing --"Charged"--> cdn-config
      v         v         v            cdn-config --"Failed"--> billing
  links    billing   cdn-config        billing --"Refunded"--> links
  (calls each step, tracks state,      (each service reacts to events,
   runs compensations on failure)       no central brain)
```

| | Orchestration | Choreography |
| :---- | :---- | :---- |
| Flow defined in | One place (orchestrator; e.g. a Temporal or AWS Step Functions workflow) | Spread across services' event handlers |
| Easy to reason about | Yes | Gets hard past ~3–4 steps |
| Coupling | Orchestrator knows every service | Services know events, not each other |

### What sagas give up

- **No isolation.** Between T1 and C1, other users can see alias "sale" as taken, even if the saga will later release it. Mitigate with explicit states (`status = PENDING`) that other code treats specially, sometimes called a semantic lock.
- **Compensation is not rollback.** You can refund money, but you can't un-send an email. Order steps so irreversible ones go **last**, after everything that might fail.
- **Every step and compensation must be idempotent** (2.6) and retried until it succeeds, because the saga runner can crash and replay. A compensation that can fail permanently needs a human (alert + dead-letter queue, 10.5).
- Each step should publish its result reliably together with its local DB change, which is the transactional outbox (10.6).

> 💡 **Interview tip:** When asked "how do you keep two services consistent?", answer "a saga with compensations, steps made idempotent, events published via an outbox," and name the cost: "intermediate states are visible, so we model them explicitly as PENDING."

> 🔗 **Connections:** Idempotency: 2.6. Outbox and CDC: 10.6. Dead-letter queues for compensations that keep failing: 10.5. Choreography is event-driven architecture (10.1, 10.7).

## 9.9 Object Storage and Erasure Coding

**Object storage** (S3, Google Cloud Storage, Azure Blob, Ceph RGW, MinIO) stores **immutable blobs** by key in a flat namespace (`bucket/key`; the "folders" are just key prefixes). You PUT and GET whole objects (or byte ranges); you don't update a byte in the middle.

```
               PUT bucket/qr/abc.png
                       |
                       v
          +--------------------------+
          | front end / API          |
          +-----+--------------+-----+
                |              |
                v              v
     +------------------+   +-------------------------------+
     | metadata service |   | storage nodes                 |
     | key -> chunks,   |   | chunks/fragments on disks,    |
     | size, version,   |   | spread across racks and AZs   |
     | (a partitioned,  |   +-------------------------------+
     |  replicated KV)  |
     +------------------+
```

- **Designed for durability and cheap capacity**, not low latency: first-byte latency is typically tens of milliseconds, versus ~1 ms for a cache or KV store.
- **Consistency:** S3 has provided strong read-after-write consistency for PUTs and deletes since December 2020; older articles that say "eventually consistent" are out of date.
- S3 is designed for 99.999999999% ("eleven nines") annual object durability.
- In the shortener: object storage holds **backups, WAL archives (9.4), click logs for analytics (10.8), and generated QR-code images**, not the hot `code → URL` mapping.

### Erasure coding

Replicating every object 3× costs 3× the disk. **Erasure coding** (usually Reed–Solomon) splits data into **k data fragments** and computes **m parity fragments**; **any k of the k+m** fragments rebuild the original.

```
RS(6,3): object split into 6 data fragments + 3 parity fragments

   [d1][d2][d3][d4][d5][d6][p1][p2][p3]      9 fragments, 9 different
     \___ any 6 of these 9 rebuild ___/      disks/racks (or AZs)

   lose any 3 fragments -> still readable
   storage cost = 9/6 = 1.5x   (vs 3x for triple replication)
```

| Scheme | Storage overhead | Losses survived | Notes |
| :---- | :---- | :---- | :---- |
| 3× replication | 3.0× | 2 | Fast reads/writes, cheap repair (copy one replica) |
| RS(6,3) | 1.5× | 3 | HDFS's default erasure-coding policy |
| RS(10,4) | 1.4× | 4 | Used by Facebook's f4 warm-blob storage |

```
1 PB of logical data:
   3x replication:  3.0 PB of disk
   RS(6,3):         1.5 PB of disk   -> saves 1.5 PB, AND survives 3 losses
```

Why not erasure-code everything:
- **Repair is expensive.** Rebuilding one lost fragment means reading k fragments over the network (6× the data for RS(6,3)), versus copying one replica.
- **Degraded reads are slower:** if a data fragment is missing, the read must fetch k fragments and decode.
- **Small objects** suffer: splitting a 4 KB object into 9 fragments is all overhead. Systems pack small objects into larger stripes first.
- **CPU** for encoding/decoding (modest with modern SIMD, but not zero).

So the common pattern is **hot data replicated, warm/cold data erasure-coded**, which is what tiered storage classes in cloud object stores are built on.

> 💡 **Interview tip:** For any "store petabytes" question, say "replicate hot data for fast reads and cheap repair, erasure-code the rest: RS(6,3) is 1.5× overhead and survives 3 failures, versus 3× for replication." It shows cost awareness, which NALSD interviews explicitly test.

> 🔗 **Connections:** Placing fragments across racks and AZs is failure-domain thinking (12.2, 1.7). Backups and restores to object storage: 12.8. Storage estimation with replication overhead: 1.5. GFS, the ancestor of many of these systems, is in 14.11.

---

# 10. Messaging and Streams

So far most interactions have been request/response: a caller waits for an answer. This part is about the other half of distributed systems: putting work or facts into a durable buffer and letting someone else deal with them later. Queues and logs are how you decouple services, absorb spikes, and keep several data stores in sync without distributed transactions (9.7).

## 10.1 Why Asynchronous Messaging

In a **synchronous** call, the caller blocks until the callee answers. Its latency and availability are chained to the callee's: if the callee is slow, the caller is slow; if the callee is down, the caller fails (serial availability, 1.3).

In **asynchronous messaging**, the producer writes a message to a **broker** (queue or log) and moves on. A **consumer** picks it up later.

```
SYNCHRONOUS                              ASYNCHRONOUS
client -> redirect svc -> analytics DB   client -> redirect svc -> broker
           (waits ~5 ms, fails if                   (append ~1 ms, returns)
            analytics DB is down)                          |
                                                           v  (later, at its
                                                  analytics consumer  own pace)
                                                           |
                                                           v
                                                     analytics DB
```

What you buy:

| Benefit | What it means | URL shortener example |
| :---- | :---- | :---- |
| **Decoupling** | Producer doesn't know or care who consumes. | Redirect service emits `ClickEvent`; analytics, fraud, billing each consume it. |
| **Load leveling** | The broker absorbs bursts; consumers drain at a steady rate. | A viral link spikes clicks to 50k/s for 2 minutes; analytics consumers keep running at 10k/s and catch up. |
| **Failure isolation** | Consumer down = backlog grows, producer unaffected. | Analytics DB outage doesn't break redirects. |
| **Fan-out** | One event, many independent readers. | Same click stream feeds dashboards and abuse detection. |
| **Retry for free** | Unprocessed messages stay in the broker. | A crashed consumer resumes from its last checkpoint. |

What you pay:

- **Eventual consistency** (4.5): the click count on the dashboard is seconds behind.
- **No immediate answer**: you can't tell the user "your write failed downstream". Async is for work the user doesn't need to wait for.
- **New failure modes**: duplicates (10.3), reordering (10.4), poison messages (10.5), unbounded backlog.
- **Another stateful system** to run, replicate (Part 5), and monitor (consumer lag, 12.10).

> 💡 **Interview tip:** A crisp rule: "keep the user-facing path synchronous and minimal; push everything else to a queue." For a URL shortener, the redirect does one cache/DB lookup and returns a 302; click counting goes to Kafka. That keeps redirect p99 low and makes analytics outages invisible to users.

> 🔗 **Connections:** Async messaging is the tool behind the transactional outbox (10.6), cross-region cache invalidation (8.5), sagas (9.8), and load shedding (12.5: a queue is a buffer, not infinite capacity). Little's Law (2.4) tells you how big the backlog gets.

## 10.2 Queues vs Logs (SQS/RabbitMQ vs Kafka)

There are two families of broker, and they behave very differently.

### Message queue (SQS, RabbitMQ, ActiveMQ)

A **queue** hands each message to **one** consumer, and the message is **deleted once acknowledged**. The broker tracks per-message state.

```
producer --> [ m5 m4 m3 m2 m1 ] --> consumer A gets m1
                                --> consumer B gets m2   (competing consumers)
             consumer A: ACK m1  -> m1 deleted forever
             consumer B: crashes -> m2 becomes visible again after timeout
```

- **Competing consumers**: add consumers to process faster; the broker load-balances messages.
- **Visibility timeout** (SQS, default 30 s) / **unacked redelivery** (RabbitMQ): if a consumer takes a message and doesn't ack it in time, it's redelivered. That's why queues are **at-least-once** (10.3).
- Ordering is weak: SQS standard queues are best-effort ordered; SQS FIFO and single-consumer RabbitMQ queues give order at the cost of throughput.
- Good for **task distribution**: "resize this image", "send this email", "scan this URL for malware".

### Log (Kafka, Pulsar, Kinesis, Redpanda)

A **log** is an **append-only, ordered sequence of records** that is **retained** (for days, or forever) regardless of whether anyone has read it. Each consumer tracks its own position, the **offset**.

```
partition 0 of topic "clicks":

 offset:   0     1     2     3     4     5     6     7
         +-----+-----+-----+-----+-----+-----+-----+-----+
         | c0  | c1  | c2  | c3  | c4  | c5  | c6  | c7  |  <-- producer appends
         +-----+-----+-----+-----+-----+-----+-----+-----+
                       ^                       ^
          analytics group @ offset 2           fraud group @ offset 6

 Reading doesn't delete. Each group just moves its own pointer.
 Retention deletes old segments (e.g. after 7 days) for everyone.
```

- Many independent consumer groups read the same data at their own pace.
- **Replay**: rewind the offset to reprocess history (bug fix, new consumer, backfill).
- Ordering is guaranteed **within a partition** (10.4).
- Throughput is huge because it's sequential disk I/O plus the OS page cache (a log is the friendliest possible workload for disks).
- Kafka replicates each partition to several brokers (a leader plus followers, an **in-sync replica set**, ISR), which is single-leader replication (5.2) per partition.

### Side by side

| Property | Queue (SQS, RabbitMQ) | Log (Kafka, Kinesis) |
| :---- | :---- | :---- |
| After consumption | Message deleted | Record retained until retention expires |
| Consumer state | Broker tracks per-message acks | Consumer tracks an offset per partition |
| Multiple readers | One consumer per message (fan-out needs extra queues/exchanges) | Any number of consumer groups |
| Ordering | Weak (except FIFO variants) | Strict per partition |
| Replay | No | Yes, seek to any retained offset |
| Parallelism | Add consumers freely | Capped at number of partitions per group |
| Per-message retry / delay | Native (visibility timeout, DLQ) | Awkward: one slow message blocks its partition |
| Best for | Jobs and tasks | Event streams, CDC, audit, analytics, state rebuild |

> 💡 **Interview tip:** If the interviewer asks "Kafka or SQS?", answer by need: "If I need replay, multiple independent consumers, or ordering per key, a log. If I need simple task distribution with per-message retries and no ops burden, a queue." Naming *why* matters more than the product.

> 🔗 **Connections:** A Kafka partition is a replicated log, the same abstraction as a database WAL (9.4) and a Raft log (6.4). That's not a coincidence: "the log" is the core data structure of distributed systems. CDC (10.6) literally turns a DB's WAL into a Kafka topic.

## 10.3 Delivery Semantics: At-Most-Once, At-Least-Once, Effectively-Once

The network can lose the message or the ack (2.1, 2.2). The consumer can crash between processing and acknowledging. Where you put the ack decides what happens.

```
AT-MOST-ONCE: ack (commit offset) BEFORE processing

 consumer:  receive m1 --> commit offset --> process m1 --X crash
 restart:   resumes after m1                 m1 is LOST

AT-LEAST-ONCE: ack AFTER processing

 consumer:  receive m1 --> process m1 --> commit offset --X crash before commit
 restart:   resumes at m1 --> process m1 AGAIN     m1 is DUPLICATED
```

| Semantic | How | Failure outcome | Use when |
| :---- | :---- | :---- | :---- |
| **At-most-once** | Ack first, or fire-and-forget | Messages lost | Losing some is fine: metrics samples, debug logs |
| **At-least-once** | Ack after processing; retry on failure | Duplicates | The default for almost everything |
| **Exactly-once** (effectively-once) | At-least-once delivery + idempotent or transactional processing | Each message's *effect* applied once | Money, counters you bill on |

### Exactly-once is an effect, not a delivery

Exactly-once *delivery* over an unreliable network is impossible in general (it's the Two Generals problem, 6.1). What real systems give you is **effectively-once processing**: duplicates may arrive, but their *effect* is applied once. Two ways:

**1. Idempotent consumer (dedup).** Each message has a unique ID. The consumer records processed IDs in the same transaction as the effect (2.6):

```sql
BEGIN;
  INSERT INTO processed_msgs (msg_id) VALUES ('click-8f2a'); -- dup => PK error
  UPDATE url_stats SET clicks = clicks + 1 WHERE code = 'aZ3kQ9x';
COMMIT;
-- 2nd delivery: INSERT fails on the PK, txn rolls back, no double count
```

Or make the operation naturally idempotent: `SET clicks_for_minute[12:01] = 4182` instead of `INCR`.

**2. Transactions inside the log system.** Kafka has an **idempotent producer** (broker dedups retries using a producer ID + sequence number) and **transactions** that atomically write output records *and* commit input offsets. That gives exactly-once for read-from-Kafka, process, write-to-Kafka pipelines (Kafka Streams, Flink). The guarantee ends at Kafka's boundary: if your consumer also calls an external API or DB, that side effect needs its own idempotency.

> 💡 **Interview tip:** Say "at-least-once delivery plus idempotent consumers" whenever you introduce a queue. If the interviewer asks about exactly-once, explain that it's achieved by dedup or atomic offset+output commits, not by the network delivering once. That one sentence signals you've actually run these systems.

> 🔗 **Connections:** Same three outcomes as an RPC timeout (2.2): lost, processed-but-reply-lost, or slow. Idempotency keys (2.6) and retry rules (2.7) are the same toolkit. The dedup table is a small-scale version of the outbox's guarantees (10.6).

## 10.4 Ordering, Partitions and Consumer Groups

A single totally ordered log can only go as fast as one machine. Kafka scales by splitting a topic into **partitions**; order is guaranteed **only within a partition**.

### Choosing the partition

The producer picks the partition, usually `hash(key) % num_partitions` (7.3). All messages with the same key land in the same partition, in the order they were produced.

```
topic "url-events", 4 partitions, key = short code

 "aZ3kQ9x": Created, Updated(dest=B), Deleted --> hash % 4 = 2 --> P2
 "Qp7Lm2c": Created                            --> hash % 4 = 0 --> P0

 P0: [Qp7 Created] ...
 P1: ...
 P2: [aZ3 Created] [aZ3 Updated] [aZ3 Deleted]   <- per-key order preserved
 P3: ...
```

So **pick the key that defines "what must stay ordered"**: the short code for URL lifecycle events, the user ID for a user's actions, the account ID for ledger entries. Events for different keys may interleave arbitrarily, and that's fine.

What breaks per-key order:

- **Changing the partition count**: `hash % 4` ≠ `hash % 8`, so a key moves to a new partition and its new events can be read before old ones in the old partition. Over-provision partitions up front.
- **Producer retries with several in-flight requests**: batch 1 fails, batch 2 succeeds, batch 1 retried lands after it. Kafka's idempotent producer prevents this.
- **Parallel processing inside a consumer**: if you hand messages from one partition to a thread pool, you've thrown order away. Parallelize *by key* inside the consumer if you need to.

### Consumer groups

A **consumer group** is a set of consumers sharing the work for one logical subscriber. Each partition is assigned to **exactly one** consumer in the group.

```
topic: 6 partitions          group "analytics" (3 consumers)
  P0 P1  ----------------->  consumer A
  P2 P3  ----------------->  consumer B
  P4 P5  ----------------->  consumer C

 Add consumer D -> rebalance -> each gets 1-2 partitions.
 Add consumers E, F, G       -> G sits IDLE: 6 partitions, max 6 active.
```

- **Max parallelism per group = partition count.** Choose partitions for the peak consumer count you'll need, e.g. "peak 60k msgs/s ÷ ~5k msgs/s per consumer = 12 consumers → 24 partitions for headroom".
- **Rebalancing**: when a consumer joins, leaves or misses heartbeats (6.9), partitions are reassigned. During a classic "stop-the-world" rebalance the group pauses; uncommitted work is redone (at-least-once again).
- A different group gets its own full copy of the stream.

### Hot partitions

If one key is very hot (a viral short code), its partition gets all the traffic and one consumer can't keep up. It's the same problem as hot shards (7.5), with the same fixes: salt the key (`code#0..code#9`) if strict per-key order isn't needed, or pre-aggregate in the producer (count clicks locally for 1 s, emit one record).

> 🔗 **Connections:** Partitioning a topic is hash partitioning (7.3) with the same rebalancing pain (7.6). Ordering per key is the messaging version of "single leader per key" (5.2). Consumer heartbeats and rebalances are a failure detector (6.9).

## 10.5 Backpressure and Dead-Letter Queues

### Backpressure

**Backpressure** is a downstream component telling upstream "slow down", instead of silently buffering until it falls over. Every buffer is finite; a queue only *delays* overload unless consumers can eventually catch up.

How long is the backlog? Little's Law (2.4) applied to a queue:

```
consumer lag = 1,800,000 messages
drain rate   = consume rate - produce rate = 12,000 - 9,000 = 3,000 msg/s
time to drain = 1,800,000 / 3,000 = 600 s = 10 minutes

If consume rate < produce rate, lag grows forever. No buffer fixes that.
```

Options, from gentle to brutal:

| Mechanism | How | Example |
| :---- | :---- | :---- |
| **Bounded buffer + block** | Producer blocks when the buffer is full | Kafka producer `buffer.memory` full → `send()` blocks |
| **Pull-based consumption** | Consumers fetch only what they can handle | Kafka consumers poll; the broker never pushes |
| **Credit / window** | Receiver grants N credits | TCP receive window, HTTP/2 and gRPC flow control |
| **Reject with a signal** | Return 429/503 + `Retry-After` | Rate limiting (11.7), load shedding (12.5) |
| **Drop** | Discard lowest-value work | Sample analytics events at 10% under overload |
| **Scale consumers** | Autoscale on lag | KEDA scaling on Kafka lag (bounded by partitions, 10.4) |

**Consumer lag** (messages or seconds behind the head) is *the* metric for a stream pipeline. Alert on lag in seconds, not raw count.

### Poison messages and DLQs

A **poison message** fails every time: malformed payload, a bug path, a reference to a deleted row. With at-least-once and infinite retry, it gets redelivered forever and, in a log, **blocks its whole partition**.

A **dead-letter queue (DLQ)** is where messages go after N failed attempts, so the main flow keeps moving:

```
main queue --> consumer --fail--> retry (backoff) --fail x N--> DLQ
                  |                                              |
                  +--success--> done                 alert, inspect, fix bug,
                                                     redrive back to main
```

- SQS: a **redrive policy** with `maxReceiveCount` moves the message to a DLQ automatically.
- Kafka: no built-in DLQ; the consumer catches the error and produces the record to a `clicks.dlq` topic, often via retry topics with increasing delays (`clicks.retry.1m`, `clicks.retry.10m`).
- Separate **transient** errors (DB timeout: retry with backoff, 2.7) from **permanent** errors (schema invalid: DLQ immediately, retrying won't help).
- A DLQ nobody watches is a black hole. Alert on DLQ depth > 0 and keep a **redrive** tool.

> 🔗 **Connections:** Backpressure is the queueing version of load shedding (12.5) and circuit breaking (12.4). Unbounded queues are a classic cascading-failure ingredient (12.3): latency grows with queue length (Little's Law, 2.4) until every request times out.

## 10.6 Transactional Outbox and Change Data Capture

### The dual-write problem

A service often needs to update its DB **and** tell the world. Doing both as two independent writes is broken:

```
POST /urls/aZ3kQ9x  (change destination to https://new.example)

 app:  UPDATE urls SET dest=... WHERE code='aZ3kQ9x';   -- OK, committed
 app:  publish UrlUpdated to Kafka                      --X crash / timeout

 Result: DB says new dest, cache invalidation event never sent.
         Every region keeps serving the OLD destination until TTL expires.

 Reverse order (publish first, then DB write fails) is just as bad:
 caches are invalidated for a change that never happened.
```

There's no transaction spanning Postgres and Kafka (2PC, 9.7, is rarely available or worth it). Two standard fixes both lean on the DB's own transaction.

### Transactional outbox

Write the event into an **outbox table in the same DB transaction** as the business change. A separate **relay** reads the outbox and publishes to the broker.

```
 +--------------------- one ACID transaction --------------------+
 | UPDATE urls   SET dest='https://new.example' WHERE code=...   |
 | INSERT INTO outbox (id, topic, key, payload)                  |
 |        VALUES (uuid, 'url-events', 'aZ3kQ9x', '{Updated...}') |
 +---------------------------------------------------------------+
                              |
                    relay (poll or CDC)
                              v
                   Kafka "url-events" ----> cache invalidators (all regions)
                                       ----> search indexer, analytics
```

- Either both rows commit or neither does: no lost event, no phantom event.
- The relay may publish the same row twice (it crashed after publishing, before marking it sent) → consumers must be **idempotent** (10.3). An invalidation ("delete key aZ3kQ9x") is naturally idempotent.
- Order per key is preserved if the relay publishes in outbox order and keys by the entity ID.

### Change Data Capture (CDC)

**CDC** reads the database's own replication log (Postgres WAL via logical decoding, MySQL binlog, DynamoDB Streams, MongoDB change streams) and turns every committed row change into an event. **Debezium** is the common open-source tool; it streams into Kafka.

```
app --> Postgres --WAL--> Debezium --> Kafka "db.urls" --> consumers
           (app does a normal write; nothing else to remember)
```

| | Outbox | CDC on business tables |
| :---- | :---- | :---- |
| Event shape | Designed domain event (`UrlDestinationChanged`) | Raw row diff (`before`/`after` columns) |
| Coupling | Consumers see a stable contract | Consumers coupled to your table schema |
| App changes | Must write outbox rows | None |
| Misses writes? | Only if a code path forgets | Never: every committed change is in the WAL |

A popular combination: **outbox + CDC**. The app writes outbox rows; Debezium tails the WAL for the outbox table only. You get domain events with no polling.

> 💡 **Interview tip:** When the URL shortener's destination changes, say: "The update and an outbox event commit in one transaction; CDC publishes it to Kafka; a consumer in every region deletes the key from the regional cache and purges the CDN. TTL is the safety net if an invalidation is lost." This answers the cache-invalidation question the dual-write way interviewers want.

> 🔗 **Connections:** CDC is replication (Part 5) repurposed: the same WAL (9.4) that feeds read replicas feeds Kafka. The outbox is how sagas (9.8) emit their step events reliably, and it's the backbone of cross-region cache invalidation (8.5, 14.7).

## 10.7 Event Sourcing and CQRS

### Event sourcing

In a normal (state-oriented) design you store the **current state** and overwrite it. In **event sourcing**, the source of truth is the **append-only list of events**; current state is derived by replaying (folding) them.

```
events for short code aZ3kQ9x (the source of truth):
  1  UrlCreated        dest=https://a.example   owner=u42   t=10:00
  2  DestinationChanged dest=https://b.example             t=11:30
  3  ExpirySet          expires=2027-01-01                 t=11:31
  4  UrlDisabled        reason=phishing-report              t=14:02

current state = fold(apply, events) =
  { dest: https://b.example, expires: 2027-01-01, disabled: true }
```

Why do it:

- **Full audit trail** for free: "who changed this link's destination and when?"
- **Time travel**: rebuild state as of any point; debug by replaying.
- **New views later**: a new consumer replays history to build a projection nobody planned for.
- Natural fit with logs (10.2): the event store is a log.

Costs:

- Replaying millions of events is slow → periodic **snapshots** (state at event N, then replay N+1..).
- **Schema evolution**: old events live forever; you need upcasters or versioned handlers.
- Hard to "fix" bad data: you append a compensating event, never edit history (GDPR deletion needs crypto-shredding or similar).
- Querying current state across entities requires projections (that's CQRS).

### CQRS

**Command Query Responsibility Segregation** splits the **write model** (validates commands, enforces invariants) from one or more **read models** (denormalized, shaped for each query).

```
          commands                               queries
 user --> [ write model ] --events--> Kafka --> [ read model A: Redis
           (Postgres, or                          code -> dest, for redirects ]
            event store)                     --> [ read model B: ClickHouse
                                                  per-link stats dashboard ]
                                             --> [ read model C: Elasticsearch
                                                  "search my links" ]
```

- Each read model is a **projection**, updated asynchronously → **eventually consistent** (4.5). A user who just edited a link may read the old value from the read model: the classic read-your-writes problem (4.4, 5.4). Fixes: read from the write model for that user for a few seconds, or return the new version with the command response and have the UI use it.
- CQRS doesn't require event sourcing, and vice versa, but they're often paired.

> 💡 **Interview tip:** Don't propose event sourcing for a URL shortener's core data; it's overkill and the interviewer may probe its costs. Do mention CQRS-lite: "writes go to the primary DB; redirects read from a cache/KV read model fed by CDC." That's the same idea at the right size.

> 🔗 **Connections:** Event sourcing is the application-level version of a WAL (9.4) or a Raft log (6.4): state = replay of the log. Projections lagging the write model are replication lag (5.4) under a new name.

## 10.8 Batch vs Stream Processing (MapReduce, windowing, watermarks)

**Batch processing** runs over a **bounded** dataset (yesterday's logs) and produces a complete answer later. **Stream processing** runs over an **unbounded** dataset continuously and produces answers incrementally.

| | Batch | Stream |
| :---- | :---- | :---- |
| Input | Bounded (files in S3/HDFS) | Unbounded (Kafka topic) |
| Latency | Minutes to hours | Milliseconds to seconds |
| Correctness | Easy: all data present | Hard: late and out-of-order data |
| Failure recovery | Re-run the job | Checkpoint state + replay from offsets |
| Examples | MapReduce, Spark, BigQuery, Hive | Flink, Kafka Streams, Spark Structured Streaming, Dataflow |

### MapReduce

MapReduce (Google, 2004) made batch on thousands of machines simple: you write two functions, the framework handles distribution and failures.

```
count clicks per short code from 1 TB of click logs

 input splits      MAP                 SHUFFLE (by key)        REDUCE
 [log part 1] -> (aZ3,1) (Qp7,1) ---\                     /-> aZ3: sum -> 5,012
 [log part 2] -> (aZ3,1) (x9K,1) ----+--> group by key ---+-> Qp7: sum ->   811
 [log part 3] -> (Qp7,1) (aZ3,1) ---/   (network-heavy)   \-> x9K: sum ->    97
```

- **Map** runs where the data lives (data locality on GFS/HDFS).
- **Shuffle** moves all values for a key to one reducer: the expensive, network-bound step. **Combiners** pre-aggregate on the mapper side to shrink it.
- **Fault tolerance**: tasks are deterministic and inputs are immutable, so a failed task is just re-run elsewhere. Stragglers get backup tasks (the same idea as hedged requests, 2.3).
- Spark generalized it to multi-stage DAGs kept in memory.

### Windowing

An unbounded stream has no "end" to aggregate at, so you aggregate over **windows**:

```
time ---> 12:00      12:01      12:02      12:03

TUMBLING (fixed, non-overlapping, 1 min):
          [----w1----)[----w2----)[----w3----)

SLIDING / HOPPING (1 min long, every 30 s):
          [----w1----)
               [----w2----)
                    [----w3----)

SESSION (gap-based, per key, closes after 30 s of inactivity):
  user A: [*  * *]           [* *    *]
```

### Event time vs processing time, and watermarks

- **Event time**: when the click actually happened (timestamp set on the phone or edge server).
- **Processing time**: when the stream processor sees it.

They differ because of network delays, mobile devices going offline, and consumer lag. If you window by processing time, a 5-minute consumer outage dumps all those clicks into the wrong minute. Window by event time for correct results. But then: when is the 12:00–12:01 window "done"?

A **watermark** is the processor's assertion "I believe I've now seen all events with event time ≤ T". When the watermark passes the end of a window, the window fires.

```
event time of arriving clicks:  12:00:58  12:00:59  12:01:03  12:00:57(late!)
watermark (max seen - 5 s):      12:00:53  12:00:54  12:00:58  12:00:58
                                                      ^
                       window [12:00,12:01) fires when watermark >= 12:01:00

 Too-aggressive watermark -> fires early, misses late events (wrong counts)
 Too-conservative         -> results delayed
 Late events after firing -> drop, or allowed-lateness + emit an updated result
```

Watermarks are a heuristic trade-off between latency and completeness. There's no global clock (3.1) to tell you when everything has arrived.

### Lambda vs Kappa

- **Lambda architecture**: a stream layer for fast approximate results plus a batch layer that recomputes exact results later; two codebases computing the same thing.
- **Kappa architecture**: stream only; to recompute, replay the log (10.2) through a new version of the job. Works because Kafka retains history.

> 💡 **Interview tip:** For a "clicks per link per minute" dashboard: "Kafka topic keyed by short code, a Flink job with 1-minute tumbling event-time windows and a watermark allowing ~1 minute of lateness, writing aggregates to a column store. Exact daily numbers come from a nightly batch over the raw logs in S3." That shows you know both and why each exists.

> 🔗 **Connections:** Event time vs processing time is the clock problem of Part 3 in data-pipeline form. Exactly-once in Flink is checkpoints + replayable offsets (10.3). Approximate stream aggregates often use sketches: Count-Min for heavy hitters (13.2), HyperLogLog for unique visitors (13.3).

---

# 11. Scalability Building Blocks

This part is the parts bin: the standard components that appear in nearly every design, from load balancers and CDNs to ID generators and rate limiters. Interviewers expect you to place these without hesitation and to know the one or two trade-offs that matter for each.

## 11.1 Vertical vs Horizontal Scaling; Stateless Services

**Vertical scaling (scale up)**: buy a bigger machine. **Horizontal scaling (scale out)**: add more machines.

| | Vertical | Horizontal |
| :---- | :---- | :---- |
| Effort | None: same code, bigger box | Code must be distributable |
| Ceiling | Hard limit (~hundreds of cores, a few TB RAM per box) | Practically unbounded |
| Cost curve | Super-linear at the high end | Roughly linear |
| Failure | One box = one failure domain | Lose 1 of N; survive with headroom (12.2) |
| Consistency | Easy: one machine, one memory | All of Parts 4–6 |

Don't dismiss vertical scaling. A single modern Postgres box can handle a lot, and "one machine first" (14.3) is a legitimate NALSD step: it tells you *where* scaling is actually needed. But the app tier should scale horizontally from day one, and that requires **statelessness**.

### Stateless services

A **stateless** service keeps no client-specific state in its own memory between requests. Any instance can serve any request, so you can add, remove, or kill instances freely, and a load balancer can spread requests with no affinity.

```
STATEFUL (sticky)                         STATELESS
LB --(user 42 always)--> app-1 [session]  LB --> any of app-1..app-N
       app-1 dies -> session lost                    |
                                                     v
                                     shared state: Redis (sessions), DB,
                                     or a signed token the client carries
```

Where the state goes instead:

- **Shared store**: Redis/Memcached for sessions, the DB for durable data.
- **Client-side**: a signed token (JWT, 11.9) or cookie carries the session.
- **Partitioned stateful tier**: when you genuinely need state in memory (a game server, a chat connection), route by key (7.7) and accept that losing a node loses or moves that state.

The URL shortener's redirect tier is the ideal stateless service: input is a short code, output is a 302. All state is in cache and DB.

> 🔗 **Connections:** Stateless tiers are what make autoscaling, rolling deploys (12.7) and N+1/N+2 headroom (1.7, 12.2) trivial. The hard part of a system is always the stateful tier: replication (Part 5) and partitioning (Part 7).

## 11.2 Load Balancing (L4 vs L7, algorithms, health checks)

A **load balancer (LB)** spreads requests across a pool of backends and removes unhealthy ones.

### L4 vs L7

| | **L4 (transport)** | **L7 (application)** |
| :---- | :---- | :---- |
| Sees | IP + port, TCP/UDP packets | Full HTTP request: path, headers, cookies |
| Routing by | 5-tuple hash | URL path, host, header, method |
| TLS | Passes through (or not at all) | Usually terminates TLS |
| Cost per request | Very low; can forward packets without full TCP termination | Higher: parse HTTP, buffer |
| Features | Raw throughput, any protocol | Retries, header rewrite, auth, per-request balancing over HTTP/2 |
| Examples | AWS NLB, Google Maglev, IPVS, Katran | Envoy, nginx, HAProxy, AWS ALB |

A common layout: **L4 at the edge** (spreads TCP connections across a fleet of L7 proxies) → **L7 proxies** (terminate TLS, route `/api/*` vs `/{code}`) → app servers.

One L7 subtlety: with long-lived HTTP/2 or gRPC connections, an L4 balancer balances *connections*, not *requests*. One busy client stuck to one backend creates imbalance. You need L7 (per-request) balancing for gRPC.

### Algorithms

| Algorithm | How | Good / bad |
| :---- | :---- | :---- |
| **Round robin** | Next backend in order | Simple; ignores load and heterogeneous costs |
| **Weighted round robin** | Proportional to weights | Mixed instance sizes, canaries (12.7) |
| **Least connections / least outstanding requests** | Fewest in-flight requests | Adapts to slow backends (Little's Law: slow ⇒ more in flight, 2.4) |
| **Power of two choices** | Pick 2 random backends, send to less loaded | Near-optimal, no global state; Envoy's least-request balancer uses it by default |
| **Hash (consistent/Maglev/ring)** | `hash(key)` → backend | Affinity for caches or sharded state (7.4, 13.5) |
| **Random** | Uniform random | Surprisingly fine at scale; no coordination |

Why power of two choices is great: with many LBs each choosing "the least loaded" from stale global data, they all pick the *same* idle server and stampede it. Random sampling of two avoids herding while still steering away from the worst backends.

### Health checks

- **Active**: the LB probes `GET /healthz` every few seconds; N failures → remove, M successes → re-add.
- **Passive (outlier detection)**: watch real traffic; eject a backend returning 5xx or timing out (Envoy outlier detection).
- Use **readiness**, not liveness, semantics (12.6): "can this instance serve traffic now?"
- **Fail open**: if *all* backends look unhealthy, the health check itself is probably wrong (or a shared dependency is down). Envoy's **panic threshold** ignores health status and spreads over all hosts instead of sending everything to the last 2 survivors and killing them too (12.3).

### The LB isn't a single point of failure

LBs run as a fleet: several instances behind the same virtual IP via ECMP routing or anycast (11.3), with DNS returning several addresses. Managed LBs (ALB, Google Cloud LB) are already distributed.

> 💡 **Interview tip:** Say "L4 at the edge, L7 for routing, least-outstanding-requests or power-of-two-choices, active plus passive health checks". If the backend is a cache tier, switch to consistent hashing for hit ratio (8.2) and explain why.

> 🔗 **Connections:** Consistent hashing in the LB is 7.4/13.5. Health checks are failure detectors (6.9) and suffer the same false-positive problems. LB retries must follow retry budgets (2.7), or they amplify outages (12.3).

## 11.3 DNS, GeoDNS and Anycast

**DNS** maps a name (`sho.rt`) to IP addresses. It's the first hop of every request (0.3) and a powerful, coarse-grained load-balancing tool.

```
browser -> OS stub resolver -> recursive resolver (ISP / 8.8.8.8)
              cache?               cache? else walks:
                                   root -> .rt TLD -> sho.rt authoritative NS
                                                      returns A 203.0.113.10
                                                      TTL 60
```

- **TTL** controls caching. Low TTL (30–60 s) lets you fail over quickly but costs more lookups; high TTL is cheap but slow to change. Many resolvers and clients cap or ignore TTLs, so DNS failover is always somewhat slow and leaky.
- Returning multiple A records gives crude client-side spreading.

### GeoDNS / latency-based DNS

The authoritative server answers **differently depending on who asks**: EU resolvers get the EU load balancer's IP, US resolvers get the US one (Route 53 latency/geo routing, NS1, Cloudflare). It also removes unhealthy regions from answers.

Caveat: the server sees the **resolver's** IP, not the user's. A user on a far-away public resolver gets a bad answer. The **EDNS Client Subnet** extension passes a truncated client subnet to fix that, where supported.

### Anycast

With **anycast**, the *same IP address* is announced via BGP from many locations. Internet routing delivers each packet to the "closest" (in BGP terms) location.

```
          announces 198.51.100.1 from all PoPs
   +-----------+     +-----------+     +-----------+
   | PoP Paris |     | PoP Ohio  |     | PoP Tokyo |
   +-----^-----+     +-----^-----+     +-----^-----+
         |                 |                 |
     EU users          US users          JP users     (same IP, different
                                                       physical servers)
```

- Great for DNS itself (UDP, single packet) and for CDN/edge front doors (Cloudflare, Google's front end).
- Failover is automatic: withdraw the route and traffic flows to the next PoP.
- Caveat for TCP: if routing changes mid-connection, packets land at a different PoP that doesn't know the connection. In practice route changes are rare enough; providers mitigate with connection-aware L4 layers.

| | GeoDNS | Anycast |
| :---- | :---- | :---- |
| Granularity | Per DNS answer (per resolver) | Per packet route |
| Failover speed | TTL-bound (seconds to minutes, leaky) | BGP convergence (seconds) |
| Needs | Smart authoritative DNS | Own IP space + BGP at many sites |
| Typical use | Pick a region | Pick an edge PoP; DNS servers |

> 💡 **Interview tip:** For a multi-region URL shortener: "Anycast or GeoDNS sends users to the nearest region's LB; health-checked DNS drops a failed region." Then mention the consequence: a user may be routed to a different region than the one that accepted their write, which brings in replication lag (5.4, 14.8).

> 🔗 **Connections:** DNS is the oldest eventually consistent, heavily cached system in production: TTLs are cache invalidation (8.5) with no explicit invalidation at all. GeoDNS is how active-active multi-region (5.8) gets its traffic split.

## 11.4 CDNs

A **CDN (Content Delivery Network)** is a globally distributed fleet of caching reverse proxies (**edge PoPs**) close to users. Examples: Cloudflare, Akamai, Fastly, CloudFront, Google Cloud CDN.

```
user (Berlin) --~5 ms--> edge PoP Frankfurt --cache HIT--> response
                               |
                            MISS (~1-10% of requests, ideally)
                               v
                         origin shield (one regional mid-tier cache)
                               |  MISS
                               v
                         origin (your LB in us-east) ~90 ms away
```

What it buys:

- **Latency**: RTT to a nearby PoP is a few ms vs ~100+ ms cross-continent (1.1). TLS handshake also terminates at the edge.
- **Offload**: a 95% hit ratio means origin sees 5% of traffic (8.2).
- **DDoS absorption**: huge aggregate capacity in front of your origin.

Key concepts:

- **Pull CDN**: edge fetches from origin on a miss and caches by `Cache-Control` (8.9). Default for most sites. **Push CDN**: you upload content ahead of time (large static assets, video).
- **Cache key**: by default URL (+ host); varying on headers/cookies fragments the cache and kills the hit ratio.
- **Origin shield**: a designated mid-tier PoP so 300 edge misses become 1 origin request (request coalescing at the CDN level, 8.7).
- **Purge/invalidation**: API to evict a URL from all PoPs, usually taking seconds globally; combine with short TTLs.
- **Edge compute** (Cloudflare Workers, Lambda@Edge): run small logic at the PoP, e.g. a KV lookup for redirects.

### CDN and the URL shortener

Redirects are cacheable, but with a big trade-off (8.9):

- **301** + long `max-age`: browsers and CDN cache the redirect. Very cheap, but you lose click analytics for cached hits and **can't change the destination** until caches expire.
- **302/307** + `Cache-Control: private, max-age=0` or short CDN-only TTL (`s-maxage=60`): every click is visible (or at least every miss), destination edits take effect within the TTL plus a purge.

Typical answer: 302 with a short CDN TTL, purge on destination change, analytics logged at the edge.

> 🔗 **Connections:** CDN hit-ratio math is 8.2; invalidation by purge is 8.5; stampede protection is 8.7. Anycast (11.3) is how users reach the nearest PoP.

## 11.5 API Gateways, Reverse Proxies, Service Discovery, Service Mesh

These names overlap a lot. They're all proxies; what differs is *where* they sit and *what* they're responsible for.

```
internet
   |
   v
[ L4 LB ] -> [ API gateway / edge L7 proxy ]   north-south traffic
                 auth, rate limit, routing, TLS
                    |                 |
                    v                 v
             +-------------+   +-------------+
             | svc A       |   | svc B       |
             | [sidecar]---+---+->[sidecar]  |   east-west traffic,
             +-------------+   +-------------+   service mesh (mTLS, retries)
                    \                 /
                     v               v
                  service registry (Consul / etcd / k8s API)
```

| Component | Job | Examples |
| :---- | :---- | :---- |
| **Reverse proxy** | Sits in front of servers: TLS termination, compression, caching, buffering slow clients | nginx, HAProxy, Envoy |
| **API gateway** | Reverse proxy + API concerns: authentication, rate limiting (11.7), request validation, routing `/v1/*` to services, API keys, quotas | Kong, AWS API Gateway, Apigee, Envoy Gateway |
| **Service discovery** | "Where are the healthy instances of service B right now?" | Consul, etcd, ZooKeeper, Kubernetes Services + DNS, Eureka |
| **Service mesh** | Per-service sidecar (or node) proxies doing mTLS, retries, timeouts, circuit breaking, telemetry for *every* internal call | Istio, Linkerd, Consul Connect (Envoy data plane) |

### Service discovery patterns

- **Client-side discovery**: the client queries the registry and picks an instance itself (gRPC with xDS, Netflix Eureka + Ribbon). Fewer hops; every client needs the logic.
- **Server-side discovery**: the client calls a stable name/VIP, a load balancer consults the registry (Kubernetes `ClusterIP` Service, AWS ALB target groups). Simple clients; one more hop.
- Registration: instances register themselves with a **TTL/heartbeat** (6.9), or the orchestrator registers them. Stale registrations are the main failure mode: always pair with health checks.

The registry itself must be highly available and consistent enough: that's why it's built on consensus stores (etcd, ZooKeeper, Consul's Raft, 6.8).

### Why a service mesh

Without a mesh, every service in every language must implement timeouts, retries with budgets, circuit breakers, mTLS and tracing correctly. A mesh moves that into the proxy, configured centrally. Cost: an extra hop per call (~sub-ms each), more moving parts, and a control plane that can itself fail.

> 🔗 **Connections:** Mesh policies implement 2.7 (retries), 12.4 (circuit breakers), 11.9 (mTLS) and 12.9 (traces). Service discovery is membership (6.9) built on coordination services (6.8).

## 11.6 Unique ID Generation (auto-increment, UUID, Snowflake, ticket server, pre-allocated ranges, base62)

On one machine, IDs are trivial: `AUTO_INCREMENT`. Across many machines, you need IDs that are **unique** without coordinating on every write, and often **short**, **roughly time-ordered**, or **unguessable**. The URL shortener's short code is exactly this problem (14.6).

### The options

| Approach | Size | Sortable by time | Coordination | Guessable | Notes |
| :---- | :---- | :---- | :---- | :---- | :---- |
| **DB auto-increment** | 64-bit | Yes | Every insert hits one DB | Yes | Single serial bottleneck (2.5) and SPOF |
| **UUID v4** | 128-bit (122 random) | No | None | No | Random inserts fragment B-tree indexes (9.2) |
| **UUID v7** | 128-bit | Yes (48-bit ms prefix) | None | Partly (time part) | Index-friendly UUID |
| **Snowflake** | 64-bit | Roughly (ms) | Assign worker IDs once | Partly | Twitter, 2010; many variants |
| **Ticket server** | 64-bit | Yes | Every ID (or batch) | Yes | Flickr: MySQL `REPLACE INTO` + `LAST_INSERT_ID()` |
| **Pre-allocated ranges** | Any | Roughly | Once per block | Yes, unless scrambled | Counter blocks leased from etcd/DB |
| **Random + check** | Any (e.g. 7 base62 chars) | No | Uniqueness check per insert | No | Simple; collisions rise as space fills |

### Snowflake

A **Snowflake ID** packs a timestamp, a machine ID and a per-millisecond counter into one 64-bit integer:

```
 bit 63  62                                 22 21          12 11          0
+-----+--------------------------------------+--------------+-------------+
|  0  |  timestamp: ms since custom epoch    |  machine ID  |  sequence   |
|sign |              41 bits                 |   10 bits    |   12 bits   |
+-----+--------------------------------------+--------------+-------------+
                                             (5 DC + 5 worker in Twitter's)

 41 bits of ms : 2^41 ms = 2.2 x 10^12 ms  = ~69 years from the epoch
 10 bits       : 2^10    = 1,024 generators
 12 bits       : 2^12    = 4,096 IDs per ms per generator
               = ~4.1 million IDs/s per generator
```

- Generated **locally** with no network call: read clock, combine with machine ID, bump sequence. If the sequence overflows within one ms, wait for the next ms.
- IDs are **k-sorted** (roughly ordered by time), which keeps B-tree inserts at the right edge and makes "newest first" cheap.
- **Machine IDs must be unique**: assign them once from ZooKeeper/etcd (6.8) or from config. Two generators with the same ID will produce duplicates.
- **Clock going backwards** (NTP step, 3.1) can produce duplicates. Implementations refuse to generate (or wait) if the clock is behind the last timestamp used. This is why Snowflake should read a clock that never steps backward (3.2) or guard explicitly.

### Ticket server and pre-allocated ranges

A **ticket server** is a dedicated DB whose only job is handing out increasing numbers. Flickr ran two, one issuing odd and one even numbers (`auto_increment_increment = 2`, offsets 1 and 2), for availability.

**Pre-allocated ranges** remove the per-ID round trip: each app server leases a block of the counter and hands out IDs from memory.

```
 coordinator (etcd key "next_block" or a DB row, updated with CAS)
      |
      |-- app-1 leases [1,000,000 .. 1,999,999]
      |-- app-2 leases [2,000,000 .. 2,999,999]
      `-- app-3 leases [3,000,000 .. 3,999,999]

 one coordinator write per 1M IDs. app-2 crashes -> unused rest of its
 block is skipped (gaps are fine; duplicates are not)
```

### Base62 and URL shortener short codes

Short codes use **base62**: `[0-9a-zA-Z]`, 62 URL-safe characters. Each character carries log2(62) ≈ 5.95 bits.

```
 62^6 = 56,800,235,584           ~ 56.8 billion
 62^7 = 3,521,614,606,208        ~ 3.5 trillion
 62^8 = 218,340,105,584,896      ~ 218 trillion

 Need: 100M new URLs/month x 12 x 10 years = 12 billion codes
 7 chars: 3.5T / 12B = ~290x headroom   -> 7 chars is the standard answer
 6 chars: 56.8B / 12B = ~4.7x           -> tight, and random codes would
                                           collide often as the space fills

 A full 64-bit Snowflake ID in base62 needs 11 chars (62^10 < 2^63 < 62^11):
 too long for a "short" link. Snowflake fits internal IDs, not short codes.
```

Encoding a counter: repeated division by 62.

```
 n = 125 -> 125 = 2*62 + 1 -> digits [2, 1] -> "21"  (alphabet 0-9a-zA-Z)
```

Three ways to produce 7-char codes:

1. **Counter (pre-allocated ranges) + base62**: no collisions, no uniqueness check, very fast. Problem: **sequential codes are enumerable**: anyone can crawl `aaaaab`, `aaaaac`, ... and harvest private links. Fix: scramble the counter with a bijection on [0, 62^7) before encoding (e.g. a small Feistel network, or multiply by a constant coprime to 62^7 and take the result modulo 62^7), so codes look random but are still unique.
2. **Random 7 chars + insert-if-not-exists**: unguessable and coordination-free, but each insert must check uniqueness (a conditional write: `INSERT ... ON CONFLICT DO NOTHING`, DynamoDB `attribute_not_exists`). At 12B used out of 3.5T, a new random code collides with probability ~0.34%: retry and it's fine. A Bloom filter (13.1) can pre-screen.
3. **Hash of the long URL** (first 7 chars of base62(SHA-256(url))): same URL → same code (dedup for free), but collisions between different URLs must be handled, and two users shortening the same URL get the same code (bad if links have per-user analytics or expiry).

> 💡 **Interview tip:** Lead with "7 base62 characters gives 62^7 ≈ 3.5 trillion codes, ~290× what we need for 10 years." Then pick: "pre-allocated counter ranges, scrambled so codes aren't enumerable" or "random with conditional insert". Both avoid a per-write global bottleneck (2.5), which is the point the interviewer is probing.

> 🔗 **Connections:** A single auto-increment is an Amdahl serial bottleneck (2.5). Snowflake depends on clock behaviour (3.1, 3.2) and unique worker IDs from a coordination service (6.8). Range leases are leases (6.6) without the fencing complexity, since gaps are harmless. Full design walkthrough in 14.6.

## 11.7 Rate Limiting (token bucket, leaky bucket, fixed/sliding window, distributed)

**Rate limiting** caps how many requests a client (user, API key, IP) can make per unit of time. It protects your capacity from abuse and noisy neighbours, enforces quotas, and is a first line of load shedding (12.5). Over-limit requests get **HTTP 429 Too Many Requests**, ideally with `Retry-After`.

### Token bucket

A bucket holds up to **b** tokens and refills at **r** tokens/s. Each request takes one token; empty bucket → reject.

```
 r = 10 tokens/s, b = 20 (burst)

 tokens
  20 |####                               ######
     |    ##                          ###
  10 |      ##                     ###
     |        ##  burst of 20   ###    refill at 10/s
   0 |__________##############_______________________ time
              ^ 20 requests at once: all allowed, then 10/s sustained
```

- Allows **bursts** up to `b` while enforcing an average of `r`. This matches real clients well.
- State per key is just `(tokens, last_refill_time)`; refill lazily on each request: `tokens = min(b, tokens + r × elapsed)`.
- Used by AWS API Gateway, Stripe, Envoy local rate limit, and many others.

### Leaky bucket

Requests enter a FIFO queue that **drains at a constant rate**; if the queue is full, reject. Output is perfectly smooth, which is good for protecting a fragile downstream, but bursts are delayed rather than served. (As a *meter*, it's equivalent to a token bucket; as a *queue*, it shapes traffic.)

### Fixed window counter

Count requests per key per calendar window (`user42:12:01` → `INCR`, expire after 60 s). Cheap, but has a **boundary burst**:

```
 limit 100/min
        12:00                          12:01                          12:02
          |-----------------------------|-----------------------------|
                               100 reqs ^ 100 reqs
                         at 12:00:59    | at 12:01:00
          => 200 requests in ~1 second, both windows "within limit"
```

### Sliding window log

Store a timestamp per request (Redis sorted set); count those within the last 60 s. Exact, but memory is O(requests per window) per key.

### Sliding window counter

Keep the fixed-window counters for the current and previous window and weight the previous one by how much of it still overlaps:

```
 limit 100/min, now = 15 s into the current minute (25% elapsed)
 previous window count = 80, current window count = 30

 estimate = 30 + 80 x (1 - 0.25) = 30 + 60 = 90   -> allowed (< 100)
```

O(1) memory, smooths the boundary problem, slightly approximate (assumes the previous window's requests were evenly spread). Cloudflare uses this approach.

| Algorithm | Memory/key | Bursts | Accuracy |
| :---- | :---- | :---- | :---- |
| Token bucket | O(1) | Allowed up to b | Exact for its model |
| Leaky bucket (queue) | O(queue) | Smoothed | Exact |
| Fixed window | O(1) | Up to 2× at boundaries | Coarse |
| Sliding log | O(N) | None | Exact |
| Sliding window counter | O(1) | Small | ~Approximate |

### Distributed rate limiting

With 50 API servers behind an LB, a per-user limit must be shared:

- **Central store**: Redis holds the counters. Read-modify-write must be **atomic**: a naive `GET` then `SET` from two servers races (lost update, 9.6). Use `INCR` + `EXPIRE`, or a **Lua script** that implements the token bucket atomically. Cost: one Redis round trip (~sub-ms in-zone) per request; Redis becomes a dependency, so decide to **fail open** (allow) if it's unreachable.
- **Local limits**: each of N servers enforces `limit / N`. No network hop, but inaccurate if traffic isn't evenly spread (and N changes with autoscaling).
- **Hybrid**: local token buckets that sync with the central store periodically (e.g. every 100 ms), accepting brief over-admission. Envoy's global rate limit service and many CDNs work roughly this way.
- **Shard by key** (7.3) so one Redis doesn't hold every counter; a hot key (one abusive IP) is a hot shard (7.5).

For the URL shortener: limit `POST /shorten` per API key and per IP (say 100/hour anonymous) to stop spam and code-space exhaustion; limit redirects only as DDoS protection at the CDN/edge.

> 💡 **Interview tip:** Name token bucket, give its two parameters (rate, burst), and say where it lives: "at the API gateway (11.5), state in Redis with an atomic Lua script, failing open if Redis is down." Mention the fixed-window boundary burst if asked why not the simplest option.

> 🔗 **Connections:** Rate limiting is load shedding with fairness (12.5). Counting heavy users cheaply at the edge can use Count-Min Sketch (13.2). Retry-After cooperates with client backoff (2.7).

## 11.8 Pagination (offset vs cursor)

Any list endpoint ("my links", "click history") needs pagination.

### Offset pagination

```sql
SELECT * FROM urls WHERE owner = 42 ORDER BY created_at DESC
LIMIT 20 OFFSET 10000;
```

- Simple; supports "jump to page 500".
- **Slow for deep pages**: the DB still reads and discards the first 10,000 rows. Cost grows linearly with the offset.
- **Unstable**: if a new link is created between page 1 and page 2, everything shifts by one, and the user sees a duplicate (or misses an item on deletes).
- Across shards (7.9) it's worse: to get global rows 10,000–10,020, each shard must return its top 10,020.

### Cursor (keyset) pagination

Remember where you stopped, and continue from there using an index:

```sql
-- first page
SELECT code, created_at, id FROM urls WHERE owner = 42
ORDER BY created_at DESC, id DESC LIMIT 20;

-- next page: the client sends back the last row's (created_at, id)
SELECT code, created_at, id FROM urls WHERE owner = 42
  AND (created_at, id) < ('2026-10-01 12:00:00', 98231)
ORDER BY created_at DESC, id DESC LIMIT 20;
```

- **O(page size)** regardless of depth: an index seek, then 20 rows.
- **Stable** under inserts and deletes: no duplicates or gaps from shifting.
- Needs a **unique, ordered sort key**: add the ID as a tie-breaker, since timestamps collide.
- Give clients an **opaque cursor** (base64 of `{created_at, id}`, ideally signed) so you can change the internals.
- Can't jump to an arbitrary page; that's rarely needed (infinite scroll, "next" links, API iteration).

| | Offset | Cursor |
| :---- | :---- | :---- |
| Deep page cost | O(offset + limit) | O(limit) |
| Consistency under writes | Duplicates/gaps | Stable |
| Random page access | Yes | No |
| Examples | Admin UIs, small tables | Stripe, Slack, GitHub GraphQL, DynamoDB `LastEvaluatedKey`, Kafka offsets |

> 🔗 **Connections:** Keyset pagination is a B-tree range scan (9.2) starting at a seek. Snowflake/UUIDv7 IDs (11.6) make great cursors because they're time-ordered and unique. Kafka offsets (10.2) are cursors into a log.

## 11.9 Security Basics (AuthN vs AuthZ, OAuth2/OIDC, JWT, TLS/mTLS, encryption at rest, abuse prevention)

Design interviews rarely go deep on security, but you should place the standard pieces correctly.

### AuthN vs AuthZ

- **Authentication (AuthN)**: *who are you?* Password + MFA, SSO, API key, client certificate.
- **Authorization (AuthZ)**: *what may you do?* "User 42 may edit link `aZ3kQ9x` because they own it." Models: RBAC (roles), ABAC (attributes), ReBAC (relationships, e.g. Google Zanzibar, OpenFGA).

Authenticate once at the edge (gateway, 11.5); authorize close to the data, in the service that owns the resource.

### OAuth 2.0 and OIDC

- **OAuth 2.0** is a *delegated authorization* framework: a user lets an app access their resources at another service without giving it their password. The app gets an **access token** with scopes (`links:read`).
- The **authorization code flow with PKCE** is the standard for web and mobile apps: browser is redirected to the identity provider, user logs in, the app receives a short-lived code and exchanges it for tokens.
- **OpenID Connect (OIDC)** adds *authentication* on top: an **ID token** (a JWT) saying who the user is. "Sign in with Google" is OIDC.
- **Client credentials** flow is for service-to-service calls with no user.

### JWT

A **JWT** is `base64url(header).base64url(payload).signature`:

```
{ "alg": "RS256", "kid": "key-2026-10" }          header
{ "sub": "user42", "scope": "links:write",
  "iss": "https://auth.sho.rt", "exp": 1793000000 } payload (claims)
signature = RSA-sign(header.payload, issuer private key)
```

- Any service can **verify it locally** with the issuer's public key: no session lookup per request, which fits stateless services (11.1).
- It's **signed, not encrypted**: anyone can read the payload. Don't put secrets in it.
- The catch: **revocation**. A stolen token is valid until `exp`. Keep access tokens short-lived (~5–15 min) with refresh tokens, or keep a denylist for emergencies (which reintroduces a lookup).
- Validate `alg` explicitly (classic bug: accepting `alg: none`), plus `iss`, `aud`, `exp`.

### TLS and mTLS

- **TLS** gives encryption, integrity and *server* authentication (certificate chain to a trusted CA). TLS 1.3 needs **1 RTT** for a full handshake (on top of TCP's), which is why terminating TLS at a nearby edge PoP (11.4) helps latency so much.
- **mTLS** (mutual TLS): the client also presents a certificate. Standard for service-to-service traffic inside a zero-trust network; a service mesh (11.5) issues and rotates the certs automatically (SPIFFE identities in Istio).

### Encryption at rest

Disks, backups and object storage are encrypted, typically with **envelope encryption**: data encrypted with a data key (DEK), the DEK encrypted by a key-encryption key held in a **KMS** (AWS KMS, Cloud KMS, Vault). Rotating the KEK only re-wraps small DEKs. Deleting a key makes data unreadable ("crypto-shredding", also handy for event-sourced GDPR deletion, 10.7).

### Abuse prevention (URL shortener)

URL shorteners are a favourite tool for phishing and malware because they hide the destination. Expect this to come up:

| Threat | Mitigation |
| :---- | :---- |
| Spam / phishing links | Check destinations against threat lists (e.g. Google Safe Browsing) at creation, re-scan asynchronously (10.1), disable on reports |
| Bulk creation / code-space exhaustion | Rate limit `POST /shorten` per key/IP (11.7), CAPTCHA for anonymous, require auth for high volume |
| Enumerating private links | Non-sequential codes (11.6); longer codes for "unlisted" links |
| Open-redirect abuse | Interstitial warning page for flagged or new-and-suspicious links |
| DDoS on redirects | CDN + anycast absorption (11.3, 11.4), edge rate limits |

> 💡 **Interview tip:** One sentence covers most of it: "TLS at the edge, mTLS internally via the mesh, OIDC for user login, short-lived JWTs verified at the gateway, authorization in the owning service, rate limits and Safe-Browsing checks on link creation."

> 🔗 **Connections:** JWT verification keeps the app tier stateless (11.1). Abuse scanning is an async consumer (10.1). Key management services are replicated with consensus (6.8) because losing keys loses data.

---

# 12. Reliability, Resilience and Observability

Everything fails eventually: disks, machines, racks, AZs, deploys, dependencies, and your own retry logic. This part is about deciding how reliable a system needs to be, building it so failures stay small, and seeing what's happening when they don't. It's where "it works" becomes "it works at 3 a.m. while an AZ is down".

## 12.1 SLIs, SLOs, SLAs and Error Budgets

| Term | What it is | URL shortener example |
| :---- | :---- | :---- |
| **SLI** (indicator) | A measured ratio of good events to total events | Fraction of redirects that return a correct 3xx in < 100 ms, measured at the LB |
| **SLO** (objective) | Target for an SLI over a window | 99.9% of redirects good, over a rolling 30 days |
| **SLA** (agreement) | Contract with consequences (credits, refunds) if missed | 99.5% monthly availability for paying API customers |
| **Error budget** | 1 − SLO: how much failure you're *allowed* | 0.1% of requests, ~43 min of full outage per 30 days |

Rules of thumb:

- SLIs should reflect **what users experience**: measure at the LB or client, not CPU on the box. "Good" usually combines success *and* latency.
- **SLA < SLO**: the internal target is stricter than the contract, so you notice and fix things before paying penalties.
- 100% is the wrong target: users can't tell 99.99% from 100% (their own Wi-Fi is worse), and each extra nine costs ~10× (1.3).

### Error budget math

```
SLO 99.9% over 30 days
 time budget:     30 days x 24 h x 60 min x 0.001 = 43.2 minutes

URL shortener (1.8): 100M URLs/month x 100 redirects = 10B redirects/month
 request budget:  10,000,000,000 x 0.001 = 10 million bad redirects / month

 a 20-minute total outage at average ~4,000 QPS:
   4,000 x 1,200 s = 4.8 million bad requests = ~48% of the monthly budget
```

### Error budgets as a policy

The budget turns "reliability vs velocity" from an argument into arithmetic:

- Budget left → ship features, run experiments, do risky migrations.
- Budget spent → freeze risky launches, prioritize reliability work, until the window recovers.
- It also tells you **what not to fix**: if you're well within SLO, a flaky but rare error isn't urgent.

> 💡 **Interview tip:** In NALSD, state an SLO early: "99.9% of redirects succeed in under 100 ms p99 within a region." It anchors later choices: why you need N+1, multi-AZ (12.2), and why a cross-region synchronous call on the redirect path is a non-starter (~100–150 ms RTT, 1.1).

> 🔗 **Connections:** Nines and serial/parallel availability are in 1.3; SLO alerting is 12.10. Tail latency SLIs connect to 2.3: an SLO on p99 is an SLO on the slowest 1%.

## 12.2 Redundancy and Failure Domains

**Redundancy** means having more than you need so a failure doesn't cause an outage. It only works if the redundant copies **don't fail together**. A **failure domain** is a set of components that share a single point of failure and so can fail together.

```
 failure domains, nested (bigger = rarer, but more damaging)

 +------------------------------- region ------------------------------+
 | +------------ AZ a ------------+  +--- AZ b ---+  +--- AZ c ---+    |
 | | +--- rack ---+ +--- rack ---+|  |            |  |            |    |
 | | | host  host | | host  host ||  |    ...     |  |    ...     |    |
 | | | [proc]     | |            ||  |            |  |            |    |
 | | +-- ToR switch, PDU --------+|  |            |  |            |    |
 | +-- power, cooling, network ---+  +------------+  +------------+    |
 +---- control plane, regional services, shared config ---------------+
   + global: DNS, deploy pipeline, config push, your own bad release
```

| Domain | Typical cause | Defence |
| :---- | :---- | :---- |
| Process | Crash, OOM, GC pause | Multiple instances + LB health checks (11.2) |
| Host | Hardware fault, kernel panic | N+1 hosts |
| Rack | ToR switch, power distribution | Spread replicas across racks (rack-aware placement in Cassandra, HDFS) |
| AZ | Power, cooling, network partition of a datacenter | Run in ≥ 3 AZs, replicas spread across them |
| Region | Regional control-plane bug, large disaster | Multi-region (5.8), DR (12.8) |
| Global | Bad config/deploy pushed everywhere, DNS, certificates expiring | Staged rollouts (12.7), regional isolation of config |

The last row is the most common cause of big outages: **correlated failures from change**, not hardware. Redundancy doesn't help if every copy runs the same bad config.

### Capacity math: surviving the loss of one of 3 AZs

Redundancy must include **capacity**, not just copies. From the URL shortener envelope (1.8, 1.7): peak 12,000 QPS ÷ 2,000 QPS per server = **6 servers needed** at peak.

```
 Spread across 3 AZs. If one AZ dies, only 2/3 of the fleet survives,
 and those survivors must still carry 100% of peak.

   servers to deploy = needed / surviving fraction = 6 / (2/3) = 9

   +---- AZ a ----+  +---- AZ b ----+  +---- AZ c ----+
   |  s1  s2  s3  |  |  s4  s5  s6  |  |  s7  s8  s9  |
   +--------------+  +--------------+  +--------------+
   normal peak: 12,000 / 9 = ~1,333 QPS per server   (~33% CPU)
   AZ c lost:   12,000 / 6 =  2,000 QPS per server   (~50% CPU, the
                                                      planned ceiling)

 Compare: 2 AZs -> 6 / (1/2) = 12 servers (100% overhead)
          3 AZs -> 9 servers (50% overhead)
          4 AZs -> 6 / (3/4) = 8 servers (33% overhead)
```

More, smaller failure domains are cheaper to protect. Note the 2,000 QPS/server figure already runs servers at ~50% CPU (2.4), so in the AZ-loss case you're at the planned ceiling, not at collapse. If you also want to survive an AZ loss *while* a deploy or one extra host is down, add N+1 on top: 6 + 1 = 7 needed after the loss, ÷ 2/3 = 10.5 → 11, and in practice 12 (4 per AZ) to keep the AZs symmetric.

### Replicas across failure domains

- A Raft/Paxos group (6.2) of **3 replicas, one per AZ** survives losing any AZ (2 of 3 = majority).
- **5 replicas across 3 AZs** placed 2/2/1: lose an AZ with 2 → 3 of 5 left, still a majority.
- A Dynamo-style store with N = 3 needs replicas in different AZs, not just different hosts, or one AZ outage takes all 3 copies.

> 💡 **Interview tip:** Always say the formula out loud: "Needed capacity divided by the fraction that survives the largest failure I'm designing for: 6 ÷ 2/3 = 9, three per AZ." Interviewers check whether your headroom matches your failure domain, not just whether you have "some" redundancy.

> 🔗 **Connections:** This is the same arithmetic as 1.7, applied. Quorum sizing (6.2), multi-AZ vs multi-region (5.8), and the failure walkthrough in 14.9 all build on it.

## 12.3 Cascading Failures

A **cascading failure** is a failure that grows by feeding on itself: one part's failure increases load on the others until they fail too. Most large outages are cascades, not single failures.

### Mechanism 1: lost capacity → overload → more lost capacity

```
 9 servers at peak, ~1,333 QPS each (fine)
 AZ c lost -> 6 servers at 2,000 QPS (at the planned limit, fine)
 + traffic spike 20% -> 6 servers at 2,400 QPS -> latency climbs
 -> health checks time out -> LB ejects 2 "unhealthy" servers
 -> 4 servers at 3,600 QPS -> they fail health checks too
 -> 0 servers.  The health check turned "slow" into "dead".
```

### Mechanism 2: latency → queueing → resource exhaustion

Little's Law (2.4): in-flight requests = arrival rate × latency. If a dependency slows from 10 ms to 1 s, in-flight requests rise **100×** at the same QPS:

```
 12,000 QPS x 0.010 s = 120 in flight      (normal)
 12,000 QPS x 1.000 s = 12,000 in flight   (dependency slow)

 Thread pool of 200 per server x 9 = 1,800 -> exhausted.
 Requests queue, queue memory grows, GC thrashes, everything times out,
 including requests that don't even touch the slow dependency.
```

### Mechanism 3: retries amplify load

When a service gets slow, clients time out and retry, multiplying load exactly when capacity is lowest. With retries at 3 layers × 3 attempts, a failing leaf sees up to 27× the traffic (2.7).

### Mechanism 4: cold caches and thundering herds

A cache cluster restarts or a hot key expires: the DB suddenly gets the full read load it was never sized for (8.7). With a 95% hit ratio, losing the cache is a **20×** load increase on the DB.

### The usual defences

| Defence | Breaks which loop | Section |
| :---- | :---- | :---- |
| Timeouts based on downstream p99 | Stops waiting forever, frees threads | 2.7 |
| Retry budgets, retries at one layer, jitter | Retry amplification | 2.7 |
| Circuit breakers | Stop calling a failing dependency | 12.4 |
| Bulkheads (separate pools) | One slow dependency can't eat all threads | 12.4 |
| Load shedding, bounded queues | Overloaded servers reject quickly instead of dying slowly | 12.5 |
| Panic thresholds in LBs | Don't eject everyone for being slow | 11.2 |
| Capacity headroom per failure domain | Survive the loss without overload | 12.2 |
| Request coalescing, stale-while-revalidate | Cache-loss stampedes | 8.7 |

Recovery is its own problem: when the overloaded service comes back, the queued backlog and all retries arrive at once and knock it over again. Bring traffic back gradually (ramp the LB weight), and keep shedding until queues drain.

> 🔗 **Connections:** Cascades are where Parts 2, 8 and 12 meet: Little's Law (2.4), retries (2.7), cache stampedes (8.7), and backpressure (10.5) all describe the same positive feedback loop.

## 12.4 Circuit Breakers and Bulkheads

### Circuit breaker

A **circuit breaker** wraps calls to a dependency and stops calling it when it's failing, so you fail fast instead of tying up threads on timeouts and hammering a struggling service.

```
                 failures > threshold
                 (e.g. >50% of last 100 calls, or 5 consecutive)
     +--------+ ------------------------------> +--------+
     | CLOSED |                                 |  OPEN  |  calls fail
     | (calls |                                 | (fail  |  immediately
     |  pass) | <---+                           |  fast) |  (or fallback)
     +--------+     |                           +--------+
                    |                               |
          probe     |                               | after cool-down
          succeeds  |       +-----------+           | (e.g. 30 s)
                    +------ | HALF-OPEN | <---------+
                            | let a few |
                            |  probes   | ---probe fails---> back to OPEN
                            +-----------+
```

- While **open**, return a **fallback**: cached/stale data, a default, a degraded response (12.5), or a fast error.
- Thresholds should be on **error rate over a window** with a minimum request count, not a single failure.
- Libraries: Resilience4j (Java), Polly (.NET), Hystrix (Netflix, now retired); Envoy does it at the proxy level (max connections, max pending requests, outlier ejection).

URL shortener: the redirect service calls the DB on cache miss. If the DB is failing, the breaker opens: cache hits still work (most traffic), misses fail fast with a 503 instead of holding threads for the full timeout.

### Bulkheads

Named after a ship's watertight compartments: **isolate resources** so one failure can't sink everything.

```
 WITHOUT bulkheads                 WITH bulkheads
 +---------------------------+     +-------------+-------------+--------+
 | one pool of 200 threads   |     | redirect    | create-link | stats  |
 | stats DB slow -> all 200  |     | path: 150   | path: 30    | pool:20|
 | threads stuck on stats -> |     | threads     | threads     | (stuck |
 | redirects fail too        |     |  (fine)     |  (fine)     |  only) |
 +---------------------------+     +-------------+-------------+--------+
```

Bulkheads at different scales:

- **Thread / connection pools** per dependency.
- **Separate deployments** for critical vs non-critical endpoints (redirect fleet separate from the analytics API).
- **Cells / shards**: split users into independent cells, each a full copy of the stack, so a bad deploy or poison request affects one cell (AWS calls this cell-based architecture). **Shuffle sharding** assigns each customer a random small subset of workers so two customers rarely share the exact same set.

> 💡 **Interview tip:** Pair them: "Bulkheads limit *how much* a bad dependency can consume; circuit breakers limit *how long* we keep trying it." Then say what the fallback is, because a breaker with no fallback just turns slow errors into fast errors (which is still better).

> 🔗 **Connections:** Circuit breakers are the client-side complement to load shedding (12.5) and need good failure detection (6.9). Service meshes implement both (11.5). Cells are partitioning (Part 7) applied to failure isolation rather than data size.

## 12.5 Load Shedding and Graceful Degradation

### Load shedding

When a server is beyond capacity, the worst thing it can do is accept everything: every request gets slow, all of them time out, **goodput** (useful completed work) drops to zero. **Load shedding** deliberately rejects some requests so the rest succeed.

```
 goodput
   ^         without shedding: collapses past capacity
   |        ____
   |       /    \
   |      /      \_______
   |     /    with shedding: stays at capacity
   |    /___________________________
   |   /
   +--+---------+------------------------> offered load
            capacity
```

How to shed well:

- **Reject early and cheaply**: return 503 (or 429 for per-client limits, 11.7) *before* doing expensive work. A rejection must cost far less than serving.
- **Signal on concurrency or queue time**, not CPU alone: e.g. reject when in-flight requests > limit, or when a request has waited in the queue > X ms (it'll likely time out anyway).
- **Prioritize**: tag requests with criticality. Shed batch and analytics first, then logged-out traffic, never health checks or the core redirect. Google services propagate a criticality level with each RPC.
- **Deadline propagation**: pass the remaining time budget downstream (gRPC deadlines). If it's already expired, don't start work nobody will wait for.
- **Adaptive concurrency limits** (Netflix concurrency-limits, Envoy adaptive concurrency): measure latency and lower the limit when it grows, like TCP congestion control.
- Under overload, serving the queue **LIFO** (newest first) can help: the oldest requests' clients have likely already given up.

### Graceful degradation

**Graceful degradation** means turning off non-essential features to keep the essential one alive.

| Situation | Degraded behaviour (URL shortener) |
| :---- | :---- |
| Analytics pipeline down | Redirects work; click counts catch up later from Kafka (10.1) |
| Primary DB down | Serve redirects from cache (stale OK); disable link creation with a clear error |
| Cache cluster down | Rate-limit cache-miss traffic to DB; serve top-N hot codes from an in-process cache |
| Overload | Disable link previews and custom-alias checks; keep the 302 path |
| Region down | Other regions serve; newly created, not-yet-replicated links may 404 for a while (5.4, 14.8) |

Design for it explicitly: decide the essential path, and make sure it doesn't depend on non-essential components (no synchronous analytics write on redirect).

> 🔗 **Connections:** Shedding is the server side of backpressure (10.5) and complements circuit breakers (12.4). Rate limiting (11.7) is per-client shedding. Serving stale data on failure is a deliberate availability-over-consistency choice (4.6).

## 12.6 Health Checks: Liveness vs Readiness

Orchestrators and load balancers ask two different questions; mixing them up causes outages.

| | **Liveness** | **Readiness** |
| :---- | :---- | :---- |
| Question | "Is this process stuck beyond repair?" | "Should this instance receive traffic right now?" |
| On failure | **Restart** the container | **Remove from LB**, don't restart |
| Should check | The process itself: event loop responsive, not deadlocked | Warm-up done, caches loaded, can reach *its own* critical deps, not overloaded |
| Should NOT check | Dependencies (DB, cache) | Shared deps in a way that fails *all* instances at once (see below) |

Kubernetes also has a **startup probe**: liveness checks don't start until it passes, so slow-starting apps aren't killed while booting.

### The two classic traps

**1. Liveness checks dependencies.** The DB blips for 30 s; every pod's liveness fails; Kubernetes restarts *every* pod; they all reconnect at once (thundering herd, 8.7) and come up with cold caches. A 30 s DB blip became a 10-minute total outage. Liveness should only fail when restarting would actually fix something.

**2. Readiness fails everywhere at once.** If readiness depends on the shared DB, a DB issue removes every instance from the LB, so even cache-servable requests get errors. Options: readiness checks only things that are per-instance; or rely on the LB's panic threshold / fail-open behaviour (11.2).

```
 GET /livez  -> 200 if the process can respond at all       (cheap, local)
 GET /readyz -> 200 if warmed up, not draining, not overloaded,
                and local resources OK; 503 while shutting down
```

### Graceful shutdown

On deploy: mark not-ready → LB stops sending new requests (wait a few seconds for it to notice) → finish in-flight requests → exit. Skipping the "wait" step drops requests on every deploy.

> 🔗 **Connections:** Health checks are failure detectors (6.9) with the same false-positive vs detection-speed trade-off. They feed LBs (11.2) and safe deploys (12.7), and a bad one can start a cascade (12.3).

## 12.7 Safe Deployments: Canary, Blue/Green, Feature Flags, Rollback

Most outages are caused by **changes**: code, config, schema, infrastructure. Safe deployment is about limiting how much a bad change can hurt (blast radius) and how long it hurts (time to rollback).

### Rolling deployment

Replace instances a few at a time (`maxUnavailable`, `maxSurge` in Kubernetes). Simple, no extra capacity, but old and new versions run side by side, and a bad version reaches everyone fairly quickly.

### Canary

Send a small fraction of traffic to the new version, compare its metrics to the old version, then expand in stages.

```
 time --->
 v2 traffic:  1% --bake 30m--> 10% --bake 1h--> 50% --> 100%
                    |                 |
          compare v2 vs v1:   error rate, p99 latency, saturation (12.10)
          worse?  -> automatic rollback to 0%

 then the next region / cell, one at a time (never all regions at once)
```

- Canary analysis compares **canary vs baseline at the same time**, not vs yesterday, to cancel out traffic changes (Spinnaker/Kayenta, Argo Rollouts, Flagger).
- Stage by **failure domain** too: one AZ, then one region, then the rest. Config changes deserve the same treatment as code.

### Blue/green

Run two full environments; switch the LB from blue (old) to green (new) at once. Rollback is flipping back, which is very fast. Costs double capacity during the switch, and the switch is all-at-once, so pair it with a canary step.

### Feature flags

**Feature flags** decouple *deploy* (code is on servers) from *release* (users see it). Ship the code dark, then turn it on for 1% of users, internal users, or one customer, and turn it off in seconds without a deploy. Flags are config, so they need the same staged rollout discipline, and stale flags must be cleaned up.

### Rollback, and schema changes

Rollback must be **fast and boring**: one command, rehearsed. The thing that most often makes rollback impossible is a **schema or data format change**. Use **expand/contract** (also called parallel change):

```
 1. EXPAND   add new column `dest_v2` (nullable); old code ignores it
 2. DEPLOY   code writes both columns, reads old       <- rollback safe
 3. BACKFILL copy old -> new for existing rows
 4. DEPLOY   code reads new, still writes both         <- rollback safe
 5. CONTRACT stop writing old; later drop old column   <- point of no return
```

The same rule applies to message formats in Kafka (10.2): consumers must handle both old and new event versions while producers roll out.

> 💡 **Interview tip:** "Every change, code or config, goes canary then region-by-region with automated rollback on SLO burn; schema changes use expand/contract so rollback is always possible." That one line covers what most interviewers want for deployment safety.

> 🔗 **Connections:** Canary metrics are the golden signals (12.10); automated rollback is triggered by burn-rate alerts (12.10); the staging order follows failure domains (12.2). Readiness and graceful shutdown (12.6) make rolling deploys lossless.

## 12.8 Disaster Recovery, Backups and Chaos Engineering

### Disaster recovery

**Disaster recovery (DR)** is the plan for losing a whole region or a whole dataset. Two numbers define it (5.9): **RPO** (how much data you may lose) and **RTO** (how long until you're back).

| Strategy | Standby | RPO | RTO | Cost |
| :---- | :---- | :---- | :---- | :---- |
| Backup & restore | Backups in another region only | Hours (last backup) | Hours–days | $ |
| Pilot light | Data replicated, minimal compute off | Seconds–minutes | Tens of minutes | $$ |
| Warm standby | Scaled-down copy running | Seconds–minutes | Minutes | $$$ |
| Active-active | Full capacity in each region | ~0 to seconds | ~0 (traffic shift) | $$$$ |

### Backups

Replication is **not** a backup. A bad `UPDATE urls SET dest = ...` without a `WHERE`, or a buggy migration, is faithfully replicated to every replica in milliseconds. Backups protect against **logical** corruption and human error.

- **Full snapshots + continuous WAL/binlog archiving** → **point-in-time recovery (PITR)**: restore to 14:31:59, one second before the bad statement (9.4).
- **3-2-1 rule**: 3 copies, 2 different media/services, 1 off-site (other region/account). Use a separate account with write-once retention (S3 Object Lock) so a compromised admin or ransomware can't delete backups.
- **Test restores regularly.** An untested backup is a hope, not a backup. Measure the restore time: that *is* your RTO for data loss.

```
 restore time estimate: 10 TB database, restore throughput ~500 MB/s
   10,000,000 MB / 500 MB/s = 20,000 s = ~5.5 hours  (+ WAL replay)
 If the RTO is 1 hour, backup-and-restore alone can't meet it.
```

### Chaos engineering

**Chaos engineering** injects failures on purpose to verify the system tolerates them, before a real incident does it for you.

- **Hypothesis first**: "If one AZ's app servers are killed, redirect success stays > 99.9% and p99 < 100 ms."
- **Start small**, in staging, then production with a limited blast radius and an abort button.
- Examples: Netflix **Chaos Monkey** (kill random instances), Chaos Kong (evacuate a region); Google **DiRT** (Disaster Recovery Testing) exercises; AWS Fault Injection Service; injecting latency, packet loss, dependency errors, clock skew.
- **Game days**: rehearse failovers (5.5) and restores with the people who'd be on call.

> 🔗 **Connections:** RPO/RTO are defined in 5.9 and driven by replication mode (5.3) and failover (5.5). Chaos tests check redundancy math (12.2) and the cascade defences (12.3). Erasure-coded object storage (9.9) is where backups usually live.

## 12.9 Observability: Metrics, Logs, Traces

**Observability** is being able to answer new questions about a running system from the outside, without shipping new code. The three classic signal types answer different questions:

| | **Metrics** | **Logs** | **Traces** |
| :---- | :---- | :---- | :---- |
| What | Numeric time series (counters, gauges, histograms) | Timestamped event records | The path of one request across services |
| Answers | "Is something wrong? How much? Since when?" | "What exactly happened in this case?" | "Where did the time go? Which service failed?" |
| Cost | Cheap: aggregated, fixed size per series | Expensive: per event | Expensive: usually sampled |
| Tools | Prometheus, Monarch, Datadog | Loki, Elasticsearch, Cloud Logging | Jaeger, Zipkin, Tempo, Dapper (Google) |

**OpenTelemetry** is the vendor-neutral standard for producing all three.

### Metrics

- **Counter** (monotonic: requests_total), **gauge** (current value: queue_depth), **histogram** (latency buckets → p50/p99).
- You can't average percentiles across servers. Aggregate **histograms** and compute percentiles from the merged buckets.
- **Cardinality** is the main cost: each unique label combination is a separate series. Labels like `status`, `region`, `endpoint` are fine; `short_code` or `user_id` as a label creates billions of series and kills the metrics system. Put high-cardinality data in logs/traces.

### Logs

- **Structured** (JSON key/values), not free text: `{"ts":..., "level":"warn", "code":"aZ3kQ9x", "trace_id":"4bf9...", "msg":"cache miss, db slow", "db_ms":812}`.
- Include the **trace ID** in every log line so you can jump from a trace to its logs.
- Volume control: sample debug logs, keep error logs; at 12,000 QPS one 500-byte log line per request is 6 MB/s ≈ 518 GB/day.

### Distributed traces

A **trace** is a tree of **spans**; each span is one unit of work with start, duration, and parent. The **trace context** (W3C `traceparent` header: trace ID + parent span ID) is propagated on every call.

```
 trace 4bf9...  GET /aZ3kQ9x                          total 48 ms
 |-- edge LB                       |=|                  1 ms
 |-- redirect-svc                   |====================================| 46
 |    |-- redis GET aZ3kQ9x          |=| miss            1 ms
 |    |-- postgres SELECT              |=============================| 40 ms
 |    |-- redis SET aZ3kQ9x                                           |=| 1
 |    `-- kafka produce click (async)                                  |= |
 time ---------------------------------------------------------------->
                         the DB call is the problem
```

- **Head sampling**: decide at the start (keep 1%). Cheap, but misses most rare errors.
- **Tail sampling**: buffer the whole trace and keep it if it was slow or failed. Captures what matters; needs a collector tier that sees complete traces.

> 🔗 **Connections:** Trace spans make fan-out latency (2.3) visible. Metrics feed the golden signals and SLO alerts (12.10) and canary analysis (12.7). The timestamps in traces across machines are subject to clock skew (3.1), so child spans can appear to start before their parents.

## 12.10 Golden Signals, RED and USE; SLO Burn-Rate Alerting

### Three checklists for what to measure

| Framework | Signals | Applies to | Origin |
| :---- | :---- | :---- | :---- |
| **Four golden signals** | Latency, Traffic, Errors, Saturation | User-facing services | Google SRE book |
| **RED** | Rate, Errors, Duration | Request-driven services (microservices) | Tom Wilkie |
| **USE** | Utilization, Saturation, Errors | Resources: CPU, memory, disk, NICs, pools, queues | Brendan Gregg |

- RED is the golden signals without saturation; USE is the view from the resource side. Use RED/golden signals per service, USE per resource, and you'll cover most failures.
- Measure latency of **successful and failed requests separately**: fast errors can make average latency look great during an outage.
- **Saturation** is the early warning: queue depth, thread-pool usage, consumer lag (10.5), connection pool waits. It moves before errors do.

### Alerting on symptoms, with burn rates

Page on **symptoms users feel** (SLO violations), not on causes (CPU 90%). Causes go on dashboards.

A naive "alert if error rate > 0.1%" is either too noisy (a 2-minute blip pages) or too slow. **Burn-rate alerting** asks: *how fast are we spending the error budget?*

```
 burn rate = observed error rate / error budget rate
 SLO 99.9% -> budget rate = 0.1%

 burn rate 1    : budget lasts exactly 30 days (fine)
 burn rate 14.4 : error rate 1.44%  -> 30 days / 14.4 = ~2 days to exhaust
                  over 1 hour that burns 14.4 / 720 h = 2% of the monthly budget
```

The multiwindow, multi-burn-rate scheme from the Google SRE Workbook:

| Severity | Burn rate | Long window | Short window | Budget consumed when it fires |
| :---- | :---- | :---- | :---- | :---- |
| Page | 14.4× | 1 h | 5 min | 2% |
| Page | 6× | 6 h | 30 min | 5% |
| Ticket | 1× | 3 days | 6 h | 10% |

- The **long window** ensures enough budget has really been burned to matter (no paging for a 2-minute blip).
- The **short window** must also be over threshold, so the alert **resets quickly** once the problem is fixed.
- Fast burns page quickly; slow burns create a ticket instead of waking someone.

> 💡 **Interview tip:** If asked "how would you monitor this?", answer in layers: "SLIs at the LB for redirect success and latency; page on multiwindow burn rate against the 99.9% SLO; RED dashboards per service, USE for DB and cache; consumer lag for the Kafka pipeline; traces with tail sampling for slow requests." That's a complete, senior-sounding answer in 20 seconds.

> 🔗 **Connections:** Burn rates are error budgets (12.1) turned into alerts. Saturation is Little's Law (2.4) made visible. These same signals gate canaries (12.7) and trigger load shedding (12.5).

---

# 13. Probabilistic and Distributed Data Structures

A handful of clever data structures show up again and again inside distributed systems: they trade a little accuracy for huge savings in memory, or make it cheap to compare, merge, or locate data across machines. You rarely implement them in an interview, but naming the right one, with its error trade-off and a size estimate, is a strong signal.

| Structure | Answers | Error | Size (typical) | Used in |
| :---- | :---- | :---- | :---- | :---- |
| Bloom filter | "Is x in the set?" | False positives only | ~10 bits/item for 1% | LSM trees, caches, Chrome Safe Browsing (historically) |
| Count-Min Sketch | "How often has x appeared?" | Overestimates only | KBs | Heavy hitters, hot keys |
| HyperLogLog | "How many distinct items?" | ~0.8% std error | 12 KB | Redis `PFCOUNT`, BigQuery `APPROX_COUNT_DISTINCT` |
| Merkle tree | "Where do two datasets differ?" | Exact | One hash per node | Dynamo/Cassandra repair, Git, certificate transparency |
| CRDT | "Merge concurrent updates without coordination" | Exact (by design) | Varies | Riak, Redis active-active, collaborative editors |
| Geohash / quadtree | "What's near this point?" | Cell granularity | Index | Maps, ride-hailing, Elasticsearch |

## 13.1 Bloom Filters

A **Bloom filter** is a bit array of **m** bits plus **k** hash functions. It answers set membership with **no false negatives** and a tunable rate of **false positives**.

```
 m = 16 bits, k = 3 hashes

 add("aZ3kQ9x"): h1=2  h2=7  h3=11      add("Qp7Lm2c"): h1=4  h2=7  h3=14
 bits: 0 0 1 0 1 0 0 1 0 0 0 1 0 0 1 0
           ^   ^     ^       ^     ^
 query("x9KdeF1"): h1=2 h2=4 h3=14 -> all 1 -> "MAYBE present" (false positive!)
 query("Bb81zzQ"): h1=3 ...        -> bit 3 is 0 -> "DEFINITELY absent"
```

- "No" is certain: if any bit is 0, the item was never added. "Yes" means "maybe".
- **Can't delete** (clearing a bit might remove other items). A **counting Bloom filter** uses small counters instead of bits to support deletes, at ~4× the memory. Cuckoo filters also support deletion.

### Sizing

```
 false-positive rate  p ≈ (1 - e^(-kn/m))^k
 optimal hashes       k = (m/n) x ln 2
 bits per item        m/n = -ln(p) / (ln 2)^2  ≈ 1.44 x log2(1/p)

   p = 1%    ->  ~9.6 bits/item, k = 7
   p = 0.1%  -> ~14.4 bits/item, k = 10

 1 billion short codes at 1%:  1e9 x 9.6 bits = 9.6e9 bits = ~1.2 GB
```

Note the size doesn't depend on how long the keys are: a 2 KB URL costs the same 9.6 bits.

### Where it's used

- **LSM trees** (9.3): each SSTable has a Bloom filter; a read for a missing key skips SSTables without touching disk. Cassandra, RocksDB, HBase, Bigtable.
- **Cache penetration** (8.8): requests for codes that don't exist (scanners, typos) miss the cache and hit the DB every time. A Bloom filter of all existing codes in front of the DB rejects ~99% of them in memory.
- **Short code generation** (11.6): pre-screen random codes for collisions before the conditional insert.
- **Malicious URL check**: a compact local filter of known-bad URL hashes, with a remote confirm on "maybe" (Chrome's early Safe Browsing used this pattern).

> 🔗 **Connections:** LSM reads (9.3), negative caching (8.8), and anti-entropy filters. The "no false negatives, some false positives" shape means a Bloom filter is always a pre-check in front of an authoritative source, never the source itself.

## 13.2 Count-Min Sketch

A **Count-Min Sketch (CMS)** estimates how many times each item has appeared in a stream, in fixed memory. It's a **d × w** grid of counters with one hash function per row.

```
 d = 3 rows, w = 8 columns

 add(x):  row r: counters[r][h_r(x)] += 1   for each row

          0  1  2  3  4  5  6  7
 row 0: [ 0  5  0  9  0  0  2  0 ]   h0(x)=3
 row 1: [ 3  0  0  0 12  0  0  1 ]   h1(x)=4
 row 2: [ 0  0  9  0  0  4  0  3 ]   h2(x)=2

 estimate(x) = min(9, 12, 9) = 9      (collisions only ever ADD, so take min)
```

- **Never underestimates**; overestimates by at most **ε × N** (N = total count) with probability **1 − δ**, where `w = ⌈e/ε⌉` and `d = ⌈ln(1/δ)⌉`.

```
 ε = 0.1%, δ = 1%:  w = 2,719, d = 5  -> 13,595 counters x 4 B = ~54 KB
 for a stream of 1 billion clicks: error <= 1e6 per item, with 99% probability
```

So it's accurate for **heavy hitters** and useless for rare items (their true counts are below the error).

Uses:

- **Hot key detection** (7.5): which short codes are getting hammered right now? Sketch per server, report the top items (with a small heap) to decide what to replicate or cache in-process.
- **Rate limiting / abuse** (11.7): approximate per-IP request counts at the edge without a counter per IP.
- **Top-K** in stream processing (10.8): CMS + a min-heap of candidates.

> 🔗 **Connections:** Same "hash into a fixed array, tolerate collisions" idea as the Bloom filter (13.1), with counters instead of bits. Sketches are mergeable (add the grids), so per-partition sketches combine in a stream job.

## 13.3 HyperLogLog

**HyperLogLog (HLL)** estimates the number of **distinct** items (cardinality) in fixed, tiny memory.

Intuition: hash each item to random bits. A run of **ρ leading zeros** happens with probability 2^-ρ, so if the longest run you've seen is 20, you've probably seen ~2^20 distinct items. One estimate is noisy, so HLL splits items into **m buckets** (by the first bits of the hash), tracks the max run per bucket, and combines them with a harmonic mean.

```
 hash(item) = [ first p bits ][ remaining bits ...          ]
                bucket index    position of first 1 bit (rho)
                (p = 14 ->      -> bucket = max(bucket, rho)
                 16,384 buckets)

 standard error = 1.04 / sqrt(m)
   m = 16,384 -> 1.04 / 128 = ~0.81%
 memory: 16,384 registers x 6 bits = ~12 KB, whether you count 1,000 or 1e9
```

- **Mergeable**: union = per-bucket max. Daily HLLs merge into a weekly one; per-region HLLs merge into a global one. You **can't** subtract or intersect accurately.
- Redis: `PFADD`, `PFCOUNT`, `PFMERGE` (12 KB per key, 0.81% error).

URL shortener: "unique visitors per link per day". Exact counting needs a set of visitor IDs per link (MBs for a popular link); HLL is 12 KB per link-day and answers within ~1%.

> 🔗 **Connections:** A natural output of stream windows (10.8). Mergeability makes it a state-based CRDT-like structure (13.6): merge is commutative, associative, and idempotent.

## 13.4 Merkle Trees

A **Merkle tree** is a tree of hashes: leaves hash data blocks (or key ranges), each parent hashes its children. The **root hash** summarizes the whole dataset.

```
                     root = H(H12 + H34)
                    /                   \
          H12 = H(H1 + H2)          H34 = H(H3 + H4)
           /          \              /          \
     H1=H(keys 0-24) H2=H(25-49)  H3=H(50-74)  H4=H(75-99)

 replica A root != replica B root
   -> compare H12: equal  -> skip half the data
   -> compare H34: differ -> descend: H3 equal, H4 differs
   -> sync only keys 75-99
```

- Finding differences costs **O(log n)** hash comparisons plus the differing ranges, instead of shipping the whole dataset.
- **Anti-entropy** in leaderless stores (5.7): Dynamo, Cassandra (`nodetool repair`), and Riak build Merkle trees per token range and exchange them to repair divergent replicas.
- **Integrity**: Git commits and trees, IPFS, blockchains, and certificate transparency logs; a client can verify that one item is in the set with a short **proof** (the sibling hashes along the path).

> 🔗 **Connections:** Anti-entropy repair (5.7) catches what hinted handoff and read repair miss. Content-addressed hashing also underpins dedup in object storage (9.9).

## 13.5 Consistent Hash Ring and Rendezvous Hashing (recap, pointer to 7.4)

Full treatment is in 7.4; this is the one-screen recap.

**Problem:** `hash(key) % N` remaps almost every key when N changes (N = 4 → 5 moves ~80% of keys), which empties caches and forces mass data movement.

**Consistent hashing:** put nodes and keys on a ring (hash space 0..2^32); a key belongs to the first node clockwise. Adding or removing a node moves only **~1/N** of the keys. **Virtual nodes** (each server at ~100–256 ring points) even out the load and spread a dead node's keys over all survivors.

```
            0
        D       A        key k1 -> next clockwise -> A
     k3              k1   key k2 -> B
     C                B   add node E between A and B:
        k2                 only keys in (A, E] move, to E
```

**Rendezvous (highest random weight) hashing:** for each key, compute `score(key, node)` for every node and pick the highest. Removing a node moves only its keys; no ring to maintain. Cost is O(N) per lookup, fine for tens or hundreds of nodes. Easy to get top-R nodes for replicas.

**Jump consistent hash** and **Maglev hashing** are other variants with better speed or balance for LB use (11.2).

> 🔗 **Connections:** 7.4 (full detail), 7.6 (rebalancing), 11.2 (consistent-hash load balancing for cache affinity), 5.7 (Dynamo's preference list walks the ring).

## 13.6 CRDTs

A **CRDT (Conflict-free Replicated Data Type)** is a data type designed so that replicas can be **updated independently, without coordination**, and always **converge** to the same state once they've seen the same updates. They're the principled answer to multi-leader and leaderless conflicts (5.6, 5.7), instead of last-writer-wins silently dropping data.

For state-based CRDTs, the merge function must be **commutative**, **associative** and **idempotent**, so it doesn't matter in which order, grouping, or how many times replicas exchange state.

### G-Counter (grow-only counter)

Each replica increments only **its own slot**; merge takes the per-slot max; value is the sum.

```
 click counter for aZ3kQ9x, replicas in 3 regions

 us:  {us: 5, eu: 0, ap: 0}      eu:  {us: 0, eu: 3, ap: 0}
          \                          /
           merge = per-slot max ----
           {us: 5, eu: 3, ap: 0}  -> value = 8

 merging the same state twice changes nothing (idempotent),
 so duplicate gossip messages are harmless.
```

A naive "everyone sends `count = count + 1`" or LWW on a single integer would lose increments.

### Others to know

| CRDT | Idea |
| :---- | :---- |
| **PN-Counter** | Two G-Counters (increments, decrements); value = P − N |
| **LWW-Register** | Keep the value with the highest timestamp (HLC helps, 3.6); simple but drops concurrent writes |
| **MV-Register** | Keep all concurrent values (with vector clocks, 3.5), let the app resolve: Dynamo's shopping cart |
| **OR-Set** (observed-remove) | Each add gets a unique tag; remove deletes only tags it has seen, so concurrent add wins |
| **Sequence CRDTs** (RGA, Yjs, Automerge) | Collaborative text editing |

Costs: metadata grows (per-replica slots, tombstones for removed elements), and CRDTs only fit operations that can be made to commute. You can't build "balance must never go below zero" from a CRDT alone: that invariant needs coordination (consensus, Part 6).

Used in: Riak data types, Redis Enterprise active-active databases, and collaborative apps built on Automerge or Yjs.

> 🔗 **Connections:** Multi-leader conflicts (5.6), leaderless replication (5.7), vector clocks (3.5), eventual consistency (4.5). Strong eventual consistency (same updates seen ⇒ same state) is the CRDT promise.

## 13.7 Geohash and Quadtrees

"Find things near me" queries need a spatial index: a B-tree on latitude alone or longitude alone can't answer "within 2 km" efficiently.

### Geohash

A **geohash** recursively halves the world, alternating longitude and latitude, and interleaves the resulting bits into a single string (base32, 5 bits per character).

```
 lon: [-180, 180] -> right half? 1   lat: [-90, 90] -> upper half? 1  ...
 bits interleaved: lon lat lon lat ... -> base32 -> "u33dc0"  (Berlin area)

 +---------------- u33d ----------------+   longer prefix = smaller cell:
 |  +--- u33dc ---+                     |   "u33dc0" is inside "u33dc",
 |  |  +-------+  |                     |   which is inside "u33d"
 |  |  |u33dc0 |  |                     |
 |  |  +-------+  |                     |   precision (approx, varies
 |  +-------------+                     |   with latitude):
 +--------------------------------------+     5 chars ~ 4.9 km x 4.9 km
                                              6 chars ~ 1.2 km x 0.6 km
                                              7 chars ~ 153 m  x 153 m
```

- **Points close together usually share a prefix**, so "nearby" becomes a **prefix range scan** on an ordinary sorted index (B-tree, Redis sorted set, DynamoDB sort key). Redis `GEOADD`/`GEOSEARCH` uses geohash-encoded scores.
- **Edge problem**: two points 10 m apart across a cell boundary can have completely different prefixes. Always query the target cell **plus its 8 neighbours**, then filter by exact distance.

### Quadtree

A **quadtree** splits a 2D region into 4 children whenever it holds more than K points, so dense areas get small cells and empty areas stay large.

```
 +-----------+-----------+
 |           |  +--+--+  |      split when a node has > K points (e.g. K = 100)
 |   (few)   |  |  |::|  |      downtown: deep, small cells
 |           |  +--+--+  |      ocean: one big leaf
 +-----------+--+--+--+--+
 |           |           |
 |           |           |
 +-----------+-----------+
```

- Adapts to density better than fixed-size geohash cells (a city vs a desert).
- Usually an **in-memory** structure rebuilt or updated per server; moving objects (drivers) require frequent updates.
- Related: **R-trees** (PostGIS, for shapes), Google **S2** (cells on a sphere via a Hilbert curve), Uber **H3** (hexagons, equal neighbour distances).

> 🔗 **Connections:** Geohash turns 2D into 1D so range partitioning (7.2) and sorted indexes (9.2) work, with the same hot-spot risk (7.5) for dense cities. Sharding a location service by geohash prefix is range partitioning.

## 13.8 Skip Lists and Tries

### Skip lists

A **skip list** is a sorted linked list with extra "express lane" levels. Each node is promoted to the next level with probability 1/2, giving **O(log n) expected** search, insert, and delete, with simple code and no rebalancing.

```
 level 3:  H ---------------------------------> 50 ------------------> nil
 level 2:  H -------------> 17 ---------------> 50 -------> 80 -----> nil
 level 1:  H ----> 8 -----> 17 ----> 31 ------> 50 -> 62 -> 80 -----> nil
 level 0:  H -> 3 -> 8 -> 12 -> 17 -> 25 -> 31 -> 42 -> 50 -> 62 -> 80 -> nil

 search 62: L3: 50 (next nil) -> L2: 80 too big -> L1: 62 found
```

Why distributed systems like them:

- **Concurrency-friendly**: inserts touch only a few local pointers, so lock-free versions are practical (Java `ConcurrentSkipListMap`).
- **Ordered iteration** for range scans.
- Used as the **memtable** in LevelDB and RocksDB (9.3), and for **Redis sorted sets** (`ZADD`, `ZRANGE`: leaderboards, time-ordered feeds, sliding-window rate-limit logs, 11.7).

### Tries

A **trie** (prefix tree) stores strings by character, so all keys sharing a prefix share a path. Lookup cost is O(key length), independent of how many keys exist.

```
            (root)
           /      \
          s        u
          |        |
          h        r
         / \       |
        o   a      l *
        |   |
        r * r *

 keys: "shor", "shar", "url"    (* = end of a key)
 prefix query "sh" -> walk 2 nodes, then enumerate the subtree
```

- **Autocomplete / typeahead**: store top-K completions at each node so a prefix query is one walk.
- **Longest-prefix match**: IP routing tables use compressed tries (radix / Patricia trees); so do HTTP routers matching `/api/v1/*`.
- **Radix trees** compress single-child chains to save memory; Redis, for example, uses one (`rax`) internally for Streams.

> 🔗 **Connections:** Skip lists are the in-memory half of LSM storage engines (9.3). Tries are the structure behind prefix routing in L7 proxies (11.2, 11.5) and geohash prefix lookups (13.7).

---

# 14. Putting It All Together: Designing a URL Shortener, Every Layer (NALSD style)

This part designs the URL shortener from the mock interview from start to finish, with real numbers at every step, and answers each question that got a "don't know": how to size servers without a given per-server QPS, how to replicate across regions, how to handle "created in the US, clicked in the EU", and how to invalidate caches when a destination changes. NALSD (Non-Abstract Large System Design) means every box gets a count and every arrow gets a rate, so this part uses the arithmetic from Parts 1–2 throughout and points to the part behind each decision.

The NALSD rhythm is always the same, and the sections follow it:

```
 requirements --> envelope math --> one machine --> scale out --> data layer
   (14.1)           (14.2)           (14.3)          (14.4)        (14.5)
                                                                     |
 bill of    <-- break it on  <-- multi-region <-- caching <-- IDs <--+
 materials      purpose          (14.8)          (14.7)     (14.6)
 (14.10)        (14.9)
```

## 14.1 Requirements

Ask before you draw. These are the requirements the interviewer gave, plus the ones you should ask about:

**Functional**

| Operation | API | Notes |
| :---- | :---- | :---- |
| Create | `POST /v1/urls {long_url}` + `Idempotency-Key` header → `201 {code}` | Optional custom alias. Retries must not create a second code (2.6). |
| Redirect | `GET /{code}` → `302 Location: <long_url>` | The hot path. 301 vs 302 is a real decision (8.9, 14.7). |
| Edit | `PATCH /v1/urls/{code} {long_url}` | Changing the destination is why cache invalidation matters (8.5). |
| Disable | `DELETE /v1/urls/{code}` | Abuse takedown: must take effect fast everywhere (11.9). |
| Analytics | click counts per code | Asynchronous, never on the redirect path (Part 10). |

**Non-functional**

| Requirement | Value | What it forces |
| :---- | :---- | :---- |
| New URLs | 100M / month | ~40 writes/s: writes are not the hard part. |
| Read:write ratio | 100 redirects per URL | Read-heavy: cache aggressively (Part 8). |
| Retention | URLs never expire | Storage grows forever; plan for 5 years. |
| Availability | 99.99% for redirects | ≤ ~4.3 min/month of downtime: more than one AZ, really more than one region (1.3, 12.1). |
| Latency | p99 < 100 ms for redirects | Reads must be served in-region; one cross-ocean RTT is ~100–150 ms (1.1). |
| Durability | A created link is never lost | Synchronous replication across AZs before acknowledging (5.3). |
| Users | Global (at least US and EU) | Multi-region (5.8, 14.8). |

Also state the consistency you need, because it drives the rest (Part 4):

- **Create → redirect by the same user:** read-your-writes (4.4). The creator pastes the link and clicks it immediately; a 404 there looks like a bug.
- **Create → redirect by someone else, elsewhere:** a few seconds of lag is OK, as long as the system doesn't serve a *wrong* answer (a 404 that sticks). See 14.8.
- **Edit → redirect:** bounded staleness. "Within ~1 minute everywhere" is a reasonable target to propose; ask the interviewer.

> 💡 **Interview tip:** Saying "redirects need high availability and low latency, edits can tolerate a minute of staleness, and creates need durability more than speed" sets up every later choice. It tells the interviewer you know the read and write paths have different requirements and you'll treat them differently.

## 14.2 Envelope Math (recap of 1.8)

This is the part you got right in the interview. Here it is in one place, with the extra lines (writes, cache, total IDs) you'll need later:

```
WRITES
  100M URLs/month / 2.6M s/month           ~= 40 creates/s   (peak ~120/s)
  5 years: 100M x 12 x 5                   =  6B URLs total

STORAGE (logical)
  per URL: 500 B URL + 100 B metadata      =  600 B
  per month: 100M x 600 B                  =  60 GB
  5 years: 60 GB x 12 x 5                  =  3.6 TB
  + short code, primary index, overhead    ~  x1.5  -> ~5.4 TB
  x 3 replicas (one per AZ)                ~= 16 TB per region

READS
  100M x 100 redirects / month             =  10B redirects/month
  10B / (30 x 86,400 s)                    ~= 3,858  -> ~4K QPS average
  peak = 3 x average                       ~= 12K QPS

BANDWIDTH (peak, outbound)
  12K/s x 1 KB = 12 MB/s x 8 bits/byte     =  96 Mbps   (<1% of a 10 Gbps NIC)

SERVERS
  12K QPS / 2,000 QPS per server           =  6 servers at peak
  survive losing 1 of 3 AZs: 6 / (2/3)     =  9 servers (3 per AZ)   (1.7)
```

What the numbers tell you, which is the point of the exercise:

- **Writes are tiny.** 120/s at peak fits on any database. Write throughput won't drive the design; write *durability* and *ID uniqueness* will.
- **Storage is modest.** 16 TB per region is a handful of machines. Storing a **full copy in every region** is affordable, which makes the multi-region design much simpler (14.8).
- **Reads are the workload**, and they're small point lookups by key. That's the perfect shape for a key-value store plus a cache.
- **Bandwidth is not a bottleneck.** CPU, concurrency and latency are (14.4).

> 🔗 **Connections:** The "×1.5 overhead" is a placeholder for index and storage-engine overhead (9.2, 9.3); the "×3" is the replication factor (5.1). The AZ math (6 ÷ 2/3) comes from 1.7 and gets tested in the failure walkthrough (14.9).

## 14.3 One Machine First

NALSD interviews reward starting with the simplest thing that works and then naming exactly why it stops working. So: could one machine do this?

```
+-------------------------- one big server -----------------------------+
|  app (nginx + service)  -->  Postgres, one table:                     |
|                                urls(code CHAR(7) PRIMARY KEY,         |
|                                     long_url TEXT, created_at, ...)   |
|  64 cores, 512 GB RAM, 8 TB NVMe                                       |
+-----------------------------------------------------------------------+
```

- **Throughput:** 12K point lookups/s by primary key is within reach of one well-tuned server when the hot rows are in the buffer pool. The B-tree is ~4 levels deep, and the upper levels are always in RAM (9.2).
- **Storage:** 3.6 TB logical, ~5.4 TB with indexes, fits on 8 TB of NVMe.
- **Writes:** 120/s is nothing.

So one machine *almost* works. It fails on the other two reasons to distribute (0.1):

| Problem | Why one machine can't fix it | Fix | Where |
| :---- | :---- | :---- | :---- |
| **Availability** | Every reboot, kernel patch, disk or PSU failure is a full outage. 99.99% allows ~4.3 min/month. | Replicas in other AZs, automatic failover. | 5.2, 5.5, 12.2 |
| **Latency** | One location: EU users pay ~100–150 ms RTT, plus TCP/TLS handshakes. | Serve from a region near each user. | 1.1, 5.8 |
| **Blast radius** | A bad deploy or one runaway query takes down everything. | Many small, independent servers; canaries. | 12.2, 12.7 |
| **Growth** | Data keeps growing forever; one box is a ceiling. | Partitioning. | Part 7 |

> 💡 **Interview tip:** Say it out loud: "This fits on one machine in terms of throughput; I'm distributing it for availability and latency." Interviewers like hearing that you know *why* you're adding complexity. It also lets you keep the design small: 9 app servers, not 900.

## 14.4 Scaling Out: Stateless App Tier

The app servers hold no state that matters: every request carries everything needed (the short code), and all persistent state lives in the cache and database. That makes them **stateless** (11.1), so any server can serve any request, the load balancer can spread traffic freely (11.2), and a dead server costs only its in-flight requests.

### How Many Servers? Sizing Without a Given Per-Server QPS

In the interview, the per-server number (2,000 QPS) was handed to you. If it isn't, you derive it. There are two limits to check: **CPU** (how much work per second the cores can do) and **concurrency** (how many requests are in flight at once, which uses threads, memory and connections). Little's Law (2.4) connects them.

**Step 1: CPU capacity.** Estimate (or, in real life, measure with a load test and a profiler) how much CPU time one redirect costs. A redirect is TLS work, HTTP parsing, a cache call and a small response. Assume ~2 ms of CPU per request on a server with 8 cores:

```
CPU capacity = cores / CPU time per request
             = 8 / 0.002 s
             = 4,000 requests/s   (at 100% CPU)
```

**Step 2: Don't plan to run at 100%.** Response time explodes as utilization approaches 100%, because requests queue behind each other. For the simple M/M/1 queueing model, mean response time is the service time divided by the idle fraction:

```
response time  ~=  service time / (1 - utilization)

utilization  50%  ->  2x service time
utilization  80%  ->  5x
utilization  90%  -> 10x
```

Planning for ~50% CPU at peak keeps latency flat and leaves room for a traffic spike, GC pauses or a noisy neighbor. So:

```
per-server QPS = 4,000 x 0.5 = 2,000 QPS    <- the number you were given
servers        = 12,000 / 2,000 = 6 at peak
```

**Step 3: Concurrency, with Little's Law.** L = λ × W: requests in flight = arrival rate × time each spends in the system. With a ~10 ms server-side latency (mostly waiting on the cache, sometimes the DB):

```
whole fleet:   L = 12,000/s x 0.010 s = 120 requests in flight
per server:    L =  2,000/s x 0.010 s =  20 requests in flight
```

Twenty concurrent requests per server is trivial for an async server and fine for a thread pool of 50–100. Little's Law also sizes the pools *behind* the app:

```
cache:  12,000/s x 0.001 s (1 ms)                  =  12 connections in use
DB:     12,000/s x 10% miss = 1,200/s x 0.005 s    =   6 connections in use
```

So a pool of ~10 DB connections per app server (~90 fleet-wide) is plenty, and that matters: databases like Postgres don't like thousands of connections.

**Step 4: Check what happens when latency goes up.** This is where Little's Law earns its keep. If the database slows down so that W rises from 10 ms to 200 ms:

```
per server:  L = 2,000/s x 0.200 s = 400 requests in flight
```

Now 400 requests per server are waiting, the thread pool (say 100) is exhausted, new requests queue, latency rises further and health checks start failing. That's a **cascading failure** (12.3), caused not by more traffic but by a slower dependency. The defenses all come from this one formula: tight timeouts (2.7) cap W, bounded pools plus load shedding (12.5) cap L, and circuit breakers (12.4) stop waiting on a dependency that's clearly down.

**Step 5: Add headroom for failure domains** (1.7). Six servers is the bare minimum at peak. Spread across 3 AZs, losing one AZ leaves 2/3 of them:

```
servers needed = 6 / (2/3) = 9   ->  3 per AZ
check: lose 1 AZ  -> 6 servers x 2,000 = 12,000 QPS   = peak, still at 50% CPU
       lose 1 box -> 8 servers x 2,000 = 16,000 QPS   > peak
```

The 8 from the interview (N+2) survives two server failures but **not** an AZ failure: losing the AZ holding 3 of the 8 leaves 5 × 2,000 = 10,000 QPS, below the 12,000 peak. With 3 AZs the AZ rule (9) dominates N+2. Rolling deploys take a server out at a time, which is one more reason some teams round up to 12 (4 per AZ).

> 💡 **Interview tip:** If you don't know the per-server QPS, say: "I'd measure the CPU cost per request with a load test. Assume ~2 ms of CPU on 8 cores: that's 4,000 QPS at 100%, so I'd plan for ~2,000 at 50% to keep queueing delay low. Then Little's Law tells me how many concurrent requests and connections that means." That turns a "don't know" into the best answer in the room.

> 🔗 **Connections:** The 50% target is the same queueing effect as a CPU run queue on one machine. Health checks that pull a server out (12.6) and the load balancer algorithm (least-outstanding-requests beats round robin when latencies vary, 11.2) are what keep traffic off a slow server. The app tier's statelessness is what makes the failure walkthrough (14.9) boring, which is the goal.

## 14.5 Data Layer: Partitioning and Replication

### What Kind of Store?

The access pattern decides (9.1): almost every query is "get the row for this exact key", plus rare inserts and updates. No joins, no range scans on the hot path. That's a **key-value** workload.

| Option | Fits? | Notes |
| :---- | :---- | :---- |
| Sharded Postgres/MySQL (e.g. Vitess) | Yes | Familiar, strong per-shard consistency (single leader per shard, 5.2). You run the sharding and cross-region replication yourself. |
| Cassandra / ScyllaDB | Yes | Leaderless, built-in consistent hashing (7.4) and multi-datacenter replication (5.7). LSM storage (9.3) is great for "write once, read many". |
| DynamoDB (+ Global Tables) | Yes | Managed key-value with multi-region active-active replication, last-writer-wins on conflicts (5.6). |
| Spanner / CockroachDB | Yes, more than needed | Synchronous consensus across regions: strong consistency, but every write pays a cross-region quorum (6.4, 3.6). |

Any of them works. What matters is that you can explain how the one you pick partitions, replicates and fails. The walkthrough below uses a **Cassandra-style** store (Dynamo lineage, 14.11), because it makes the replication choices explicit, and notes the equivalent for a sharded SQL design.

### Partitioning

**Partition key = short code. Hash partitioning** (7.3): the codes are already random-looking (14.6), so load spreads evenly, and the only query is by exact key, so range partitioning would buy nothing (7.2). The cluster places keys on a **consistent hash ring with virtual nodes** (7.4), so adding a node moves only ~1/N of the data (7.6).

How many nodes? Sizing is driven by **storage**, not QPS:

```
data per region (year 5)   ~5.4 TB logical incl. indexes
replication factor          3 (one replica per AZ)
total                      ~16 TB per region
target per node            ~1.5 TB data on a ~4 TB NVMe disk
                           (room for compaction, repair, growth: 9.3)
nodes                      16 TB / 1.5 TB ~= 11  -> 12 nodes (4 per AZ)
```

Reads that miss the cache: ~1,200/s at peak (10% of 12K). Writes: ~120/s. Twelve nodes are idle on QPS; they exist to hold the data. You'd start year one with ~1/5 of this and add nodes as data grows (7.6).

> 🔗 **Connections:** A secondary access pattern like "list all links created by user X" needs a **secondary index** (7.8). Partitioned by code, that's a scatter-gather query across all nodes, or a separate table partitioned by `user_id` (a global index, updated asynchronously). It's off the redirect path, so either is fine.

### Replication Inside a Region

Each key is stored on **3 replicas, one per AZ** (rack-aware placement), so losing an AZ loses one copy, never two (12.2).

Reads and writes use **quorums** (5.7, 6.2): with N = 3, write to W = 2 and read from R = 2. Since R + W = 4 > 3, every read quorum overlaps every write quorum in at least one replica, so a read in the same region sees the latest acknowledged write. In Cassandra this is `LOCAL_QUORUM`.

```
create aB3xK9p in us-east (N=3, W=2):

   app ---> coordinator ---+--> replica AZ-a  ack --+
                           +--> replica AZ-b  ack --+--> 2 acks: return 201
                           +--> replica AZ-c  (slow, catches up later)
                           +~~> eu-west replicas  (async, not waited for)
```

This answers the durability requirement: an acknowledged create is on disk in **two AZs** before the user sees the code. Losing a whole AZ can't lose it (RPO = 0 for AZ failures, 5.9).

With a sharded SQL design, the equivalent is one leader per shard plus two followers in the other AZs, with **semi-synchronous** replication (wait for at least one follower, 5.3) and automatic failover managed by a consensus-backed tool (5.5, 6.8).

### Replication Across Regions: Which Choice and Why

This is the question from the interview: "Would you replicate all the data across regions, or keep the three replicas in one region?" Lay out the options with numbers:

| Option | Read latency (EU) | Write latency | Region-loss behavior | Storage | Verdict |
| :---- | :---- | :---- | :---- | :---- | :---- |
| **A. 3 replicas in one region only** | ~100–150 ms per miss | Local | Total outage | 16 TB | Fails p99 and 99.99%. |
| **B. Full copy per region, async replication** | Local, ~1–10 ms | Local (W=2 in-region) | Others keep serving; lose last ~seconds of creates in the failed region (RPO = lag) | 16 TB × regions | **Pick this.** |
| **C. Synchronous cross-region quorum** (Spanner-style) | Local for stale reads; strong reads may need a remote round trip | +~100 ms per write | No data loss; survives a region if quorum spans 3+ regions | 16 TB × regions | Correct, but you pay latency on every write for a guarantee this product doesn't need. |
| **D. Cache-only in remote regions** | Local on hit, remote on miss | Local | Remote regions can't serve misses | Small | Cheap, but a region loss still takes out the source of truth. |

Why B fits this product:

- **Storage is cheap here.** An extra 16 TB per region costs far less than an engineer's time spent on partial replication schemes (14.2).
- **Every redirect is served in-region**, so p99 < 100 ms is achievable (2.3).
- **Creates are rare and tolerate small loss windows only in a region disaster.** And even then, the window is seconds of creates, and those users can recreate.
- **Write conflicts are nearly impossible by construction**, because each code has a single **home region** that owns its writes (14.6, 14.8). That turns multi-leader replication (5.6) into "many single-leader key spaces", which is the safe kind.

This is **active-active** for reads and **active-active with ownership** for writes (5.8): every region accepts creates (for codes it allocates), every region serves all redirects.

> 💡 **Interview tip:** When asked "replicate everything to every region?", answer with numbers: "The data is ~16 TB per region with 3× replication. Duplicating that per region is cheap, and it's the only way every redirect is served locally under 100 ms. I'd replicate asynchronously, because synchronous cross-region writes add ~100 ms and the product doesn't need linearizable creates." That's PACELC (4.7) applied: in normal operation (E) we choose latency (L) over consistency (C).

> 🔗 **Connections:** Asynchronous cross-region replication creates replication lag (5.4), which is the source of the "created in US, clicked in EU" problem solved in 14.8. The RPO/RTO trade-off is in 5.9, and the region-loss behavior is walked through in 14.9.

## 14.6 Short Code Generation

We need ~6B unique codes over 5 years, 7 characters from `[0-9a-zA-Z]` (base62, 11.6):

```
62^6 =    56.8 billion       (enough for 6B, but only ~10x headroom)
62^7 = 3,521.6 billion       (~590x headroom)       <- use 7
```

### Options

| Approach | How | Problem | Where |
| :---- | :---- | :---- | :---- |
| **Hash the URL** | `base62(md5(long_url))[:7]` | Truncation collides; the same URL from two users gets the same code (is that wanted?); must check-and-retry. | 11.6 |
| **Random code** | 7 random base62 chars, insert-if-absent | At 6B used, a new code collides with probability 6B / 3.5T ≈ 0.17%: fine with a conditional insert and retry. Needs a conditional write (linearizable per key). | 2.6, 4.2 |
| **Global counter** | One DB sequence, `base62(next_id)` | A single serial point: every create in every region waits on it (Amdahl, 2.5); codes are guessable (`aaaaab` follows `aaaaaa`). | 2.5 |
| **Pre-allocated ranges** | Each app server leases a block of 10,000 IDs from a coordinator, hands them out locally | Gaps when a server dies with an unused block (harmless). Sequential within a block, so scramble. | 11.6, 6.8 |
| **Snowflake-style** | timestamp + machine ID + sequence | 64-bit IDs are 11 base62 chars; too long here. | 11.6 |

### The Pick: Ranges + Region Bit + Scramble

```
 40-bit ID (2^40 ~= 1.1 trillion, fits in 7 base62 chars)
 +--------+---------------------------------------+
 | region |  39-bit counter, handed out in ranges |
 | 1 bit  |  of 10,000 by a per-region etcd       |
 +--------+---------------------------------------+
               |
               v  keyed 40-bit permutation (e.g. a small Feistel network)
           scrambled 40-bit number (still unique: a permutation is 1-to-1)
               |
               v  base62, padded to 7 chars
            "aB3xK9p"
```

- **No cross-region coordination.** Each region allocates from its own half of the ID space. A region can keep creating links while partitioned from the others (4.6).
- **Almost no in-region coordination.** At ~120 creates/s at peak, a block of 10,000 lasts a server minutes. The coordinator (an etcd key updated with compare-and-swap, 6.8) sees one write every minute or so, so it's nowhere near a serial bottleneck (2.5).
- **Not guessable.** The permutation turns consecutive counters into unrelated-looking codes. It's reversible with the key, so the service can decode a code back to its ID, and therefore its **home region** (14.8).
- **Uniqueness without checking.** A permutation of unique inputs gives unique outputs, so no collision checks are needed. Custom aliases are the exception: they need an insert-if-absent (a conditional write, Cassandra's lightweight transactions or a SQL unique constraint).

### Idempotent Creates

The double-click problem (2.6): the client sends `Idempotency-Key: 7f3c...` with `POST /v1/urls`. The server stores `key → code` (with a TTL of ~24 h) in the same write. A retry with the same key returns the same code instead of allocating a new one. Without it, a timeout-then-retry (2.2, 2.7) silently creates two links.

> 💡 **Interview tip:** Mention the trade-off you're making: "Ranges mean one server crash wastes up to 10,000 IDs. With 1.1 trillion IDs and 6 billion needed, that's irrelevant, and it removes the global counter from the write path."

> 🔗 **Connections:** Encoding the home region in the ID is the same idea as routing by key in partitioning (7.7): the key tells you where the data lives. Lease-based range allocation is a mini version of the leases in 6.6.

## 14.7 Caching and Invalidation

### The Cache Layers

From the user inward (8.1):

| Layer | What it holds | TTL | Why |
| :---- | :---- | :---- | :---- |
| Browser | The redirect itself, if we allow it | `max-age` we choose (or none) | Zero cost to us, but we can't invalidate it (8.9). |
| CDN / edge | The redirect response | Short `s-maxage` (e.g. 60 s) + purge API | Absorbs viral links; edge purge exists for takedowns (11.4). |
| In-process (per app server) | Only the hottest few thousand codes | ~1–5 s | Protects one Redis shard from a single viral code (7.5). |
| Redis / Memcached cluster (per region) | Hot set of `code → long_url` | ~24 h | The main cache: ~90% of redirects end here. |
| DB buffer pool / page cache | Recently read pages | n/a | The DB's own cache (9.2). |

**Sizing the main cache.** Under the interview's assumption (each URL gets its clicks in the month it's created), the hot set is roughly the last 30 days of URLs:

```
100M URLs x ~600 B                     =  60 GB raw
+ per-key overhead in Redis (~2x for small values, roughly)  -> ~100-120 GB
3 shards x 64 GB primaries (+1 replica each in another AZ)   =  192 GB
```

**What the hit ratio buys** (8.2):

```
hit ratio 90%:  DB reads = 12,000 x 0.10 = 1,200/s at peak
hit ratio 99%:  DB reads = 12,000 x 0.01 =   120/s
```

Real traffic is more skewed than "uniform over a month" (a few links get most clicks), which usually makes the hit ratio higher, not lower.

### Read Path: Cache-Aside

```
redirect(code):
    url = cache.get(code)                          # ~1 ms
    if url is NEGATIVE: return 404                 # see 8.8, 14.8
    if url: return 302(url)
    row = db.get(code, consistency=LOCAL_QUORUM)   # ~5 ms
    if row is None: row = ask_home_region(code)    # rare, 14.8
    if row is None:
        cache.set(code, NEGATIVE, ttl=30s)
        return 404
    cache.add(code, row.url, ttl=24h)              # add = only if absent
    return 302(row.url)
```

This is **cache-aside** (8.3): the app, not the cache, talks to the database. A cache outage degrades latency but doesn't break correctness.

### The Write Path: Delete, Don't Update

This is the "destination changed" question. When a user edits `aB3xK9p` from `old.example` to `new.example`:

```
 1. write the new URL to the DB (home region, W=2)
 2. DELETE the cache key   (not SET the new value)
 3. let the next read refill it from the DB
```

Why **delete** instead of writing the new value into the cache (8.5)? Because two concurrent writers can apply their DB writes in one order and their cache writes in the other, leaving the cache permanently holding the losing value. A delete can't be "out of order" in that way: the worst case is an extra cache miss.

### The Write/Fill Race (and Why TTL Still Matters)

Delete-on-write has one known race, between a reader filling the cache and a writer deleting it:

```
 reader (cache miss)          DB                writer (edit)        cache
 -------------------          --                -------------        -----
 read DB --------------> old.example
                                 <------ write new.example
                                                 delete key ------> (empty)
 set(key, old.example) ----------------------------------------->  old.example
                                                       STALE until TTL expires
```

The reader read before the write and filled after the delete. Fixes, from simplest to strongest (8.5):

- **TTL as a safety net.** Every entry expires (24 h above). That bounds how long *any* bug, race or lost invalidation can serve the old value. Never cache without a TTL.
- **Leases** (from Facebook's memcache design, 14.11): on a miss the cache hands the reader a token; a delete invalidates the token, so the late `set` is rejected.
- **Versioned values:** store `(url, version)` and only overwrite with a higher version.

For a URL shortener, where edits are rare, delete + TTL + a short delayed second delete (delete again ~1 s later) is usually enough.

### Invalidating Across Regions

The EU cache also holds `aB3xK9p → old.example`. The edit happened in the US. Who deletes the EU copy, and **when**?

The trap is deleting too early:

```
 us-east                         eu-west DB replica        eu-west cache
 -------                         ------------------        -------------
 write new.example
 send "delete aB3xK9p" ----------------------------------> delete
                                 (still old: lagging)
                       EU reader misses, reads replica:    set old.example
 replication arrives ----------> new.example
                                                     STALE until TTL
```

If the invalidation outruns replication, an EU reader refills the cache from the lagging replica with the old value. The fix is to **invalidate from each region's own replication stream**: a small process in each region tails the local database's change log (**change data capture**, 10.6) and deletes the cache key **after the local replica has applied the change**.

```
 us-east DB --async replication--> eu-west DB replica
                                         |
                                         | CDC: "aB3xK9p changed"
                                         v
                                   invalidator --> DELETE eu-west cache key
                                   (runs only after the replica has the new row,
                                    so the refill reads new.example)
```

This is the same pattern Facebook described for memcache across regions (14.11): invalidations are driven by the database's commit log, not by the app. If the CDC pipeline is down or slow, the 24 h TTL still bounds staleness, and you can lower it if the product needs faster edits.

The staleness budget for an edit then looks like: replication lag (~1 s) + CDC delay (~1 s) + CDN `s-maxage` (≤ 60 s) + browser cache (0 with 302, see below). That meets the "within ~1 minute" target from 14.1.

### 301 vs 302: the Cache You Don't Control

The last cache is the user's browser (8.9):

| Status | Meaning | Browser caching | Edits take effect? | Click analytics |
| :---- | :---- | :---- | :---- | :---- |
| **301** Moved Permanently | "Always go there" | Cacheable by default; browsers may keep it a long time without asking us again | No: a browser that cached it never asks again | Lost after the first click |
| **302** Found / **307** Temporary Redirect | "Go there this time" | Not cached unless we send `Cache-Control`/`Expires` | Yes, on the next click | Every click reaches us (or the CDN) |
| **308** Permanent Redirect | Like 301, keeps the method | Like 301 | No | Lost |

Since this product supports edits, takedowns and analytics, use **302** with `Cache-Control: public, max-age=0, s-maxage=60`: browsers don't cache it (`max-age=0`), while the CDN may cache it for up to 60 s (`s-maxage` applies only to shared caches). Don't use `private` here: it forbids the CDN from storing the response at all (8.9). 301 saves traffic but hands control of the mapping to millions of browsers you can't invalidate.

> 💡 **Interview tip:** "301 vs 302" is really a cache invalidation question. Answer: "A 301 is cached by browsers and I can never invalidate it, so edits and takedowns wouldn't work and I'd lose click counts. I'd use 302, and cache at the CDN with a short TTL where I *can* purge."

### Negative Caching and Stampedes

- **Negative caching** (8.8): a 404 is cached too, otherwise a bot scanning random codes sends every request to the database (and, in 14.8, across the ocean). Keep negative TTLs **short** (~30 s), because a code that doesn't exist *here* may exist soon (14.8). A rate limiter (11.7) stops the scanner itself. A Bloom filter of all existing codes (13.1) can reject most non-existent codes without a lookup: 6B codes at a 1% false-positive rate need ~9.6 bits each, ~7 GB, so it's an option, not a necessity.
- **Stampede** (8.7): when a viral code's entry expires, thousands of concurrent requests miss at once and all hit the DB. Use **request coalescing** (one fill per key per server, others wait for it) and jittered TTLs so hot keys don't expire together.

> 🔗 **Connections:** Delete-on-write and the write/fill race are the distributed version of cache coherence on one machine: MESI invalidates other cores' copies on a write, and the TTL plays the role a coherence protocol can't. CDC for invalidation is the transactional outbox idea (10.6) applied to caches. The negative cache is what keeps 14.8's cross-region fallback from turning into a cross-region amplification attack.

## 14.8 Going Multi-Region (and the "created in US, clicked in EU" problem)

### The Setup

Two regions (`us-east`, `eu-west`), each with the full stack and a full copy of the data (14.5). GeoDNS or anycast sends each user to the nearest healthy region (11.3). Each code has a **home region** encoded in its ID (14.6), and its creates and edits are written there; the other region receives them by asynchronous replication.

### The Problem

A user in New York creates `aB3xK9p` and pastes it into a chat. A colleague in Paris clicks it two seconds later:

```
 time ->   0 s                   ~1 s                  2 s
 us-east   create, W=2, 201 OK
 replication                     ...in flight...
 eu-west                                               GET /aB3xK9p
                                                       cache: miss
                                                       local DB: not found (lag)
                                                       -> 404 ??   <- WRONG
```

The EU replica isn't wrong, it's **behind** (5.4). Replication lag is usually well under a second, but under load, during a network blip or a compaction storm, it can be seconds or more. A 404 for a link that exists is a correctness bug, and if that 404 is then cached for an hour it's an outage for that link.

### The Options

| Option | How | Cost | Verdict |
| :---- | :---- | :---- | :---- |
| Synchronous cross-region writes | Don't ack the create until EU has it | +~100 ms per create, and creates fail when the regions can't talk | Overkill (14.5, option C). |
| Read every code from its home region | EU always asks the US | +~100–150 ms on **every** EU redirect | Breaks p99. |
| **Read locally, fall back to the home region on a miss** | EU tries locally; only a miss goes to the home region | +~100–150 ms on a tiny fraction of requests | **Pick this.** |
| Push to remote caches on create | US also writes `aB3xK9p` into the EU cache asynchronously | Extra cross-region traffic on every create; still racy | Optional accelerator. |

### The Fix: Local Read, Fallback to Origin, Short Negative Cache

```
 Paris user        eu-west app              eu-west cache/DB     us-east (home)
 ----------        -----------              ----------------     --------------
 GET /aB3xK9p --->
                   cache.get ------------->  miss
                   db.get (LOCAL_QUORUM) ->  not found
                   decode code -> home = us-east
                   GET aB3xK9p (timeout ~150 ms) ----------------->  found
                   <------------------------------------------- long_url
                   cache.add(code, url, ttl=short, e.g. 60 s)
 <--- 302 ---------
```

The rules, each one there for a reason:

1. **Read locally first.** 99.99+% of redirects end here, in a few ms.
2. **On a local miss, ask the home region**, which is authoritative for codes it allocated: the create was acknowledged there with W=2, so a `LOCAL_QUORUM` read in the home region is guaranteed to see it (5.7). The home region comes from the code itself (14.6), so it's one targeted call, not a broadcast.
3. **Cache the positive answer with a short TTL** (~60 s). Normal replication will deliver the row soon, and the CDC invalidator (14.7) won't see an edit for this key until the local replica has it; the short TTL keeps the window for a missed edit small.
4. **Only negative-cache a 404 confirmed by the home region**, and only briefly (~30 s). Never cache "not found locally" as a 404: that's the bug that turns a 1-second lag into a broken link.
5. **If the home region is unreachable**, don't claim the link doesn't exist. Return a 503 (or a 404 that is not cached) and let a retry succeed later (12.5).
6. **Protect the fallback.** It's a cross-region call, so it gets its own timeout (2.7), a circuit breaker (12.4) and a rate limit (11.7), plus the negative cache. Otherwise a bot scanning random codes in the EU would turn every guess into a transatlantic request.

**Does the fallback break p99?** Only if more than 1% of redirects take it. Under the interview's uniform-traffic assumption, the share of clicks that land within a ~2 s lag window after creation is about 2 s / 30 days ≈ 0.0001%. Real clicks are front-loaded (many arrive in the first minutes), but even then the fraction within seconds of creation, and from the *other* region, is far below 1%. Those few requests take ~100–150 ms; the p99 stays at a few ms.

What about the **creator** clicking their own link? They're in the home region, so their read hits the same quorum that acknowledged the write (W = 2, R = 2, N = 3): **read-your-writes** comes for free (4.4). If they hop regions (a VPN, a phone switching networks), the fallback covers them too.

> 💡 **Interview tip:** The one-sentence answer is: "Serve reads locally; on a miss, fall back to the code's home region, which is authoritative for it; cache the result; and only negative-cache a 404 that the home region confirms, with a short TTL." Then mention that the home region is encoded in the code, so the fallback is a single targeted call.

### Why Not Multi-Leader Without Ownership?

If both regions could accept edits for the same code, two concurrent edits would conflict and need resolution: last-writer-wins by timestamp (which silently drops one edit and trusts clocks, 3.1, 5.6), or something smarter (13.6). With a home region per code, edits are serialized in one place, and the only multi-region work is one-way replication. Conflicts are designed out instead of resolved.

> 🔗 **Connections:** This is the replication-lag fix "fall back to the origin region on miss" from 5.4, combined with negative caching (8.8) and session guarantees (4.4). The home-region idea is partitioning (Part 7) applied to *write ownership* across regions. What happens when the home region itself is down is in 14.9.

## 14.9 Failure Walkthrough: lose a server, an AZ, a region

A design isn't done until you've broken it on purpose (12.8). For each failure, ask four questions: **how is it detected, what do users see, is there enough capacity left, and is any data lost?**

The starting point, per region: 9 app servers (3 per AZ, 2,000 QPS each at ~50% CPU), a Redis cluster of 3 primaries + 3 replicas, 12 database nodes with RF = 3 (one replica per AZ), 3 etcd nodes. Peak is 12K QPS globally; say ~60% lands in `us-east` and ~40% in `eu-west`.

```
             us-east                                 eu-west
 +-------- AZ-a -------+-- AZ-b --+-- AZ-c --+   +-- same layout --+
 | app x3              | app x3   | app x3   |   |                 |
 | redis P1, R3        | P2, R1   | P3, R2   |   |                 |
 | db x4 (1 replica of | db x4    | db x4    |   |                 |
 |   every key)        |          |          |   |                 |
 | etcd x1             | etcd x1  | etcd x1  |   |                 |
 +---------------------+----------+----------+   +-----------------+
```

### Lose One App Server

- **Detection:** the load balancer's health checks fail, e.g. 3 misses at a 2 s interval ≈ 6 s (12.6). Passive detection (connection errors) is often faster.
- **User impact:** requests in flight on that server fail. Redirects are idempotent GETs, so the LB or edge can retry them once on another server (2.6, 2.7). Most users see nothing; a few see one slow request.
- **Capacity:** 8 × 2,000 = 16,000 QPS ≥ 12,000. Fine even if *all* global traffic were in this region.
- **Data:** none lost: the app tier is stateless (11.1).
- **Recovery:** the autoscaler or orchestrator replaces it (Borg/Kubernetes, 14.11).

### Lose One Cache Node

- **Detection and failover:** Redis Cluster promotes the replica of the lost primary after its node timeout (seconds, configurable).
- **User impact:** while the shard fails over, and while the new primary's data is cold, ~1/3 of keys miss. Latency for those goes from ~1 ms to ~5–10 ms: still under 100 ms.
- **Capacity check on the DB** (this is the one people forget):

```
normal:          12,000 x 10% miss                     = 1,200 reads/s
one shard down:  1,200 + 12,000 x 90% x 1/3            = 4,800 reads/s
per DB node:     4,800 / 12 nodes                       ~=  400 reads/s   fine
```

  The database must be sized for the **cache-failure** load, not the steady-state load. If it can't take that, a cache failure becomes a database failure (12.3). Request coalescing (8.7) keeps a hot key's misses down to one DB read per server.

### Lose One Database Node

- **Leaderless (Cassandra-style):** each key has 3 replicas; a `LOCAL_QUORUM` needs 2. Losing one node leaves 2 for every key it held, so reads and writes keep working with no failover at all. Writes meant for the dead node are kept as **hints** and replayed when it returns (hinted handoff, 5.7); read repair and anti-entropy with Merkle trees fix anything left over (5.7, 13.4).
- **Single-leader per shard (SQL):** if the dead node was a shard leader, a consensus-backed failover elects a follower (5.5, 6.3), typically within seconds to tens of seconds. Redirects keep working from the cache and followers; **creates** for that shard fail briefly and are retried with the same idempotency key (2.6). Fencing prevents the old leader from accepting writes if it comes back confused (6.6).
- **Data:** none lost: every acknowledged write was on 2 of 3 replicas (5.3).

### Lose an Availability Zone

Everything in AZ-a disappears at once: 3 app servers, a Redis primary and a replica, 4 DB nodes, 1 etcd node.

```
app servers:  9 -> 6     6 x 2,000 = 12,000 QPS  = global peak   OK
              (this is why we bought 9, not 8:  6 / (2/3) = 9)
              (with 8 split 3/3/2, losing a 3-server AZ leaves 10,000 < peak)
redis:        P1 lost -> R1 (in AZ-b) promoted; R3 lost (P3 now unreplicated)
database:     every key loses 1 of 3 replicas -> 2 left -> quorum (2) OK
etcd:         3 -> 2 members, majority of 3 is 2            -> still works
```

- **Detection:** health checks and the load balancer's zone health; the LB stops sending traffic to AZ-a within seconds (11.2, 12.6).
- **User impact:** a short burst of failed or slow requests (in-flight ones, and requests during cache failover), well within the error budget. Writes keep working because every quorum has 2 of 3 members left (6.2).
- **Capacity:** exactly enough at peak, and "enough" already includes the 50% CPU headroom from 14.4, so latency holds. But the region now has **no headroom for a second failure**. That's the moment to freeze deploys and add capacity in the remaining AZs (12.7).
- **Data:** none lost: every acknowledged write was on 2 AZs.

> 💡 **Interview tip:** The AZ math is the most common NALSD follow-up: "To survive losing one of 3 AZs, I need the peak capacity in the remaining 2: 6 ÷ (2/3) = 9 servers, 3 per AZ. N+2 = 8 doesn't survive an AZ loss, because the AZ that dies can take 3 of them."

### Lose a Region

`eu-west` goes dark (power, network, or a bad config push that broke the whole region).

**Detection and traffic shift.** Global health checks fail, and EU users must be sent to `us-east`:

| Mechanism | Failover time | Notes |
| :---- | :---- | :---- |
| GeoDNS with health checks, TTL 60 s | ~1–3 min | Some resolvers and clients cache longer than the TTL (11.3). |
| Anycast / global load balancer | ~seconds to tens of seconds | Routing changes upstream of DNS; no client caching problem (11.3). |

Error budget (12.1) for DNS failover: 99.99% of a 30-day month is ~4.3 minutes of full outage. If EU carries 40% of traffic and fails for ~3 minutes:

```
budget used = 0.40 x 3 min = 1.2 full-outage minutes  ~= 28% of 4.3 min
```

One region failure per month would be survivable; two would not. That's the argument for anycast or a global load balancer if the SLO is 99.99%.

**Capacity.** `us-east` now gets all 12K QPS at peak:

```
us-east app:   9 x 2,000 = 18,000 QPS  >  12,000   OK
...and if an AZ in us-east also fails: 6 x 2,000 = 12,000   still OK
```

Because each region was sized for the global peak *with* AZ-loss headroom, the surviving region absorbs the full load. The EU users' hot set isn't in the US cache yet, so the hit ratio dips for a while:

```
hit ratio ~70%:  DB reads = 12,000 x 0.30 = 3,600/s = ~300/s per node  OK
```

**Latency.** EU users now pay ~80–150 ms of RTT to reach the US. Their p99 goes over 100 ms for the duration: that's an explicit, temporary SLO miss for that slice of users, which is the right trade against failing their requests (12.5).

**Data.**

- **Redirects:** `us-east` has a full copy of everything that had replicated before the failure. Every link works except those created in `eu-west` within roughly the last replication-lag window (~seconds).
- **Those last few EU-created links:** the fallback to their home region (14.8) times out, so the app returns a non-cached 503 or 404. When `eu-west` comes back, replication resumes and they start working again. The short negative-cache TTL means nothing "remembers" them as missing. If `eu-west` were lost permanently, they'd be gone: **RPO = replication lag** (5.9). That's the price of async replication, stated plainly.
- **Creates:** keep working. `us-east` allocates from its own half of the ID space (14.6), so there's nothing to coordinate.
- **Edits to EU-homed codes:** their home is down. Simplest safe choice: reject those edits with a 503 until the region returns (graceful degradation, 12.5). Moving ownership to `us-east` is possible but needs a fenced, explicit takeover (6.6), or you risk two regions both accepting edits for the same code when EU returns.

**Failback** is its own risk. Wait for `eu-west` to catch up on replication, warm its cache, then shift traffic back gradually (e.g. 10% → 50% → 100%) rather than all at once, which would send a cold cache 40% of the traffic in one step (8.7, 12.7).

### Summary

| Failure | Detection | Redirects | Creates | Data lost | Capacity left |
| :---- | :---- | :---- | :---- | :---- | :---- |
| 1 app server | LB health check, ~seconds | Retried, ~no impact | Retried with idempotency key | None | 16K |
| 1 cache node | Redis failover, ~seconds | Slower for ~1/3 of keys | Unaffected | None | DB takes 4.8K reads/s |
| 1 DB node | Quorum / failover | Unaffected (leaderless) or cache-served | Brief errors (single-leader) | None | 2 of 3 replicas |
| 1 AZ | LB zone health | Brief errors, then normal | Normal (quorum 2 of 3) | None | 12K = peak |
| 1 region | Global health checks, ~sec to min | Shifted to other region, higher latency for those users | Normal in surviving region | Last ~seconds of creates in the lost region (until it returns) | 18K in surviving region |

> 🔗 **Connections:** This walkthrough uses the failure-domain math (1.7, 12.2), quorums (6.2), hinted handoff (5.7), failover and fencing (5.5, 6.6), health checks (12.6), error budgets (12.1) and RPO/RTO (5.9). Practicing these failures for real is chaos engineering (12.8).

## 14.10 Bill of Materials

Per region, sized for year 5 and for absorbing the **global** peak alone (14.9). Two regions, so double the per-region column.

| Component | Count per region | Spec (approximate) | Sized by | Where |
| :---- | :---- | :---- | :---- | :---- |
| L7 load balancer | Managed, spans 3 AZs (or ~3–6 Envoy instances) | — | 12K QPS, TLS termination | 11.2, 11.5 |
| App servers | 9 (3 per AZ) | 8 vCPU, 16 GB RAM | 6 ÷ (2/3) for AZ loss; Little's Law for pools | 14.4 |
| Redis cluster | 6 (3 primaries + 3 replicas, spread over AZs) | 64 GB RAM | ~100–120 GB hot set (last month of URLs) | 14.7 |
| Database nodes | 12 (4 per AZ), RF = 3 | 16 vCPU, 64–128 GB RAM, ~4 TB NVMe | ~16 TB replicated data at year 5; ~1.5 TB per node | 14.5 |
| etcd (ID ranges, config) | 3 (1 per AZ) | Small, low-latency SSD | Majority survives an AZ loss | 6.2, 6.8, 14.6 |
| Kafka (clicks, CDC) | 3 brokers (1 per AZ), RF = 3 | ~1–2 TB disk each | ~67 GB/day of click events at ~200 B each, 7-day retention | 10.2, 10.6 |
| CDC invalidators | 2–3 small instances | — | Tail the local DB changes, delete cache keys | 14.7 |
| Analytics consumers | A few | — | Aggregate clicks per code per minute | 10.8, 13.2, 13.3 |

**Global (shared):**

| Component | Purpose | Where |
| :---- | :---- | :---- |
| GeoDNS with health checks, or anycast / global LB | Route users to the nearest healthy region; fail over a region | 11.3 |
| CDN | Optional: cache 302s briefly, purge on edit or takedown | 11.4, 8.9 |
| Object storage backups in a third location | Daily snapshots + change log. Replication copies mistakes instantly; backups let you go back in time | 9.9, 12.8 |
| Metrics, logs, traces; SLO burn-rate alerts | Know when redirect p99 or error rate is burning budget | 12.9, 12.10 |

**Totals:**

```
app servers      9 x 2 regions  = 18
redis nodes      6 x 2          = 12
database nodes  12 x 2          = 24   (~32 TB of replicated data at year 5)
etcd             3 x 2          =  6
kafka brokers    3 x 2          =  6
```

Click event volume, as a check:

```
4K clicks/s avg x 86,400 s/day  ~= 333M events/day x ~200 B  ~= 67 GB/day
x 7 days retention x 3 replicas ~= 1.4 TB  -> fits in 3 brokers of 1-2 TB
```

> 💡 **Interview tip:** End the design with a table like this. NALSD interviewers specifically look for **counts with a reason next to each one**. "12 DB nodes because 16 TB at ~1.5 TB per node" is worth more than any amount of box-drawing.

> 🔗 **Connections:** Every count above comes from a formula earlier in these notes: storage from 1.5, bandwidth from 1.6, servers from 1.7 and 2.4, quorum sizes from 6.2, cache from 8.2. Unique click counting (how many *different* visitors) would use HyperLogLog (13.3), and top-N links a Count-Min Sketch (13.2).

## 14.11 Classic Papers and Systems to Know

Interviewers often ask "how does X do it?" These are the systems whose ideas show up everywhere, with the parts of these notes where each idea lives:

| System / paper | Year | Key ideas | Where |
| :---- | :---- | :---- | :---- |
| **GFS** (Google File System) | 2003 | One metadata master + many chunkservers; large chunks; 3× replication; relaxed consistency for appends | 5.2, 9.9 |
| **MapReduce** | 2004 | Batch computation as map + shuffle + reduce; re-execute failed tasks instead of making them reliable | 10.8 |
| **Bigtable** | 2006 | Sorted wide-column store; range-partitioned tablets; LSM (memtable + SSTables); Chubby for master election | 7.2, 9.1, 9.3 |
| **Chubby** | 2006 | Paxos-based lock service; coarse-grained locks with leases; sequencers (fencing) | 6.5, 6.6, 6.8 |
| **Dynamo** (Amazon) | 2007 | Leaderless, consistent hashing with virtual nodes, sloppy quorums, hinted handoff, vector clocks, Merkle-tree anti-entropy | 3.5, 5.7, 7.4, 13.4 |
| **Cassandra** | 2009 | Dynamo's distribution + Bigtable's data model; tunable consistency; multi-datacenter replication | 5.7, 5.8, 9.3 |
| **Kafka** | 2011 | A partitioned, replicated, append-only log; consumers track offsets; retention instead of deletion on read | 10.2, 10.4 |
| **Spanner** | 2012 | Globally distributed SQL; Paxos per shard; 2PC across shards; TrueTime and commit wait for external consistency | 3.6, 4.2, 9.7 |
| **Scaling Memcache at Facebook** | 2013 | Cache-aside at huge scale; leases against stale sets and stampedes; invalidation from the DB commit log across regions | 8.5, 8.7, 14.7 |
| **Raft** | 2014 | Consensus designed to be understandable: leader election, log replication, safety | 6.3, 6.4 |
| **Borg** (Google) | 2015 | Cluster manager; bin-packing jobs onto machines; the ancestor of Kubernetes | 11.1, 12.2 |

The URL shortener above borrows from almost all of them: Dynamo/Cassandra for the data layer, Chubby/etcd for the ID ranges, Kafka for clicks and invalidations, the Facebook memcache paper for invalidation, and Spanner as the "what if we needed strong consistency across regions" alternative.

> 💡 **Interview tip:** You don't need to have read every paper. You need one sentence per system about the idea it contributed, and to recognize when your design reuses it. "This is the Dynamo approach: consistent hashing plus hinted handoff" shows breadth in one line.

> 🔗 **Connections:** Paxos (6.5) predates all of these. Most of the papers above either use it (Chubby, Spanner) or explain why they avoid it on the data path (Dynamo).

---

# 15. Questions and Answers

These are the follow-up questions interviewers ask once the boxes are on the whiteboard. Each answer points to the sections behind it. Try answering before reading.

## Estimation and Sizing

### How do I size the app tier if the interviewer doesn't give me a per-server QPS?

**Derive it from CPU cost per request, plan for ~50% utilization, then check concurrency with Little's Law.** With ~2 ms of CPU per request on 8 cores, one server does 8 / 0.002 = 4,000 QPS at 100% CPU. Queueing delay grows like 1 / (1 − utilization) (14.4), so plan for ~50%: 2,000 QPS per server, and 12,000 / 2,000 = 6 servers at peak. Then Little's Law (2.4): 2,000/s × 10 ms = 20 requests in flight per server, which sizes thread and connection pools. Finally add failure-domain headroom (1.7). In a real system you'd measure the 2 ms with a load test, not guess it.

### Why 9 servers to survive an AZ failure, when N+2 = 8 already gives spare capacity?

**Because an AZ failure removes a third of the fleet at once, not one or two servers.** You need peak capacity in the 2 surviving AZs: 6 ÷ (2/3) = 9, i.e. 3 per AZ. With 8 servers split 3/3/2, losing an AZ with 3 leaves 5 × 2,000 = 10,000 QPS, below the 12,000 peak (1.7, 14.9). N+1/N+2 protects against independent machine failures; failure-domain math protects against correlated ones (12.2). Size for whichever is larger.

### Why not just run servers at 90% CPU and buy fewer of them?

**Because latency explodes near saturation, and you'd have no room for failures or spikes.** In a simple queueing model, mean response time ≈ service time / (1 − utilization): 2× at 50%, 10× at 90% (14.4). A 90%-busy fleet also can't absorb the traffic from a failed AZ (1.7) or a retry burst (2.7), and its p99 is dominated by queueing, not work (2.3). Batch systems that don't care about latency can run hot; latency-sensitive services shouldn't.

### My service depends on three others, each at 99.99%. What's my availability?

**At best ~99.97%, because serial dependencies multiply.** 0.9999³ ≈ 0.9997, which is ~13 minutes of downtime a month instead of ~4.3 (1.3). Adding your own failures makes it worse. That's why critical paths keep few hard dependencies, and why the redirect path in Part 14 degrades gracefully when analytics (Kafka) or the CDN are down instead of failing (12.5). Parallel redundancy goes the other way: two independent replicas at 99.9% give 1 − 0.001² = 99.9999%, *if* failures are truly independent and failover is instant, which they rarely are.

### Why do fan-out requests make tail latency so much worse?

**The request is as slow as its slowest sub-request.** If each backend has a 1% chance of being slow, a request that fans out to 100 backends hits at least one slow one with probability 1 − 0.99¹⁰⁰ ≈ 63% (2.3). Your p99 becomes their p50-ish. Fixes: fewer, larger fan-outs; hedged requests (send a second copy after the p95 delay); and partial results with a deadline (12.5). The URL shortener avoids the problem by design: a redirect is one key lookup on one shard (7.3).

## Failure, Timeouts and Retries

### Why does adding retries make an outage worse?

**Retries multiply load exactly when the system has the least capacity to absorb it.** If a backend slows down, every caller's retries add traffic on top of the original traffic. With 3 layers each retrying 3 times, one user request can become 3 × 3 × 3 = 27 requests at the bottom (2.7). The overloaded service gets slower, more requests time out, more retries are sent: a feedback loop and a **cascading failure** (12.3). The fixes: retry at only one layer, with exponential backoff and jitter, under a **retry budget** (e.g. retries ≤ 10% of requests), only for idempotent operations and transient errors; plus circuit breakers (12.4) and load shedding (12.5) on the server side.

### A request to the database timed out. Did the write happen?

**You can't know.** A timeout means one of three things: the request never arrived, it was processed but the reply was lost, or it's still running (2.2). That's why a retry must be safe to repeat: make the operation **idempotent**, or attach an **idempotency key** the server uses to deduplicate (2.6). For the URL shortener, `POST /v1/urls` with an `Idempotency-Key` returns the same short code on a retry instead of creating a second one (14.6).

### What's the difference between liveness and readiness checks, and what goes wrong if I mix them up?

**Liveness asks "should this process be restarted?"; readiness asks "should it get traffic right now?"** (12.6). If a liveness check includes a dependency (say, the database), then a database blip makes the orchestrator restart *every* app server at once, turning a dependency problem into a full outage. Liveness should check only the process itself; readiness can check dependencies and warm-up, so a server that's cold or cut off is drained, not killed.

### How do circuit breakers and load shedding differ?

**A circuit breaker protects the caller from a failing dependency; load shedding protects a service from its own callers.** A breaker (12.4) notices that, say, the cross-region fallback in 14.8 is timing out and fails fast for a while instead of tying up threads (Little's Law, 2.4). Load shedding (12.5) rejects excess requests early (cheaply, with a 503) when the server is over capacity, so the requests it does accept still meet their deadline. You usually want both.

## Time and Ordering

### Why can't I just order events across machines by their timestamps?

**Because clocks on different machines disagree, and can even jump backward.** NTP keeps clocks within a few ms in a good datacenter, but drift, bad NTP sources, VM pauses and leap-second handling can produce larger errors (3.1). Last-writer-wins by wall-clock timestamp can then silently discard a write that actually happened later (5.6). Use logical clocks to track causality (3.3, 3.4, 3.5), a single leader or consensus to order writes (6.4), or bounded-uncertainty time like TrueTime (3.6).

### What does a vector clock tell me that a Lamport clock doesn't?

**Whether two events are concurrent.** A Lamport clock guarantees that if A happened before B, then L(A) < L(B), but L(A) < L(B) doesn't tell you whether A actually happened before B (3.4). A vector clock does: if neither vector is ≤ the other, the events were concurrent, which is exactly the case where a replicated store must keep both versions or merge them (3.5, 5.6). Dynamo-style stores used them to detect conflicting writes (14.11).

### How does Spanner give strong consistency across the globe?

**Paxos inside each shard, 2PC across shards, and TrueTime to assign commit timestamps that respect real time.** TrueTime returns an interval guaranteed to contain the true time; on commit, Spanner waits out the uncertainty (**commit wait**, a few ms) before making the write visible, so any transaction that starts later gets a larger timestamp (3.6). The cost: every write needs a cross-region Paxos majority if replicas span regions, so writes take ~tens to hundreds of ms (4.2, 6.5).

## Consistency and Replication

### Does CAP mean I pick two of consistency, availability and partition tolerance?

**No. Partitions happen whether you like it or not, so the real choice is what to do during one: stay consistent (refuse some requests) or stay available (serve possibly stale data).** CAP's "C" is linearizability and its "A" means *every* non-failed node answers, which are both stronger than everyday use (4.6). Outside partitions, the interesting trade-off is latency vs consistency: that's PACELC (4.7). The URL shortener is "PA/EL" for redirects (serve locally, possibly stale) and keeps creates consistent within a region via quorums.

### A user creates a link and immediately gets a 404 when they click it. Why, and how do you fix it?

**They read from a replica that hadn't received the write yet: replication lag violating read-your-writes** (5.4, 4.4). Fixes: read from the leader (or a write quorum) for a short time after a write; make the session sticky to the region where the write happened; or, across regions, fall back to the code's home region on a local miss and never cache a 404 the home region hasn't confirmed (14.8). In Part 14, the creator's own read in the home region uses R = 2, W = 2, N = 3, so read-your-writes comes for free.

### Synchronous or asynchronous replication: what do I lose with each?

**Synchronous costs latency and availability; asynchronous costs durability on failover.** Sync: every write waits for the replica, so a slow or unreachable replica stalls writes (5.3). Async: writes are fast, but if the leader dies, writes it acknowledged but hadn't shipped yet are lost: RPO > 0 (5.9). The common compromise is synchronous to one nearby replica (another AZ, ~1 ms) and asynchronous to far ones (other regions, ~100 ms), which is exactly Part 14's layout (14.5).

### With N = 3, W = 2, R = 2, are reads linearizable?

**Not necessarily, even though R + W > N.** The quorum overlap guarantees a read *contacts* a replica with the latest acknowledged write, but edge cases break linearizability: sloppy quorums that write to substitute nodes, concurrent writes resolved by last-writer-wins, a write that failed overall but succeeded on one replica, and a reader seeing a new value while a later reader still sees the old one if read repair hasn't finished (5.7, 4.2). If you need linearizability, use consensus (Part 6) or the store's linearizable mode (e.g. Cassandra's lightweight transactions).

### What is split brain, and how do fencing tokens prevent it?

**Split brain is two nodes both believing they're the leader and both accepting writes** (5.5). It happens when the old leader is paused or partitioned (not dead), a new one is elected, and the old one wakes up still acting as leader. A **fencing token** is a number that increases with every leadership change (e.g. the Raft term or a lock's version); the storage layer rejects any write carrying a token older than the newest it has seen, so the stale leader's writes bounce (6.6). Timeouts alone can't solve this; only the resource that receives the writes can enforce it.

### Active-active or active-passive across regions for the URL shortener?

**Active-active for redirects, with per-code write ownership.** Every region serves all reads from a full local copy, which is what meets p99 < 100 ms for users everywhere (5.8, 14.5). Every region also accepts creates, but each code has a home region (encoded in its ID) that owns its writes, so there are no write conflicts to resolve (14.6, 14.8). Active-passive would make the passive region's users pay cross-region latency on every redirect, and failover would be a rarely tested event, which is how failovers fail (12.8).

### Is replication a backup?

**No. Replication copies mistakes as faithfully as it copies data.** A bad deploy that deletes rows, or a bug that overwrites destinations, reaches every replica within seconds (5.1). Backups (snapshots plus a change log, stored somewhere independent like object storage in another region) let you go back to a point *before* the mistake (9.4, 12.8). Part 14's bill of materials lists them separately for this reason (14.10).

## Consensus and Coordination

### Can a Raft cluster of 4 nodes tolerate 2 failures?

**No. It tolerates 1, the same as a 3-node cluster.** Raft needs a majority to elect a leader and commit entries: ⌊4/2⌋ + 1 = 3 nodes. With 2 failed, 2 remain, which isn't a majority (6.2, 6.3). In general, 2f + 1 nodes tolerate f failures, so even sizes buy nothing: 4 nodes tolerate 1 failure, like 3, but need 3 acknowledgments per write instead of 2. That's why consensus clusters are 3 or 5 nodes.

### Why not 7 or 9 etcd nodes for extra safety?

**Every write must reach a majority, so more members means slower writes and more chances of a slow member being on the critical path.** 5 nodes tolerate 2 failures, which already covers losing an AZ plus one more node. Beyond that, write latency and leader load grow while the failure scenarios you're protecting against (three simultaneous failures) are dominated by correlated events like a bad config push, which more replicas don't fix (6.8, 12.2). Scale reads with followers or caches, not with a bigger voting membership.

### Why not just use 2PC across regions?

**2PC blocks when the coordinator fails, and across regions it multiplies both latency and failure probability.** After participants vote "yes" in the prepare phase, they hold locks and can't decide on their own until they hear from the coordinator; if it crashes, they wait (9.7). Across regions, each transaction needs at least two cross-region round trips (~200–300 ms), and it commits only if *every* participant region is reachable, so availability is the product of all of them (1.3). Spanner does use 2PC across shards, but each participant is itself a Paxos group, so a "participant" never fails as a single node (14.11). Most designs avoid cross-region transactions entirely: partition ownership (14.8) or sagas with compensating actions (9.8).

### Is Redlock safe for mutual exclusion?

**Not for correctness, according to its best-known critique.** Redlock acquires a lock on a majority of independent Redis nodes with a TTL. Martin Kleppmann's analysis points out that it assumes bounded clock drift, network delays and process pauses, and that it doesn't provide fencing tokens, so a client that pauses (GC, VM migration) past its TTL can keep writing while another client holds the lock (6.7). It's fine for efficiency locks (avoiding duplicate work), where an occasional double holder is harmless. For correctness, use a consensus-based store (etcd, ZooKeeper) and fencing tokens (6.6, 6.8).

### If I have a lease, why do I still need a fencing token?

**Because the lease holder can't reliably know its lease has expired.** A process checks "lease valid", then pauses for 30 seconds (GC, swap, VM freeze), then writes. The lease expired during the pause and someone else holds it now (6.6). The process's own clock check happened before the pause, so it can't protect against this. Only the storage, by rejecting writes with an old token, can.

## Partitioning

### Why consistent hashing instead of `hash(key) mod N`?

**With mod N, changing N moves almost every key; with consistent hashing, only ~1/N of them move.** Going from 10 to 11 nodes with mod N remaps ~10/11 of the keys, which for a cache means a near-total miss storm and for a database means moving almost all data (7.3, 7.4). On a hash ring with virtual nodes, a new node takes small slices from many existing nodes, so load stays balanced and only its share moves (7.6, 13.5).

### One link goes viral at 100K clicks per second. What breaks, and how do you handle it?

**One cache shard (and possibly one DB partition) becomes a hot spot, because all requests share one key** (7.5). Consistent hashing spreads keys, not requests for the *same* key. Fixes, from the edge inward: let the CDN cache the 302 briefly (`s-maxage`, 8.9, 11.4); keep a tiny in-process cache on each app server with a 1–5 s TTL (14.7); coalesce concurrent misses so only one request per server fills the cache (8.7); and if a single cache node still can't keep up, replicate the hot key under several suffixed keys and read a random one.

### "List all links created by user X": why is that harder than a redirect?

**Because the data is partitioned by short code, not by user.** Finding a user's links means asking every partition (scatter-gather, latency set by the slowest one, 2.3, 7.9) unless you maintain a **global secondary index** partitioned by `user_id`, which is a second write per create and is usually updated asynchronously, so it can briefly lag (7.8). It's off the redirect path, so lag there is acceptable.

## Caching

### When a destination URL changes, why delete the cache entry instead of updating it?

**Concurrent updates can apply to the DB and the cache in different orders, leaving the cache stuck on the losing value; a delete can't get "out of order" that way.** Worst case, a delete causes one extra miss (8.5, 14.7). There's still a race between a reader filling the cache with an old value and the writer's delete, which is why every entry needs a TTL, and why stronger designs use leases or versioned values (8.5, 14.11).

### How do you invalidate caches in other regions without serving stale data?

**Drive invalidation from each region's own replication stream, not from the writer.** If the US sends "delete key" straight to the EU cache, it can arrive before the EU database replica has the new value, and the next EU miss refills the cache with the old one (14.7). Instead, a CDC process in each region tails the local database changes (10.6) and deletes the local cache key only after the local replica has applied the write. The TTL bounds staleness if that pipeline stalls.

### 301 or 302 for a URL shortener?

**302 (or 307), unless links are truly immutable and you don't need analytics.** Browsers cache a 301 aggressively and won't ask you again, so edits, takedowns and click counting stop working for anyone who already clicked (8.9). A 302 isn't cached unless you send caching headers, so every click reaches you or your CDN, where *you* control the TTL and can purge (11.4, 14.7).

### A bot requests millions of random short codes. How do you protect the database?

**Rate limit the client, negative-cache 404s briefly, and optionally put a Bloom filter in front.** The rate limiter (11.7) caps the bot. Negative caching (8.8) makes repeated misses cheap, but keep its TTL short (~30 s), since a code that doesn't exist yet might soon. A Bloom filter of existing codes (13.1) answers "definitely doesn't exist" without a lookup, with no false negatives; for 6B codes at 1% false positives it needs ~7 GB (14.7). In a multi-region design this also protects the cross-region fallback from being used as an amplifier (14.8).

### What happens when a cache node dies at peak?

**Its keys all miss at once, and the database must absorb that load.** With 3 cache shards and a 90% hit ratio, losing one shifts 12,000 × 0.9 × 1/3 = 3,600 extra reads/s onto the database (14.9). If the DB was sized only for steady-state misses (1,200/s), that's a 4× jump and possibly a cascading failure (12.3). Size the DB for cache failure, use cache replicas for fast failover, and coalesce misses (8.7).

## Storage and Messaging

### B-tree or LSM tree for the URL shortener's database?

**Either works; the workload is small point reads and rare writes, which B-trees serve with predictable latency.** A B-tree does a lookup in ~3–4 page reads, with the top levels always in memory (9.2). An LSM tree (9.3) shines at high write rates; on reads it may check several SSTables, but per-SSTable Bloom filters (13.1) skip most of them, and the hot set is cached anyway (14.7). The choice usually follows from the database you pick (Postgres → B-tree, Cassandra → LSM) more than from this workload.

### What does "exactly-once" in Kafka actually mean?

**Effectively-once processing within Kafka, not exactly-once delivery to the outside world.** An idempotent producer deduplicates retried sends, and transactions let a consumer-processor atomically write its outputs and commit its input offsets (10.3). Once you cause side effects outside Kafka (a database write, an email), you're back to at-least-once and need idempotent consumers, e.g. upserts keyed by event ID (2.6).

### How do I update the database and publish an event without losing one of them?

**Don't write to both directly: write the event into the database in the same transaction (an outbox table), or derive events from the database's change log (CDC).** Writing to the DB and then to Kafka can crash in between, losing the event; doing it in the other order can publish an event for a write that never committed (10.6). With an outbox or CDC, the database commit is the single source of truth and the event is published at least once from it. Part 14's cross-region cache invalidation uses CDC for exactly this reason (14.7).

### How do you count clicks at 12K/s without hammering the database?

**Emit click events asynchronously to a log and aggregate them in a stream processor.** The redirect handler appends to Kafka (10.2) and returns; a consumer group aggregates per code per minute and writes a few rows instead of 12,000 increments a second (10.4, 10.8). Increments aren't idempotent (2.6), so either dedupe by event ID or accept tiny over-counts on retries. For unique visitors, a HyperLogLog per code estimates distinct counts in ~12 KB (13.3); for "top links right now", a Count-Min Sketch (13.2).

## SLOs and Operations

### What's an error budget, and how does it change decisions?

**It's the amount of unreliability your SLO allows: for 99.99%, about 4.3 minutes of full outage per 30 days, or 0.01% of requests.** (12.1) While budget remains, the team can ship features and take risks; when it's burning fast, deploys freeze and reliability work takes priority. It also turns design arguments into arithmetic: a 3-minute DNS failover that affects 40% of traffic uses ~28% of a month's budget (14.9), which is the case for anycast over DNS-only failover. Alert on **burn rate** (how fast you're spending it), not on single errors (12.10).

### Why roll out a config change region by region instead of everywhere at once?

**Because the most common cause of a multi-region outage is the same bad change applied everywhere.** Redundancy across regions only helps if the regions fail independently, and a global config push makes them fail together (12.2). Canary in one AZ, then one region, watching SLO metrics before going further, with fast rollback (12.7). That keeps each mistake's blast radius to one failure domain, which the capacity math in 14.9 already covers.

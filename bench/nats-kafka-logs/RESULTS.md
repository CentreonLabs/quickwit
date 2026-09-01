# NATS vs Kafka source: indexing throughput

16 vCPU Intel Xeon Processor (SapphireRapids), 21 GiB RAM, broker stores and split store on tmpfs. Measured 2026-09-01 on feat-nats-source @ 7bc158d7.

| configuration | coop | wall | e2e MiB/s | steady MiB/s | 1st publish | CPU-s/GiB | qw cores | dup | done |
|---|---|--:|--:|--:|--:|--:|--:|--:|:--:|
| kafka · 1 pipeline | off | 58 s | 83.3 | 95.6 | 20 s | 27.9 | 2.3 | 1.00 | yes |
| kafka · 1 pipeline | on | 100 s | 48.9 | 97.3 | 62 s | 27.9 | 1.3 | 1.00 | yes |
| kafka · 16 pipelines, backfill, commit 60 s | off | 147 s | 33.0 | 39.3 | 12 s | 237.5 | 7.7 | 8.41 | yes |
| kafka · 16 pipelines, backfill, commit 60 s | on | 1447 s | 3.4 | 3.6 | 3 s | 155.0 | 0.5 | 6.29 | yes |
| kafka · 16 pipelines, commit 60 s | off | 66 s | 74.2 | 461.2 | 65 s | 33.3 | 2.4 | 1.00 | yes |
| kafka · 16 pipelines, commit 60 s | on | 122 s | 40.0 | 57.0 | 5 s | 28.6 | 1.1 | 1.00 | yes |
| kafka · 16 pipelines, commit 10 s | off | 26 s | 189.8 | 396.3 | 16 s | 42.5 | 7.9 | 1.24 | yes |
| kafka · 16 pipelines, commit 10 s | on | 23 s | 213.8 | 355.5 | 5 s | 33.4 | 7.0 | 1.00 | yes |
| nats · 1 source | off | 86 s | 56.3 | 60.3 | 26 s | 35.5 | 2.0 | 1.00 | yes |
| nats · 1 source | on | 91 s | 53.2 | 60.4 | 32 s | 35.3 | 1.8 | 1.00 | yes |
| nats · 16 sources, backfill, commit 60 s | off | 243 s | 20.0 | 20.9 | 64 s | 49.8 | 1.0 | 1.00 | yes |
| nats · 16 sources, backfill, commit 60 s | on | 1072 s | 4.5 | 6.3 | 3 s | 56.1 | 0.2 | 1.00 | yes |
| nats · 16 sources on 16 indexes | off | 247 s | 19.7 | 20.5 | 64 s | 50.4 | 1.0 | 1.00 | yes |
| nats · 16 sources, commit 10 s | off | 250 s | 19.5 | 20.6 | 13 s | 53.9 | 1.0 | 1.00 | yes |
| nats · 16 sources, commit 10 s | on | 278 s | 17.5 | 18.5 | 3 s | 50.6 | 0.9 | 1.00 | yes |

`e2e` is the corpus over the wall clock. `steady` is the steady-state slope divided by `dup`, i.e. how fast the corpus itself advanced, excluding startup and the final flush. `1st publish` is the time to the first published split, which is what cooperative indexing delays. `dup` above 1.00 means documents were indexed more than once.

Regenerate with `./run-matrix.sh` then `python3 report.py`.

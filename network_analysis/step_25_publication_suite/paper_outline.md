# Paper: Robust Yet Fragile
# DNS基础设施多层复杂网络分析

## Abstract
We present the first multi-layer complex network analysis of DNS infrastructure,
modeling 8,874,756 domains across 8 TLD zones as a 5-layer dependency
network. Key findings:
(1) Scale-free: NS α=1.75, IP α=1.29;
(2) Small-world: σ≫1, high clustering with short paths;
(3) Rich-club oligarchy among core ASes;
(4) Robust to random failures (f_c=0.29) but fragile to targeted attacks
    (f_c=0.05), confirming the Barabási paradigm;
(5) Bow-tie CNAME structure: many sources → few sinks;
(6) Network topology predicts security deployment (AUC=0.678).

## Novel Contributions
1. First 5-layer DNS dependency network model
2. Rigorous power-law verification (Clauset et al.)
3. Rich-club analysis of DNS trust concentration
4. Percolation-based resilience thresholds
5. Topology→Security prediction from centrality features
6. Common Crawl integration for web visibility analysis

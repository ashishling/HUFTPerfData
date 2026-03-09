PERFORMANCE MARKETING REPORT (DEC 2025 TO FEB 2026)

EXECUTIVE SUMMARY
- Total paid spend: Rs 5,219,546
- Budget split: Demand Capture 43.2% | Demand Generation 56.8%
- Demand Capture weighted ROAS (platform): 2.47x
- Demand Generation weighted ROAS (platform): 4.29x

1) BUDGET ALLOCATION BETWEEN DEMAND CAPTURE AND DEMAND GENERATION
month_start  demand_capture  demand_generation  capture_share  generation_share
   Dec 2025       638490.46         1409651.57      31.174130         68.825870
   Jan 2026       821030.31          775995.84      51.409948         48.590052
   Feb 2026       795083.87          779293.57      50.501478         49.498522

Interpretation:
- Mix moved from generation-heavy in Dec to near 50:50 in Jan-Feb.
- This indicates active reallocation toward capture over the quarter.

2) WITHIN DEMAND CAPTURE - FINDINGS AND RECOMMENDATIONS
  channel      spend    revenue  orders     roas        cpa  spend_share
   google 1000224.90 2775421.81 1200.82 2.774798 832.951566    44.363649
  blinkit  973474.35 2386704.00 7826.00 2.451738 124.389771    43.177164
instamart  280905.39  395693.00 1193.00 1.408634 235.461350    12.459186

Findings:
- Capture spend concentrated in Google and Blinkit.
- Instamart is smaller and lower-ROAS versus Google/Blinkit in current window.
- Jan to Feb: total capture spend reduced while total capture revenue increased (efficiency improvement
signal).

Recommendations:
- Keep capture near 50% of budget until weekly efficiency is stable.
- Tighten Google search and PMax segmentation before scale.
- Maintain CPA guardrails on Blinkit/Instamart and shift budget weekly toward best 2-week cohorts.

3) WITHIN DEMAND GENERATION - FINDINGS AND RECOMMENDATIONS
month_start      spend    revenue  orders     roas
    2025-12 1409651.57 5137099.71  2259.0 3.644234
    2026-01  775995.84 4119368.80  1881.0 5.308493
    2026-02  779293.57 3475003.11  1655.0 4.459171

Meta demand-generation ROAS trend: Dec 2025 3.64x, Jan 2026 5.31x, Feb 2026 4.46x

Demand-generation signal metrics:
   month  meta_impressions  meta_reach meta_new_customers  d2c_new_customers  branded_search_volume
Dec 2025        16193131.0   6339382.0               None             7735.0                 3250.0
Jan 2026        11100465.0   4023140.0               None             6165.0                 3230.0
Feb 2026        11946131.0   3865727.0               None             4442.0                    0.0

Findings:
- Meta remains primary generation engine; ROAS is strong but has cooled from Jan to Feb.
- Reach and D2C new-customer trend softened into Feb.
- Branded search has a missing Feb value in current source and should be backfilled before final
inference.

Recommendations:
- Refresh creative every 2-3 weeks and preserve an 80:20 proven:test split.
- Add a monthly incrementality test design (geo/cohort holdout).
- Monitor new-customer and branded-search trend as joint health indicators.

INCREMENTAL ROAS OBSERVATIONS (DIRECTIONAL)
  objective_class               period  delta_spend  delta_revenue  iroas_proxy
   demand_capture Dec 2025 -> Jan 2026    182539.85      -77919.42    -0.426863
   demand_capture Jan 2026 -> Feb 2026    -25946.44      218038.80          NaN
demand_generation Dec 2025 -> Jan 2026   -633655.73    -1017730.91     1.606126
demand_generation Jan 2026 -> Feb 2026      3297.73     -644365.69          NaN

Note: iROAS proxy is Delta Revenue / Delta Spend across adjacent months. Treat as directional (short
window, attribution noise).
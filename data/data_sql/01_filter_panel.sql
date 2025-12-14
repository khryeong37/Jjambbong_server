-- INPUTS:
--   swap_enriched: (sender, timestamp, txHash, tx_volume, tokenIn/Out denom+amount hop1~3, side, base_token_mapped, date_kst ...)
--   (central only) account_roi: (sender, start_ts, end_ts, roi_pct)
--
-- PARAMS:
--   :start_ts, :end_ts
--   (filter) :start_date, :end_date, :lag_min, :lag_max, :coin_toggle
--   (central) :coin_toggle, :v_cap
--
-- OUTPUT:
--   <what this query returns>

WITH
/* =========================================================
0) PARAMS
========================================================= */
params AS (
  SELECT
    CAST(:start_date AS date) AS start_date,
    CAST(:end_date   AS date) AS end_date,
    CAST(:lag_min    AS int)  AS lag_min,   -- e.g., -7
    CAST(:lag_max    AS int)  AS lag_max    -- e.g., +7
),

/* =========================================================
1) BASE: 기간 필터(Time Range) 적용된 스왑 로그
- 이후 모든 지표는 이 범위 안에서만 재집계
========================================================= */
swap_in_range AS (
  SELECT
    s.*,
    CAST(s.date_kst AS date) AS date_kst_d
  FROM swap_data s
  JOIN params p ON 1=1
  WHERE CAST(s.date_kst AS date) BETWEEN p.start_date AND p.end_date
),

/* =========================================================
2) COIN FLOW 정의 (기획서: out=매수, in=매도) 기반 파생
- ATOM_out: 받은 ATOM 수량, ATOM_in: 지불한 ATOM 수량
- ATONE도 동일 (문서: ATOM_out - ATOM_in 로 일별 순매수흐름 정의)   [oai_citation:5‡1. 좌측 필터 패널 기획서_수정본.pdf](sediment://file_00000000cc3071faa907963bbe0ac336)
========================================================= */
swap_flows AS (
  SELECT
    sender,
    timestamp_converted AS ts_kst,
    date_kst_d,

    tokenInDenom1,
    tokenOutDenom1,

    /* 코인 단위 수량 흐름 */
    CASE WHEN tokenOutDenom1='ATOM'  THEN tokenOutAmount1 ELSE 0 END AS atom_out_amt,
    CASE WHEN tokenInDenom1 ='ATOM'  THEN tokenInAmount1  ELSE 0 END AS atom_in_amt,

    CASE WHEN tokenOutDenom1='ATONE' THEN tokenOutAmount1 ELSE 0 END AS atone_out_amt,
    CASE WHEN tokenInDenom1 ='ATONE' THEN tokenInAmount1  ELSE 0 END AS atone_in_amt,

    /* USD 환산 거래량(Scale/Share/가중 결합에 사용) */
    tx_volume,

    /* "체인 기준 거래량"은 실무상 tokenIn 기준으로 쪼개는 게 가장 일관됨 */
    CASE WHEN tokenInDenom1='ATOM'  THEN tx_volume ELSE 0 END AS volume_atom,
    CASE WHEN tokenInDenom1='ATONE' THEN tx_volume ELSE 0 END AS volume_atone
  FROM swap_in_range
),

/* =========================================================
3) Scale (거래규모)
- 총 거래 수량(tx_volume): 기간 내 Σ(tx_volume)
- 거래 횟수(Tx Count): 기간 내 COUNT(*)
========================================================= */
scale_metrics AS (
  SELECT
    sender,
    SUM(tx_volume)           AS tx_volume,
    COUNT(*)                 AS tx_count
  FROM swap_flows
  GROUP BY 1
),

/* =========================================================
4) Behavior (거래 패턴)
- net buy ratio: (buy - sell)/(buy + sell)
  - buy: out(받은) 기준, sell: in(지불한) 기준
  - 토글(ATOM/ATONE/ALL)은 UI에서 선택하지만,
    SQL에서는 3개를 모두 미리 산출해두는 걸 추천
========================================================= */
behavior_metrics AS (
  SELECT
    sender,

    /* ATOM 기준 buy/sell (USD 환산이 아니라 "코인 수량" 기반으로도 가능하지만,
       너희는 앞에서 ratio를 tx_volume 기반으로 진행했으니 USD 기반 예시) */
    SUM(CASE WHEN tokenOutDenom1='ATOM' THEN tx_volume ELSE 0 END) AS buy_atom,
    SUM(CASE WHEN tokenInDenom1 ='ATOM' THEN tx_volume ELSE 0 END) AS sell_atom,

    SUM(CASE WHEN tokenOutDenom1='ATONE' THEN tx_volume ELSE 0 END) AS buy_atone,
    SUM(CASE WHEN tokenInDenom1 ='ATONE' THEN tx_volume ELSE 0 END) AS sell_atone
  FROM swap_in_range
  GROUP BY 1
),

behavior_ratios AS (
  SELECT
    sender,

    /* net buy ratio (ATOM) */
    CASE
      WHEN (buy_atom + sell_atom) = 0 THEN 0
      ELSE (buy_atom - sell_atom) / (buy_atom + sell_atom)
    END AS net_buy_ratio_atom,

    /* net buy ratio (ATONE) */
    CASE
      WHEN (buy_atone + sell_atone) = 0 THEN 0
      ELSE (buy_atone - sell_atone) / (buy_atone + sell_atone)
    END AS net_buy_ratio_atone,

    /* net buy ratio (ALL) */
    CASE
      WHEN ((buy_atom+buy_atone) + (sell_atom+sell_atone)) = 0 THEN 0
      ELSE ((buy_atom+buy_atone) - (sell_atom+sell_atone))
           / ((buy_atom+buy_atone) + (sell_atom+sell_atone))
    END AS net_buy_ratio_all
  FROM behavior_metrics
),

/* =========================================================
5) Chain / Mobility
- ATOM Volume Share = volume_atom / (volume_atom + volume_atone)
- ATONE Volume Share = 1 - ATOM share
- Chain Switching Index:
  기간 내 거래 순서에서 tokenInDenom이 ATOM<->ATONE으로 바뀐 비율
  (정의가 기획서에 수식으로 딱 박혀있진 않아, 가장 표준적인 정의로 작성)
========================================================= */
chain_base AS (
  SELECT
    sender,
    SUM(volume_atom)  AS volume_atom,
    SUM(volume_atone) AS volume_atone
  FROM swap_flows
  GROUP BY 1
),

chain_shares AS (
  SELECT
    sender,
    volume_atom,
    volume_atone,
    CASE
      WHEN (volume_atom + volume_atone) = 0 THEN 0
      ELSE volume_atom / (volume_atom + volume_atone)
    END AS atom_volume_share,
    CASE
      WHEN (volume_atom + volume_atone) = 0 THEN 0
      ELSE volume_atone / (volume_atom + volume_atone)
    END AS atone_volume_share
  FROM chain_base
),

switching AS (
  SELECT
    sender,
    SUM(
      CASE
        WHEN prev_coin IS NULL THEN 0
        WHEN prev_coin IN ('ATOM','ATONE')
         AND curr_coin IN ('ATOM','ATONE')
         AND prev_coin <> curr_coin THEN 1
        ELSE 0
      END
    ) AS chain_switch_count,
    COUNT(*) AS coin_tx_count
  FROM (
    SELECT
      sender,
      tokenInDenom1 AS curr_coin,
      LAG(tokenInDenom1) OVER (PARTITION BY sender ORDER BY timestamp_converted) AS prev_coin
    FROM swap_in_range
  ) t
  GROUP BY 1
),

chain_mobility_metrics AS (
  SELECT
    s.sender,
    s.chain_switch_count,
    CASE
      WHEN (s.coin_tx_count - 1) <= 0 THEN 0
      ELSE s.chain_switch_count::float / (s.coin_tx_count - 1)
    END AS chain_switching_index
  FROM switching s
),

/* =========================================================
6) Activity (활동성)
- 활동 일수(Active Days) = COUNT(DISTINCT date_kst)
- 최근 활동 시점(Last Active) = MAX(date_kst)
  + "최근 3/7/30일"은 종료일 기준으로 판단(기획서 예시)  [oai_citation:6‡1. 좌측 필터 패널 기획서_수정본.pdf](sediment://file_00000000cc3071faa907963bbe0ac336)
========================================================= */
activity_metrics AS (
  SELECT
    sender,
    COUNT(DISTINCT date_kst_d) AS active_days,
    MAX(date_kst_d)            AS last_active_date
  FROM swap_in_range
  GROUP BY 1
),

/* =========================================================
7) Price tables + daily returns (외부 결합 필수)
- return_t = ln(P_t/P_{t-1}) 또는 (P_t - P_{t-1})/P_{t-1}  [oai_citation:7‡1. 좌측 필터 패널 기획서_수정본.pdf](sediment://file_00000000cd0c7209bab3424454d865ff)
========================================================= */
atom_ret AS (
  SELECT
    date_kst,
    atom_price_close,
    LN(atom_price_close / LAG(atom_price_close) OVER (ORDER BY date_kst)) AS atom_return
  FROM atom_daily_price
),
atone_ret AS (
  SELECT
    date_kst,
    atone_price_close,
    LN(atone_price_close / LAG(atone_price_close) OVER (ORDER BY date_kst)) AS atone_return
  FROM atone_daily_price
),

/* =========================================================
8) account_daily_netflow (계정별 날짜별 순매수량)
- net_atom_day = Σ(ATOM_out - ATOM_in)
- net_atone_day = Σ(ATONE_out - ATONE_in)  [oai_citation:8‡1. 좌측 필터 패널 기획서_수정본.pdf](sediment://file_00000000cc3071faa907963bbe0ac336)
========================================================= */
account_daily_netflow AS (
  SELECT
    sender,
    date_kst_d AS date_kst,

    SUM(atom_out_amt - atom_in_amt)   AS net_atom_day,
    SUM(atone_out_amt - atone_in_amt) AS net_atone_day
  FROM swap_flows
  GROUP BY 1,2
),

/* =========================================================
9) Timing: best_lag per coin (corr 최대 절댓값 lag 선택)  [oai_citation:9‡1. 좌측 필터 패널 기획서_수정본.pdf](sediment://file_00000000cc3071faa907963bbe0ac336)
- corr( net_*_day , return shifted by L )
========================================================= */
lags AS (
  SELECT generate_series((SELECT lag_min FROM params), (SELECT lag_max FROM params)) AS lag
),

timing_corr_atom AS (
  SELECT
    n.sender,
    l.lag,
    corr(n.net_atom_day, r.atom_return) AS c
  FROM account_daily_netflow n
  JOIN lags l ON 1=1
  JOIN atom_ret r
    ON r.date_kst = (n.date_kst + (l.lag || ' day')::interval)::date
  GROUP BY 1,2
),

timing_corr_atone AS (
  SELECT
    n.sender,
    l.lag,
    corr(n.net_atone_day, r.atone_return) AS c
  FROM account_daily_netflow n
  JOIN lags l ON 1=1
  JOIN atone_ret r
    ON r.date_kst = (n.date_kst + (l.lag || ' day')::interval)::date
  GROUP BY 1,2
),

best_lag_atom AS (
  SELECT DISTINCT ON (sender)
    sender,
    lag AS best_lag_atom,
    c   AS best_corr_atom
  FROM timing_corr_atom
  ORDER BY sender, ABS(c) DESC NULLS LAST
),

best_lag_atone AS (
  SELECT DISTINCT ON (sender)
    sender,
    lag AS best_lag_atone,
    c   AS best_corr_atone
  FROM timing_corr_atone
  ORDER BY sender, ABS(c) DESC NULLS LAST
),

/* =========================================================
10) Unified Timing (토글 없이 단일 기준)
- w_atom = volume_atom / (volume_atom + volume_atone)
- best_lag_unified = round(w_atom*best_lag_atom + w_atone*best_lag_atone)  [oai_citation:10‡1. 좌측 필터 패널 기획서_수정본.pdf](sediment://file_00000000cc3071faa907963bbe0ac336)
- 유형 분류(선행/동행/후행)는 프로젝트에서 기준 확정 필요 (아래는 실무 기본값)
========================================================= */
timing_unified AS (
  SELECT
    c.sender,
    COALESCE(bla.best_lag_atom, 0)  AS best_lag_atom,
    COALESCE(blt.best_lag_atone, 0) AS best_lag_atone,

    /* 활동 비중 가중치 */
    CASE WHEN (c.volume_atom + c.volume_atone)=0 THEN 0
         ELSE c.volume_atom / (c.volume_atom + c.volume_atone) END AS w_atom,
    CASE WHEN (c.volume_atom + c.volume_atone)=0 THEN 0
         ELSE c.volume_atone / (c.volume_atom + c.volume_atone) END AS w_atone,

    ROUND(
      (
        (CASE WHEN (c.volume_atom + c.volume_atone)=0 THEN 0
              ELSE c.volume_atom / (c.volume_atom + c.volume_atone) END) * COALESCE(bla.best_lag_atom, 0)
        +
        (CASE WHEN (c.volume_atom + c.volume_atone)=0 THEN 0
              ELSE c.volume_atone / (c.volume_atom + c.volume_atone) END) * COALESCE(blt.best_lag_atone, 0)
      )::numeric
    )::int AS best_lag_unified
  FROM chain_base c
  LEFT JOIN best_lag_atom  bla ON bla.sender=c.sender
  LEFT JOIN best_lag_atone blt ON blt.sender=c.sender
),

timing_classified AS (
  SELECT
    sender,
    best_lag_atom,
    best_lag_atone,
    w_atom, w_atone,
    best_lag_unified,
    CASE
      WHEN best_lag_unified <= -2 THEN 'LEAD'        -- 선행
      WHEN best_lag_unified >=  2 THEN 'LAG'         -- 후행
      ELSE 'COINCIDENT'                               -- 동행
    END AS timing_class_unified
  FROM timing_unified
),

/* =========================================================
11) Price Correlation (가격 상관도)
- corr_atom = corr(net_atom_day, atom_return) (–1~+1)
- corr_atone = corr(net_atone_day, atone_return)
- unified = w_atom*corr_atom + w_atone*corr_atone  [oai_citation:11‡1. 좌측 필터 패널 기획서_수정본.pdf](sediment://file_00000000cd0c7209bab3424454d865ff)
========================================================= */
corr_atom AS (
  SELECT
    n.sender,
    corr(n.net_atom_day, r.atom_return) AS corr_atom
  FROM account_daily_netflow n
  JOIN atom_ret r ON r.date_kst = n.date_kst
  GROUP BY 1
),
corr_atone AS (
  SELECT
    n.sender,
    corr(n.net_atone_day, r.atone_return) AS corr_atone
  FROM account_daily_netflow n
  JOIN atone_ret r ON r.date_kst = n.date_kst
  GROUP BY 1
),
corr_unified AS (
  SELECT
    c.sender,
    ca.corr_atom,
    ct.corr_atone,
    /* 한 쪽 거래량이 0이면 다른 쪽만 사용 (기획서)  [oai_citation:12‡1. 좌측 필터 패널 기획서_수정본.pdf](sediment://file_00000000cd0c7209bab3424454d865ff) */
    CASE
      WHEN c.volume_atom=0 AND c.volume_atone=0 THEN 0
      WHEN c.volume_atom=0 THEN COALESCE(ct.corr_atone, 0)
      WHEN c.volume_atone=0 THEN COALESCE(ca.corr_atom, 0)
      ELSE
        (c.volume_atom/(c.volume_atom+c.volume_atone))*COALESCE(ca.corr_atom,0)
        + (c.volume_atone/(c.volume_atom+c.volume_atone))*COALESCE(ct.corr_atone,0)
    END AS correlation_score_unified
  FROM chain_base c
  LEFT JOIN corr_atom  ca ON ca.sender=c.sender
  LEFT JOIN corr_atone ct ON ct.sender=c.sender
),

/* =========================================================
12) AII 구성요소 + 점수화
- scale/share/timing/corr를 0~100 점수화 후 가중합 (기획서)  [oai_citation:13‡1. 좌측 필터 패널 기획서_수정본.pdf](sediment://file_00000000554872099e6228d3a0f30b08)
- 정규화 방식(퍼센타일 vs min-max)은 택1이라고 되어 있으므로,
  여기서는 PERCENT_RANK()로 퍼센타일 0~100 사용
========================================================= */
market_totals AS (
  SELECT SUM(tx_volume) AS market_total_volume
  FROM scale_metrics
),

aii_components AS (
  SELECT
    sm.sender,
    sm.tx_volume AS total_volume_token,
    (sm.tx_volume / NULLIF(mt.market_total_volume,0)) AS market_share_token,

    /* Scale Score: total_volume 퍼센타일 */
    (PERCENT_RANK() OVER (ORDER BY sm.tx_volume) * 100.0) AS scale_score,

    /* Share Score: market_share 퍼센타일 */
    (PERCENT_RANK() OVER (ORDER BY (sm.tx_volume / NULLIF(mt.market_total_volume,0))) * 100.0) AS share_score,

    /* Timing Score: best_lag 기반 점수화(선행일수 클수록 가점)
       - 기획서: "best_lag 기반 점수화(선행일수 클수록 가점)"  [oai_citation:14‡1. 좌측 필터 패널 기획서_수정본.pdf](sediment://file_00000000554872099e6228d3a0f30b08)
       - 아래는 예시: lag가 -7이면 100, +7이면 0 (선형 스케일)
    */
    ( ( ( (SELECT lag_max FROM params) - tc.best_lag_unified )::float
        / NULLIF(((SELECT lag_max FROM params) - (SELECT lag_min FROM params))::float, 0)
      ) * 100.0
    ) AS timing_score_all,

    /* Corr Score: (-1~+1) -> (0~100) 변환  [oai_citation:15‡1. 좌측 필터 패널 기획서_수정본.pdf](sediment://file_00000000554872099e6228d3a0f30b08) */
    ( (cu.correlation_score_unified + 1.0) / 2.0 ) * 100.0 AS corr_score_all

  FROM scale_metrics sm
  CROSS JOIN market_totals mt
  LEFT JOIN timing_classified tc ON tc.sender=sm.sender
  LEFT JOIN corr_unified      cu ON cu.sender=sm.sender
),

aii_final AS (
  SELECT
    sender,
    scale_score,
    share_score,
    timing_score_all,
    corr_score_all,

    /* 너희 결정: 0.25 균등 가중 */
    (0.25*scale_score + 0.25*share_score + 0.25*timing_score_all + 0.25*corr_score_all) AS aii_score
  FROM aii_components
)

/* =========================================================
13) 최종: 필터 패널용 account_metrics (기간 내 재집계 결과)
========================================================= */
SELECT
  sm.sender,

  /* 2-2 Scale */
  sm.tx_volume,
  sm.tx_count,

  /* 2-3 Behavior */
  br.net_buy_ratio_atom,
  br.net_buy_ratio_atone,
  br.net_buy_ratio_all,

  /* 2-4 Chain / Mobility */
  cs.atom_volume_share,
  cs.atone_volume_share,
  cmm.chain_switching_index,

  /* 2-5 Activity */
  am.active_days,
  am.last_active_date,

  /* 2-6 Impact */
  af.aii_score,
  tc.timing_class_unified,
  cu.correlation_score_unified

FROM scale_metrics sm
LEFT JOIN behavior_ratios        br  ON br.sender=sm.sender
LEFT JOIN chain_shares           cs  ON cs.sender=sm.sender
LEFT JOIN chain_mobility_metrics cmm ON cmm.sender=sm.sender
LEFT JOIN activity_metrics       am  ON am.sender=sm.sender
LEFT JOIN aii_final              af  ON af.sender=sm.sender
LEFT JOIN timing_classified      tc  ON tc.sender=sm.sender
LEFT JOIN corr_unified           cu  ON cu.sender=sm.sender
;
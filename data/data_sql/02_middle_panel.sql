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

-- =========================
-- Central Bubble Chart (Impact Map) - Split Buy Interpretation
-- tokenIn: 1 asset sold
-- tokenOut1~3: multiple assets bought (split execution), not "route"
-- =========================
-- PARAMS:
--   :start_ts      -- inclusive (timestamp)
--   :end_ts        -- exclusive (timestamp)
--   :coin_toggle   -- 'ATOM' | 'ATOMONE' | 'ALL'   (X축 Net Flow 기준)
--   :v_cap         -- size scaling cap (e.g., p99.5 cross_volume)
--
WITH base AS (
  SELECT
    timestamp_converted AS ts_kst,
    sender,
    txHash,
    tx_volume,

    tokenInAmount1,
    tokenInDenom1,

    tokenOutAmount1, tokenOutDenom1,
    tokenOutAmount2, tokenOutDenom2,
    tokenOutAmount3, tokenOutDenom3,

    price_atom,
    price_atone,

    -- UI/분류 기준으로 ATONE을 ATOMONE으로 통일
    CASE WHEN tokenInDenom1  = 'ATONE' THEN 'ATOMONE' ELSE tokenInDenom1  END AS in_denom_m,

    CASE WHEN tokenOutDenom1 = 'ATONE' THEN 'ATOMONE' ELSE tokenOutDenom1 END AS out1_denom_m,
    CASE WHEN tokenOutDenom2 = 'ATONE' THEN 'ATOMONE' ELSE tokenOutDenom2 END AS out2_denom_m,
    CASE WHEN tokenOutDenom3 = 'ATONE' THEN 'ATOMONE' ELSE tokenOutDenom3 END AS out3_denom_m
  FROM swap_data
  WHERE timestamp_converted >= :start_ts
    AND timestamp_converted <  :end_ts
),

/* -------------------------------------------------------
1) OUT(매수) 레그별 가치(USD) 계산
- 분할 매수이므로 out1~3 중 ATOM/ATOMONE에 해당하는 값만 추출
- tx_volume(총 USD)을 out 가치 비율로 배분해 buy를 계산(과대계산 방지)
-------------------------------------------------------- */
out_values AS (
  SELECT
    b.*,

    -- out 수량(ATOM/ATOMONE) 합 (tokenOut은 1~3 모두 매수로 간주)
    (CASE WHEN out1_denom_m='ATOM'    THEN COALESCE(tokenOutAmount1,0) ELSE 0 END
     + CASE WHEN out2_denom_m='ATOM'  THEN COALESCE(tokenOutAmount2,0) ELSE 0 END
     + CASE WHEN out3_denom_m='ATOM'  THEN COALESCE(tokenOutAmount3,0) ELSE 0 END
    ) AS atom_out_amt,

    (CASE WHEN out1_denom_m='ATOMONE' THEN COALESCE(tokenOutAmount1,0) ELSE 0 END
     + CASE WHEN out2_denom_m='ATOMONE' THEN COALESCE(tokenOutAmount2,0) ELSE 0 END
     + CASE WHEN out3_denom_m='ATOMONE' THEN COALESCE(tokenOutAmount3,0) ELSE 0 END
    ) AS atone_out_amt,

    -- out 가치(USD): 수량 * 해당 코인 가격
    ( (CASE WHEN out1_denom_m='ATOM'   THEN COALESCE(tokenOutAmount1,0) ELSE 0 END
       +CASE WHEN out2_denom_m='ATOM'  THEN COALESCE(tokenOutAmount2,0) ELSE 0 END
       +CASE WHEN out3_denom_m='ATOM'  THEN COALESCE(tokenOutAmount3,0) ELSE 0 END
      ) * COALESCE(price_atom, 0)
    ) AS out_value_atom,

    ( (CASE WHEN out1_denom_m='ATOMONE' THEN COALESCE(tokenOutAmount1,0) ELSE 0 END
       +CASE WHEN out2_denom_m='ATOMONE' THEN COALESCE(tokenOutAmount2,0) ELSE 0 END
       +CASE WHEN out3_denom_m='ATOMONE' THEN COALESCE(tokenOutAmount3,0) ELSE 0 END
      ) * COALESCE(price_atone, 0)
    ) AS out_value_atone
  FROM base b
),

out_value_total AS (
  SELECT
    o.*,
    (COALESCE(out_value_atom,0) + COALESCE(out_value_atone,0)) AS out_value_target_total
  FROM out_values o
),

/* -------------------------------------------------------
2) SELL(매도) 가치(USD) 계산 (tokenIn은 1개)
-------------------------------------------------------- */
in_values AS (
  SELECT
    o.*,

    CASE WHEN in_denom_m='ATOM'    THEN COALESCE(tokenInAmount1,0) * COALESCE(price_atom,0)  ELSE 0 END AS sell_value_atom,
    CASE WHEN in_denom_m='ATOMONE' THEN COALESCE(tokenInAmount1,0) * COALESCE(price_atone,0) ELSE 0 END AS sell_value_atone

  FROM out_value_total o
),

/* -------------------------------------------------------
3) BUY(매수) 가치(USD) 계산
- tx_volume은 "총 거래 USD"이므로,
  out_value 비율로 배분해 코인별 buy_value를 만든다.
- 타겟 out이 둘 다 0이면 buy_value는 0 처리.
-------------------------------------------------------- */
buy_values AS (
  SELECT
    i.*,

    CASE
      WHEN out_value_target_total <= 0 THEN 0
      ELSE tx_volume * (COALESCE(out_value_atom,0)  / out_value_target_total)
    END AS buy_value_atom,

    CASE
      WHEN out_value_target_total <= 0 THEN 0
      ELSE tx_volume * (COALESCE(out_value_atone,0) / out_value_target_total)
    END AS buy_value_atone
  FROM in_values i
),

/* -------------------------------------------------------
4) 계정 단위 집계: X축 Net Flow Ratio 구성용 buy/sell
-------------------------------------------------------- */
netflow_agg AS (
  SELECT
    sender,
    SUM(buy_value_atom)  AS buy_atom,
    SUM(sell_value_atom) AS sell_atom,
    SUM(buy_value_atone)  AS buy_atone,
    SUM(sell_value_atone) AS sell_atone
  FROM buy_values
  GROUP BY 1
),

netflow_ratio AS (
  SELECT
    sender,
    CASE
      WHEN :coin_toggle = 'ATOM' THEN
        CASE WHEN (buy_atom + sell_atom)=0 THEN 0
             ELSE (buy_atom - sell_atom)/(buy_atom + sell_atom) END
      WHEN :coin_toggle = 'ATOMONE' THEN
        CASE WHEN (buy_atone + sell_atone)=0 THEN 0
             ELSE (buy_atone - sell_atone)/(buy_atone + sell_atone) END
      ELSE
        CASE WHEN ((buy_atom+buy_atone) + (sell_atom+sell_atone))=0 THEN 0
             ELSE ((buy_atom+buy_atone) - (sell_atom+sell_atone))
                  / ((buy_atom+buy_atone) + (sell_atom+sell_atone)) END
    END AS net_flow_ratio
  FROM netflow_agg
),

/* -------------------------------------------------------
5) Cross Volume (size)
- Cross: 한 tx 안에 ATOM과 ATOMONE이 같이 등장(매수/매도 어느 쪽이든)
- tx_atom_leg = max(ATOM_in_total, ATOM_out_total)
  - ATOM_in_total: tokenIn이 ATOM이면 tokenInAmount1, 아니면 0
  - ATOM_out_total: out1~3에서 ATOM인 amount 합
-------------------------------------------------------- */
cross_tx AS (
  SELECT
    sender,
    txHash,

    CASE WHEN in_denom_m='ATOM' THEN COALESCE(tokenInAmount1,0) ELSE 0 END AS atom_in_total,

    (CASE WHEN out1_denom_m='ATOM' THEN COALESCE(tokenOutAmount1,0) ELSE 0 END
     + CASE WHEN out2_denom_m='ATOM' THEN COALESCE(tokenOutAmount2,0) ELSE 0 END
     + CASE WHEN out3_denom_m='ATOM' THEN COALESCE(tokenOutAmount3,0) ELSE 0 END
    ) AS atom_out_total,

    -- tx 내 ATOMONE 존재 여부: tokenIn 또는 tokenOut들 중 하나라도 ATOMONE이면 true
    CASE
      WHEN in_denom_m='ATOMONE'
        OR out1_denom_m='ATOMONE' OR out2_denom_m='ATOMONE' OR out3_denom_m='ATOMONE'
      THEN 1 ELSE 0
    END AS has_atone_any,

    -- tx 내 ATOM 존재 여부
    CASE
      WHEN in_denom_m='ATOM'
        OR out1_denom_m='ATOM' OR out2_denom_m='ATOM' OR out3_denom_m='ATOM'
      THEN 1 ELSE 0
    END AS has_atom_any
  FROM buy_values
),

cross_agg AS (
  SELECT
    sender,
    SUM(
      CASE
        WHEN has_atom_any=1 AND has_atone_any=1
          THEN GREATEST(atom_in_total, atom_out_total)
        ELSE 0
      END
    ) AS cross_volume_atom_leg
  FROM cross_tx
  GROUP BY 1
),

/* -------------------------------------------------------
6) Chain Bias (color)
- "분할 매수" 해석이므로, 선호도는 OUT(매수) 가치 기준이 가장 자연스럽다.
-------------------------------------------------------- */
bias_agg AS (
  SELECT
    sender,
    COUNT(*) AS tx_count,
    SUM(tx_volume) AS total_volume,

    SUM(COALESCE(out_value_atom,0))  AS atom_buy_value,
    SUM(COALESCE(out_value_atone,0)) AS atone_buy_value
  FROM buy_values
  GROUP BY 1
),

bias AS (
  SELECT
    sender,
    tx_count,
    total_volume,
    atom_buy_value,
    atone_buy_value,

    CASE
      WHEN (atom_buy_value + atone_buy_value) = 0 THEN NULL
      ELSE atom_buy_value / (atom_buy_value + atone_buy_value)
    END AS atom_volume_share,

    CASE
      WHEN (atom_buy_value + atone_buy_value) = 0 THEN NULL
      ELSE atone_buy_value / (atom_buy_value + atone_buy_value)
    END AS atone_volume_share,

    CASE
      WHEN (atom_buy_value + atone_buy_value) = 0 THEN NULL
      WHEN atom_buy_value / (atom_buy_value + atone_buy_value) >= 0.70 THEN 'ATOM'
      WHEN atone_buy_value / (atom_buy_value + atone_buy_value) >= 0.70 THEN 'ATOMONE'
      ELSE 'MIXED'
    END AS bias_class,

    -- 기준자산은 bias로 결정 (MIXED는 ATOM 고정)
    CASE
      WHEN (atom_buy_value + atone_buy_value) = 0 THEN NULL
      WHEN atom_buy_value / (atom_buy_value + atone_buy_value) >= 0.70 THEN 'ATOM'
      WHEN atone_buy_value / (atom_buy_value + atone_buy_value) >= 0.70 THEN 'ATOMONE'
      ELSE 'ATOM'
    END AS base_asset_for_roi
  FROM bias_agg
)

SELECT
  b.sender AS account_address,

  -- X: Net Flow Ratio
  n.net_flow_ratio AS x_net_flow_ratio,

  -- Y: ROI (백엔드 계산 결과 조인)
  r.roi_pct AS y_roi_pct,

  -- Size: Cross Volume
  COALESCE(c.cross_volume_atom_leg, 0) AS size_cross_volume,

  -- Size normalized (log1p + cap)
  LEAST(
    1.0,
    CASE
      WHEN :v_cap <= 0 THEN 0
      ELSE LN(1 + COALESCE(c.cross_volume_atom_leg,0)) / LN(1 + :v_cap)
    END
  ) AS size_norm_0_1,

  -- Color: Chain Bias
  b.bias_class,
  b.atom_volume_share,
  b.atone_volume_share,

  -- Optional / debug
  b.tx_count,
  b.total_volume,
  b.base_asset_for_roi

FROM bias b
JOIN netflow_ratio n
  ON n.sender = b.sender
LEFT JOIN cross_agg c
  ON c.sender = b.sender
LEFT JOIN account_roi r
  ON r.sender   = b.sender
 AND r.start_ts = :start_ts
 AND r.end_ts   = :end_ts
;
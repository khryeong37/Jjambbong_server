-- =========================
-- ROI Simulation Input SQL
-- Split-buy interpretation
-- =========================
-- PARAMS:
--   :start_ts  -- inclusive (timestamp)
--   :end_ts    -- exclusive (timestamp)
--
WITH base AS (
  SELECT
    timestamp_converted AS ts_kst,
    sender,
    txHash,

    tokenInAmount1,
    tokenInDenom1,

    tokenOutAmount1, tokenOutDenom1,
    tokenOutAmount2, tokenOutDenom2,
    tokenOutAmount3, tokenOutDenom3,

    price_atom,
    price_atone,

    -- ATONE → ATOMONE 통일
    CASE WHEN tokenInDenom1='ATONE' THEN 'ATOMONE' ELSE tokenInDenom1 END AS in_denom_m,

    CASE WHEN tokenOutDenom1='ATONE' THEN 'ATOMONE' ELSE tokenOutDenom1 END AS out1_denom_m,
    CASE WHEN tokenOutDenom2='ATONE' THEN 'ATOMONE' ELSE tokenOutDenom2 END AS out2_denom_m,
    CASE WHEN tokenOutDenom3='ATONE' THEN 'ATOMONE' ELSE tokenOutDenom3 END AS out3_denom_m
  FROM swap_data
  WHERE timestamp_converted >= :start_ts
    AND timestamp_converted <  :end_ts
),

/* -------------------------------------------------------
1) 분할 매수 기준 OUT 수량/가치 계산
-------------------------------------------------------- */
out_calc AS (
  SELECT
    b.*,

    -- ATOM / ATOMONE out 수량 합
    (CASE WHEN out1_denom_m='ATOM' THEN COALESCE(tokenOutAmount1,0) ELSE 0 END
     +CASE WHEN out2_denom_m='ATOM' THEN COALESCE(tokenOutAmount2,0) ELSE 0 END
     +CASE WHEN out3_denom_m='ATOM' THEN COALESCE(tokenOutAmount3,0) ELSE 0 END
    ) AS atom_out_amt,

    (CASE WHEN out1_denom_m='ATOMONE' THEN COALESCE(tokenOutAmount1,0) ELSE 0 END
     +CASE WHEN out2_denom_m='ATOMONE' THEN COALESCE(tokenOutAmount2,0) ELSE 0 END
     +CASE WHEN out3_denom_m='ATOMONE' THEN COALESCE(tokenOutAmount3,0) ELSE 0 END
    ) AS atone_out_amt
  FROM base b
),

/* -------------------------------------------------------
2) 계정별 체인 선호도 → 기준자산 결정
-------------------------------------------------------- */
bias AS (
  SELECT
    sender,

    SUM(atom_out_amt * price_atom)  AS atom_buy_value,
    SUM(atone_out_amt * price_atone) AS atone_buy_value
  FROM out_calc
  GROUP BY 1
),

base_asset AS (
  SELECT
    sender,
    CASE
      WHEN (atom_buy_value + atone_buy_value) = 0 THEN 'ATOM'
      WHEN atom_buy_value / (atom_buy_value + atone_buy_value) >= 0.70 THEN 'ATOM'
      WHEN atone_buy_value / (atom_buy_value + atone_buy_value) >= 0.70 THEN 'ATOMONE'
      ELSE 'ATOM'
    END AS base_asset
  FROM bias
),

/* -------------------------------------------------------
3) tx_B_leg 계산
-------------------------------------------------------- */
tx_leg AS (
  SELECT
    o.sender,
    o.txHash,
    o.ts_kst,
    ba.base_asset,

    -- B_in / B_out
    CASE
      WHEN ba.base_asset='ATOM' AND o.in_denom_m='ATOM'
        THEN COALESCE(tokenInAmount1,0)
      WHEN ba.base_asset='ATOMONE' AND o.in_denom_m='ATOMONE'
        THEN COALESCE(tokenInAmount1,0)
      ELSE 0
    END AS b_in_amt,

    CASE
      WHEN ba.base_asset='ATOM' THEN atom_out_amt
      WHEN ba.base_asset='ATOMONE' THEN atone_out_amt
      ELSE 0
    END AS b_out_amt,

    -- 환율 rate
    CASE
      WHEN ba.base_asset='ATOM' AND o.in_denom_m='ATOM'
        THEN atom_out_amt / NULLIF(tokenInAmount1,0)
      WHEN ba.base_asset='ATOMONE' AND o.in_denom_m='ATOMONE'
        THEN atone_out_amt / NULLIF(tokenInAmount1,0)
      WHEN ba.base_asset='ATOM' AND o.in_denom_m='ATOMONE'
        THEN tokenInAmount1 / NULLIF(atom_out_amt,0)
      WHEN ba.base_asset='ATOMONE' AND o.in_denom_m='ATOM'
        THEN tokenInAmount1 / NULLIF(atone_out_amt,0)
      ELSE NULL
    END AS rate
  FROM out_calc o
  JOIN base_asset ba
    ON ba.sender = o.sender
),

/* -------------------------------------------------------
4) direction + tx_B_leg
-------------------------------------------------------- */
tx_direction AS (
  SELECT
    *,
    CASE
      WHEN b_in_amt > 0 AND b_out_amt = 0 THEN 'B_TO_O'
      WHEN b_out_amt > 0 AND b_in_amt = 0 THEN 'O_TO_B'
      ELSE NULL
    END AS direction,

    GREATEST(b_in_amt, b_out_amt) AS tx_B_leg
  FROM tx_leg
  WHERE rate IS NOT NULL
),

/* -------------------------------------------------------
5) share_i 계산
-------------------------------------------------------- */
share_calc AS (
  SELECT
    *,
    SUM(tx_B_leg) OVER (PARTITION BY sender) AS total_B_leg
  FROM tx_direction
)

SELECT
  sender,
  txHash,
  ts_kst,
  base_asset,
  direction,
  rate,
  tx_B_leg,
  CASE
    WHEN total_B_leg = 0 THEN 0
    ELSE tx_B_leg / total_B_leg
  END AS share_i,
  ROW_NUMBER() OVER (PARTITION BY sender ORDER BY ts_kst) AS tx_order
FROM share_calc
WHERE direction IS NOT NULL
;
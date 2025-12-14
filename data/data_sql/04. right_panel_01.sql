-- Right Panel Required A) Header Summary (single account)
-- Params: :account, :start_ts, :end_ts, :coin_toggle

WITH base AS (
  SELECT
    timestamp_converted AS ts_kst,
    CAST(timestamp_converted AS DATE) AS date_kst,
    sender,
    txHash,
    tx_volume,

    COALESCE(tokenInAmount1,0)::numeric AS tokenInAmount1,
    CASE WHEN tokenInDenom1='ATONE' THEN 'ATOMONE' ELSE tokenInDenom1 END AS tokenInDenom1,

    COALESCE(tokenOutAmount1,0)::numeric AS tokenOutAmount1,
    CASE WHEN tokenOutDenom1='ATONE' THEN 'ATOMONE' ELSE tokenOutDenom1 END AS tokenOutDenom1,

    COALESCE(tokenOutAmount2,0)::numeric AS tokenOutAmount2,
    CASE WHEN tokenOutDenom2='ATONE' THEN 'ATOMONE' ELSE tokenOutDenom2 END AS tokenOutDenom2,

    COALESCE(tokenOutAmount3,0)::numeric AS tokenOutAmount3,
    CASE WHEN tokenOutDenom3='ATONE' THEN 'ATOMONE' ELSE tokenOutDenom3 END AS tokenOutDenom3,

    COALESCE(price_atom,0)::numeric  AS price_atom,
    COALESCE(price_atone,0)::numeric AS price_atone
  FROM swap_data
  WHERE sender = :account
    AND timestamp_converted >= :start_ts
    AND timestamp_converted <  :end_ts
),

-- out(분할매수) 수량/가치
out_calc AS (
  SELECT
    b.*,
    (CASE WHEN tokenOutDenom1='ATOM' THEN tokenOutAmount1 ELSE 0 END
     +CASE WHEN tokenOutDenom2='ATOM' THEN tokenOutAmount2 ELSE 0 END
     +CASE WHEN tokenOutDenom3='ATOM' THEN tokenOutAmount3 ELSE 0 END) AS atom_out_amt,

    (CASE WHEN tokenOutDenom1='ATOMONE' THEN tokenOutAmount1 ELSE 0 END
     +CASE WHEN tokenOutDenom2='ATOMONE' THEN tokenOutAmount2 ELSE 0 END
     +CASE WHEN tokenOutDenom3='ATOMONE' THEN tokenOutAmount3 ELSE 0 END) AS atone_out_amt,

    ((CASE WHEN tokenOutDenom1='ATOM' THEN tokenOutAmount1 ELSE 0 END
      +CASE WHEN tokenOutDenom2='ATOM' THEN tokenOutAmount2 ELSE 0 END
      +CASE WHEN tokenOutDenom3='ATOM' THEN tokenOutAmount3 ELSE 0 END) * price_atom) AS out_value_atom,

    ((CASE WHEN tokenOutDenom1='ATOMONE' THEN tokenOutAmount1 ELSE 0 END
      +CASE WHEN tokenOutDenom2='ATOMONE' THEN tokenOutAmount2 ELSE 0 END
      +CASE WHEN tokenOutDenom3='ATOMONE' THEN tokenOutAmount3 ELSE 0 END) * price_atone) AS out_value_atone
  FROM base b
),

buy_sell_value AS (
  SELECT
    o.*,
    (COALESCE(out_value_atom,0) + COALESCE(out_value_atone,0)) AS out_value_total,

    -- sell value (tokenIn 1개)
    CASE WHEN tokenInDenom1='ATOM'    THEN tokenInAmount1 * price_atom  ELSE 0 END AS sell_value_atom,
    CASE WHEN tokenInDenom1='ATOMONE' THEN tokenInAmount1 * price_atone ELSE 0 END AS sell_value_atone,

    -- buy value는 tx_volume을 out 가치비율로 배분(분할매수 과대계산 방지)
    CASE WHEN (COALESCE(out_value_atom,0)+COALESCE(out_value_atone,0))<=0 THEN 0
         ELSE tx_volume * (COALESCE(out_value_atom,0)  / (COALESCE(out_value_atom,0)+COALESCE(out_value_atone,0))) END AS buy_value_atom,

    CASE WHEN (COALESCE(out_value_atom,0)+COALESCE(out_value_atone,0))<=0 THEN 0
         ELSE tx_volume * (COALESCE(out_value_atone,0) / (COALESCE(out_value_atom,0)+COALESCE(out_value_atone,0))) END AS buy_value_atone
  FROM out_calc o
),

agg AS (
  SELECT
    :account AS sender,
    COUNT(DISTINCT txHash) AS tx_count,
    SUM(tx_volume) AS total_volume,
    COUNT(DISTINCT date_kst) AS active_days,
    MAX(ts_kst) AS last_active,

    SUM(buy_value_atom)  AS buy_atom,
    SUM(sell_value_atom) AS sell_atom,
    SUM(buy_value_atone)  AS buy_atone,
    SUM(sell_value_atone) AS sell_atone,

    SUM(out_value_atom)  AS atom_buy_value,
    SUM(out_value_atone) AS atone_buy_value,

    -- cross volume: tx 내 ATOM & ATOMONE이 같이 등장하면 ATOM leg = max(ATOM_in, ATOM_out)
    SUM(
      CASE
        WHEN (
          (tokenInDenom1='ATOM' OR tokenOutDenom1='ATOM' OR tokenOutDenom2='ATOM' OR tokenOutDenom3='ATOM')
          AND
          (tokenInDenom1='ATOMONE' OR tokenOutDenom1='ATOMONE' OR tokenOutDenom2='ATOMONE' OR tokenOutDenom3='ATOMONE')
        )
        THEN GREATEST(
          CASE WHEN tokenInDenom1='ATOM' THEN tokenInAmount1 ELSE 0 END,
          (CASE WHEN tokenOutDenom1='ATOM' THEN tokenOutAmount1 ELSE 0 END
           +CASE WHEN tokenOutDenom2='ATOM' THEN tokenOutAmount2 ELSE 0 END
           +CASE WHEN tokenOutDenom3='ATOM' THEN tokenOutAmount3 ELSE 0 END)
        )
        ELSE 0
      END
    ) AS cross_volume_atom_leg
  FROM buy_sell_value
),

bias_and_ratio AS (
  SELECT
    a.*,
    CASE
      WHEN (atom_buy_value + atone_buy_value)=0 THEN 'MIXED'
      WHEN atom_buy_value/(atom_buy_value+atone_buy_value) >= 0.70 THEN 'ATOM'
      WHEN atone_buy_value/(atom_buy_value+atone_buy_value) >= 0.70 THEN 'ATOMONE'
      ELSE 'MIXED'
    END AS bias_tag,

    CASE
      WHEN :coin_toggle='ATOM' THEN
        CASE WHEN (buy_atom+sell_atom)=0 THEN 0 ELSE (buy_atom-sell_atom)/(buy_atom+sell_atom) END
      WHEN :coin_toggle='ATOMONE' THEN
        CASE WHEN (buy_atone+sell_atone)=0 THEN 0 ELSE (buy_atone-sell_atone)/(buy_atone+sell_atone) END
      ELSE
        CASE WHEN ((buy_atom+buy_atone)+(sell_atom+sell_atone))=0 THEN 0
             ELSE ((buy_atom+buy_atone)-(sell_atom+sell_atone))/((buy_atom+buy_atone)+(sell_atom+sell_atone)) END
    END AS net_flow_ratio
  FROM agg a
)

SELECT
  sender AS account_address,
  bias_tag,
  tx_count,
  total_volume,
  active_days,
  last_active,
  net_flow_ratio,
  cross_volume_atom_leg,

  -- 옵션: 이미 저장된 결과가 있다면 조인해서 채움
  aii.aii_score,
  roi.roi_pct
FROM bias_and_ratio b
LEFT JOIN account_aii aii
  ON aii.sender=b.sender AND aii.start_ts=:start_ts AND aii.end_ts=:end_ts
LEFT JOIN account_roi roi
  ON roi.sender=b.sender AND roi.start_ts=:start_ts AND roi.end_ts=:end_ts
;
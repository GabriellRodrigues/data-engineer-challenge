SELECT
    'deleted transaction' AS kind,
    e.transaction_id,
    e.cdc_timestamp AS happened_at,
    e.amount,
    e.currency,
    e.merchant_id,
    e._source_file AS evidence
FROM fact_transaction_event AS e
WHERE e.op = 'D'
UNION ALL
SELECT
    'reversed settlement',
    s.transaction_id,
    s.settled_at,
    s.amount,
    s.currency,
    s.merchant_id,
    s._source_file
FROM fact_settlement AS s
WHERE s.is_reversed
ORDER BY kind, happened_at

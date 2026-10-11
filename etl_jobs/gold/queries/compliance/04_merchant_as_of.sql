SELECT
    t.transaction_id,
    t.created_at,
    t.merchant_id,
    then_.legal_name AS legal_name_at_transaction,
    now_.legal_name AS legal_name_today,
    then_.primary_cnae AS cnae_at_transaction,
    now_.primary_cnae AS cnae_today,
    then_.document_masked
FROM fact_transaction AS t
JOIN dim_merchant AS then_ ON then_.merchant_sk = t.merchant_sk
JOIN dim_merchant AS now_ ON now_.merchant_id = t.merchant_id AND now_.valid_to IS NULL
WHERE then_.legal_name <> now_.legal_name OR then_.primary_cnae IS DISTINCT FROM now_.primary_cnae
ORDER BY t.created_at
LIMIT 20

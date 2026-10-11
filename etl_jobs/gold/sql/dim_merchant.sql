SELECT
    md5(merchant_id || '|' || epoch_us(valid_from)::VARCHAR) AS merchant_sk,
    merchant_id,
    legal_name,
    trade_name,
    mask_cnpj(document) AS document_masked,
    sha256(document) AS document_hash,
    primary_cnae,
    left(primary_cnae, 2) AS cnae_division,
    valid_from,
    valid_to,
    is_current,
    is_deleted,
    false AS is_unknown
FROM silver_merchants
UNION ALL
SELECT
    'UNKNOWN',
    'UNKNOWN',
    'Unknown merchant',
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    TIMESTAMPTZ '1900-01-01 00:00:00+00',
    NULL,
    true,
    false,
    true

SELECT * FROM (
    VALUES
        ('MATCHED', 'Present on both sides within the R$ 0.01 tolerance', 'both', false, 1::BIGINT),
        ('MISMATCHED', 'Present on both sides with a difference above the tolerance', 'both', true, 2::BIGINT),
        ('UNRECONCILED_PROCESSOR', 'Settled by the processor without an internal record', 'processor_only', true, 3::BIGINT),
        ('UNRECONCILED_INTERNAL', 'Internal record not seen in the processor file', 'internal_only', true, 4::BIGINT)
) AS t(category, description, side, is_discrepancy, sort_order)

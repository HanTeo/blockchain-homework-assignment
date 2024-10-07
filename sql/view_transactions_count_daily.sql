USE raw;
CREATE VIEW transactions_count_daily AS
SELECT
    toDate(blockTimestamp) AS timestamp,
    to AS contract_address,
    COUNT(*) AS transactions_count
FROM
    transactions
WHERE
    to != ''
    AND to IS NOT NULL
GROUP BY
    timestamp,
    contract_address
ORDER BY
    timestamp,
    contract_address;
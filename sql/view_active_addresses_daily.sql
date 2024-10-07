USE raw;
CREATE VIEW active_addresses_daily AS
SELECT
    toDate(blockTimestamp) AS timestamp,
    to AS contract_address,
    COUNT(DISTINCT from) AS active_addresses_count
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
USE raw;
CREATE VIEW gas_used_daily AS
SELECT
    toDate(t.blockTimestamp) AS timestamp,
    t.to AS contract_address,
    SUM(r.gasUsed) AS gas_used
FROM
    transactions t
INNER JOIN
    receipts r ON t.hash = r.transactionHash
WHERE
    t.to != ''
    AND t.to IS NOT NULL
GROUP BY
    timestamp,
    contract_address
ORDER BY
    timestamp,
    contract_address;
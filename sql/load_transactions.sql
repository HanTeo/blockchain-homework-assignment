CREATE DATABASE IF NOT EXISTS raw;

CREATE TABLE raw.transactions
(
  `blockTimestamp` DateTime,
  `blockHash` Nullable(String),
  `blockNumber` Int64,
  `from` String,
  `gas` Int256,
  `gasPrice` Int256,
  `hash` String,
  `input` String,
  `nonce` Int64,
  `to` Nullable(String),
  `transactionIndex` Int64,
  `value` UInt256,
  `type` Nullable(String),
  `chainId` Nullable(String),
  `v` Nullable(String),
  `r` Nullable(String),
  `s` Nullable(String),
  `maxFeePerGas` Nullable(Int256),
  `maxPriorityFeePerGas` Nullable(Int256),
  `accessList` Array(Nullable(String)),
  `yParity` Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(blockTimestamp)
ORDER BY blockTimestamp;

INSERT INTO raw.transactions SELECT
  parseDateTimeBestEffort(blockTimestamp) AS blockTimestamp,
  blockHash,
  blockNumber AS blockNumber,
  `from`,
  reinterpretAsUInt256(reverse(unhex(substring(gas, 3)))) AS gas,
  reinterpretAsUInt256(reverse(unhex(substring(gasPrice, 3)))) AS gasPrice,
  hash,
  input,
  nonce AS nonce,
  `to`,
  transactionIndex AS transactionIndex,
  reinterpretAsUInt256(reverse(unhex(substring(value, 3)))) AS VALUE,
  type, 
  chainId, 
  v, 
  r, 
  s,  maxFeePerGas AS maxFeePerGas,
  maxPriorityFeePerGas AS maxPriorityFeePerGas, accessList, yParity
FROM file('./transactions/transactions_*.jsonl', 'JSONEachRow');
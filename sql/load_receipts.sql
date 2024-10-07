CREATE DATABASE IF NOT EXISTS raw;
CREATE TABLE raw.receipts
(
  `blockNumber` Int64,
  `blockTimestamp` DateTime,
  `blockHash` String,
  `contractAddress` String,
  `cumulativeGasUsed` Int256,
  `effectiveGasPrice` Int256,
  `from` String,
  `gasUsed` Int256,
  `logs` Array(Tuple(
    address String,
    blockHash String,
    blockNumber Int64,
    data String,
    logIndex String,
    removed Bool,
    topics Array(String),
    transactionHash String,
    transactionIndex Int64
  )),
  `logsBloom` String,
  `status` String,
  `to` String,
  `transactionHash` String,
  `transactionIndex` Int64,
  `type` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(blockTimestamp)
ORDER BY blockTimestamp;

INSERT INTO raw.receipts
SELECT
  blockNumber,
  parseDateTimeBestEffort(blockTimestamp) AS blockTimestamp,
  blockHash,
  contractAddress,
  reinterpretAsInt256(reverse(unhex(substring(cumulativeGasUsed, 3)))) AS cumulativeGasUsed,
  reinterpretAsInt256(reverse(unhex(substring(effectiveGasPrice, 3)))) AS effectiveGasPrice,
  from,
  reinterpretAsInt256(reverse(unhex(substring(gasUsed, 3)))) AS gasUsed,
  arrayMap(
    log_tuple -> (
      log_tuple.address,
      log_tuple.blockHash,
      log_tuple.blockNumber,
      log_tuple.data,
      log_tuple.logIndex,  -- We will handle this below
      log_tuple.removed,
      log_tuple.topics,
      log_tuple.transactionHash,
      reinterpretAsInt64(reverse(unhex(substring(log_tuple.transactionIndex, 3))))  -- Convert '0x0' to Int64
    ), logs) AS logs,
  logsBloom,
  status,
  to,
  transactionHash,
  transactionIndex,
  type
FROM file('./receipts/receipts_*.jsonl', 'JSONEachRow');



CREATE DATABASE IF NOT EXISTS raw;
CREATE TABLE raw.blocks
(
	`timestamp` DateTime,
	`number` Int64,
	`baseFeePerGas` Nullable(Int256),
	`difficulty` Nullable(Int256),
	`extraData` Nullable(String),
	`gasLimit` Nullable(Int256),
	`gasUsed` Nullable(Int256),
	`hash` String,
	`logsBloom` Nullable(String),
	`miner` Nullable(String),
	`mixHash` Nullable(String),
	`nonce` Nullable(String),
	`parentHash` Nullable(String),
	`receiptsRoot` Nullable(String),
	`sha3Uncles` Nullable(String),
	`size` Nullable(Int256),
	`stateRoot` Nullable(String),
	`totalDifficulty` Nullable(Int256),
	`transactionsRoot` Nullable(String),
	`uncles` Array(Nullable(String))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY timestamp;

INSERT INTO raw.blocks
SELECT
    parseDateTimeBestEffort(timestamp) AS timestamp,
    number,
    reinterpretAsInt256(reverse(unhex(substring(baseFeePerGas, 3)))) AS baseFeePerGas,
    reinterpretAsInt256(reverse(unhex(substring(difficulty, 3)))) AS difficulty,
    extraData,
    reinterpretAsInt256(reverse(unhex(substring(gasLimit, 3)))) AS gasLimit,
    reinterpretAsInt256(reverse(unhex(substring(gasUsed, 3)))) AS gasUsed,
    hash,
    logsBloom,
    miner,
    mixHash,
    nonce,
    parentHash,
    receiptsRoot,
    sha3Uncles,
    reinterpretAsInt256(reverse(unhex(substring(size, 3)))) AS size,
    stateRoot,
    reinterpretAsInt256(reverse(unhex(substring(totalDifficulty, 3)))) AS totalDifficulty,
    transactionsRoot,
    uncles
FROM file('./blocks/blocks_*.jsonl', 'JSONEachRow');

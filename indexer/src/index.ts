import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';
import { getBlock, getBlockReceipts } from './utils';

dotenv.config();

const START_BLOCK = parseInt(process.env.START || '0', 10);
const BLOCK_COUNT = parseInt(process.env.COUNT || '0', 10);
const BLOCK_RANGE_SIZE = parseInt(process.env.BLOCK_RANGE_SIZE || '1000', 10);

const outputDir = path.join(__dirname, '..', 'data');
if (!fs.existsSync(outputDir)) {
  fs.mkdirSync(outputDir);
}

const blocksOutputDir = path.join(outputDir, 'blocks');
const transactionsOutputDir = path.join(outputDir, 'transactions');
const receiptsOutputDir = path.join(outputDir, 'receipts');

if (!fs.existsSync(blocksOutputDir)) fs.mkdirSync(blocksOutputDir);
if (!fs.existsSync(transactionsOutputDir)) fs.mkdirSync(transactionsOutputDir);
if (!fs.existsSync(receiptsOutputDir)) fs.mkdirSync(receiptsOutputDir);


const fileStreams: {
  [key: string]: {
    blocksFile: fs.WriteStream;
    transactionsFile: fs.WriteStream;
    receiptsFile: fs.WriteStream;
  };
} = {};

async function main() {
  let currentRangeStart = -1;

  console.log(`Starting block processing from ${START_BLOCK} to ${START_BLOCK + BLOCK_COUNT - 1}`);

  for (let i = START_BLOCK; i < START_BLOCK + BLOCK_COUNT; i++) {
    try {
      const rangeStart = Math.floor(i / BLOCK_RANGE_SIZE) * BLOCK_RANGE_SIZE;
      const rangeEnd = rangeStart + BLOCK_RANGE_SIZE - 1;
      const rangeKey = `${rangeStart}_${rangeEnd}`;

      if (currentRangeStart !== rangeStart) {
        if (currentRangeStart !== -1) {
          const prevRangeKey = `${currentRangeStart}_${currentRangeStart + BLOCK_RANGE_SIZE - 1}`;
          await closeFileStreams(prevRangeKey);
        }

        openFileStreams(rangeKey);

        currentRangeStart = rangeStart;
      }

      const { blocksFile, transactionsFile, receiptsFile } = fileStreams[rangeKey];

      console.log(`Processing block ${i}`);

      const block = await getBlock(i);
      if (!block) {
        console.error(`No data returned for block ${i}`);
        continue;
      }

      const blockData = transformBlock(block);
      blocksFile.write(JSON.stringify(blockData) + '\n');

      const blockReceipts = await getBlockReceipts(i);
      if (!blockReceipts) {
        console.error(`No receipts returned for block ${i}`);
        continue;
      }

      await processTransactions(block, blockReceipts, transactionsFile, receiptsFile);
    } catch (error) {
      console.error(`Error processing block ${i}:`, error);
    }
  }

  const finalRangeKey = `${currentRangeStart}_${currentRangeStart + BLOCK_RANGE_SIZE - 1}`;
  if (fileStreams[finalRangeKey]) {
    await closeFileStreams(finalRangeKey);
  }

  console.log('Block processing completed.');
}

function openFileStreams(rangeKey: string) {
  const blocksFileName = `blocks_${rangeKey}.jsonl`;
  const transactionsFileName = `transactions_${rangeKey}.jsonl`;
  const receiptsFileName = `receipts_${rangeKey}.jsonl`;

  fileStreams[rangeKey] = {
    blocksFile: fs.createWriteStream(path.join(blocksOutputDir, blocksFileName), { flags: 'a' }),
    transactionsFile: fs.createWriteStream(path.join(transactionsOutputDir, transactionsFileName), { flags: 'a' }),
    receiptsFile: fs.createWriteStream(path.join(receiptsOutputDir, receiptsFileName), { flags: 'a' }),
  };
}

async function closeFileStreams(rangeKey: string) {
  const { blocksFile, transactionsFile, receiptsFile } = fileStreams[rangeKey];

  blocksFile.end();
  transactionsFile.end();
  receiptsFile.end();

  await Promise.all([
    new Promise((resolve) => blocksFile.on('finish', resolve)),
    new Promise((resolve) => transactionsFile.on('finish', resolve)),
    new Promise((resolve) => receiptsFile.on('finish', resolve)),
  ]);

  delete fileStreams[rangeKey];
}

async function processTransactions(
  block: any,
  blockReceipts: any[],
  transactionsFile: fs.WriteStream,
  receiptsFile: fs.WriteStream
) {
  const blockTimestamp = new Date(parseInt(block.timestamp, 16) * 1000).toISOString();

  const receiptMap = new Map<string, any>();
  for (const receipt of blockReceipts) {
    receiptMap.set(receipt.transactionHash, receipt);
  }

  for (const tx of block.transactions) {
    try {
      const txData = transformTransaction(tx, blockTimestamp);
      transactionsFile.write(JSON.stringify(txData) + '\n');

      const receipt = receiptMap.get(tx.hash);

      if (receipt) {
        const receiptData = transformReceipt(receipt, blockTimestamp);
        receiptsFile.write(JSON.stringify(receiptData) + '\n');
      } else {
        console.error(`No receipt found for transaction ${tx.hash} in block ${block.number}`);
      }
    } catch (error) {
      console.error(`Error processing transaction ${tx.hash}:`, error);
    }
  }
}

function transformBlock(block: any) {
  return {
    timestamp: new Date(parseInt(block.timestamp, 16) * 1000).toISOString(),
    number: parseInt(block.number, 16),
    baseFeePerGas: block.baseFeePerGas || null,
    difficulty: block.difficulty || null,
    extraData: block.extraData || null,
    gasLimit: block.gasLimit || null,
    gasUsed: block.gasUsed || null,
    hash: block.hash,
    logsBloom: block.logsBloom || null,
    miner: block.miner || null,
    mixHash: block.mixHash || null,
    nonce: block.nonce || null,
    parentHash: block.parentHash || null,
    receiptsRoot: block.receiptsRoot || null,
    sha3Uncles: block.sha3Uncles || null,
    size: block.size || null,
    stateRoot: block.stateRoot || null,
    totalDifficulty: block.totalDifficulty || null,
    transactionsRoot: block.transactionsRoot || null,
    uncles: block.uncles || []
  };
}

function transformTransaction(tx: any, blockTimestamp: string) {
  return {
    blockTimestamp: blockTimestamp,
    blockHash: tx.blockHash || null,
    blockNumber: parseInt(tx.blockNumber, 16),
    from: tx.from,
    gas: tx.gas || null,
    gasPrice: tx.gasPrice || null,
    hash: tx.hash,
    input: tx.input,
    nonce: tx.nonce || null,
    to: tx.to || null,
    transactionIndex: parseInt(tx.transactionIndex, 16),
    value: tx.value || null,
    type: tx.type || null,
    chainId: tx.chainId || null,
    v: tx.v || null,
    r: tx.r || null,
    s: tx.s || null,
    maxFeePerGas: tx.maxFeePerGas || null,
    maxPriorityFeePerGas: tx.maxPriorityFeePerGas || null,
    accessList: tx.accessList || [],
    yParity: tx.yParity || null,
  };
}


function transformReceipt(receipt: any, blockTimestamp: string) {
  return {
    blockNumber: parseInt(receipt.blockNumber, 16),
    blockTimestamp: blockTimestamp,
    blockHash: receipt.blockHash || null,
    contractAddress: receipt.contractAddress || null,
    cumulativeGasUsed: receipt.cumulativeGasUsed || null,
    effectiveGasPrice: receipt.effectiveGasPrice || null,
    from: receipt.from,
    gasUsed: receipt.gasUsed || null,
    logs: transformLogs(receipt.logs),
    logsBloom: receipt.logsBloom || null,
    status: receipt.status || null,
    to: receipt.to || null,
    transactionHash: receipt.transactionHash,
    transactionIndex: parseInt(receipt.transactionIndex, 16),
    type: receipt.type || null,
  };
}

function transformLogs(logs: any[]) {
  return logs.map((log) => ({
    address: log.address,
    blockHash: log.blockHash,
    blockNumber: parseInt(log.blockNumber, 16),
    data: log.data,
    logIndex: log.logIndex || null,
    removed: log.removed || false,
    topics: log.topics || [],
    transactionHash: log.transactionHash,
    transactionIndex: log.transactionIndex || null,
  }));
}

main().catch((error) => {
  console.error('Error in main:', error);
});
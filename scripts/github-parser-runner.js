import { Readable } from 'stream';
import { createGzip } from 'zlib';
import { S3Client } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';
const replayDownloader = require('fortnite-replay-downloader');
const parser = require('@theauthenticator/fortnite-replay-parser');

// --- AWS Clients ---
const s3 = new S3Client({
  region: process.env.AWS_REGION || 'us-east-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  }
});

const dynamo = new DynamoDBClient({
  region: process.env.AWS_REGION || 'us-east-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  }
});

const sessionId = process.env.SESSION_ID;
const eventId = process.env.EVENT_ID || 'test_event';
const windowId = process.env.WINDOW_ID || 'test_window';
const callbackUrl = process.env.VPS_CALLBACK_URL;

const MAX_RETRIES = 3;
const PARSE_TIMEOUT_MS = 300000; // 5 minutes timeout per attempt

async function sendCallback(status, errorMsg = null) {
  if (!callbackUrl) return;
  try {
    await fetch(callbackUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id: sessionId,
        event_id: eventId,
        window_id: windowId,
        status: status,
        error: errorMsg,
        timestamp: Date.now()
      })
    });
  } catch (err) {
    console.error(`Failed to send webhook callback to VPS:`, err.message);
  }
}

async function uploadReplayToS3(replayJson, sessionId, timestamp) {
  const date = new Date(timestamp);
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const key = `replays/event_id=${eventId}/window_id=${windowId}/year=${year}/month=${month}/${sessionId}.json.gz`;

  const jsonString = JSON.stringify(replayJson, (_k, v) => (typeof v === 'bigint' ? v.toString() : v));
  const jsonStream = Readable.from(jsonString);
  const gzip = createGzip({ level: 6 });

  const upload = new Upload({
    client: s3,
    params: {
      Bucket: process.env.S3_BUCKET,
      Key: key,
      Body: jsonStream.pipe(gzip),
      ContentType: "application/json",
      ContentEncoding: "gzip"
    }
  });

  await upload.done();
  return key;
}

async function writeMetadataToDynamo(sessionId, s3Key) {
  const params = {
    TableName: process.env.DYNAMO_TABLE || 'fortnite-replay-sessions',
    Item: {
      session_id: { S: sessionId },
      event_id: { S: eventId },
      window_id: { S: windowId },
      s3_key: { S: s3Key },
      parsed_at: { N: Date.now().toString() }
    }
  };
  await dynamo.send(new PutItemCommand(params));
}

async function parseWithTimeout(metadata) {
  return Promise.race([
    parser.parseStreaming(metadata, {
      parseLevel: 10,
      useCheckpoints: false,
      maxConcurrentDownloads: 6,
      maxConcurrentEventDownloads: 6,
      parseTimeoutMs: PARSE_TIMEOUT_MS,
      debug: false
    }),
    new Promise((_, reject) => 
      setTimeout(() => reject(new Error('PARSE_TIMEOUT')), PARSE_TIMEOUT_MS)
    )
  ]);
}

async function run() {
  if (!sessionId) {
    console.error('Error: Missing SESSION_ID environment variable.');
    process.exit(1);
  }

  let attempt = 0;
  let success = false;
  let lastError = null;

  while (attempt < MAX_RETRIES && !success) {
    attempt++;
    console.log(`[Attempt ${attempt}/${MAX_RETRIES}] Downloading metadata for session ${sessionId}...`);
    
    try {
      const metadata = await replayDownloader.downloadMetadata({
        matchId: sessionId,
        chunkDownloadLinks: true,
      });

      console.log(`Starting stream parse for session ${sessionId}...`);
      const parsedReplay = await parseWithTimeout(metadata);

      const rawStats = parsedReplay.statsExport || [];
      if (rawStats.length === 0) {
        throw new Error('EMPTY_STATS');
      }

      console.log(`Parse successful. Uploading to S3 and DynamoDB...`);
      const timestamp = Date.now();
      const s3Key = await uploadReplayToS3(parsedReplay, sessionId, timestamp);
      await writeMetadataToDynamo(sessionId, s3Key);

      success = true;
      console.log(`✅ Successfully processed and stored session ${sessionId}`);
      await sendCallback('SUCCESS');
    } catch (err) {
      lastError = err.message;
      console.warn(`⚠️ Attempt ${attempt} failed for session ${sessionId}: ${lastError}`);
      
      // If it's a permanent error like corrupted file, break early without exhausting all retries
      if (lastError.includes('NOT_ALLOWED') || lastError.includes('CORRUPTED')) {
        break;
      }
    }
  }

  if (!success) {
    console.error(`❌ All attempts failed for session ${sessionId}. Last error: ${lastError}`);
    await sendCallback('FAILURE', lastError);
    process.exit(1);
  }
}

run();
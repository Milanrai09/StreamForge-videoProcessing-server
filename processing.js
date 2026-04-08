import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import fs from "fs/promises";
import { createWriteStream } from "fs";
import { Writable } from "stream";
import { spawn } from "child_process";
import path from "path";
import os from "os";
import { randomUUID } from "crypto";

const s3 = new S3Client({ region: process.env.AWS_REGION });
const bucket = process.env.S3_BUCKET;
const backendUrl = process.env.BACKEND_URL;
const videoUrl = process.env.VIDEO_URL;
const namespace = process.env.NAMESPACE;
const processedPrefix = process.env.S3_PROCESSED_PREFIX || "videos/processed";

const TMP_ROOT = path.join(os.tmpdir(), `transcode-${randomUUID()}`);
const OUTPUT_DIR = path.join(TMP_ROOT, "outputs");

const RENDITIONS = [
  { name: "1080p", width: 1920, height: 1080, bitrate: "5000k", audio_bitrate: "128k", bandwidth: 6000000 },
  { name: "720p",  width: 1280, height: 720,  bitrate: "3000k", audio_bitrate: "128k", bandwidth: 3500000 },
  { name: "480p",  width: 854,  height: 480,  bitrate: "1000k", audio_bitrate: "96k",  bandwidth: 1400000 },
  { name: "360p",  width: 640,  height: 360,  bitrate: "600k",  audio_bitrate: "64k",  bandwidth: 800000  },
];

function runCmd(cmd, args, opts = {}) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { stdio: ["ignore", "inherit", "inherit"], ...opts });
    p.on("error", reject);
    p.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`${cmd} ${args.join(" ")} exited with code ${code}`));
    });
  });
}

function probeDuration(inputPath) {
  return new Promise((resolve, reject) => {
    const args = [
      "-v", "error",
      "-show_entries", "format=duration",
      "-of", "default=noprint_wrappers=1:nokey=1",
      inputPath,
    ];
    const p = spawn("ffprobe", args, { stdio: ["ignore", "pipe", "inherit"] });
    let out = "";
    p.stdout.on("data", (b) => (out += b.toString()));
    p.on("error", reject);
    p.on("close", (code) => {
      if (code !== 0) return reject(new Error("ffprobe failed"));
      const v = parseFloat(out.trim());
      if (Number.isNaN(v)) return reject(new Error("could not parse duration"));
      resolve(v);
    });
  });
}

function mimeTypeForPath(p) {
  const ext = path.extname(p).toLowerCase();
  if (ext === ".m3u8") return "application/vnd.apple.mpegurl";
  if (ext === ".ts")   return "video/mp2t";
  if (ext === ".webm") return "video/webm";
  if (ext === ".jpg" || ext === ".jpeg") return "image/jpeg";
  if (ext === ".mp4")  return "video/mp4";
  return "application/octet-stream";
}

async function ensureDir(dir) {
  await fs.mkdir(dir, { recursive: true });
}

async function listFiles(dir) {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  const files = [];
  for (const e of entries) {
    const full = path.join(dir, e.name);
    if (e.isDirectory()) files.push(...(await listFiles(full)));
    if (e.isFile()) files.push(full);
  }
  return files;
}

async function validateEnvironment() {
  const required = ["AWS_REGION", "S3_BUCKET", "VIDEO_URL", "NAMESPACE"];
  const missing = required.filter((v) => !process.env[v]);
  if (missing.length) {
    throw new Error(`Missing environment variables: ${missing.join(", ")}`);
  }

  await runCmd("ffmpeg", ["-version"]);
  await runCmd("ffprobe", ["-version"]);
}

// ── FIXED: native fetch returns a WHATWG ReadableStream which does not have
//    .pipe(). Use .pipeTo() with Writable.toWeb() for streaming, or fall back
//    to arrayBuffer() for simplicity. We use the streaming path here so large
//    files are never fully buffered in RAM.
async function downloadVideo() {
  console.log("Downloading video from URL:", videoUrl);
  await ensureDir(TMP_ROOT);

  const response = await fetch(videoUrl);
  if (!response.ok) {
    throw new Error(`Failed to download video: ${response.status} ${response.statusText}`);
  }

  const localPath = path.join(TMP_ROOT, "input.mp4");
  const fileStream = createWriteStream(localPath);

  await response.body.pipeTo(Writable.toWeb(fileStream));

  return localPath;
}

async function uploadToS3(localPath, s3Key, contentType) {
  const fileBuffer = await fs.readFile(localPath);
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: s3Key,
      Body: fileBuffer,
      ContentType: contentType,
    })
  );
  return `https://${bucket}.s3.amazonaws.com/${s3Key}`;
}

async function createS3NamespacePrefix(prefix) {
  const folderKey = prefix.endsWith("/") ? prefix : `${prefix}/`;
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: folderKey,
      Body: "",
      ContentType: "application/x-directory",
    })
  );
}

async function transcodeH264Rendition(inputPath, rendition, outDir) {
  const name = rendition.name;
  const playlist = path.join(outDir, `${name}_h264.m3u8`);
  const segmentPattern = path.join(outDir, `${name}_h264_%03d.ts`);

  const args = [
    "-y",
    "-i", inputPath,
    "-c:v", "libx264",
    "-b:v", rendition.bitrate,
    "-vf", `scale=${rendition.width}:${rendition.height}:force_original_aspect_ratio=decrease:force_divisible_by=2`,
    "-preset", "fast",
    "-c:a", "aac",
    "-b:a", rendition.audio_bitrate,
    "-hls_time", "6",
    "-hls_playlist_type", "vod",
    "-hls_segment_filename", segmentPattern,
    playlist,
  ];

  await runCmd("ffmpeg", args);

  return {
    playlist,
    name: `${name}_h264`,
    codec: "h264",
    bandwidth: rendition.bandwidth,
    resolution: `${rendition.width}x${rendition.height}`,
  };
}

async function generateMasterPlaylist(renditionInfos, outDir) {
  const masterPath = path.join(outDir, "master.m3u8");
  let content = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-INDEPENDENT-SEGMENTS\n\n";

  const sorted = [...renditionInfos].sort((a, b) => a.bandwidth - b.bandwidth);
  for (const r of sorted) {
    content += `#EXT-X-STREAM-INF:BANDWIDTH=${r.bandwidth},RESOLUTION=${r.resolution},CODECS="avc1.640028,mp4a.40.2"\n`;
    content += `${path.basename(r.playlist)}\n\n`;
  }

  await fs.writeFile(masterPath, content, "utf8");
  return masterPath;
}

async function generateThumbnails(inputPath, outDir) {
  const thumbsDir = path.join(outDir, "thumbnails");
  await ensureDir(thumbsDir);

  const duration = await probeDuration(inputPath);
  const timestamps = [
    1,
    Math.max(2, Math.floor(duration * 0.25)),
    Math.max(3, Math.floor(duration * 0.50)),
    Math.max(4, Math.floor(duration * 0.75)),
  ];

  const thumbs = [];
  for (let i = 0; i < timestamps.length; i += 1) {
    const out = path.join(thumbsDir, `thumb_${String(i + 1).padStart(2, "0")}.jpg`);
    await runCmd("ffmpeg", [
      "-y",
      "-ss", `${timestamps[i]}`,
      "-i", inputPath,
      "-frames:v", "1",
      "-q:v", "2",
      out,
    ]);
    thumbs.push(out);
  }

  return thumbs;
}

async function processVideo(inputPath) {
  await ensureDir(OUTPUT_DIR);

  const duration = await probeDuration(inputPath);
  console.log(`Video duration: ${duration.toFixed(2)}s`);

  const renditionInfos = [];
  for (const r of RENDITIONS) {
    renditionInfos.push(await transcodeH264Rendition(inputPath, r, OUTPUT_DIR));
  }

  await generateMasterPlaylist(renditionInfos, OUTPUT_DIR);
  await generateThumbnails(inputPath, OUTPUT_DIR);
}

async function uploadAllFiles() {
  const basePrefix = `${processedPrefix}/${namespace}`;

  await createS3NamespacePrefix(basePrefix);

  const files = await listFiles(OUTPUT_DIR);
  const uploadedFiles = [];

  for (const localPath of files) {
    const relativePath = path.relative(OUTPUT_DIR, localPath).split(path.sep).join("/");
    const s3Key = `${basePrefix}/${relativePath}`;
    const contentType = mimeTypeForPath(localPath);
    const url = await uploadToS3(localPath, s3Key, contentType);
    uploadedFiles.push({ key: s3Key, url, type: contentType });
    console.log(`Uploaded: ${s3Key}`);
  }

  return uploadedFiles;
}

async function updateDatabase(uploadedFiles) {
  if (!backendUrl) return;

  const masterPlaylist = uploadedFiles.find((f) => f.key.endsWith("master.m3u8"));
  const thumbnails = uploadedFiles.filter((f) => f.key.includes("thumbnails/"));
  if (!masterPlaylist) throw new Error("Master playlist not found in uploaded files");

  const response = await fetch(backendUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      namespace,
      masterPlaylist: masterPlaylist.url,
      thumbnails: thumbnails.map((t) => t.url),
      allFiles: uploadedFiles,
      totalFiles: uploadedFiles.length,
      status: "processed",
      processedAt: new Date().toISOString(),
    }),
  });

  if (!response.ok) {
    throw new Error(`Backend update failed: ${response.status} ${response.statusText}`);
  }
}

(async () => {
  try {
    await validateEnvironment();
    const inputPath = await downloadVideo();
    await processVideo(inputPath);
    const uploadedFiles = await uploadAllFiles();
    await updateDatabase(uploadedFiles);
    await fs.rm(TMP_ROOT, { recursive: true, force: true });
    console.log("Pipeline complete. Uploaded files:", uploadedFiles.length);
  } catch (err) {
    console.error("Pipeline failed:", err.message);
    await fs.rm(TMP_ROOT, { recursive: true, force: true });
    process.exit(1);
  }
})();
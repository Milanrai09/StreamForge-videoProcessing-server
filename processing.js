// processor.js - Complete video processing pipeline with all features
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import fs from "fs-extra";
import { spawn } from "child_process";
import fetch from "node-fetch";
import path from "path";
import os from "os";
import { v4 as uuidv4 } from "uuid";





const s3 = new S3Client({ region: process.env.AWS_REGION });
const bucket = process.env.S3_BUCKET;
const backendUrl = process.env.BACKEND_URL;
const videoUrl = process.env.VIDEO_URL; // New: download URL from .env

const TMP_ROOT = path.join(os.tmpdir(), 'transcode-' + uuidv4());
const OUTPUT_DIR = path.join(TMP_ROOT, 'outputs');

const RENDITIONS = [
  { name: '1080p', width: 1920, height: 1080, bitrate: '5000k', audio_bitrate: '128k', bandwidth: 6000000 },
  { name: '720p', width: 1280, height: 720, bitrate: '3000k', audio_bitrate: '128k', bandwidth: 3500000 },
  { name: '480p', width: 854, height: 480, bitrate: '1000k', audio_bitrate: '96k', bandwidth: 1400000 },
  { name: '360p', width: 640, height: 360, bitrate: '600k', audio_bitrate: '64k', bandwidth: 800000 }
];

// ============= UTILITY FUNCTIONS =============

function runCmd(cmd, args, opts = {}) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { stdio: ['ignore', 'inherit', 'inherit'], ...opts });
    p.on('error', reject);
    p.on('close', (code) => {
      if (code === 0) resolve();
      else reject(new Error(`${cmd} ${args.join(' ')} exited with code ${code}`));
    });
  });
}

function probeDuration(inputPath) {
  return new Promise((resolve, reject) => {
    const args = [
      '-v', 'error',
      '-show_entries', 'format=duration',
      '-of', 'default=noprint_wrappers=1:nokey=1',
      inputPath
    ];
    const p = spawn('ffprobe', args, { stdio: ['ignore', 'pipe', 'inherit'] });
    let out = '';
    p.stdout.on('data', (b) => out += b.toString());
    p.on('error', reject);
    p.on('close', (code) => {
      if (code !== 0) return reject(new Error('ffprobe failed'));
      const v = parseFloat(out.trim());
      if (isNaN(v)) return reject(new Error('could not parse duration'));
      resolve(v);
    });
  });
}

function mimeTypeForPath(p) {
  const ext = path.extname(p).toLowerCase();
  if (ext === '.m3u8') return 'application/vnd.apple.mpegurl';
  if (ext === '.ts') return 'video/mp2t';
  if (ext === '.webm') return 'video/webm';
  if (['.jpg', '.jpeg'].includes(ext)) return 'image/jpeg';
  if (ext === '.mp4') return 'video/mp4';
  return 'application/octet-stream';
}

async function listFiles(dir) {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  const files = [];
  for (const e of entries) {
    const full = path.join(dir, e.name);
    if (e.isDirectory()) {
      const sub = await listFiles(full);
      files.push(...sub);
    } else if (e.isFile()) {
      files.push(full);
    }
  }
  return files;
}

// ============= VALIDATION =============

async function validateEnvironment() {
  const required = ['AWS_REGION', 'S3_BUCKET', 'BACKEND_URL', 'VIDEO_URL'];
  const missing = required.filter(v => !process.env[v]);
  
  if (missing.length > 0) {
    throw new Error(`Missing environment variables: ${missing.join(', ')}`);
  }

  // Check if ffmpeg and ffprobe are available
  try {
    await runCmd('ffmpeg', ['-version']);
    await runCmd('ffprobe', ['-version']);
  } catch (err) {
    throw new Error('ffmpeg or ffprobe not found. Please install FFmpeg.');
  }
}

// ============= S3 OPERATIONS =============

async function downloadVideo() {
  console.log("⬇️ Downloading video from URL:", videoUrl);
  await fs.ensureDir(TMP_ROOT);
  
  const response = await fetch(videoUrl);
  if (!response.ok) {
    throw new Error(`Failed to download video: ${response.statusText}`);
  }

  const localPath = path.join(TMP_ROOT, "input.mp4");
  const fileStream = fs.createWriteStream(localPath);
  
  await new Promise((resolve, reject) => {
    response.body.pipe(fileStream);
    fileStream.on('finish', resolve);
    fileStream.on('error', reject);
    response.body.on('error', reject);
  });
  
  console.log("✓ Download complete:", localPath);
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

// ============= TRANSCODING FUNCTIONS =============

async function transcodeH264Rendition(inputPath, rendition, outDir) {
  const name = rendition.name;
  const playlist = path.join(outDir, `${name}_h264.m3u8`);
  const segmentPattern = path.join(outDir, `${name}_h264_%03d.ts`);

  const args = [
    '-y',
    '-i', inputPath,
    '-c:v', 'libx264',
    '-b:v', rendition.bitrate,
    '-vf', `scale=${rendition.width}:${rendition.height}:force_original_aspect_ratio=decrease:force_divisible_by=2`,
    '-preset', 'fast',
    '-c:a', 'aac',
    '-b:a', rendition.audio_bitrate,
    '-hls_time', '6',
    '-hls_playlist_type', 'vod',
    '-hls_segment_filename', segmentPattern,
    playlist
  ];

  console.log(`🎞 Transcoding ${name} (H.264)...`);
  await runCmd('ffmpeg', args);
  
  return {
    playlist,
    name: `${name}_h264`,
    codec: 'h264',
    bandwidth: rendition.bandwidth,
    resolution: `${rendition.width}x${rendition.height}`
  };
}

async function generateMasterPlaylist(renditionInfos, outDir) {
  const masterPath = path.join(outDir, 'master.m3u8');
  let content = '#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-INDEPENDENT-SEGMENTS\n\n';
  
  // Sort by bandwidth ascending for proper adaptive streaming
  const sorted = [...renditionInfos].sort((a, b) => a.bandwidth - b.bandwidth);
  
  for (const r of sorted) {
    content += `#EXT-X-STREAM-INF:BANDWIDTH=${r.bandwidth},RESOLUTION=${r.resolution},CODECS="avc1.640028,mp4a.40.2"\n`;
    content += `${path.basename(r.playlist)}\n\n`;
  }
  
  await fs.writeFile(masterPath, content, 'utf8');
  console.log('✓ Master playlist created');
  return masterPath;
}

async function generateThumbnails(inputPath, outDir) {
  const thumbsDir = path.join(outDir, 'thumbnails');
  await fs.ensureDir(thumbsDir);

  console.log('🖼 Generating thumbnails...');
  
  const duration = await probeDuration(inputPath);
  
  if (duration < 4) {
    console.log('⚠️ Video is too short for full thumbnail coverage, adjusting...');
  }

  const thumbs = [];
  const timestamps = [
    1, // 1 second
    Math.max(2, Math.floor(duration * 0.25)), // 25%
    Math.max(3, Math.floor(duration * 0.50)), // 50%
    Math.max(4, Math.floor(duration * 0.75))  // 75%
  ];

  for (let i = 0; i < timestamps.length; i++) {
    const out = path.join(thumbsDir, `thumb_${String(i + 1).padStart(2, '0')}.jpg`);
    await runCmd('ffmpeg', [
      '-y',
      '-ss', `${timestamps[i]}`,
      '-i', inputPath,
      '-frames:v', '1',
      '-q:v', '2',
      out
    ]);
    thumbs.push(out);
  }
  
  console.log(`✓ Generated ${thumbs.length} thumbnails`);
  return thumbs;
}

// ============= MAIN PROCESSING =============

async function processVideo(inputPath) {
  await fs.ensureDir(OUTPUT_DIR);
  
  console.log("🎬 Starting video processing pipeline...");
  
  const duration = await probeDuration(inputPath);
  console.log(`Video duration: ${duration.toFixed(2)}s`);

  const renditionInfos = [];
  
  for (const r of RENDITIONS) {
    const h264Info = await transcodeH264Rendition(inputPath, r, OUTPUT_DIR);
    renditionInfos.push(h264Info);
  }

  await generateMasterPlaylist(renditionInfos, OUTPUT_DIR);
  await generateThumbnails(inputPath, OUTPUT_DIR);

  console.log("✅ All renditions, master playlist, and thumbnails generated");
  return { outputDir: OUTPUT_DIR, renditionInfos };
}

async function uploadAllFiles() {
  const baseFolder = 'video_' + uuidv4().substring(0, 8);
  
  console.log("⬆️ Uploading all processed files to S3...");
  
  const files = await listFiles(OUTPUT_DIR);
  const uploadedFiles = [];
  let uploadCount = 0;

  for (const localPath of files) {
    const relativePath = path.relative(OUTPUT_DIR, localPath).split(path.sep).join('/');
    const s3Key = `processed/${baseFolder}/${relativePath}`;
    const contentType = mimeTypeForPath(localPath);
    
    const url = await uploadToS3(localPath, s3Key, contentType);
    uploadedFiles.push({ key: s3Key, url, type: contentType });
    
    uploadCount++;
    if (uploadCount % 10 === 0) {
      console.log(`  ✓ Uploaded ${uploadCount} files...`);
    }
  }

  console.log(`✅ Upload complete: ${uploadedFiles.length} files`);
  return uploadedFiles;
}

async function updateDatabase(uploadedFiles) {
  console.log("🔁 Updating backend database...");
  
  const masterPlaylist = uploadedFiles.find(f => f.key.includes('master.m3u8'));
  const thumbnails = uploadedFiles.filter(f => f.key.includes('thumbnails/'));
  
  if (!masterPlaylist) {
    throw new Error('Master playlist not found in uploaded files');
  }

  const response = await fetch(`${backendUrl}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      masterPlaylist: masterPlaylist.url,
      thumbnails: thumbnails.map(t => t.url),
      allFiles: uploadedFiles,
      totalFiles: uploadedFiles.length,
      status: "processed",
      processedAt: new Date().toISOString()
    }),
  });

  if (!response.ok) {
    throw new Error(`Backend update failed: ${response.statusText}`);
  }

  const result = await response.json();
  console.log("✓ Database updated successfully");
  return result;
}

// ============= MAIN EXECUTION =============

(async () => {
  const startTime = Date.now();
  
  try {
    console.log("🚀 Starting complete video processing pipeline");
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    
    // Validate environment
    await validateEnvironment();
    
    // Step 1: Download
    const inputPath = await downloadVideo();
    
    // Step 2: Process (transcode + thumbnails)
    const { outputDir } = await processVideo(inputPath);
    
    // Step 3: Upload to S3
    const uploadedFiles = await uploadAllFiles();
    
    // Step 4: Update database
    await updateDatabase(uploadedFiles);
    
    // Cleanup
    console.log("🧹 Cleaning up temporary files...");
    await fs.remove(TMP_ROOT);
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    console.log(`✅ Pipeline complete in ${duration}s`);
    console.log(`📊 Total files: ${uploadedFiles.length}`);
    console.log(`📹 Renditions: 4 (4 resolutions × H.264)`);
    console.log(`🖼️ Thumbnails: 4`);
    console.log(`🎯 Master playlist: 1`);
    
  } catch (err) {
    console.error("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    console.error("❌ Pipeline failed:", err.message);
    console.error(err.stack);
    
    // Cleanup on error
    try {
      await fs.remove(TMP_ROOT);
    } catch (cleanupErr) {
      console.error("Failed to cleanup:", cleanupErr);
    }
    
    process.exit(1);
  }
})();
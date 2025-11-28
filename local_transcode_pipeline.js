/**
 * local_transcode_pipeline.js
 * Simplified local video processing pipeline (no S3)
 *
 * Usage:
 *   node local_transcode_pipeline.js ./input.mp4
 */

const { spawn } = require('child_process');
const fs = require('fs-extra');
const path = require('path');
const os = require('os');

if (process.argv.length < 3) {
  console.error('Usage: node local_transcode_pipeline.js <input_video>');
  process.exit(1);
}

const INPUT = process.argv[2];
const TMP_DIR = path.join(os.tmpdir(), 'transcode-test');
const OUTPUT_DIR = path.join(process.cwd(), 'outputs');

const RENDITIONS = [
  { name: '1080p', width: 1920, height: 1080, bitrate: '5000k' },
  { name: '720p', width: 1280, height: 720, bitrate: '3000k' },
  { name: '480p', width: 854, height: 480, bitrate: '1000k' },
  { name: '360p', width: 640, height: 360, bitrate: '600k' },
];

async function runCmd(cmd, args) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { stdio: 'inherit' });
    p.on('error', reject);
    p.on('close', code => (code === 0 ? resolve() : reject(new Error(`${cmd} failed`))));
  });
}

async function probeDuration(inputPath) {
  return new Promise((resolve, reject) => {
    const args = [
      '-v', 'error',
      '-show_entries', 'format=duration',
      '-of', 'default=noprint_wrappers=1:nokey=1',
      inputPath
    ];
    const p = spawn('ffprobe', args, { stdio: ['ignore', 'pipe', 'inherit'] });
    let out = '';
    p.stdout.on('data', b => (out += b.toString()));
    p.on('close', () => {
      const dur = parseFloat(out.trim());
      if (isNaN(dur)) reject(new Error('Could not determine duration'));
      else resolve(dur);
    });
  });
}

async function transcodeToH264(input, outDir, rendition) {
  const { name, width, height, bitrate } = rendition;
  const playlist = path.join(outDir, `${name}_h264.m3u8`);
  const segmentPattern = path.join(outDir, `${name}_h264_%03d.ts`);

  const args = [
    '-y',
    '-i', input,
    '-c:v', 'libx264',
    '-b:v', bitrate,
    '-vf', `scale=${width}:${height}:force_original_aspect_ratio=decrease`,
    '-preset', 'fast',
    '-c:a', 'aac',
    '-b:a', '128k',
    '-hls_time', '6',
    '-hls_playlist_type', 'vod',
    '-hls_segment_filename', segmentPattern,
    playlist
  ];

  console.log(`🎞 Transcoding ${name} (H.264)...`);
  await runCmd('ffmpeg', args);
}

async function transcodeToVP9(input, outDir, rendition) {
  const { name, width, height, bitrate } = rendition;
  const playlist = path.join(outDir, `${name}_vp9.m3u8`);
  const segmentPattern = path.join(outDir, `${name}_vp9_%03d.webm`);

  const args = [
    '-y',
    '-i', input,
    '-c:v', 'libvpx-vp9',
    '-b:v', bitrate,
    '-vf', `scale=${width}:${height}:force_original_aspect_ratio=decrease`,
    '-c:a', 'libopus',
    '-b:a', '128k',
    '-f', 'segment',
    '-segment_time', '6',
    '-reset_timestamps', '1',
    segmentPattern
  ];

  console.log(`🎞 Transcoding ${name} (VP9)...`);
  await runCmd('ffmpeg', args);
}

async function generateThumbnails(input, outDir) {
  const thumbsDir = path.join(outDir, 'thumbnails');
  await fs.ensureDir(thumbsDir);

  const duration = await probeDuration(input);
  const offsets = [1, Math.floor(duration * 0.25), Math.floor(duration * 0.5), Math.floor(duration * 0.75)];

  console.log('🖼 Generating thumbnails...');
  for (let i = 0; i < offsets.length; i++) {
    const out = path.join(thumbsDir, `thumb_${i + 1}.jpg`);
    const args = ['-y', '-ss', `${offsets[i]}`, '-i', input, '-frames:v', '1', '-q:v', '2', out];
    await runCmd('ffmpeg', args);
  }
}

async function main() {
  try {
    await fs.ensureDir(OUTPUT_DIR);
    console.log(`🚀 Starting local video pipeline`);
    console.log(`Input: ${INPUT}`);

    for (const r of RENDITIONS) {
      await transcodeToH264(INPUT, OUTPUT_DIR, r);
      await transcodeToVP9(INPUT, OUTPUT_DIR, r);
    }

    await generateThumbnails(INPUT, OUTPUT_DIR);

    console.log('\n✅ All renditions and thumbnails generated successfully!');
    console.log(`Outputs are saved in: ${OUTPUT_DIR}`);
  } catch (err) {
    console.error('❌ Pipeline failed:', err);
  }
}

main();


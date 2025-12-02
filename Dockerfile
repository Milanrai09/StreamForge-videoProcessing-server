# Use Node.js 20 with Alpine for smaller image size
FROM node:20-alpine

# Install FFmpeg and required dependencies
RUN apk add --no-cache \
    ffmpeg \
    python3 \
    make \
    g++

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy application code
COPY . .

# Create temp directory for video processing
RUN mkdir -p /tmp/video-processing && chmod 777 /tmp/video-processing

# Set environment variables
ENV NODE_ENV=production
ENV FFMPEG_PATH=/usr/bin/ffmpeg
ENV FFPROBE_PATH=/usr/bin/ffprobe

# Expose port (if running as API)
EXPOSE 8000

# Health check (optional)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD node -e "console.log('healthy')" || exit 1

# Run the processor
CMD ["node", "processing.js"]


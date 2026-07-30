FROM node:18-slim

WORKDIR /app

# Copy package manifests and install production dependencies
COPY package*.json ./
RUN npm ci --only=production

# Copy application files
COPY server.js ./
COPY public/ ./public/

EXPOSE 3031

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD node -e "require('http').get('http://localhost:3031/', (r) => { r.on('data', () => {}); r.on('end', () => process.exit(0)); }).on('error', () => process.exit(1))"

CMD ["node", "server.js"]

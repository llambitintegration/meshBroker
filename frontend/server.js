// Simple HTTP server to serve static files without express dependencies
const http = require('http');
const fs = require('fs');
const path = require('path');

const port = process.env.PORT || 5000;
const publicDir = path.join(__dirname, 'public');

// Log startup info
console.log(`Starting server on port ${port}`);
console.log(`Serving static files from: ${publicDir}`);

const mimeTypes = {
  '.html': 'text/html',
  '.js': 'text/javascript',
  '.css': 'text/css',
  '.json': 'application/json',
  '.png': 'image/png',
  '.jpg': 'image/jpg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.wav': 'audio/wav',
  '.mp3': 'audio/mpeg',
  '.woff': 'application/font-woff',
  '.ttf': 'application/font-ttf',
  '.eot': 'application/vnd.ms-fontobject',
  '.otf': 'application/font-otf',
  '.wasm': 'application/wasm'
};

const server = http.createServer((req, res) => {
  console.log(`${new Date().toISOString()} - ${req.method} ${req.url}`);
  
  // Enable CORS for all requests
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  
  // Handle OPTIONS requests for CORS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }
  
  // Handle API health check
  if (req.url === '/api/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', timestamp: new Date().toISOString() }));
    return;
  }
  
  // Clean up URL and get the file path
  let filePath;
  if (req.url === '/' || req.url === '') {
    filePath = path.join(publicDir, 'index.html');
  } else {
    // Remove query parameters
    const cleanUrl = req.url.split('?')[0];
    filePath = path.join(publicDir, cleanUrl);
  }
  
  // Debug file path resolution
  console.log(`Request for ${req.url} => ${filePath}`);
  
  // Use stat instead of access to get more information
  fs.stat(filePath, (err, stats) => {
    if (err || !stats.isFile()) {
      console.log(`File not found or not a file: ${filePath}`);
      // File not found, serve index.html for SPA
      filePath = path.join(publicDir, 'index.html');
    }
    
    // Get file extension and content type
    const extname = String(path.extname(filePath)).toLowerCase();
    const contentType = mimeTypes[extname] || 'application/octet-stream';
    
    // Read and serve the file
    fs.readFile(filePath, (err, content) => {
      if (err) {
        if (err.code === 'ENOENT') {
          // File not found
          console.error(`404 Not Found: ${filePath}`);
          res.writeHead(404);
          res.end('404 Not Found');
        } else {
          // Server error
          console.error(`500 Server Error: ${err.code} for ${filePath}`);
          res.writeHead(500);
          res.end(`Server Error: ${err.code}`);
        }
      } else {
        // Success
        console.log(`200 OK: ${filePath} (${contentType})`);
        res.writeHead(200, { 'Content-Type': contentType });
        res.end(content, 'utf-8');
      }
    });
  });
});

server.listen(port, '0.0.0.0', () => {
  console.log(`Frontend server running at http://0.0.0.0:${port}`);
});

// Handle server shutdown gracefully
process.on('SIGTERM', () => {
  console.log('SIGTERM signal received: closing HTTP server');
  server.close(() => {
    console.log('HTTP server closed');
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  console.log('SIGINT signal received: closing HTTP server');
  server.close(() => {
    console.log('HTTP server closed');
    process.exit(0);
  });
});

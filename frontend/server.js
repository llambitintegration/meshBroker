// Simple HTTP server to serve static files without express dependencies
const http = require('http');
const fs = require('fs');
const path = require('path');
const { request } = require('http');
const { URL } = require('url');

const port = process.env.PORT || 5000;
const publicDir = path.join(__dirname, 'public');
const backendUrl = process.env.BACKEND_URL || 'http://localhost:8000';

// Log startup info
console.log(`Starting server on port ${port}`);
console.log(`Serving static files from: ${publicDir}`);
console.log(`Backend API URL: ${backendUrl}`);

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

// Helper function to proxy requests to the backend
function proxyRequest(req, res, targetPath) {
  // Parse the backend URL
  const backendUrlObj = new URL(backendUrl);
  
  // Create options for the proxied request
  const options = {
    hostname: backendUrlObj.hostname,
    port: backendUrlObj.port,
    path: targetPath,
    method: req.method,
    headers: {
      ...req.headers,
      host: backendUrlObj.host, // Override the host header
    }
  };
  
  console.log(`Proxying request to backend: ${req.method} ${targetPath} -> ${backendUrlObj.hostname}:${backendUrlObj.port}${targetPath}`);
  
  // Create the proxied request
  const proxyReq = request(options, (proxyRes) => {
    // Copy the status code
    res.writeHead(proxyRes.statusCode, proxyRes.headers);
    
    // Log the response
    console.log(`Backend responded: ${proxyRes.statusCode} for ${req.method} ${targetPath}`);
    
    // Pipe the response data
    proxyRes.pipe(res);
  });
  
  // Handle errors
  proxyReq.on('error', (error) => {
    console.error(`Error proxying request to ${targetPath}:`, error);
    res.writeHead(502);
    res.end(JSON.stringify({ 
      error: 'Backend server error', 
      message: error.message,
      code: error.code || 'UNKNOWN_ERROR'
    }));
  });
  
  // Set a timeout for the proxy request
  proxyReq.setTimeout(10000, () => {
    console.error(`Timeout proxying request to ${targetPath}`);
    proxyReq.destroy();
    res.writeHead(504);
    res.end(JSON.stringify({ error: 'Gateway Timeout', message: 'Backend server did not respond in time' }));
  });
  
  // If there's request data, pipe it to the proxied request
  if (['POST', 'PUT', 'PATCH'].includes(req.method)) {
    req.pipe(proxyReq);
  } else {
    proxyReq.end();
  }
}

const server = http.createServer((req, res) => {
  console.log(`${new Date().toISOString()} - ${req.method} ${req.url}`);
  
  // Enable CORS for all requests
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, DELETE');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-API-Key, Authorization');
  
  // Handle OPTIONS requests for CORS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }
  
  // Proxy API requests to the backend
  if (req.url.startsWith('/status') || 
      req.url.startsWith('/broker_status') || 
      req.url.startsWith('/api/') || 
      req.url.startsWith('/auth/') ||
      req.url.startsWith('/meshtastic/') ||
      req.url.startsWith('/topics') ||
      req.url.startsWith('/subscribe') ||
      req.url.startsWith('/unsubscribe') ||
      req.url.startsWith('/publish') ||
      req.url.startsWith('/ws')) {
    // For WebSocket upgrade requests, special handling is needed
    if (req.url.startsWith('/ws') && req.headers.upgrade && req.headers.upgrade.toLowerCase() === 'websocket') {
      res.writeHead(400);
      res.end('WebSocket connections should be made directly to the backend server');
      return;
    }
    
    // Special handling for API health endpoint for better debugging
    if (req.url === '/api/health') {
      console.log('Received health check request, proxying to backend');
    }
    
    // Proxy the request to the backend
    proxyRequest(req, res, req.url);
    return;
  }
  
  // Handle redirect for backward compatibility
  if (req.url === '/api/health') {
    proxyRequest(req, res, '/status');
    return;
  }
  
  // Handle redirects for common mistakes
  if (req.url === '/device.html' || req.url === '/device') {
    // Redirect to the correct devices.html page
    res.writeHead(302, { 'Location': '/devices.html' });
    res.end();
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
      
      // For HTML requests, redirect to index.html for SPA
      if (req.url.endsWith('.html') || req.url.indexOf('.') === -1) {
        filePath = path.join(publicDir, 'index.html');
      } else {
        // For non-HTML resources, return 404
        res.writeHead(404);
        res.end('404 Not Found');
        return;
      }
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

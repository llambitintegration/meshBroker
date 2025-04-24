/**
 * Node Map functionality for Meshtastic MQTT Bridge UI
 */

// Main map object
let nodeMap = null;

// Node markers
const nodeMarkers = {};

// Node paths
const nodePaths = {};

// Selected node
let selectedNode = null;

// Map settings
const mapSettings = {
  showLabels: true,
  showPaths: true,
  showSignal: true,
  mapStyle: 'streets',
  measureMode: false,
  geofenceMode: false
};

// Base map layers
const baseMaps = {
  streets: L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    maxZoom: 19
  }),
  satellite: L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
    attribution: 'Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community',
    maxZoom: 18
  }),
  terrain: L.tileLayer('https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png', {
    attribution: 'Map data: &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, <a href="http://viewfinderpanoramas.org">SRTM</a> | Map style: &copy; <a href="https://opentopomap.org">OpenTopoMap</a>',
    maxZoom: 17
  }),
  dark: L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
    subdomains: 'abcd',
    maxZoom: 19
  })
};

// Initialize the map when dashboard is ready
document.addEventListener('DOMContentLoaded', () => {
  // Initialize map when Map tab is first selected
  const mapLink = document.querySelector('a[data-page="map"]');
  if (mapLink) {
    mapLink.addEventListener('click', () => {
      if (!nodeMap) {
        initializeMap();
      }
    });
  }
  
  // Set up event handlers for map controls
  setupMapControls();
});

/**
 * Initialize the map
 */
function initializeMap() {
  const mapContainer = document.getElementById('node-map-container');
  if (!mapContainer) return;
  
  console.log('Initializing node map');
  
  // Create map centered at a default location (will be adjusted based on nodes)
  nodeMap = L.map('node-map-container').setView([0, 0], 2);
  
  // Add default base layer
  baseMaps[mapSettings.mapStyle].addTo(nodeMap);
  
  // Add scale control
  L.control.scale().addTo(nodeMap);
  
  // Create layer groups
  const nodeLayer = L.layerGroup().addTo(nodeMap);
  const pathLayer = L.layerGroup().addTo(nodeMap);
  
  // Update the map with existing nodes
  updateNodeMap();
  
  // Set up event handlers
  nodeMap.on('click', onMapClick);
}

/**
 * Set up map control event handlers
 */
function setupMapControls() {
  // Map style selector
  const mapStyleSelect = document.getElementById('map-style');
  if (mapStyleSelect) {
    mapStyleSelect.addEventListener('change', (event) => {
      const newStyle = event.target.value;
      changeMapStyle(newStyle);
    });
  }
  
  // Toggle controls
  const showLabelsToggle = document.getElementById('show-labels');
  if (showLabelsToggle) {
    showLabelsToggle.addEventListener('change', (event) => {
      mapSettings.showLabels = event.target.checked;
      updateNodeMap();
    });
  }
  
  const showPathsToggle = document.getElementById('show-paths');
  if (showPathsToggle) {
    showPathsToggle.addEventListener('change', (event) => {
      mapSettings.showPaths = event.target.checked;
      updateNodeMap();
    });
  }
  
  const showSignalToggle = document.getElementById('show-signal');
  if (showSignalToggle) {
    showSignalToggle.addEventListener('change', (event) => {
      mapSettings.showSignal = event.target.checked;
      updateNodeMap();
    });
  }
  
  // Button controls
  const refreshMapBtn = document.getElementById('refresh-map');
  if (refreshMapBtn) {
    refreshMapBtn.addEventListener('click', () => {
      updateNodeMap();
    });
  }
  
  const centerMapBtn = document.getElementById('center-map');
  if (centerMapBtn) {
    centerMapBtn.addEventListener('click', () => {
      centerMapOnNodes();
    });
  }
  
  const measureDistanceBtn = document.getElementById('measure-distance');
  if (measureDistanceBtn) {
    measureDistanceBtn.addEventListener('click', () => {
      toggleMeasureMode();
    });
  }
  
  const addGeofenceBtn = document.getElementById('add-geofence');
  if (addGeofenceBtn) {
    addGeofenceBtn.addEventListener('click', () => {
      toggleGeofenceMode();
    });
  }
}

/**
 * Update the map with node data
 */
async function updateNodeMap() {
  if (!nodeMap) return;
  
  try {
    // Check if nodes data needs to be fetched
    if (Object.keys(mqttClient.nodes).length === 0) {
      await mqttClient.fetchNodes();
    }
    
    const nodes = mqttClient.nodes;
    
    // Clear existing markers
    clearNodeMarkers();
    
    // Add markers for each node with position data
    let hasValidPositions = false;
    const bounds = L.latLngBounds();
    
    for (const nodeId in nodes) {
      const node = nodes[nodeId];
      
      // Skip nodes without position
      if (!node.position || !node.position.latitude || !node.position.longitude) {
        continue;
      }
      
      // Create or update marker
      createNodeMarker(node);
      
      // Extend bounds
      bounds.extend([node.position.latitude, node.position.longitude]);
      hasValidPositions = true;
    }
    
    // Update node paths if enabled
    if (mapSettings.showPaths) {
      updateNodePaths();
    }
    
    // Center map on nodes if we have valid positions
    if (hasValidPositions) {
      nodeMap.fitBounds(bounds, { padding: [50, 50] });
    }
  } catch (error) {
    console.error('Error updating node map:', error);
  }
}

/**
 * Create or update a node marker
 * @param {Object} node - Node data
 */
function createNodeMarker(node) {
  if (!nodeMap || !node.position) return;
  
  const nodeId = node.id;
  const latLng = [node.position.latitude, node.position.longitude];
  
  // Create marker icon based on node status
  const markerIcon = createNodeIcon(node);
  
  // Remove existing marker if any
  if (nodeMarkers[nodeId]) {
    nodeMarkers[nodeId].remove();
  }
  
  // Create new marker
  const marker = L.marker(latLng, { icon: markerIcon })
    .addTo(nodeMap)
    .on('click', () => selectNode(nodeId));
  
  // Add tooltip if labels are enabled
  if (mapSettings.showLabels) {
    const tooltipContent = `
      <strong>${node.name || node.id}</strong><br>
      Last seen: ${new Date(node.last_seen).toLocaleString()}
    `;
    marker.bindTooltip(tooltipContent, { permanent: false });
  }
  
  // Add popup with node info
  const popupContent = `
    <div class="node-popup">
      <h6>${node.name || 'Unknown'}</h6>
      <p><strong>Node ID:</strong> ${node.id}</p>
      <p><strong>Position:</strong> ${node.position.latitude.toFixed(6)}, ${node.position.longitude.toFixed(6)}</p>
      <p><strong>Altitude:</strong> ${node.position.altitude || 'N/A'} m</p>
      <p><strong>Last Seen:</strong> ${new Date(node.last_seen).toLocaleString()}</p>
      <button class="btn btn-sm btn-primary popup-details-btn" data-node-id="${nodeId}">View Details</button>
    </div>
  `;
  
  marker.bindPopup(popupContent);
  
  // Add the marker to our tracking object
  nodeMarkers[nodeId] = marker;
}

/**
 * Create custom icon for node marker
 * @param {Object} node - Node data
 * @returns {L.Icon} Leaflet icon
 */
function createNodeIcon(node) {
  // Determine icon color based on node status
  let color = 'blue';
  
  if (node.status === 'offline') {
    color = 'gray';
  } else if (node.battery && node.battery < 20) {
    color = 'red';
  } else if (node.signal && node.signal < -90) {
    color = 'orange';
  }
  
  // Create icon HTML
  const iconHtml = `
    <div class="node-marker ${color}">
      <i class="fas fa-satellite-dish"></i>
    </div>
  `;
  
  return L.divIcon({
    html: iconHtml,
    className: 'node-marker-container',
    iconSize: [30, 30],
    iconAnchor: [15, 15]
  });
}

/**
 * Update node path lines
 */
function updateNodePaths() {
  if (!nodeMap || !mapSettings.showPaths) return;
  
  // Clear existing paths
  for (const nodeId in nodePaths) {
    nodePaths[nodeId].remove();
  }
  
  // Create new paths
  const nodes = mqttClient.nodes;
  
  for (const nodeId in nodes) {
    const node = nodes[nodeId];
    
    // Skip nodes without position history
    if (!node.position_history || node.position_history.length < 2) {
      continue;
    }
    
    // Create polyline from position history
    const pathPoints = node.position_history.map(pos => [pos.latitude, pos.longitude]);
    const pathLine = L.polyline(pathPoints, {
      color: 'rgba(65, 105, 225, 0.7)',
      weight: 3,
      dashArray: '5, 5'
    }).addTo(nodeMap);
    
    // Store reference
    nodePaths[nodeId] = pathLine;
  }
}

/**
 * Clear all node markers from the map
 */
function clearNodeMarkers() {
  // Remove markers
  for (const nodeId in nodeMarkers) {
    nodeMarkers[nodeId].remove();
  }
  
  // Clear the markers object
  Object.keys(nodeMarkers).forEach(key => delete nodeMarkers[key]);
}

/**
 * Center map on node markers
 */
function centerMapOnNodes() {
  if (!nodeMap || Object.keys(nodeMarkers).length === 0) return;
  
  const bounds = L.latLngBounds();
  
  // Extend bounds with each marker
  for (const nodeId in nodeMarkers) {
    bounds.extend(nodeMarkers[nodeId].getLatLng());
  }
  
  // Fit map to bounds
  nodeMap.fitBounds(bounds, { padding: [50, 50] });
}

/**
 * Handle map click event
 * @param {Object} e - Click event
 */
function onMapClick(e) {
  // Handle clicks based on current mode
  if (mapSettings.measureMode) {
    handleMeasureClick(e);
  } else if (mapSettings.geofenceMode) {
    handleGeofenceClick(e);
  }
}

/**
 * Change the map style
 * @param {string} style - Map style name
 */
function changeMapStyle(style) {
  if (!nodeMap || !baseMaps[style]) return;
  
  // Remove current base layer
  Object.values(baseMaps).forEach(layer => {
    if (nodeMap.hasLayer(layer)) {
      nodeMap.removeLayer(layer);
    }
  });
  
  // Add new base layer
  baseMaps[style].addTo(nodeMap);
  mapSettings.mapStyle = style;
}

/**
 * Select a node and show its details
 * @param {string} nodeId - Node ID
 */
function selectNode(nodeId) {
  selectedNode = nodeId;
  
  // Update selected node info panel
  const infoContainer = document.getElementById('selected-node-info');
  if (!infoContainer) return;
  
  const node = mqttClient.nodes[nodeId];
  if (!node) {
    infoContainer.innerHTML = '<div class="alert alert-warning">Node not found</div>';
    return;
  }
  
  infoContainer.innerHTML = `
    <div class="selected-node">
      <h6>${node.name || 'Unknown'}</h6>
      <p><strong>Node ID:</strong> ${node.id}</p>
      <p><strong>Hardware:</strong> ${node.hardware || 'Unknown'}</p>
      <p><strong>Firmware:</strong> ${node.firmware || 'Unknown'}</p>
      <p><strong>Position:</strong><br>
        Lat: ${node.position?.latitude?.toFixed(6) || 'N/A'}<br>
        Lon: ${node.position?.longitude?.toFixed(6) || 'N/A'}<br>
        Alt: ${node.position?.altitude || 'N/A'} m
      </p>
      <p><strong>Battery:</strong> ${node.battery ? node.battery + '%' : 'N/A'}</p>
      <p><strong>Signal:</strong> ${node.signal ? node.signal + ' dBm' : 'N/A'}</p>
      <p><strong>Last Seen:</strong><br>${new Date(node.last_seen).toLocaleString()}</p>
      
      <div class="d-grid gap-2 mt-3">
        <button class="btn btn-sm btn-primary" onclick="showNodeDetails('${nodeId}')">
          <i class="fas fa-info-circle"></i> Full Details
        </button>
        <button class="btn btn-sm btn-outline-primary" onclick="sendMessageToNode('${nodeId}')">
          <i class="fas fa-paper-plane"></i> Send Message
        </button>
      </div>
    </div>
  `;
}

// Add CSS for map components
document.addEventListener('DOMContentLoaded', () => {
  // Add CSS for map markers and overlays
  const style = document.createElement('style');
  style.textContent = `
    .node-marker-container {
      background: none !important;
    }
    
    .node-marker {
      width: 30px;
      height: 30px;
      border-radius: 50%;
      background-color: #3388ff;
      border: 2px solid white;
      text-align: center;
      line-height: 26px;
      color: white;
      box-shadow: 0 0 5px rgba(0,0,0,0.3);
    }
    
    .node-marker.gray { background-color: #888; }
    .node-marker.red { background-color: #d9534f; }
    .node-marker.orange { background-color: #f0ad4e; }
    
    .measure-tooltip {
      background: rgba(0,0,0,0.7);
      border: none;
      color: white;
      font-weight: bold;
      padding: 5px 8px;
    }
    
    .map-instruction-overlay {
      position: absolute;
      top: 10px;
      left: 50px;
      right: 50px;
      z-index: 1000;
    }
  `;
  document.head.appendChild(style);
}); 
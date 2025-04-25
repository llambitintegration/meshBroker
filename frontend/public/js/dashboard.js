/**
 * Dashboard functionality for Meshtastic MQTT Bridge UI
 */

// Charts
let messageChart = null;

// Page tracking
let currentPage = 'dashboard';

// Message counters
const messageCounters = {
  total: 0,
  byTopic: {},
  byNode: {},
  byType: {
    text: 0,
    position: 0,
    telemetry: 0,
    nodeid: 0,
    heartbeat: 0,
    other: 0
  }
};

// Chart data
const chartData = {
  labels: [],
  datasets: [
    {
      label: 'Messages',
      data: [],
      borderColor: 'rgba(75, 192, 192, 1)',
      backgroundColor: 'rgba(75, 192, 192, 0.2)',
      tension: 0.2,
      fill: true
    }
  ]
};

// Initialize when DOM is loaded
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initDashboard);
} else {
  // DOM already loaded, initialize immediately
  console.log('DOM already loaded, initializing dashboard');
  setTimeout(initDashboard, 0);
}

/**
 * Initialize the dashboard
 */
async function initDashboard() {
  try {
    console.log('Initializing dashboard...');
    
    // Ensure all required elements are available
    if (!document.getElementById('mqtt-status')) {
      console.error('MQTT status element not found!');
    }
    if (!document.getElementById('api-status')) {
      console.error('API status element not found!');
    }
    
    // Initialize MQTT client
    console.log('Initializing MQTT client...');
    const mqttInitSuccess = await mqttClient.init(handleMQTTMessage);
    
    if (!mqttInitSuccess) {
      console.warn('MQTT client initialization failed, but continuing with dashboard initialization');
    } else {
      console.log('MQTT client initialized successfully');
    }
    
    // Set up page navigation
    console.log('Setting up page navigation...');
    setupNavigation();
    
    // Set up message chart
    console.log('Initializing chart...');
    try {
      initializeChart();
    } catch (chartError) {
      console.error('Failed to initialize chart:', chartError);
      // Continue initialization even if chart fails
    }
    
    // Set up form handlers
    console.log('Setting up form handlers...');
    setupFormHandlers();
    
    // Set up button handlers
    console.log('Setting up button handlers...');
    setupButtonHandlers();
    
    // Update nodes list
    console.log('Updating nodes list...');
    try {
      await updateNodesList();
    } catch (nodesError) {
      console.error('Failed to update nodes list:', nodesError);
      // Continue even if nodes list update fails
    }
    
    // Set up periodic updates with error handling
    setInterval(() => {
      try {
        updateNodesList();
      } catch (error) {
        console.error('Error in periodic nodes update:', error);
      }
    }, 30000); // Update nodes every 30 seconds
    
    setInterval(() => {
      try {
        checkApiStatus();
      } catch (error) {
        console.error('Error in periodic API status check:', error);
      }
    }, 15000);  // Check API status every 15 seconds
    
    console.log('Dashboard initialized successfully');
  } catch (error) {
    console.error('Error initializing dashboard:', error);
  }
}

/**
 * Handle incoming MQTT messages
 * @param {Object} message - Message object
 */
function handleMQTTMessage(message) {
  try {
    // Update counters
    messageCounters.total++;
    
    // Update by topic
    if (!messageCounters.byTopic[message.topic]) {
      messageCounters.byTopic[message.topic] = 0;
    }
    messageCounters.byTopic[message.topic]++;
    
    // Extract node ID from topic (format: msh/{nodeId}/json/{type})
    const topicParts = message.topic.split('/');
    if (topicParts.length >= 4 && topicParts[0] === 'msh') {
      const nodeId = topicParts[1];
      const messageType = topicParts[3];
      
      // Update by node
      if (!messageCounters.byNode[nodeId]) {
        messageCounters.byNode[nodeId] = 0;
      }
      messageCounters.byNode[nodeId]++;
      
      // Update by type
      if (messageType === 'text') {
        messageCounters.byType.text++;
      } else if (messageType === 'position') {
        messageCounters.byType.position++;
      } else if (messageType === 'telemetry') {
        messageCounters.byType.telemetry++;
      } else if (messageType === 'nodeid') {
        messageCounters.byType.nodeid++;
      } else if (messageType === 'heartbeat') {
        messageCounters.byType.heartbeat++;
      } else {
        messageCounters.byType.other++;
      }
    }
    
    // Update chart data
    updateChart();
    
    // Update recent messages
    updateRecentMessages();
    
    // Update message history if on messages page
    if (currentPage === 'messages') {
      updateMessageHistory();
    }
    
    // Update MQTT explorer if on that page
    if (currentPage === 'mqtt-explorer') {
      updateMQTTExplorer(message);
    }
  } catch (error) {
    console.error('Error handling MQTT message:', error);
  }
}

/**
 * Set up page navigation
 */
function setupNavigation() {
  const navLinks = document.querySelectorAll('.nav-link');
  
  if (!navLinks.length) {
    console.warn('No navigation links found for setupNavigation');
    return;
  }
  
  console.log(`Found ${navLinks.length} navigation links`);
  
  // Remove any existing event listeners (to prevent duplicates)
  navLinks.forEach(link => {
    const newLink = link.cloneNode(true);
    link.parentNode.replaceChild(newLink, link);
  });
  
  // Get fresh collection of links after replacement
  const freshNavLinks = document.querySelectorAll('.nav-link');
  
  freshNavLinks.forEach(link => {
    link.addEventListener('click', function(event) {
      event.preventDefault();
      
      // Get the page to show
      const page = this.getAttribute('data-page');
      if (!page) {
        console.warn('Navigation link clicked without data-page attribute', this);
        return;
      }
      
      // Skip if already on this page to prevent recursion
      if (currentPage === page) {
        console.log(`Already on page: ${page}, ignoring click`);
        return;
      }
      
      console.log(`Navigating to page: ${page} from ${currentPage}`);
      
      // Update navigation links
      freshNavLinks.forEach(navLink => {
        navLink.classList.remove('active');
      });
      this.classList.add('active');
      
      // Hide all pages
      const pageElements = document.querySelectorAll('.page');
      if (!pageElements.length) {
        console.warn('No page elements found!');
        return;
      }
      
      // First remove active class from all pages
      pageElements.forEach(pageEl => {
        pageEl.classList.remove('active');
      });
      
      // Then add active class to the selected page
      const pageElement = document.getElementById(`${page}-page`);
      if (pageElement) {
        pageElement.classList.add('active');
        // Update current page tracker
        currentPage = page;
        
        // Perform page-specific updates
        try {
          if (page === 'nodes') {
            updateNodesList();
          } else if (page === 'messages') {
            updateMessageHistory();
          } else if (page === 'mqtt-explorer') {
            updateTopicsList();
          } else if (page === 'map' && typeof initializeMap === 'function') {
            // Initialize map if not already done
            initializeMap();
          }
        } catch (error) {
          console.error(`Error performing updates for page ${page}:`, error);
        }
      } else {
        console.error(`Page element not found: ${page}-page`);
      }
    });
  });
  
  // Ensure the current page is correctly marked as active
  const currentNav = document.querySelector(`.nav-link[data-page="${currentPage}"]`);
  if (currentNav) {
    currentNav.classList.add('active');
  }
}

/**
 * Initialize the message chart
 */
function initializeChart() {
  const ctx = document.getElementById('message-chart');
  if (!ctx) {
    console.warn('Message chart canvas element not found');
    return;
  }
  
  // Check if Chart.js is available
  if (typeof Chart === 'undefined') {
    console.error('Chart.js library not loaded');
    return;
  }
  
  console.log('Initializing message chart');
  
  // Initialize with empty data
  chartData.labels = [];
  chartData.datasets[0].data = [];
  
  for (let i = 0; i < 10; i++) {
    chartData.labels.push('');
    chartData.datasets[0].data.push(0);
  }
  
  try {
    messageChart = new Chart(ctx, {
      type: 'line',
      data: chartData,
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          title: {
            display: true,
            text: 'Message Activity'
          },
          legend: {
            display: false
          }
        },
        scales: {
          y: {
            beginAtZero: true,
            title: {
              display: true,
              text: 'Messages'
            }
          },
          x: {
            title: {
              display: true,
              text: 'Time'
            }
          }
        },
        animation: {
          duration: 500
        }
      }
    });
    
    console.log('Message chart initialized successfully');
  } catch (error) {
    console.error('Error initializing chart:', error);
  }
}

/**
 * Update the message chart with new data
 */
function updateChart() {
  if (!messageChart) return;
  
  // Add current timestamp
  const now = new Date();
  const timeString = now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  
  // Update chart data
  chartData.labels.push(timeString);
  chartData.datasets[0].data.push(1); // Add one for this update interval
  
  // Limit data points to 10
  if (chartData.labels.length > 10) {
    chartData.labels.shift();
    chartData.datasets[0].data.shift();
  }
  
  // Update chart
  messageChart.update();
}

/**
 * Set up form handlers
 */
function setupFormHandlers() {
  // Send message form
  const sendMessageForm = document.getElementById('send-message-form');
  if (sendMessageForm) {
    sendMessageForm.addEventListener('submit', async (event) => {
      event.preventDefault();
      
      const destination = document.getElementById('message-destination').value;
      const messageText = document.getElementById('message-text').value;
      
      if (!messageText) return;
      
      try {
        // Show loading state
        const submitButton = sendMessageForm.querySelector('button[type="submit"]');
        submitButton.disabled = true;
        submitButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Sending...';
        
        // Send the message
        const result = await mqttClient.sendMeshtasticMessage(messageText, destination);
        
        if (result.status === 'success') {
          // Clear the message input
          document.getElementById('message-text').value = '';
          
          // Show success message
          const alertDiv = document.createElement('div');
          alertDiv.className = 'alert alert-success mt-3';
          alertDiv.innerHTML = '<i class="fas fa-check-circle"></i> Message sent successfully';
          sendMessageForm.appendChild(alertDiv);
          
          // Remove success message after 3 seconds
          setTimeout(() => {
            sendMessageForm.removeChild(alertDiv);
          }, 3000);
        } else {
          // Show error message
          const alertDiv = document.createElement('div');
          alertDiv.className = 'alert alert-danger mt-3';
          alertDiv.innerHTML = `<i class="fas fa-exclamation-circle"></i> ${result.message}`;
          sendMessageForm.appendChild(alertDiv);
          
          // Remove error message after 5 seconds
          setTimeout(() => {
            sendMessageForm.removeChild(alertDiv);
          }, 5000);
        }
      } catch (error) {
        console.error('Error sending message:', error);
      } finally {
        // Restore button state
        const submitButton = sendMessageForm.querySelector('button[type="submit"]');
        submitButton.disabled = false;
        submitButton.innerHTML = '<i class="fas fa-paper-plane me-1"></i> Send';
      }
    });
  }
  
  // Publish MQTT message form
  const publishForm = document.getElementById('publish-form');
  if (publishForm) {
    publishForm.addEventListener('submit', async (event) => {
      event.preventDefault();
      
      const topic = document.getElementById('mqtt-topic').value;
      const payload = document.getElementById('mqtt-payload').value;
      const qos = parseInt(document.getElementById('mqtt-qos').value);
      const retain = document.getElementById('mqtt-retain').value === 'true';
      
      if (!topic || !payload) return;
      
      try {
        // Show loading state
        const submitButton = publishForm.querySelector('button[type="submit"]');
        submitButton.disabled = true;
        submitButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Publishing...';
        
        // Publish the message
        const result = await mqttClient.publishMessage(topic, payload, qos, retain);
        
        if (result.status === 'success') {
          // Show success message
          const alertDiv = document.createElement('div');
          alertDiv.className = 'alert alert-success mt-3';
          alertDiv.innerHTML = '<i class="fas fa-check-circle"></i> Message published successfully';
          publishForm.appendChild(alertDiv);
          
          // Remove success message after 3 seconds
          setTimeout(() => {
            publishForm.removeChild(alertDiv);
          }, 3000);
        } else {
          // Show error message
          const alertDiv = document.createElement('div');
          alertDiv.className = 'alert alert-danger mt-3';
          alertDiv.innerHTML = `<i class="fas fa-exclamation-circle"></i> ${result.message}`;
          publishForm.appendChild(alertDiv);
          
          // Remove error message after 5 seconds
          setTimeout(() => {
            publishForm.removeChild(alertDiv);
          }, 5000);
        }
      } catch (error) {
        console.error('Error publishing message:', error);
      } finally {
        // Restore button state
        const submitButton = publishForm.querySelector('button[type="submit"]');
        submitButton.disabled = false;
        submitButton.innerHTML = '<i class="fas fa-paper-plane me-1"></i> Publish';
      }
    });
  }
  
  // Topic subscription form
  const subscribeButton = document.getElementById('subscribe-topic');
  if (subscribeButton) {
    subscribeButton.addEventListener('click', async () => {
      const topicInput = document.getElementById('mqtt-topic-subscription');
      const topic = topicInput.value.trim();
      
      if (!topic) return;
      
      try {
        // Show loading state
        subscribeButton.disabled = true;
        subscribeButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
        
        // Subscribe to the topic
        const result = await mqttClient.subscribeTopic(topic);
        
        if (result.status === 'success') {
          // Clear the input
          topicInput.value = '';
          
          // Update topics list
          updateTopicsList();
        } else {
          // Show error message
          alert(`Failed to subscribe: ${result.message}`);
        }
      } catch (error) {
        console.error('Error subscribing to topic:', error);
      } finally {
        // Restore button state
        subscribeButton.disabled = false;
        subscribeButton.innerHTML = 'Subscribe';
      }
    });
  }
}

/**
 * Set up button handlers
 */
function setupButtonHandlers() {
  try {
    // Set up refresh nodes button
    const refreshNodesBtn = document.getElementById('refresh-nodes');
    if (refreshNodesBtn) {
      refreshNodesBtn.addEventListener('click', async () => {
        refreshNodesBtn.disabled = true;
        refreshNodesBtn.innerHTML = '<i class="fas fa-sync-alt fa-spin"></i>';
        
        try {
          await updateNodesList();
        } catch (error) {
          console.error('Error refreshing nodes:', error);
        }
        
        setTimeout(() => {
          refreshNodesBtn.disabled = false;
          refreshNodesBtn.innerHTML = '<i class="fas fa-sync-alt"></i>';
        }, 1000);
      });
    }
    
    // Refresh nodes detail button
    const refreshNodesDetailBtn = document.getElementById('refresh-nodes-detail');
    if (refreshNodesDetailBtn) {
      refreshNodesDetailBtn.addEventListener('click', async () => {
        refreshNodesDetailBtn.disabled = true;
        refreshNodesDetailBtn.innerHTML = '<i class="fas fa-sync-alt fa-spin"></i>';
        
        try {
          await updateNodesList();
        } catch (error) {
          console.error('Error refreshing nodes detail:', error);
        }
        
        setTimeout(() => {
          refreshNodesDetailBtn.disabled = false;
          refreshNodesDetailBtn.innerHTML = '<i class="fas fa-sync-alt"></i> Refresh';
        }, 1000);
      });
    }
    
    // Set up clear messages button
    const clearMessagesBtn = document.getElementById('clear-messages');
    if (clearMessagesBtn) {
      clearMessagesBtn.addEventListener('click', () => {
        mqttClient.clearHistory();
        updateRecentMessages();
        document.getElementById('recent-messages-container').innerHTML = 
          '<div class="alert alert-info">No messages received yet.</div>';
      });
    }
    
    // Clear MQTT messages button
    const clearMqttMessagesBtn = document.getElementById('clear-mqtt-messages');
    if (clearMqttMessagesBtn) {
      clearMqttMessagesBtn.addEventListener('click', () => {
        mqttClient.clearHistory();
        updateMQTTExplorer(null, true);
      });
    }
    
    // Refresh topics button
    const refreshTopicsBtn = document.getElementById('refresh-topics');
    if (refreshTopicsBtn) {
      refreshTopicsBtn.addEventListener('click', async () => {
        refreshTopicsBtn.disabled = true;
        refreshTopicsBtn.innerHTML = '<i class="fas fa-sync-alt fa-spin"></i>';
        
        try {
          await updateTopicsList();
        } catch (error) {
          console.error('Error refreshing topics:', error);
        }
        
        setTimeout(() => {
          refreshTopicsBtn.disabled = false;
          refreshTopicsBtn.innerHTML = '<i class="fas fa-sync-alt"></i>';
        }, 1000);
      });
    }
    
    // Set up the navbar toggler for mobile
    const navbarToggler = document.querySelector('.navbar-toggler');
    if (navbarToggler) {
      navbarToggler.addEventListener('click', function() {
        const target = document.querySelector(this.getAttribute('data-bs-target'));
        if (target) {
          target.classList.toggle('show');
        }
      });
      
      console.log('Navbar toggler initialized');
    }
    
    // Initialize all Bootstrap tooltips
    if (typeof bootstrap !== 'undefined' && bootstrap.Tooltip) {
      const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
      [...tooltipTriggerList].map(tooltipTriggerEl => new bootstrap.Tooltip(tooltipTriggerEl));
    }
    
    // Handle any broken images and replace with placeholder
    document.querySelectorAll('img').forEach(img => {
      img.addEventListener('error', function() {
        this.src = 'data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24"%3E%3Cpath fill="%23ccc" d="M21 19V5c0-1.1-.9-2-2-2H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2zM8.5 13.5l2.5 3.01L14.5 12l4.5 6H5l3.5-4.5z"/%3E%3C/svg%3E';
        this.alt = 'Image not available';
      });
    });
    
    console.log('Button handlers initialized');
  } catch (error) {
    console.error('Error setting up button handlers:', error);
  }
}

/**
 * Update the list of nodes
 */
async function updateNodesList() {
  try {
    // Fetch latest nodes
    const nodes = await mqttClient.fetchNodes();
    
    // Update nodes counter
    const nodesCounter = document.getElementById('nodes-counter');
    if (nodesCounter) {
      const nodeCount = Object.keys(nodes).length;
      nodesCounter.querySelector('.display-4').textContent = nodeCount;
    }
    
    // Update nodes list on dashboard
    const nodesList = document.getElementById('nodes-list');
    if (nodesList) {
      if (Object.keys(nodes).length === 0) {
        nodesList.innerHTML = '<div class="alert alert-info">No nodes detected yet.</div>';
      } else {
        let html = '<div class="list-group">';
        
        for (const nodeId in nodes) {
          const node = nodes[nodeId];
          const lastSeen = new Date(node.last_seen * 1000).toLocaleTimeString();
          const nodeName = node.name || nodeId;
          const isActive = (Date.now() / 1000) - node.last_seen < 300; // Active if seen in last 5 minutes
          
          html += `
            <div class="list-group-item d-flex justify-content-between align-items-center">
              <div>
                <span class="badge rounded-pill ${isActive ? 'text-bg-success' : 'text-bg-secondary'} me-2">
                  <i class="fas fa-${isActive ? 'circle' : 'circle-notch'}"></i>
                </span>
                <strong>${nodeName}</strong> <small class="text-muted">(${nodeId})</small>
              </div>
              <span class="badge text-bg-primary rounded-pill">Last seen: ${lastSeen}</span>
            </div>
          `;
        }
        
        html += '</div>';
        nodesList.innerHTML = html;
      }
    }
    
    // Update nodes table
    const nodesTableBody = document.getElementById('nodes-table-body');
    if (nodesTableBody) {
      if (Object.keys(nodes).length === 0) {
        nodesTableBody.innerHTML = `
          <tr>
            <td colspan="7" class="text-center">No nodes detected yet.</td>
          </tr>
        `;
      } else {
        let html = '';
        
        for (const nodeId in nodes) {
          const node = nodes[nodeId];
          const lastSeen = new Date(node.last_seen * 1000).toLocaleTimeString();
          const nodeName = node.name || 'Unknown';
          const isActive = (Date.now() / 1000) - node.last_seen < 300; // Active if seen in last 5 minutes
          
          let position = 'Unknown';
          if (node.position) {
            position = `
              <small>Lat: ${node.position.latitude.toFixed(6)}</small><br>
              <small>Lon: ${node.position.longitude.toFixed(6)}</small>
            `;
          }
          
          html += `
            <tr>
              <td>${nodeId}</td>
              <td>${nodeName}</td>
              <td>${node.hardware || 'Unknown'}</td>
              <td>${lastSeen}</td>
              <td>${position}</td>
              <td>
                <span class="badge rounded-pill ${isActive ? 'text-bg-success' : 'text-bg-secondary'}">
                  ${isActive ? 'Active' : 'Inactive'}
                </span>
              </td>
              <td>
                <button class="btn btn-sm btn-outline-info view-node" data-node-id="${nodeId}">
                  <i class="fas fa-info-circle"></i>
                </button>
                <button class="btn btn-sm btn-outline-primary message-node" data-node-id="${nodeId}">
                  <i class="fas fa-paper-plane"></i>
                </button>
              </td>
            </tr>
          `;
        }
        
        nodesTableBody.innerHTML = html;
        
        // Add event listeners for node action buttons
        nodesTableBody.querySelectorAll('.view-node').forEach(button => {
          button.addEventListener('click', () => {
            const nodeId = button.getAttribute('data-node-id');
            showNodeDetails(nodeId);
          });
        });
        
        nodesTableBody.querySelectorAll('.message-node').forEach(button => {
          button.addEventListener('click', () => {
            const nodeId = button.getAttribute('data-node-id');
            // Navigate to messages page and select the node
            document.querySelector('.nav-link[data-page="messages"]').click();
            const select = document.getElementById('message-destination');
            if (select) {
              // Add the node to the select if it doesn't exist
              if (!Array.from(select.options).some(option => option.value === nodeId)) {
                const option = document.createElement('option');
                option.value = nodeId;
                option.textContent = nodes[nodeId].name || nodeId;
                select.appendChild(option);
              }
              select.value = nodeId;
            }
          });
        });
      }
    }
    
    // Update message destination dropdown
    const messageDestination = document.getElementById('message-destination');
    if (messageDestination) {
      // Keep the broadcast option
      let options = '<option value="broadcast">Broadcast (All Nodes)</option>';
      
      for (const nodeId in nodes) {
        const node = nodes[nodeId];
        const nodeName = node.name || nodeId;
        options += `<option value="${nodeId}">${nodeName} (${nodeId})</option>`;
      }
      
      messageDestination.innerHTML = options;
    }
  } catch (error) {
    console.error('Error updating nodes list:', error);
  }
}

/**
 * Update the recent messages display
 */
function updateRecentMessages() {
  const messagesContainer = document.getElementById('recent-messages-container');
  if (!messagesContainer) return;
  
  const messages = mqttClient.getHistory(10);
  
  if (messages.length === 0) {
    messagesContainer.innerHTML = '<div class="alert alert-info">No messages received yet.</div>';
    return;
  }
  
  let html = '';
  
  for (const message of messages) {
    // Extract topic parts to identify message type and node
    const topicParts = message.topic.split('/');
    let messageType = 'unknown';
    let nodeId = 'unknown';
    
    if (topicParts.length >= 4 && topicParts[0] === 'msh') {
      nodeId = topicParts[1];
      messageType = topicParts[3];
    }
    
    // Format time
    const time = new Date(message.timestamp * 1000).toLocaleTimeString();
    
    // Get message payload
    let payload = message.payload;
    if (typeof payload === 'string') {
      try {
        payload = JSON.parse(payload);
      } catch (e) {
        // Keep as string if not valid JSON
      }
    }
    
    // Format message content based on type
    let content = '';
    
    if (messageType === 'text' && payload.text) {
      content = `<strong>Text:</strong> ${payload.text.text || ''}`;
    } else if (messageType === 'position' && payload.position) {
      content = `<strong>Position:</strong> Lat: ${payload.position.latitude}, Lon: ${payload.position.longitude}`;
    } else if (messageType === 'telemetry' && payload.telemetry) {
      content = `<strong>Telemetry:</strong> ${JSON.stringify(payload.telemetry).substring(0, 100)}...`;
    } else if (messageType === 'nodeid' && payload.user) {
      content = `<strong>Node Info:</strong> ${payload.user.longName || payload.user.shortName || nodeId}`;
    } else {
      content = `<strong>Data:</strong> ${typeof payload === 'object' ? JSON.stringify(payload).substring(0, 100) + '...' : payload}`;
    }
    
    // Add message to HTML
    html += `
      <div class="message-item">
        <div class="message-header">
          <span class="message-time">${time}</span>
          <span class="message-topic">${message.topic}</span>
        </div>
        <div class="message-content">
          ${content}
        </div>
      </div>
    `;
  }
  
  messagesContainer.innerHTML = html;
}

/**
 * Update the message history display
 */
function updateMessageHistory() {
  const historyContainer = document.getElementById('message-history-container');
  if (!historyContainer) return;
  
  const messages = mqttClient.getHistory();
  
  if (messages.length === 0) {
    historyContainer.innerHTML = '<div class="alert alert-info">No message history yet.</div>';
    return;
  }
  
  let html = '';
  
  // Group messages by node
  const messagesByNode = {};
  
  for (const message of messages) {
    // Extract node ID from topic
    const topicParts = message.topic.split('/');
    let nodeId = 'unknown';
    
    if (topicParts.length >= 4 && topicParts[0] === 'msh') {
      nodeId = topicParts[1];
    }
    
    if (!messagesByNode[nodeId]) {
      messagesByNode[nodeId] = [];
    }
    
    messagesByNode[nodeId].push(message);
  }
  
  // Create message groups
  for (const nodeId in messagesByNode) {
    const nodeMessages = messagesByNode[nodeId];
    const node = mqttClient.nodes[nodeId] || { name: 'Unknown' };
    const nodeName = node.name || nodeId;
    
    html += `
      <div class="message-group mb-4">
        <h6 class="message-group-header">
          <i class="fas fa-broadcast-tower me-2"></i>${nodeName} <small class="text-muted">(${nodeId})</small>
        </h6>
        <div class="message-group-content">
    `;
    
    // Add messages for this node
    for (const message of nodeMessages) {
      // Extract message type from topic
      const topicParts = message.topic.split('/');
      let messageType = 'unknown';
      
      if (topicParts.length >= 4) {
        messageType = topicParts[3];
      }
      
      // Format time
      const time = new Date(message.timestamp * 1000).toLocaleTimeString();
      
      // Get message payload
      let payload = message.payload;
      if (typeof payload === 'string') {
        try {
          payload = JSON.parse(payload);
        } catch (e) {
          // Keep as string if not valid JSON
        }
      }
      
      // Format message content based on type
      let content = '';
      let cssClass = '';
      
      if (messageType === 'text' && payload.text) {
        content = payload.text.text || '';
        cssClass = 'message-text';
      } else if (messageType === 'position' && payload.position) {
        content = `<em>Updated position: Lat ${payload.position.latitude}, Lon ${payload.position.longitude}</em>`;
        cssClass = 'message-position';
      } else if (messageType === 'telemetry' && payload.telemetry) {
        content = `<em>Telemetry update</em>`;
        cssClass = 'message-telemetry';
      } else if (messageType === 'nodeid' && payload.user) {
        content = `<em>Node identification: ${payload.user.longName || payload.user.shortName || nodeId}</em>`;
        cssClass = 'message-nodeid';
      } else {
        content = `<em>${messageType} update</em>`;
        cssClass = 'message-other';
      }
      
      // Add message to HTML
      html += `
        <div class="chat-message ${cssClass}">
          <div class="message-bubble">
            ${content}
          </div>
          <div class="message-time small text-muted">${time}</div>
        </div>
      `;
    }
    
    html += `
        </div>
      </div>
    `;
  }
  
  historyContainer.innerHTML = html;
}

/**
 * Update the MQTT explorer display
 * @param {Object} message - New message to add (optional)
 * @param {boolean} clear - Whether to clear the display
 */
function updateMQTTExplorer(message = null, clear = false) {
  const messagesContainer = document.getElementById('mqtt-messages-container');
  if (!messagesContainer) return;
  
  if (clear) {
    messagesContainer.innerHTML = '<div class="alert alert-info">No MQTT messages received yet.</div>';
    return;
  }
  
  if (!message) {
    // Just refresh from history
    const messages = mqttClient.getHistory();
    
    if (messages.length === 0) {
      messagesContainer.innerHTML = '<div class="alert alert-info">No MQTT messages received yet.</div>';
      return;
    }
    
    let html = '';
    
    for (const msg of messages) {
      const time = new Date(msg.timestamp * 1000).toLocaleTimeString();
      const payload = typeof msg.payload === 'object' 
        ? JSON.stringify(msg.payload, null, 2) 
        : msg.payload;
      
      html += `
        <div class="mqtt-message">
          <div class="message-header">
            <span class="message-time">${time}</span>
            <span class="message-topic">${msg.topic}</span>
          </div>
          <pre class="message-payload">${formatPayload(payload)}</pre>
        </div>
      `;
    }
    
    messagesContainer.innerHTML = html;
  } else {
    // Add new message to the container
    if (messagesContainer.querySelector('.alert')) {
      // Remove the "no messages" alert
      messagesContainer.innerHTML = '';
    }
    
    const time = new Date(message.timestamp * 1000).toLocaleTimeString();
    const payload = typeof message.payload === 'object' 
      ? JSON.stringify(message.payload, null, 2) 
      : message.payload;
    
    const messageElement = document.createElement('div');
    messageElement.className = 'mqtt-message';
    messageElement.innerHTML = `
      <div class="message-header">
        <span class="message-time">${time}</span>
        <span class="message-topic">${message.topic}</span>
      </div>
      <pre class="message-payload">${formatPayload(payload)}</pre>
    `;
    
    // Add to the beginning
    messagesContainer.insertBefore(messageElement, messagesContainer.firstChild);
    
    // Limit the number of messages shown
    const maxMessages = 50;
    const messages = messagesContainer.querySelectorAll('.mqtt-message');
    if (messages.length > maxMessages) {
      for (let i = maxMessages; i < messages.length; i++) {
        messagesContainer.removeChild(messages[i]);
      }
    }
  }
}

/**
 * Format payload for display
 * @param {string} payload - Payload to format
 * @returns {string} Formatted payload
 */
function formatPayload(payload) {
  if (!payload) return '';
  
  // Try to parse as JSON if it's a string
  if (typeof payload === 'string') {
    try {
      const json = JSON.parse(payload);
      return JSON.stringify(json, null, 2);
    } catch (e) {
      // Not valid JSON, return as is
      return payload;
    }
  }
  
  // If it's already an object
  return JSON.stringify(payload, null, 2);
}

/**
 * Update the topics list display
 */
async function updateTopicsList() {
  const topicList = document.getElementById('topic-list');
  if (!topicList) return;
  
  // Fetch latest topics
  const topics = await mqttClient.fetchTopics();
  
  if (topics.length === 0) {
    topicList.innerHTML = `
      <div class="list-group-item d-flex justify-content-between align-items-center text-muted">
        No active subscriptions
      </div>
    `;
    return;
  }
  
  let html = '';
  
  for (const topic of topics) {
    html += `
      <div class="list-group-item d-flex justify-content-between align-items-center">
        <span>${topic}</span>
        <button class="btn btn-sm btn-outline-danger unsubscribe-topic" data-topic="${topic}">
          <i class="fas fa-times"></i>
        </button>
      </div>
    `;
  }
  
  topicList.innerHTML = html;
  
  // Add event listeners for unsubscribe buttons
  topicList.querySelectorAll('.unsubscribe-topic').forEach(button => {
    button.addEventListener('click', async () => {
      const topic = button.getAttribute('data-topic');
      
      try {
        // Show loading state
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
        
        // Unsubscribe from the topic
        const result = await mqttClient.unsubscribeTopic(topic);
        
        if (result.status === 'success') {
          // Update topics list
          updateTopicsList();
        } else {
          // Show error message
          alert(`Failed to unsubscribe: ${result.message}`);
        }
      } catch (error) {
        console.error('Error unsubscribing from topic:', error);
      } finally {
        // Restore button state
        button.disabled = false;
        button.innerHTML = '<i class="fas fa-times"></i>';
      }
    });
  });
}

/**
 * Show node details in a modal
 * @param {string} nodeId - Node ID
 */
function showNodeDetails(nodeId) {
  const node = mqttClient.nodes[nodeId];
  if (!node) return;
  
  const modalContent = document.getElementById('node-detail-content');
  if (!modalContent) return;
  
  // Format last seen time
  const lastSeen = new Date(node.last_seen * 1000).toLocaleString();
  
  // Determine if node is active
  const isActive = (Date.now() / 1000) - node.last_seen < 300; // Active if seen in last 5 minutes
  
  let html = `
    <div class="node-details">
      <div class="node-header">
        <h4>
          <span class="badge rounded-pill ${isActive ? 'text-bg-success' : 'text-bg-secondary'} me-2">
            <i class="fas fa-${isActive ? 'circle' : 'circle-notch'}"></i>
          </span>
          ${node.name || 'Unknown Node'}
        </h4>
        <p class="text-muted">${nodeId}</p>
      </div>
      
      <div class="row mb-4">
        <div class="col-md-6">
          <h6>Basic Information</h6>
          <table class="table table-sm">
            <tr>
              <th>Last Seen</th>
              <td>${lastSeen}</td>
            </tr>
            <tr>
              <th>Hardware</th>
              <td>${node.hardware || 'Unknown'}</td>
            </tr>
            <tr>
              <th>Short Name</th>
              <td>${node.short_name || 'N/A'}</td>
            </tr>
            <tr>
              <th>Message Count</th>
              <td>${node.message_count || 0}</td>
            </tr>
          </table>
        </div>
        
        <div class="col-md-6">
          <h6>Position</h6>
          ${node.position ? `
            <table class="table table-sm">
              <tr>
                <th>Latitude</th>
                <td>${node.position.latitude.toFixed(6)}</td>
              </tr>
              <tr>
                <th>Longitude</th>
                <td>${node.position.longitude.toFixed(6)}</td>
              </tr>
              <tr>
                <th>Altitude</th>
                <td>${node.position.altitude ? node.position.altitude.toFixed(2) + ' m' : 'N/A'}</td>
              </tr>
              <tr>
                <th>Updated</th>
                <td>${node.position.timestamp ? new Date(node.position.timestamp * 1000).toLocaleString() : 'N/A'}</td>
              </tr>
            </table>
          ` : '<div class="alert alert-info">No position data available</div>'}
        </div>
      </div>
      
      ${node.telemetry ? `
        <div class="mb-4">
          <h6>Telemetry</h6>
          <pre class="telemetry-data">${JSON.stringify(node.telemetry, null, 2)}</pre>
        </div>
      ` : ''}
      
      ${node.messages && node.messages.length > 0 ? `
        <div class="mb-4">
          <h6>Recent Messages</h6>
          <div class="message-list">
            ${node.messages.map(msg => `
              <div class="message-item">
                <div class="message-content">${msg.text}</div>
                <div class="message-meta">
                  <small class="text-muted">
                    ${msg.timestamp ? new Date(msg.timestamp * 1000).toLocaleString() : 'Unknown time'}
                  </small>
                </div>
              </div>
            `).join('')}
          </div>
        </div>
      ` : ''}
    </div>
  `;
  
  modalContent.innerHTML = html;
  
  // Show the modal
  const modal = new bootstrap.Modal(document.getElementById('node-detail-modal'));
  modal.show();
}

/**
 * Check API status
 */
async function checkApiStatus() {
  try {
    if (!mqttClient) {
      console.error('MQTT client not initialized');
      return;
    }
    
    console.log('Checking API status...');
    
    // Add a timeout to the fetch request
    const timeoutPromise = new Promise((_, reject) => 
      setTimeout(() => reject(new Error('API request timeout')), 5000)
    );
    
    try {
      // Race the API request against a timeout
      const status = await Promise.race([
        mqttClient.checkStatus(),
        timeoutPromise
      ]);
      
      if (status) {
        const isConnected = status.status === 'connected' || 
                           (status.services && status.services.mqtt && status.services.mqtt.status === 'connected');
        
        mqttClient.updateApiStatus(isConnected);
        console.log(`API status updated: ${isConnected ? 'connected' : 'disconnected'}`);
        
        // Add detailed logging of the response
        console.log('API status details:', status);
        
        if (!isConnected) {
          // If API is up but reporting disconnected, show more specific message
          mqttClient.showConnectionAlert(
            'API server is reachable but reporting disconnected status. The MQTT broker may be down.'
          );
        } else {
          // Clear any existing alert if we're connected
          mqttClient.clearConnectionAlert();
        }
      } else {
        console.warn('Failed to get API status - received empty response');
        mqttClient.updateApiStatus(false);
        mqttClient.showConnectionAlert('API server returned an empty response.');
      }
    } catch (timeoutError) {
      console.error('API status check timed out:', timeoutError);
      mqttClient.updateApiStatus(false);
      mqttClient.showConnectionAlert(
        'API server is not responding (request timed out). Check if the backend is running on port 8000.'
      );
    }
  } catch (error) {
    console.error('Error checking API status:', error);
    mqttClient.updateApiStatus(false);
    mqttClient.showConnectionAlert(
      `API connection error: ${error.message}. Make sure the backend server is running.`
    );
  }
}

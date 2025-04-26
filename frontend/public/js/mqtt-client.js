/**
 * MQTT Client for Meshtastic Bridge
 * Handles communication with the backend API and WebSocket
 */

class MQTTClient {
  constructor() {
    // Determine if we should use secure websocket based on the protocol
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const hostname = window.location.hostname || '127.0.0.1';
    const apiPort = '8000'; // API port
    
    // API base URL (using the current protocol)
    this.apiBaseUrl = `${window.location.protocol}//${hostname}:${apiPort}`;
    
    // WebSocket URL (using protocol-aware connection)
    this.wsUrl = `${protocol}//${hostname}:${apiPort}/ws`;
    
    // Log initialization
    console.log(`MQTT Client initialized with:`);
    console.log(`- API URL: ${this.apiBaseUrl}`);
    console.log(`- WebSocket URL: ${this.wsUrl}`);
    
    // Auth token for WebSocket authentication
    this.authToken = localStorage.getItem('auth_token') || null;
    
    // API key for protected endpoints
    this.apiKey = localStorage.getItem('api_key') || null;
    
    // WebSocket connection
    this.ws = null;
    
    // Connection status
    this.connected = false;
    
    // Callback for received messages
    this.messageCallback = null;
    
    // Reconnection parameters
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 5;
    this.reconnectInterval = 3000;
    this.reconnectTimer = null;
    
    // Message history
    this.messageHistory = [];
    this.maxMessageHistory = 500;
    
    // Topic subscriptions
    this.subscribedTopics = new Set();
    
    // Node information
    this.nodes = {};
    
    // WebSocket channel ID (for enhanced WebSocket support)
    this.channelId = null;
  }

  /**
   * Set the API key for protected endpoints
   * @param {string} apiKey - The API key to use
   */
  setApiKey(apiKey) {
    this.apiKey = apiKey;
    if (apiKey) {
      localStorage.setItem('api_key', apiKey);
      console.log('API key set successfully');
    } else {
      localStorage.removeItem('api_key');
      console.log('API key removed');
    }
  }

  /**
   * Get the current API key
   * @returns {string|null} - The current API key or null if not set
   */
  getApiKey() {
    return this.apiKey;
  }

  /**
   * Make an authenticated fetch request
   * @param {string} url - The URL to fetch
   * @param {object} options - Fetch options
   * @returns {Promise<Response>} - The fetch response
   */
  async authenticatedFetch(url, options = {}) {
    // Initialize headers if not provided
    if (!options.headers) {
      options.headers = {};
    }
    
    // Add API key header if available
    if (this.apiKey) {
      options.headers['X-API-Key'] = this.apiKey;
    }
    
    // Add auth token if available and X-API-Key is not
    if (this.authToken && !this.apiKey) {
      options.headers['Authorization'] = `Bearer ${this.authToken}`;
    }
    
    return fetch(url, options);
  }

  /**
   * Initialize the MQTT client
   * @param {Function} messageCallback - Callback function for messages
   */
  async init(messageCallback) {
    this.messageCallback = messageCallback;
    
    try {
      console.log('Initializing MQTT client...');
      
      // Check authentication status if we have a token
      if (this.authToken) {
        try {
          console.log('Validating authentication token...');
          const authResponse = await fetch(`${this.apiBaseUrl}/auth/validate`, {
            headers: {
              'Authorization': `Bearer ${this.authToken}`
            }
          });
          
          if (!authResponse.ok) {
            console.warn('Authentication token is invalid or expired');
            this.authToken = null;
            localStorage.removeItem('auth_token');
          } else {
            console.log('Authentication token is valid');
          }
        } catch (error) {
          console.error('Error validating authentication token:', error);
          // Continue initialization even if auth fails
        }
      }
      
      // Check API status
      console.log('Checking API status...');
      try {
        const statusResponse = await this.checkStatus();
        
        if (statusResponse && statusResponse.status === 'connected') {
          this.updateApiStatus(true);
          console.log('API is connected to MQTT broker');
        } else {
          this.updateApiStatus(false);
          console.warn('API is not connected to MQTT broker');
        }
      } catch (error) {
        console.error('Error checking API status:', error);
        this.updateApiStatus(false);
        // Continue initialization even if status check fails
      }
      
      // Get existing topic subscriptions
      console.log('Fetching topic subscriptions...');
      try {
        await this.fetchTopics();
      } catch (error) {
        console.error('Error fetching topics:', error);
        // Continue initialization even if fetching topics fails
      }
      
      // Connect to WebSocket
      console.log('Connecting to WebSocket...');
      this.connectWebSocket();
      
      // Fetch nodes
      console.log('Fetching Meshtastic nodes...');
      try {
        await this.fetchNodes();
      } catch (error) {
        console.error('Error fetching nodes:', error);
        // Continue initialization even if fetching nodes fails
      }
      
      console.log('MQTT client initialization completed');
      return true;
    } catch (error) {
      console.error('Failed to initialize MQTT client:', error);
      this.updateApiStatus(false);
      this.updateMqttStatus(false);
      return false;
    }
  }

  /**
   * Connect to the WebSocket endpoint
   */
  connectWebSocket() {
    // Clear any existing reconnection timer
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    
    // Clear any existing ping timer
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
    
    // Close existing connection if any
    if (this.ws) {
      this.ws.close();
    }
    
    try {
      let wsUrl = this.wsUrl;
      
      // Add authentication token if available
      if (this.authToken) {
        wsUrl = `${this.wsUrl}?token=${this.authToken}`;
      }
      
      console.log(`Connecting to WebSocket: ${wsUrl.replace(/token=([^&]*)/, 'token=****')}`);
      
      // Show connecting status in UI
      this.updateMqttStatus('connecting');
      
      // Create new WebSocket connection
      this.ws = new WebSocket(wsUrl);
      
      this.ws.onopen = () => {
        console.log('WebSocket connection established');
        this.connected = true;
        this.reconnectAttempts = 0;
        this.updateMqttStatus(true);
        
        // Clear any connection alerts
        this.clearConnectionAlert();
        
        // Request channel assignment for topic-based subscriptions
        if (this.authToken) {
          this.requestChannelAssignment();
        }
        
        // Start sending ping messages to keep the connection alive
        this.startPing();
      };
      
      this.ws.onclose = (event) => {
        console.warn(`WebSocket connection closed: ${event.code} ${event.reason}`);
        this.connected = false;
        this.channelId = null;
        this.updateMqttStatus(false);
        
        // Show detailed info about connection closure
        const closeReason = event.reason ? event.reason : 'Connection closed';
        console.log(`WebSocket close code: ${event.code}, reason: ${closeReason}`);
        
        // Try to reconnect with exponential backoff
        if (this.reconnectAttempts < this.maxReconnectAttempts) {
          const backoffTime = this.reconnectInterval * Math.pow(1.5, this.reconnectAttempts);
          this.reconnectAttempts++;
          console.log(`Reconnecting (attempt ${this.reconnectAttempts}) in ${backoffTime}ms...`);
          this.reconnectTimer = setTimeout(() => this.connectWebSocket(), backoffTime);
          
          // Update status to show reconnecting
          this.updateMqttStatus('reconnecting');
        } else {
          console.error('Max reconnection attempts reached');
          // Show reconnection button to user
          this.showReconnectButton();
          // Show alert about connection issues
          this.showConnectionAlert('WebSocket connection failed after multiple attempts');
        }
      };
      
      this.ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        this.connected = false;
        this.updateMqttStatus(false);
        
        // Additional error details in console
        console.log('WebSocket error details:', {
          readyState: this.ws ? this.ws.readyState : 'No WebSocket',
          url: wsUrl,
          error: error.message || 'Unknown error'
        });
      };
      
      this.ws.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);
          
          // Handle ping messages immediately
          if (message.type === 'ping') {
            console.log('Received ping from server, responding with pong');
            // Respond with pong immediately to maintain connection
            if (this.ws && this.ws.readyState === WebSocket.OPEN) {
              this.ws.send(JSON.stringify({ 
                type: 'pong', 
                timestamp: message.timestamp || Date.now() 
              }));
            }
            return;
          }
          
          // Handle channel assignment response
          if (message.type === 'channel_assignment') {
            this.channelId = message.channel_id;
            console.log(`Assigned to channel: ${this.channelId}`);
            
            // Subscribe to previously subscribed topics in the new channel
            this.resubscribeTopics();
            return;
          }
          
          this.handleMessage(message);
        } catch (error) {
          console.error('Error parsing WebSocket message:', error);
          console.log('Raw message data:', event.data);
        }
      };
    } catch (error) {
      console.error('Error connecting to WebSocket:', error);
      this.updateMqttStatus(false);
      
      // Show detailed connection error
      this.showConnectionAlert(`Failed to connect to WebSocket: ${error.message}`);
      
      // Try to reconnect
      if (this.reconnectAttempts < this.maxReconnectAttempts) {
        this.reconnectAttempts++;
        const backoffTime = this.reconnectInterval * Math.pow(1.5, this.reconnectAttempts);
        console.log(`Reconnecting (attempt ${this.reconnectAttempts}) in ${backoffTime}ms...`);
        this.reconnectTimer = setTimeout(() => this.connectWebSocket(), backoffTime);
      }
    }
  }

  /**
   * Start sending periodic ping messages to keep the WebSocket connection alive
   */
  startPing() {
    // Clear any existing ping timer
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
    }
    
    // Send a ping message every 15 seconds (more frequent than the default 30s timeout)
    this.pingTimer = setInterval(() => {
      if (this.connected && this.ws && this.ws.readyState === WebSocket.OPEN) {
        console.log('Sending ping to server...');
        try {
          this.ws.send(JSON.stringify({ 
            type: 'ping', 
            timestamp: Date.now(),
            client_id: "browser-client" // Add client identifier
          }));
        } catch (error) {
          console.error('Error sending ping:', error);
          
          // If sending fails, the connection might be broken
          if (this.ws.readyState !== WebSocket.OPEN) {
            console.warn('WebSocket connection appears to be closed, attempting to reconnect...');
            clearInterval(this.pingTimer);
            this.pingTimer = null;
            this.connected = false;
            this.updateMqttStatus(false);
            
            // Attempt to reconnect
            setTimeout(() => this.connectWebSocket(), 1000);
          }
        }
      } else {
        // If we're not connected, stop pinging
        clearInterval(this.pingTimer);
        this.pingTimer = null;
      }
    }, 15000); // 15 seconds - more frequent than server timeout
  }

  /**
   * Handle incoming WebSocket message
   * @param {Object} message - Message object
   */
  handleMessage(message) {
    // Add message to history
    this.addToHistory(message);
    
    // Handle special message types
    if (message.type === 'status') {
      // Update API status
      if (message.data?.api) {
        this.updateApiStatus(message.data.api.status === 'connected');
      }
      
      // Update MQTT status
      if (message.data?.mqtt) {
        this.updateMqttStatus(message.data.mqtt.status === 'connected');
      }
      
      return;
    }
    
    if (message.type === 'mqtt_status') {
      // Update MQTT status
      this.updateMqttStatus(message.data?.status === 'connected');
      return;
    }
    
    if (message.type === 'pong') {
      console.log('Received pong from server');
      return;
    }
    
    // Pass message to callback if available
    if (this.messageCallback) {
      this.messageCallback(message);
    }
  }

  /**
   * Add a message to the history
   * @param {Object} message - Message object
   */
  addToHistory(message) {
    // Add timestamp if not present
    if (!message.timestamp) {
      message.timestamp = Date.now();
    }
    
    // Add to history
    this.messageHistory.unshift(message);
    
    // Limit history size
    if (this.messageHistory.length > this.maxMessageHistory) {
      this.messageHistory = this.messageHistory.slice(0, this.maxMessageHistory);
    }
  }

  /**
   * Clear message history
   */
  clearHistory() {
    this.messageHistory = [];
  }

  /**
   * Get message history
   * @param {number} limit - Maximum number of messages to retrieve
   * @returns {Array} Message history
   */
  getHistory(limit = null) {
    if (limit) {
      return this.messageHistory.slice(0, limit);
    }
    return this.messageHistory;
  }

  /**
   * Check API and MQTT status
   * @returns {Promise<object>} - Status object
   */
  async checkStatus() {
    try {
      console.log('Checking API status...');
      
      // First, check if the API is reachable at all
      let statusUrl = `${this.apiBaseUrl}/status`;
      
      try {
        // Use a shorter timeout for the initial API status check
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000);
        
        const response = await fetch(statusUrl, {
          method: 'GET',
          signal: controller.signal
        });
        
        // Clear the timeout
        clearTimeout(timeoutId);
        
        if (!response.ok) {
          console.warn(`API status check failed with status: ${response.status}`);
          this.updateApiStatus(false);
          return { status: 'error', message: 'API server is not responding properly' };
        }
        
        const data = await response.json();
        console.log('API status response:', data);
        
        // Update status indicators
        const apiConnected = data.api_status === 'ok';
        const mqttConnected = data.mqtt_status === 'connected';
        
        this.updateApiStatus(apiConnected);
        this.updateMqttStatus(mqttConnected);
        
        return {
          status: mqttConnected ? 'connected' : 'disconnected',
          api_status: apiConnected ? 'ok' : 'error',
          mqtt_status: data.mqtt_status,
          message: data.message || 'API is responding correctly'
        };
        
      } catch (error) {
        // Handle network errors or timeouts
        console.error('Error checking API status:', error);
        
        // Check if this was an abort error (timeout)
        if (error.name === 'AbortError') {
          console.warn('API status check timed out after 5 seconds');
          this.updateApiStatus(false);
          return { status: 'error', message: 'API server request timed out' };
        }
        
        // Other network error
        this.updateApiStatus(false);
        return { 
          status: 'error', 
          message: 'Cannot connect to API server. Please check that the server is running.'
        };
      }
    } catch (error) {
      console.error('Unexpected error in checkStatus:', error);
      this.updateApiStatus(false);
      this.updateMqttStatus(false);
      return { status: 'error', message: 'Unexpected error checking status' };
    }
  }

  /**
   * Fetch subscribed topics
   * @returns {Promise<Array>} List of topics
   */
  async fetchTopics() {
    try {
      const response = await fetch(`${this.apiBaseUrl}/topics`);
      const data = await response.json();
      
      if (data.topics) {
        this.subscribedTopics = new Set(data.topics);
        return data.topics;
      }
      return [];
    } catch (error) {
      console.error('Error fetching topics:', error);
      return [];
    }
  }

  /**
   * Subscribe to a topic
   * @param {string} topic - Topic to subscribe to
   * @returns {Promise<Object>} Result object
   */
  async subscribeTopic(topic) {
    try {
      // First, send API request to subscribe the backend
      const response = await fetch(`${this.apiBaseUrl}/subscribe`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': this.authToken ? `Bearer ${this.authToken}` : undefined
        },
        body: JSON.stringify({ topic }),
      });
      
      const data = await response.json();
      
      if (data.status === 'success') {
        // Add to local set of subscribed topics
        this.subscribedTopics.add(topic);
        
        // If we have a channel, also subscribe the channel
        if (this.channelId && this.connected) {
          await this.subscribeToChannel(topic);
        }
        
        console.log(`Successfully subscribed to topic: ${topic}`);
      } else {
        console.error(`Failed to subscribe to topic: ${topic}`, data.message);
      }
      
      return data;
    } catch (error) {
      console.error('Error subscribing to topic:', error);
      return { status: 'error', message: error.message };
    }
  }

  /**
   * Unsubscribe from a topic
   * @param {string} topic - Topic to unsubscribe from
   * @returns {Promise<Object>} Result object
   */
  async unsubscribeTopic(topic) {
    try {
      const response = await fetch(`${this.apiBaseUrl}/unsubscribe`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ topic }),
      });
      
      const data = await response.json();
      
      if (data.status === 'success') {
        this.subscribedTopics.delete(topic);
      }
      
      return data;
    } catch (error) {
      console.error('Error unsubscribing from topic:', error);
      return { status: 'error', message: error.message };
    }
  }

  /**
   * Publish a message to a topic
   * @param {string} topic - Topic to publish to
   * @param {string|Object} payload - Message payload
   * @param {number} qos - Quality of Service level
   * @param {boolean} retain - Retain flag
   * @returns {Promise<Object>} Result object
   */
  async publishMessage(topic, payload, qos = 0, retain = false) {
    try {
      // Convert object to string if needed
      if (typeof payload === 'object') {
        payload = JSON.stringify(payload);
      }
      
      const response = await fetch(`${this.apiBaseUrl}/publish`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          topic,
          payload,
          qos,
          retain
        }),
      });
      
      return await response.json();
    } catch (error) {
      console.error('Error publishing message:', error);
      return { status: 'error', message: error.message };
    }
  }

  /**
   * Fetch Meshtastic nodes
   * @returns {Promise<Object>} Nodes object
   */
  async fetchNodes() {
    try {
      const response = await fetch(`${this.apiBaseUrl}/meshtastic/nodes`);
      const data = await response.json();
      
      if (data.nodes) {
        this.nodes = data.nodes;
        return data.nodes;
      }
      return {};
    } catch (error) {
      console.error('Error fetching Meshtastic nodes:', error);
      return {};
    }
  }

  /**
   * Send a message to a Meshtastic node
   * @param {string} text - Message text
   * @param {string} destination - Destination node ID or 'broadcast'
   * @returns {Promise<Object>} Result object
   */
  async sendMeshtasticMessage(text, destination) {
    try {
      const endpoint = destination 
        ? `${this.apiBaseUrl}/meshtastic/nodes/${destination}/message` 
        : `${this.apiBaseUrl}/meshtastic/broadcast`;
      
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ text })
      });
      
      if (!response.ok) {
        throw new Error(`Error: ${response.status}`);
      }
      
      return await response.json();
    } catch (error) {
      console.error('Error sending Meshtastic message:', error);
      return { success: false, error: error.message };
    }
  }

  /**
   * Update MQTT connection status indicator
   * @param {boolean} connected - Connection status
   */
  updateMqttStatus(status) {
    const statusElement = document.getElementById('mqtt-status');
    if (!statusElement) return;
    
    if (status === true || status === 'connected') {
      statusElement.className = 'badge rounded-pill text-bg-success';
      statusElement.innerHTML = '<i class="fas fa-plug"></i> MQTT: Connected';
    } else if (status === 'connecting') {
      statusElement.className = 'badge rounded-pill text-bg-warning';
      statusElement.innerHTML = '<i class="fas fa-spinner fa-spin"></i> MQTT: Connecting...';
    } else if (status === 'reconnecting') {
      statusElement.className = 'badge rounded-pill text-bg-warning';
      statusElement.innerHTML = '<i class="fas fa-sync fa-spin"></i> MQTT: Reconnecting...';
    } else {
      statusElement.className = 'badge rounded-pill text-bg-danger';
      statusElement.innerHTML = '<i class="fas fa-plug"></i> MQTT: Disconnected';
    }
  }

  /**
   * Update API connection status indicator
   * @param {boolean} connected - Connection status
   */
  updateApiStatus(connected) {
    const statusElement = document.getElementById('api-status');
    if (!statusElement) return;
    
    if (connected) {
      statusElement.className = 'badge rounded-pill text-bg-success';
      statusElement.innerHTML = '<i class="fas fa-server"></i> API: Connected';
    } else {
      statusElement.className = 'badge rounded-pill text-bg-danger';
      statusElement.innerHTML = '<i class="fas fa-server"></i> API: Disconnected';
    }
  }
  
  /**
   * Show an alert for connection issues
   * @param {string} message - Optional custom message
   */
  showConnectionAlert(message = 'Connection to the backend API failed. Please check the server status.') {
    // Check if alert already exists
    if (document.getElementById('connection-alert')) return;
    
    // Create alert element
    const alertDiv = document.createElement('div');
    alertDiv.id = 'connection-alert';
    alertDiv.className = 'alert alert-warning alert-dismissible fade show';
    alertDiv.setAttribute('role', 'alert');
    alertDiv.style.position = 'fixed';
    alertDiv.style.top = '10px';
    alertDiv.style.left = '50%';
    alertDiv.style.transform = 'translateX(-50%)';
    alertDiv.style.zIndex = '9999';
    alertDiv.style.maxWidth = '80%';
    
    alertDiv.innerHTML = `
      <i class="fas fa-exclamation-triangle me-2"></i>
      ${message}
      <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    // Add to body
    document.body.appendChild(alertDiv);
    
    // Auto-dismiss after 10 seconds
    setTimeout(() => {
      if (alertDiv.parentNode) {
        alertDiv.parentNode.removeChild(alertDiv);
      }
    }, 10000);
  }
  
  /**
   * Clear connection alert if present
   */
  clearConnectionAlert() {
    const alertDiv = document.getElementById('connection-alert');
    if (alertDiv && alertDiv.parentNode) {
      alertDiv.parentNode.removeChild(alertDiv);
    }
  }

  /**
   * Request channel assignment for topic-based messaging
   */
  requestChannelAssignment() {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      console.error('WebSocket not connected, cannot request channel');
      return;
    }
    
    const request = {
      type: 'request_channel',
      client_type: 'web_client',
      auth_token: this.authToken
    };
    
    try {
      this.ws.send(JSON.stringify(request));
      console.log('Requested channel assignment');
    } catch (error) {
      console.error('Error requesting channel assignment:', error);
    }
  }

  /**
   * Resubscribe to previously subscribed topics after reconnection
   */
  async resubscribeTopics() {
    if (!this.channelId || !this.connected) {
      console.error('Not connected to a channel, cannot resubscribe');
      return;
    }
    
    console.log(`Resubscribing to ${this.subscribedTopics.size} topics`);
    
    for (const topic of this.subscribedTopics) {
      await this.subscribeToChannel(topic);
    }
  }

  /**
   * Subscribe to a topic in the assigned channel
   * @param {string} topic - Topic to subscribe to
   */
  async subscribeToChannel(topic) {
    if (!this.channelId || !this.connected) {
      console.error('Not connected to a channel, cannot subscribe');
      return false;
    }
    
    const request = {
      type: 'subscribe',
      channel_id: this.channelId,
      topic: topic
    };
    
    try {
      this.ws.send(JSON.stringify(request));
      console.log(`Subscribed to ${topic} in channel ${this.channelId}`);
      return true;
    } catch (error) {
      console.error(`Error subscribing to ${topic}:`, error);
      return false;
    }
  }

  /**
   * Show reconnection button in the UI
   */
  showReconnectButton() {
    const statusElement = document.getElementById('mqtt-status');
    if (statusElement) {
      statusElement.innerHTML = `
        <i class="fas fa-exclamation-triangle"></i> 
        MQTT: Disconnected 
        <button id="manual-reconnect" class="btn btn-sm btn-outline-light ms-2">
          <i class="fas fa-sync-alt"></i> Reconnect
        </button>
      `;
      
      // Add click handler to the reconnect button
      const reconnectButton = document.getElementById('manual-reconnect');
      if (reconnectButton) {
        reconnectButton.addEventListener('click', () => {
          this.reconnectAttempts = 0;
          this.connectWebSocket();
        });
      }
    }
  }

  /**
   * Fetch Meshtastic nodes
   */
  async fetchMeshtasticNodes() {
    try {
      console.log('Fetching Meshtastic nodes...');
      const response = await fetch(`${this.apiBaseUrl}/meshtastic/nodes`);
      if (!response.ok) {
        throw new Error(`Error: ${response.status}`);
      }
      const nodes = await response.json();
      return nodes;
    } catch (error) {
      console.error('Error fetching Meshtastic nodes:', error);
      return [];
    }
  }

  /**
   * Discover available Meshtastic devices
   * @param {string} connectionType - Type of connection to discover ('serial', 'ble', or 'all')
   * @returns {Promise<object>} - Discovery result
   */
  async discoverMeshtasticDevices(connectionType = 'all') {
    try {
      // Start discovery with the new API
      const response = await this.authenticatedFetch(`${this.apiBaseUrl}/meshtastic/discover`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ connection_type: connectionType })
      });
      
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to discover devices');
      }
      
      return await response.json();
    } catch (error) {
      console.error('Error discovering Meshtastic devices:', error);
      return {
        success: false,
        error: error.message || 'Failed to discover devices'
      };
    }
  }
  
  /**
   * Get connected Meshtastic devices
   * @returns {Promise<Array>} - List of connected devices
   */
  async getConnectedMeshtasticDevices() {
    try {
      // Check if API key is available
      if (!this.apiKey) {
        console.warn('API key is required to get connected devices');
        return [];
      }
      
      // Get connected devices
      const response = await this.authenticatedFetch(`${this.apiBaseUrl}/meshtastic/devices`);
      
      if (!response.ok) {
        if (response.status === 401 || response.status === 403) {
          // Authentication error
          console.warn('Authentication required to access device information');
          return [];
        }
        
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to get connected devices');
      }
      
      return await response.json();
    } catch (error) {
      console.error('Error getting connected Meshtastic devices:', error);
      return [];
    }
  }
  
  /**
   * Connect to a Meshtastic device
   * @param {string} connectionType - Type of connection ('serial', 'tcp', 'ble')
   * @param {object} connectionParams - Parameters for the connection
   * @param {string|null} deviceId - Optional device ID
   * @returns {Promise<object>} - Connection result
   */
  async connectMeshtasticDevice(connectionType, connectionParams, deviceId = null) {
    try {
      // Check if API key is available
      if (!this.apiKey) {
        console.warn('API key is required to connect to a device');
        return { success: false, error: 'Authentication required' };
      }
      
      // Prepare request data
      const requestData = {
        connection_type: connectionType,
        connection_params: connectionParams
      };
      
      // Add device ID if provided
      if (deviceId) {
        requestData.device_id = deviceId;
      }
      
      // Connect to the device
      const response = await this.authenticatedFetch(`${this.apiBaseUrl}/meshtastic/connect`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestData)
      });
      
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to connect to device');
      }
      
      const result = await response.json();
      
      // Return a consistent format
      if (result.success) {
        console.log(`Connected to device ${result.device_id}`);
        return {
          success: true,
          device_id: result.device_id,
          message: result.message || 'Successfully connected to device'
        };
      } else {
        console.warn('Device connection failed', result);
        return {
          success: false,
          error: result.error || 'Failed to connect to device'
        };
      }
    } catch (error) {
      console.error('Error connecting to Meshtastic device:', error);
      return {
        success: false,
        error: error.message || 'Failed to connect to device'
      };
    }
  }
  
  /**
   * Disconnect from a Meshtastic device
   * @param {string} deviceId - Device ID to disconnect
   * @returns {Promise<object>} - Disconnection result
   */
  async disconnectMeshtasticDevice(deviceId) {
    try {
      // Check if API key is available
      if (!this.apiKey) {
        console.warn('API key is required to disconnect from a device');
        return { success: false, error: 'Authentication required' };
      }
      
      // Disconnect from the device
      const response = await this.authenticatedFetch(`${this.apiBaseUrl}/meshtastic/devices/${deviceId}`, {
        method: 'DELETE'
      });
      
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to disconnect from device');
      }
      
      const result = await response.json();
      
      // Return a consistent format
      if (result.success) {
        console.log(`Disconnected from device ${deviceId}`);
        return {
          success: true,
          message: result.message || 'Successfully disconnected from device'
        };
      } else {
        console.warn('Device disconnection failed', result);
        return {
          success: false,
          error: result.error || 'Failed to disconnect from device'
        };
      }
    } catch (error) {
      console.error('Error disconnecting from Meshtastic device:', error);
      return {
        success: false,
        error: error.message || 'Failed to disconnect from device'
      };
    }
  }
  
  /**
   * Get device configuration
   * @param {string|null} deviceId - Device ID (uses default if null)
   */
  async getMeshtasticDeviceConfig(deviceId = null) {
    try {
      const endpoint = deviceId 
        ? `${this.apiBaseUrl}/meshtastic/devices/${deviceId}/config`
        : `${this.apiBaseUrl}/meshtastic/devices/default/config`;
        
      const response = await this.authenticatedFetch(endpoint);
      
      if (!response.ok) {
        // Check for authentication issues
        if (response.status === 403) {
          console.error('Authentication required for getting device configuration. Please set a valid API key.');
          return { success: false, error: 'Authentication required. Please set a valid API key.' };
        }
        throw new Error(`Error: ${response.status}`);
      }
      
      return await response.json();
    } catch (error) {
      console.error('Error getting Meshtastic device configuration:', error);
      return { success: false, error: error.message };
    }
  }
  
  /**
   * Set device configuration
   * @param {string} key - Configuration key
   * @param {any} value - Value to set
   * @param {string|null} deviceId - Device ID (uses default if null)
   */
  async setMeshtasticDeviceConfig(key, value, deviceId = null) {
    try {
      const endpoint = deviceId 
        ? `${this.apiBaseUrl}/meshtastic/devices/${deviceId}/config`
        : `${this.apiBaseUrl}/meshtastic/devices/default/config`;
        
      const response = await this.authenticatedFetch(endpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ key, value })
      });
      
      if (!response.ok) {
        // Check for authentication issues
        if (response.status === 403) {
          console.error('Authentication required for setting device configuration. Please set a valid API key.');
          return { success: false, error: 'Authentication required. Please set a valid API key.' };
        }
        throw new Error(`Error: ${response.status}`);
      }
      
      return await response.json();
    } catch (error) {
      console.error('Error setting Meshtastic device configuration:', error);
      return { success: false, error: error.message };
    }
  }
  
  /**
   * Get device channel settings
   * @param {string|null} deviceId - Device ID (uses default if null)
   */
  async getMeshtasticDeviceChannels(deviceId = null) {
    try {
      const endpoint = deviceId 
        ? `${this.apiBaseUrl}/meshtastic/devices/${deviceId}/channels`
        : `${this.apiBaseUrl}/meshtastic/devices/default/channels`;
        
      const response = await this.authenticatedFetch(endpoint);
      
      if (!response.ok) {
        // Check for authentication issues
        if (response.status === 403) {
          console.error('Authentication required for getting device channels. Please set a valid API key.');
          return { success: false, error: 'Authentication required. Please set a valid API key.' };
        }
        throw new Error(`Error: ${response.status}`);
      }
      
      return await response.json();
    } catch (error) {
      console.error('Error getting Meshtastic device channels:', error);
      return { success: false, error: error.message };
    }
  }
  
  /**
   * Set device channel settings
   * @param {object} settings - Channel settings to update
   * @param {number} channelIndex - Channel index (default: 0 for primary channel)
   * @param {string|null} deviceId - Device ID (uses default if null)
   */
  async setMeshtasticDeviceChannel(settings, channelIndex = 0, deviceId = null) {
    try {
      const endpoint = deviceId 
        ? `${this.apiBaseUrl}/meshtastic/devices/${deviceId}/channels`
        : `${this.apiBaseUrl}/meshtastic/devices/default/channels`;
        
      const response = await this.authenticatedFetch(endpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ settings, channel_index: channelIndex })
      });
      
      if (!response.ok) {
        // Check for authentication issues
        if (response.status === 403) {
          console.error('Authentication required for setting device channels. Please set a valid API key.');
          return { success: false, error: 'Authentication required. Please set a valid API key.' };
        }
        throw new Error(`Error: ${response.status}`);
      }
      
      return await response.json();
    } catch (error) {
      console.error('Error setting Meshtastic device channel:', error);
      return { success: false, error: error.message };
    }
  }
}

// Create global client instance
const mqttClient = new MQTTClient();

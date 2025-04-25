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
    
    // Send a ping message every 30 seconds
    this.pingTimer = setInterval(() => {
      if (this.connected && this.ws && this.ws.readyState === WebSocket.OPEN) {
        console.log('Sending ping to server...');
        this.ws.send(JSON.stringify({ type: 'ping' }));
      } else {
        // If we're not connected, stop pinging
        clearInterval(this.pingTimer);
        this.pingTimer = null;
      }
    }, 30000); // 30 seconds
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
   * Check the status of the MQTT broker
   * @returns {Promise<Object>} Status information
   */
  async checkStatus() {
    try {
      console.log('Checking broker status...');
      
      // Common headers for all requests
      const headers = {
        'Content-Type': 'application/json',
        'Authorization': this.authToken ? `Bearer ${this.authToken}` : '',
        // Add headers that help with CORS issues
        'Accept': 'application/json',
        'X-Requested-With': 'XMLHttpRequest'
      };
      
      // Use the broker_status endpoint directly since /status is not working
      try {
        console.log('Trying /broker_status endpoint...');
        const response = await fetch(`${this.apiBaseUrl}/broker_status`, {
          method: 'GET',
          mode: 'cors',
          credentials: 'same-origin',
          headers,
          cache: 'no-cache'
        });
        
        if (!response.ok) {
          // Additional details about the failed request
          console.error('Failed to fetch broker status:', {
            status: response.status,
            statusText: response.statusText,
            url: `${this.apiBaseUrl}/broker_status`
          });
          
          this.updateApiStatus(false);
          
          // Try to get more information from the response if possible
          try {
            const errorData = await response.json();
            console.error('Error details from server:', errorData);
            return { status: 'error', details: errorData };
          } catch {
            // If we can't parse the response, just return the basic info
            return { status: 'error', statusCode: response.status };
          }
        }
        
        const data = await response.json();
        console.log('Broker status:', data);
        
        // Update MQTT status based on connection status
        const connected = data.connection === 'connected' || data.status === 'connected';
        this.updateMqttStatus(connected);
        
        // Also make sure API status shows as connected since we got a valid response
        this.updateApiStatus(true);
        
        // Return a format compatible with what the checkApiStatus function expects
        return {
          status: connected ? 'connected' : 'disconnected',
          services: {
            mqtt: {
              status: connected ? 'connected' : 'disconnected'
            }
          }
        };
      } catch (error) {
        console.error('Error with /broker_status endpoint:', error);
        
        // Try to identify network or CORS issues
        if (error.name === 'TypeError' && error.message.includes('Failed to fetch')) {
          console.error('This appears to be a network or CORS issue.');
          this.showConnectionAlert('API connection failed. This may be due to CORS restrictions or network issues.');
        }
        
        throw error; // Rethrow to be caught by the outer try/catch
      }
    } catch (error) {
      console.error('Error checking broker status:', error);
      this.updateApiStatus(false);
      this.updateMqttStatus(false);
      
      // Add UI alert for connection issues
      this.showConnectionAlert();
      
      return { status: 'error', message: error.message };
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
      // Build topic based on destination
      const topic = destination === 'broadcast' 
        ? 'msh/broadcast/json/text' 
        : `msh/${destination}/json/text`;
      
      // Build payload
      const payload = {
        text: {
          text: text,
          from: 'mqtt-bridge-ui',
          to: destination === 'broadcast' ? '^all' : destination,
          time: Math.floor(Date.now() / 1000)
        }
      };
      
      return await this.publishMessage(topic, payload);
    } catch (error) {
      console.error('Error sending Meshtastic message:', error);
      return { status: 'error', message: error.message };
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
}

// Create global client instance
const mqttClient = new MQTTClient();

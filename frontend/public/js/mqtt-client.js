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
      // Check authentication status if we have a token
      if (this.authToken) {
        try {
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
        }
      }
      
      // Check API status
      const statusResponse = await this.checkStatus();
      
      if (statusResponse && statusResponse.status === 'connected') {
        this.updateApiStatus(true);
        console.log('API is connected to MQTT broker');
      } else {
        this.updateApiStatus(false);
        console.warn('API is not connected to MQTT broker');
      }
      
      // Get existing topic subscriptions
      await this.fetchTopics();
      
      // Connect to WebSocket
      this.connectWebSocket();
      
      // Fetch nodes
      await this.fetchNodes();
      
      return true;
    } catch (error) {
      console.error('Failed to initialize MQTT client:', error);
      this.updateApiStatus(false);
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
      this.ws = new WebSocket(wsUrl);
      
      this.ws.onopen = () => {
        console.log('WebSocket connection established');
        this.connected = true;
        this.reconnectAttempts = 0;
        this.updateMqttStatus(true);
        
        // Request channel assignment for topic-based subscriptions
        if (this.authToken) {
          this.requestChannelAssignment();
        }
      };
      
      this.ws.onclose = (event) => {
        console.warn(`WebSocket connection closed: ${event.code} ${event.reason}`);
        this.connected = false;
        this.channelId = null;
        this.updateMqttStatus(false);
        
        // Try to reconnect with exponential backoff
        if (this.reconnectAttempts < this.maxReconnectAttempts) {
          const backoffTime = this.reconnectInterval * Math.pow(1.5, this.reconnectAttempts);
          this.reconnectAttempts++;
          console.log(`Reconnecting (attempt ${this.reconnectAttempts}) in ${backoffTime}ms...`);
          this.reconnectTimer = setTimeout(() => this.connectWebSocket(), backoffTime);
        } else {
          console.error('Max reconnection attempts reached');
          // Show reconnection button to user
          this.showReconnectButton();
        }
      };
      
      this.ws.onerror = (error) => {
        console.error('WebSocket error:', error);
        this.connected = false;
        this.updateMqttStatus(false);
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
        }
      };
    } catch (error) {
      console.error('Error connecting to WebSocket:', error);
      this.updateMqttStatus(false);
      
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
   * Handle incoming MQTT messages
   * @param {Object} message - Message object
   */
  handleMessage(message) {
    // Store message in history
    this.addToHistory(message);
    
    // Call the callback if defined
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
   * Check MQTT broker connection status
   * @returns {Promise<Object>} Status object
   */
  async checkStatus() {
    try {
      const headers = {};
      if (this.authToken) {
        headers['Authorization'] = `Bearer ${this.authToken}`;
      }
      
      const response = await fetch(`${this.apiBaseUrl}/broker_status`, {
        headers
      });
      
      if (!response.ok) {
        throw new Error(`HTTP error ${response.status}`);
      }
      
      const data = await response.json();
      return data;
    } catch (error) {
      console.error('Error checking MQTT broker status:', error);
      return { status: 'disconnected' };
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
  updateMqttStatus(connected) {
    const statusElement = document.getElementById('mqtt-status');
    if (!statusElement) return;
    
    if (connected) {
      statusElement.className = 'badge rounded-pill text-bg-success';
      statusElement.innerHTML = '<i class="fas fa-plug"></i> MQTT: Connected';
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

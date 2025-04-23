/**
 * MQTT Client for Meshtastic Bridge
 * Handles communication with the backend API and WebSocket
 */

class MQTTClient {
  constructor() {
    // API base URL
    this.apiBaseUrl = 'http://127.0.0.1:8000';
    
    // WebSocket URL
    this.wsUrl = 'ws://127.0.0.1:8000/ws';
    
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
    
    // Message history
    this.messageHistory = [];
    this.maxMessageHistory = 500;
    
    // Topic subscriptions
    this.subscribedTopics = new Set();
    
    // Node information
    this.nodes = {};
  }

  /**
   * Initialize the MQTT client
   * @param {Function} messageCallback - Callback function for messages
   */
  async init(messageCallback) {
    this.messageCallback = messageCallback;
    
    try {
      // Check API status first
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
    // Close existing connection if any
    if (this.ws) {
      this.ws.close();
    }
    
    try {
      this.ws = new WebSocket(this.wsUrl);
      
      this.ws.onopen = () => {
        console.log('WebSocket connection established');
        this.connected = true;
        this.reconnectAttempts = 0;
        this.updateMqttStatus(true);
      };
      
      this.ws.onclose = (event) => {
        console.warn(`WebSocket connection closed: ${event.code} ${event.reason}`);
        this.connected = false;
        this.updateMqttStatus(false);
        
        // Try to reconnect
        if (this.reconnectAttempts < this.maxReconnectAttempts) {
          this.reconnectAttempts++;
          console.log(`Reconnecting (attempt ${this.reconnectAttempts})...`);
          setTimeout(() => this.connectWebSocket(), this.reconnectInterval);
        } else {
          console.error('Max reconnection attempts reached');
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
          this.handleMessage(message);
        } catch (error) {
          console.error('Error parsing WebSocket message:', error);
        }
      };
    } catch (error) {
      console.error('Error connecting to WebSocket:', error);
      this.updateMqttStatus(false);
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
      const response = await fetch(`${this.apiBaseUrl}/broker_status`);
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
      const response = await fetch(`${this.apiBaseUrl}/subscribe`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ topic }),
      });
      
      const data = await response.json();
      
      if (data.status === 'success') {
        this.subscribedTopics.add(topic);
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
}

// Create global client instance
const mqttClient = new MQTTClient();

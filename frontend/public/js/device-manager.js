/**
 * Device Manager UI functionality for Meshtastic MQTT Bridge
 */
class DeviceManager {
  /**
   * Initialize the device manager
   * @param {MQTTClient} mqttClient - The MQTT client instance
   */
  constructor(mqttClient) {
    this.mqttClient = mqttClient;
    this.deviceListElement = document.getElementById('device-list');
    this.discoveredDevicesElement = document.getElementById('discovered-devices');
    this.serialDevicesElement = document.getElementById('serial-devices');
    this.bleDevicesElement = document.getElementById('ble-devices');
    this.configFormElement = document.getElementById('device-config-form');
    this.channelFormElement = document.getElementById('device-channel-form');
    
    this.connectedDevices = [];
    this.discoveredDevices = {
      serial: [],
      ble: []
    };
    
    // Add discovery timeout property
    this.discoveryTimeout = null;
    
    // Store button references
    this.discoverSerialBtn = document.getElementById('discover-serial-btn');
    this.discoverBleBtn = document.getElementById('discover-ble-btn');
    this.discoverAllBtn = document.getElementById('discover-all-btn');
    this.connectDeviceForm = document.getElementById('connect-device-form');
    
    // Check that required elements exist
    if (!this.deviceListElement) {
      console.error('Device list element not found');
    }
    
    if (!this.serialDevicesElement) {
      console.error('Serial devices element not found');
    }
    
    if (!this.bleDevicesElement) {
      console.error('BLE devices element not found');
    }
    
    // Initialize event listeners
    this.initEventListeners();
  }
  
  /**
   * Initialize event listeners
   */
  initEventListeners() {
    console.log('Initializing device manager event listeners');
    
    // Device discovery
    if (this.discoverSerialBtn) {
      console.log('Adding click handler for discover-serial-btn');
      this.discoverSerialBtn.addEventListener('click', () => {
        console.log('Discover serial button clicked');
        this.discoverDevices('serial');
      });
    } else {
      console.error('Discover serial button not found');
    }
    
    if (this.discoverBleBtn) {
      console.log('Adding click handler for discover-ble-btn');
      this.discoverBleBtn.addEventListener('click', () => {
        console.log('Discover BLE button clicked');
        this.discoverDevices('ble');
      });
    } else {
      console.error('Discover BLE button not found');
    }
    
    if (this.discoverAllBtn) {
      console.log('Adding click handler for discover-all-btn');
      this.discoverAllBtn.addEventListener('click', () => {
        console.log('Discover all button clicked');
        this.discoverDevices('all');
      });
    } else {
      console.error('Discover all button not found');
    }
    
    // Device connection form
    if (this.connectDeviceForm) {
      console.log('Adding submit handler for connect-device-form');
      this.connectDeviceForm.addEventListener('submit', (e) => {
        console.log('Connect device form submitted');
        e.preventDefault();
        this.connectDevice();
      });
    } else {
      console.error('Connect device form not found');
    }
    
    // Config form submission
    if (this.configFormElement) {
      this.configFormElement.addEventListener('submit', (e) => {
        e.preventDefault();
        this.updateDeviceConfig();
      });
    }
    
    // Channel form submission
    if (this.channelFormElement) {
      this.channelFormElement.addEventListener('submit', (e) => {
        e.preventDefault();
        this.updateDeviceChannel();
      });
    }
  }
  
  /**
   * Load connected devices
   */
  async loadConnectedDevices() {
    try {
      // Clear any previous error messages
      if (this.deviceListElement) {
        this.deviceListElement.innerHTML = `
          <div class="list-group-item text-center">
            <div class="spinner-border spinner-border-sm text-primary" role="status">
              <span class="visually-hidden">Loading...</span>
            </div>
            Loading devices...
          </div>
        `;
      }
      
      this.connectedDevices = await this.mqttClient.getConnectedMeshtasticDevices();
      
      // Check if we received an authentication error (empty array with MQTT client error in console)
      if (this.connectedDevices.length === 0 && !this.mqttClient.getApiKey()) {
        if (this.deviceListElement) {
          this.deviceListElement.innerHTML = `
            <div class="list-group-item text-center text-danger">
              <i class="fas fa-exclamation-triangle me-2"></i>
              Authentication required
            </div>
            <div class="list-group-item">
              <p class="mb-2">An API key is required to manage devices.</p>
              <button class="btn btn-sm btn-primary w-100" data-bs-toggle="modal" data-bs-target="#apiKeyModal">
                <i class="fas fa-key me-2"></i>Set API Key
              </button>
            </div>
          `;
        }
        return;
      }
      
      this.renderDeviceList();
    } catch (error) {
      console.error('Error loading connected devices:', error);
      showToast('error', 'Failed to load connected devices');
      
      if (this.deviceListElement) {
        this.deviceListElement.innerHTML = `
          <div class="list-group-item text-center text-danger">
            <i class="fas fa-exclamation-triangle me-2"></i>
            Error loading devices
          </div>
        `;
      }
    }
  }
  
  /**
   * Render the device list
   */
  renderDeviceList() {
    if (!this.deviceListElement) return;
    
    this.deviceListElement.innerHTML = '';
    
    if (this.connectedDevices.length === 0) {
      this.deviceListElement.innerHTML = '<div class="list-group-item">No devices connected</div>';
      return;
    }
    
    for (const device of this.connectedDevices) {
      const deviceElement = document.createElement('div');
      deviceElement.className = 'list-group-item d-flex justify-content-between align-items-center';
      
      // Default flag
      let defaultBadge = '';
      if (device.is_default) {
        defaultBadge = '<span class="badge bg-primary ms-2">Default</span>';
      }
      
      // Connection type icon
      let connectionIcon = '';
      switch (device.connection_type) {
        case 'serial':
          connectionIcon = '<i class="fas fa-usb me-2"></i>';
          break;
        case 'tcp':
          connectionIcon = '<i class="fas fa-network-wired me-2"></i>';
          break;
        case 'ble':
          connectionIcon = '<i class="fab fa-bluetooth-b me-2"></i>';
          break;
        default:
          connectionIcon = '<i class="fas fa-plug me-2"></i>';
      }
      
      // Connection params summary
      let connectionDetails = '';
      if (device.connection_type === 'serial' && device.connection_params.port) {
        connectionDetails = `<small class="text-muted">${device.connection_params.port}</small>`;
      } else if (device.connection_type === 'tcp' && device.connection_params.host) {
        connectionDetails = `<small class="text-muted">${device.connection_params.host}</small>`;
      } else if (device.connection_type === 'ble' && device.connection_params.name) {
        connectionDetails = `<small class="text-muted">${device.connection_params.name}</small>`;
      }
      
      deviceElement.innerHTML = `
        <div>
          ${connectionIcon}<strong>${device.id}</strong> ${defaultBadge}
          <br>${connectionDetails}
        </div>
        <div>
          <button class="btn btn-sm btn-outline-primary me-1 config-btn" data-device-id="${device.id}">
            <i class="fas fa-cog"></i>
          </button>
          <button class="btn btn-sm btn-outline-danger disconnect-btn" data-device-id="${device.id}">
            <i class="fas fa-unlink"></i>
          </button>
        </div>
      `;
      
      // Add event listeners for config and disconnect buttons
      const configBtn = deviceElement.querySelector('.config-btn');
      const disconnectBtn = deviceElement.querySelector('.disconnect-btn');
      
      configBtn.addEventListener('click', () => this.showDeviceConfig(device.id));
      disconnectBtn.addEventListener('click', () => this.disconnectDevice(device.id));
      
      this.deviceListElement.appendChild(deviceElement);
    }
  }
  
  /**
   * Discover available devices
   * @param {string} connectionType - Type of connection to discover ('serial', 'ble', or 'all')
   */
  async discoverDevices(connectionType = 'all') {
    try {
      // Clear any previous discovery timeout
      if (this.discoveryTimeout) {
        clearTimeout(this.discoveryTimeout);
        this.discoveryTimeout = null;
      }
      
      // Keep track of polling interval
      let pollingInterval = null;
      
      // Show loading state
      if (connectionType === 'serial' || connectionType === 'all') {
        this.serialDevicesElement.innerHTML = '<div class="list-group-item text-center"><div class="spinner-border spinner-border-sm text-primary" role="status"></div> Discovering serial devices...</div>';
      }
      if (connectionType === 'ble' || connectionType === 'all') {
        this.bleDevicesElement.innerHTML = '<div class="list-group-item text-center"><div class="spinner-border spinner-border-sm text-primary" role="status"></div> Discovering BLE devices...</div>';
      }
      
      // Check for API key first
      if (!this.mqttClient.getApiKey()) {
        if (connectionType === 'serial' || connectionType === 'all') {
          this.serialDevicesElement.innerHTML = `
            <div class="list-group-item text-center text-danger">
              <i class="fas fa-exclamation-triangle me-2"></i>Authentication required
            </div>
            <div class="list-group-item">
              <p class="mb-2">An API key is required to discover devices.</p>
              <button class="btn btn-sm btn-primary w-100" data-bs-toggle="modal" data-bs-target="#apiKeyModal">
                <i class="fas fa-key me-2"></i>Set API Key
              </button>
            </div>
          `;
        }
        if (connectionType === 'ble' || connectionType === 'all') {
          this.bleDevicesElement.innerHTML = `
            <div class="list-group-item text-center text-danger">
              <i class="fas fa-exclamation-triangle me-2"></i>Authentication required
            </div>
          `;
        }
        return;
      }
      
      // Start device discovery via API
      const response = await fetch('/api/meshtastic/discover', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-API-Key': this.mqttClient.getApiKey()
        },
        body: JSON.stringify({ connection_type: connectionType })
      });
      
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to start device discovery');
      }
      
      const data = await response.json();
      
      if (!data.success) {
        throw new Error(data.error || 'Failed to start device discovery');
      }
      
      // Get the task ID
      const taskId = data.task_id;
      
      // Poll for results
      const pollDiscoveryStatus = async () => {
        try {
          const statusResponse = await fetch(`/api/meshtastic/discover/${taskId}`, {
            headers: {
              'X-API-Key': this.mqttClient.getApiKey()
            }
          });
          
          if (!statusResponse.ok) {
            throw new Error('Failed to get discovery status');
          }
          
          const statusData = await statusResponse.json();
          
          // Check if discovery is complete
          if (statusData.status === 'complete') {
            // Clear polling interval
            if (pollingInterval) {
              clearInterval(pollingInterval);
              pollingInterval = null;
            }
            
            // Handle discovery results
            this.handleDiscoveryResults(statusData.result, connectionType);
            
          } else if (statusData.status === 'error') {
            // Clear polling interval
            if (pollingInterval) {
              clearInterval(pollingInterval);
              pollingInterval = null;
            }
            
            // Show error message
            this.handleDiscoveryError(statusData.error, connectionType);
          }
          
        } catch (error) {
          console.error('Error polling discovery status:', error);
          
          // Clear polling interval on error
          if (pollingInterval) {
            clearInterval(pollingInterval);
            pollingInterval = null;
          }
          
          // Show error message
          this.handleDiscoveryError(error.message, connectionType);
        }
      };
      
      // Poll every 2 seconds
      pollingInterval = setInterval(pollDiscoveryStatus, 2000);
      
      // Also run immediately to get faster feedback
      await pollDiscoveryStatus();
      
      // Set a timeout to prevent infinite loading state (35 seconds - slightly longer than backend timeout)
      this.discoveryTimeout = setTimeout(() => {
        console.warn('Device discovery client timeout after 35 seconds');
        
        // Clear polling interval if still active
        if (pollingInterval) {
          clearInterval(pollingInterval);
          pollingInterval = null;
        }
        
        // Check if we're still in loading state and update UI
        if (connectionType === 'serial' || connectionType === 'all') {
          if (this.serialDevicesElement.innerHTML.includes('Discovering')) {
            this.serialDevicesElement.innerHTML = `
              <div class="list-group-item text-warning">
                <i class="fas fa-exclamation-triangle me-2"></i>Discovery timed out
              </div>
              <div class="list-group-item">
                <button class="btn btn-sm btn-warning w-100 retry-discovery-btn" data-type="serial">
                  <i class="fas fa-sync me-2"></i>Retry Serial Discovery
                </button>
              </div>
            `;
            // Add event listener for retry button
            const retryBtn = this.serialDevicesElement.querySelector('.retry-discovery-btn');
            if (retryBtn) {
              retryBtn.addEventListener('click', () => this.discoverDevices('serial'));
            }
          }
        }
        if (connectionType === 'ble' || connectionType === 'all') {
          if (this.bleDevicesElement.innerHTML.includes('Discovering')) {
            this.bleDevicesElement.innerHTML = `
              <div class="list-group-item text-warning">
                <i class="fas fa-exclamation-triangle me-2"></i>Discovery timed out
              </div>
              <div class="list-group-item">
                <button class="btn btn-sm btn-warning w-100 retry-discovery-btn" data-type="ble">
                  <i class="fas fa-sync me-2"></i>Retry BLE Discovery
                </button>
              </div>
            `;
            // Add event listener for retry button
            const retryBtn = this.bleDevicesElement.querySelector('.retry-discovery-btn');
            if (retryBtn) {
              retryBtn.addEventListener('click', () => this.discoverDevices('ble'));
            }
          }
        }
      }, 35000);
      
    } catch (error) {
      console.error('Error discovering devices:', error);
      
      // Handle error
      this.handleDiscoveryError(error.message, connectionType);
    }
  }
  
  /**
   * Handle device discovery results
   * @param {Object} result - Discovery results
   * @param {string} connectionType - Type of connection that was discovered
   */
  handleDiscoveryResults(result, connectionType) {
    console.log('Discovery results:', result);
    
    // Clear the timeout since we have results
    if (this.discoveryTimeout) {
      clearTimeout(this.discoveryTimeout);
      this.discoveryTimeout = null;
    }
    
    // Update the UI for serial devices
    if (connectionType === 'serial' || connectionType === 'all') {
      // Check if we have results and no errors
      if (result && result.serial && !result.serial_error) {
        // Update discovered serial devices list
        this.discoveredDevices.serial = result.serial || [];
        
        // Render serial devices
        this.renderDiscoveredDevices('serial', this.serialDevicesElement);
      } else if (result && result.serial_error) {
        // Show error message
        this.serialDevicesElement.innerHTML = `
          <div class="list-group-item text-danger">
            <i class="fas fa-exclamation-triangle me-2"></i>Error: ${result.serial_error}
          </div>
          <div class="list-group-item">
            <button class="btn btn-sm btn-warning w-100 retry-discovery-btn" data-type="serial">
              <i class="fas fa-sync me-2"></i>Retry Serial Discovery
            </button>
          </div>
        `;
        // Add event listener for retry button
        const retryBtn = this.serialDevicesElement.querySelector('.retry-discovery-btn');
        if (retryBtn) {
          retryBtn.addEventListener('click', () => this.discoverDevices('serial'));
        }
      } else {
        // No devices found but no error
        this.serialDevicesElement.innerHTML = `
          <div class="list-group-item">
            <i class="fas fa-info-circle me-2"></i>No serial devices found
          </div>
          <div class="list-group-item">
            <button class="btn btn-sm btn-primary w-100 retry-discovery-btn" data-type="serial">
              <i class="fas fa-sync me-2"></i>Retry Serial Discovery
            </button>
          </div>
        `;
        // Add event listener for retry button
        const retryBtn = this.serialDevicesElement.querySelector('.retry-discovery-btn');
        if (retryBtn) {
          retryBtn.addEventListener('click', () => this.discoverDevices('serial'));
        }
      }
    }
    
    // Update the UI for BLE devices
    if (connectionType === 'ble' || connectionType === 'all') {
      // Check if we have results and no errors
      if (result && result.ble && !result.ble_error) {
        // Update discovered BLE devices list
        this.discoveredDevices.ble = result.ble || [];
        
        // Render BLE devices
        this.renderDiscoveredDevices('ble', this.bleDevicesElement);
      } else if (result && result.ble_error) {
        // Show error message
        this.bleDevicesElement.innerHTML = `
          <div class="list-group-item text-danger">
            <i class="fas fa-exclamation-triangle me-2"></i>Error: ${result.ble_error}
          </div>
          <div class="list-group-item">
            <button class="btn btn-sm btn-warning w-100 retry-discovery-btn" data-type="ble">
              <i class="fas fa-sync me-2"></i>Retry BLE Discovery
            </button>
          </div>
        `;
        // Add event listener for retry button
        const retryBtn = this.bleDevicesElement.querySelector('.retry-discovery-btn');
        if (retryBtn) {
          retryBtn.addEventListener('click', () => this.discoverDevices('ble'));
        }
      } else {
        // No devices found but no error
        this.bleDevicesElement.innerHTML = `
          <div class="list-group-item">
            <i class="fas fa-info-circle me-2"></i>No BLE devices found
          </div>
          <div class="list-group-item">
            <button class="btn btn-sm btn-primary w-100 retry-discovery-btn" data-type="ble">
              <i class="fas fa-sync me-2"></i>Retry BLE Discovery
            </button>
          </div>
        `;
        // Add event listener for retry button
        const retryBtn = this.bleDevicesElement.querySelector('.retry-discovery-btn');
        if (retryBtn) {
          retryBtn.addEventListener('click', () => this.discoverDevices('ble'));
        }
      }
    }
  }
  
  /**
   * Handle device discovery errors
   * @param {string} errorMessage - Error message
   * @param {string} connectionType - Type of connection that had an error
   */
  handleDiscoveryError(errorMessage, connectionType) {
    console.error('Discovery error:', errorMessage);
    
    // Clear the timeout since we have an error
    if (this.discoveryTimeout) {
      clearTimeout(this.discoveryTimeout);
      this.discoveryTimeout = null;
    }
    
    // Format the error message for display
    const formattedError = errorMessage || 'Unknown error';
    
    // Update the UI for serial devices
    if (connectionType === 'serial' || connectionType === 'all') {
      this.serialDevicesElement.innerHTML = `
        <div class="list-group-item text-danger">
          <i class="fas fa-exclamation-triangle me-2"></i>Error: ${formattedError}
        </div>
        <div class="list-group-item">
          <button class="btn btn-sm btn-warning w-100 retry-discovery-btn" data-type="serial">
            <i class="fas fa-sync me-2"></i>Retry Serial Discovery
          </button>
        </div>
      `;
      // Add event listener for retry button
      const retryBtn = this.serialDevicesElement.querySelector('.retry-discovery-btn');
      if (retryBtn) {
        retryBtn.addEventListener('click', () => this.discoverDevices('serial'));
      }
    }
    
    // Update the UI for BLE devices
    if (connectionType === 'ble' || connectionType === 'all') {
      this.bleDevicesElement.innerHTML = `
        <div class="list-group-item text-danger">
          <i class="fas fa-exclamation-triangle me-2"></i>Error: ${formattedError}
        </div>
        <div class="list-group-item">
          <button class="btn btn-sm btn-warning w-100 retry-discovery-btn" data-type="ble">
            <i class="fas fa-sync me-2"></i>Retry BLE Discovery
          </button>
        </div>
      `;
      // Add event listener for retry button
      const retryBtn = this.bleDevicesElement.querySelector('.retry-discovery-btn');
      if (retryBtn) {
        retryBtn.addEventListener('click', () => this.discoverDevices('ble'));
      }
    }
    
    // Show a toast notification
    showToast('error', `Device discovery failed: ${formattedError}`);
  }
  
  /**
   * Render discovered devices
   */
  renderDiscoveredDevices() {
    // Render serial devices
    if (this.serialDevicesElement) {
      this.serialDevicesElement.innerHTML = '';
      
      if (this.discoveredDevices.serial.length === 0) {
        this.serialDevicesElement.innerHTML = '<div class="list-group-item">No serial devices found</div>';
      } else {
        for (const device of this.discoveredDevices.serial) {
          const deviceElement = document.createElement('div');
          deviceElement.className = 'list-group-item d-flex justify-content-between align-items-center';
          
          deviceElement.innerHTML = `
            <div>
              <i class="fas fa-usb me-2"></i><strong>${device.port}</strong>
            </div>
            <button class="btn btn-sm btn-primary connect-serial-btn" data-port="${device.port}">
              Connect
            </button>
          `;
          
          // Add event listener for connect button
          const connectBtn = deviceElement.querySelector('.connect-serial-btn');
          connectBtn.addEventListener('click', () => {
            document.getElementById('connection-type').value = 'serial';
            document.getElementById('connection-params').value = JSON.stringify({ port: device.port });
            // Optionally auto-submit the form
            // document.getElementById('connect-device-form').requestSubmit();
          });
          
          this.serialDevicesElement.appendChild(deviceElement);
        }
      }
    }
    
    // Render BLE devices
    if (this.bleDevicesElement) {
      this.bleDevicesElement.innerHTML = '';
      
      if (this.discoveredDevices.ble.length === 0) {
        this.bleDevicesElement.innerHTML = '<div class="list-group-item">No BLE devices found</div>';
      } else {
        for (const device of this.discoveredDevices.ble) {
          const deviceElement = document.createElement('div');
          deviceElement.className = 'list-group-item d-flex justify-content-between align-items-center';
          
          deviceElement.innerHTML = `
            <div>
              <i class="fab fa-bluetooth-b me-2"></i><strong>${device.name}</strong>
              <br><small class="text-muted">${device.address}</small>
            </div>
            <button class="btn btn-sm btn-primary connect-ble-btn" data-name="${device.name}" data-address="${device.address}">
              Connect
            </button>
          `;
          
          // Add event listener for connect button
          const connectBtn = deviceElement.querySelector('.connect-ble-btn');
          connectBtn.addEventListener('click', () => {
            document.getElementById('connection-type').value = 'ble';
            document.getElementById('connection-params').value = JSON.stringify({ 
              name: device.name,
              address: device.address
            });
            // Optionally auto-submit the form
            // document.getElementById('connect-device-form').requestSubmit();
          });
          
          this.bleDevicesElement.appendChild(deviceElement);
        }
      }
    }
  }
  
  /**
   * Connect to a device
   */
  async connectDevice() {
    try {
      const connectionType = document.getElementById('connection-type').value;
      const connectionParamsStr = document.getElementById('connection-params').value;
      const deviceId = document.getElementById('device-id').value || null;
      
      if (!connectionType || !connectionParamsStr) {
        showToast('error', 'Please provide connection type and parameters');
        return;
      }
      
      // Parse and validate connection parameters
      let connectionParams;
      try {
        connectionParams = JSON.parse(connectionParamsStr);
      } catch (error) {
        showToast('error', 'Invalid connection parameters format. Must be valid JSON.');
        return;
      }
      
      // Additional validation based on connection type
      if (connectionType === 'serial') {
        if (!connectionParams.port) {
          showToast('warning', 'No port specified for serial connection. Auto-discovery will be used.');
        } else {
          // Basic port format validation
          const validPortPattern = /^(COM\d+|\/dev\/tty\w+|\/dev\/cu\.\w+)$/i;
          if (!validPortPattern.test(connectionParams.port)) {
            if (!confirm(`The port format "${connectionParams.port}" looks unusual. Continue anyway?`)) {
              return;
            }
          }
        }
      } else if (connectionType === 'tcp') {
        if (!connectionParams.host) {
          showToast('error', 'TCP connection requires a host parameter');
          return;
        }
      } else if (connectionType === 'ble') {
        if (!connectionParams.name && !connectionParams.address) {
          showToast('error', 'BLE connection requires a name or address parameter');
          return;
        }
      }
      
      // Show loading state
      const connectButton = document.getElementById('connect-btn');
      connectButton.disabled = true;
      connectButton.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Connecting...';
      
      // Set a connection timeout (30 seconds)
      const connectionTimeout = setTimeout(() => {
        if (connectButton.disabled) {
          connectButton.disabled = false;
          connectButton.innerHTML = '<i class="fas fa-plug me-2"></i>Connect';
          showToast('error', 'Connection attempt timed out. Please try again.');
        }
      }, 30000);
      
      try {
        const result = await this.mqttClient.connectMeshtasticDevice(
          connectionType,
          connectionParams,
          deviceId
        );
        
        // Clear the timeout
        clearTimeout(connectionTimeout);
        
        // Reset button state
        connectButton.disabled = false;
        connectButton.innerHTML = '<i class="fas fa-plug me-2"></i>Connect';
        
        if (result.success === false) {
          // Check for specific error types
          const errorMsg = result.error || 'Unknown error';
          
          if (errorMsg.toLowerCase().includes('permission') || errorMsg.toLowerCase().includes('access denied')) {
            showToast('error', 'Permission error: Cannot access device. You may need to run with higher privileges.');
          } else if (errorMsg.toLowerCase().includes('timeout')) {
            showToast('error', 'Connection timed out. The device might not be responding or might be busy.');
          } else if (errorMsg.toLowerCase().includes('not found') || errorMsg.toLowerCase().includes('no such file')) {
            showToast('error', 'Device not found. Please check your connection parameters and ensure the device is connected.');
          } else if (errorMsg.toLowerCase().includes('in use') || errorMsg.toLowerCase().includes('busy')) {
            showToast('error', 'Device is in use by another application. Please close other applications that might be using it.');
          } else if (errorMsg.toLowerCase().includes('authentication')) {
            showToast('error', 'Authentication required. Please set a valid API key.');
            // Show API key modal
            const apiKeyModal = new bootstrap.Modal(document.getElementById('apiKeyModal'));
            apiKeyModal.show();
          } else {
            showToast('error', `Connection failed: ${errorMsg}`);
          }
          return;
        }
        
        // Show success message
        showToast('success', 'Device connected successfully');
        
        // Reload device list
        await this.loadConnectedDevices();
        
        // Reset form
        document.getElementById('connect-device-form').reset();
      } catch (error) {
        // Clear the timeout
        clearTimeout(connectionTimeout);
        
        console.error('Error connecting to device:', error);
        
        // Classify the error and show appropriate message
        let errorMsg;
        if (error.name === 'TypeError' && error.message.includes('Failed to fetch')) {
          errorMsg = 'Network error: Cannot connect to the API server';
        } else if (error.name === 'AbortError') {
          errorMsg = 'Connection request timed out';
        } else {
          errorMsg = error.message || 'Unknown error occurred';
        }
        
        showToast('error', `Failed to connect to device: ${errorMsg}`);
        
        // Reset button state
        connectButton.disabled = false;
        connectButton.innerHTML = '<i class="fas fa-plug me-2"></i>Connect';
      }
    } catch (error) {
      console.error('Unexpected error in connectDevice:', error);
      showToast('error', 'An unexpected error occurred');
      
      // Reset button state
      const connectButton = document.getElementById('connect-btn');
      if (connectButton) {
        connectButton.disabled = false;
        connectButton.innerHTML = '<i class="fas fa-plug me-2"></i>Connect';
      }
    }
  }
  
  /**
   * Disconnect from a device
   * @param {string} deviceId - Device ID to disconnect
   */
  async disconnectDevice(deviceId) {
    try {
      const confirmed = confirm(`Are you sure you want to disconnect device ${deviceId}?`);
      if (!confirmed) return;
      
      const result = await this.mqttClient.disconnectMeshtasticDevice(deviceId);
      
      if (result.success === false) {
        showToast('error', `Disconnection failed: ${result.error}`);
        return;
      }
      
      // Show success message
      showToast('success', 'Device disconnected successfully');
      
      // Reload device list
      await this.loadConnectedDevices();
    } catch (error) {
      console.error('Error disconnecting device:', error);
      showToast('error', 'Failed to disconnect device');
    }
  }
  
  /**
   * Show device configuration
   * @param {string} deviceId - Device ID
   */
  async showDeviceConfig(deviceId) {
    try {
      // Redirect to device config page or show modal
      window.location.href = `device-config.html?deviceId=${deviceId}`;
    } catch (error) {
      console.error('Error showing device configuration:', error);
      showToast('error', 'Failed to show device configuration');
    }
  }
  
  /**
   * Update device configuration
   */
  async updateDeviceConfig() {
    try {
      // This would be implemented on the device config page
      const key = document.getElementById('config-key').value;
      const value = document.getElementById('config-value').value;
      const deviceId = new URLSearchParams(window.location.search).get('deviceId');
      
      if (!key || !value) {
        showToast('error', 'Please provide key and value');
        return;
      }
      
      // Show loading state
      document.getElementById('update-config-btn').disabled = true;
      document.getElementById('update-config-btn').innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Updating...';
      
      const result = await this.mqttClient.setMeshtasticDeviceConfig(key, value, deviceId);
      
      // Reset button state
      document.getElementById('update-config-btn').disabled = false;
      document.getElementById('update-config-btn').innerHTML = 'Update Configuration';
      
      if (result.success === false) {
        showToast('error', `Update failed: ${result.error}`);
        return;
      }
      
      // Show success message
      showToast('success', 'Configuration updated successfully');
    } catch (error) {
      console.error('Error updating device configuration:', error);
      showToast('error', 'Failed to update device configuration');
      
      // Reset button state
      document.getElementById('update-config-btn').disabled = false;
      document.getElementById('update-config-btn').innerHTML = 'Update Configuration';
    }
  }
  
  /**
   * Update device channel
   */
  async updateDeviceChannel() {
    try {
      // This would be implemented on the device config page
      const settingsStr = document.getElementById('channel-settings').value;
      const channelIndex = parseInt(document.getElementById('channel-index').value) || 0;
      const deviceId = new URLSearchParams(window.location.search).get('deviceId');
      
      if (!settingsStr) {
        showToast('error', 'Please provide channel settings');
        return;
      }
      
      let settings;
      try {
        settings = JSON.parse(settingsStr);
      } catch (error) {
        showToast('error', 'Invalid channel settings format');
        return;
      }
      
      // Show loading state
      document.getElementById('update-channel-btn').disabled = true;
      document.getElementById('update-channel-btn').innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Updating...';
      
      const result = await this.mqttClient.setMeshtasticDeviceChannel(settings, channelIndex, deviceId);
      
      // Reset button state
      document.getElementById('update-channel-btn').disabled = false;
      document.getElementById('update-channel-btn').innerHTML = 'Update Channel';
      
      if (result.success === false) {
        showToast('error', `Update failed: ${result.error}`);
        return;
      }
      
      // Show success message
      showToast('success', 'Channel updated successfully');
    } catch (error) {
      console.error('Error updating device channel:', error);
      showToast('error', 'Failed to update device channel');
      
      // Reset button state
      document.getElementById('update-channel-btn').disabled = false;
      document.getElementById('update-channel-btn').innerHTML = 'Update Channel';
    }
  }
  
  /**
   * Initialize the device manager
   * This method is called when the page loads
   */
  async init() {
    console.log('Initializing device manager');
    try {
      // Load connected devices
      await this.loadConnectedDevices();
      
      // Check if API key is set and show appropriate status
      const connectionStatus = document.getElementById('connection-status');
      if (connectionStatus) {
        if (this.mqttClient.getApiKey()) {
          connectionStatus.innerHTML = '<i class="fas fa-circle text-success"></i> API Key Set';
        } else {
          connectionStatus.innerHTML = '<i class="fas fa-circle text-danger"></i> API Key Required';
        }
      }
      
      console.log('Device manager initialized successfully');
    } catch (error) {
      console.error('Error initializing device manager:', error);
      // Show error message
      if (this.deviceListElement) {
        this.deviceListElement.innerHTML = `
          <div class="list-group-item text-center text-danger">
            <i class="fas fa-exclamation-triangle me-2"></i>
            Initialization error
          </div>
          <div class="list-group-item">
            <p class="mb-2">Error: ${error.message || 'Unknown error'}</p>
            <button class="btn btn-sm btn-primary w-100" onclick="location.reload()">
              <i class="fas fa-sync me-2"></i>Reload Page
            </button>
          </div>
        `;
      }
    }
  }
  
  /**
   * Load device configuration
   */
  async loadDeviceConfig() {
    try {
      const deviceId = new URLSearchParams(window.location.search).get('deviceId');
      if (!deviceId) return;
      
      // Set device ID in page heading
      const deviceHeading = document.getElementById('device-heading');
      if (deviceHeading) {
        deviceHeading.textContent = `Device Configuration: ${deviceId}`;
      }
      
      // Load device config
      const config = await this.mqttClient.getMeshtasticDeviceConfig(deviceId);
      
      if (config.success === false) {
        showToast('error', `Failed to load configuration: ${config.error}`);
        return;
      }
      
      // Display config in a readable format
      const configDisplay = document.getElementById('config-display');
      if (configDisplay) {
        configDisplay.innerHTML = '';
        
        // Create a table for the config
        const table = document.createElement('table');
        table.className = 'table table-striped';
        table.innerHTML = `
          <thead>
            <tr>
              <th>Setting</th>
              <th>Value</th>
            </tr>
          </thead>
          <tbody id="config-table-body">
          </tbody>
        `;
        
        configDisplay.appendChild(table);
        
        const tableBody = document.getElementById('config-table-body');
        
        // Flatten the config object for easier display
        const flattenConfig = (obj, prefix = '') => {
          let result = {};
          
          for (const key in obj) {
            const propName = prefix ? `${prefix}.${key}` : key;
            
            if (typeof obj[key] === 'object' && obj[key] !== null && !Array.isArray(obj[key])) {
              // Recursively flatten nested objects
              Object.assign(result, flattenConfig(obj[key], propName));
            } else {
              // Add leaf properties
              result[propName] = obj[key];
            }
          }
          
          return result;
        };
        
        const flatConfig = flattenConfig(config);
        
        // Display config in table
        for (const [key, value] of Object.entries(flatConfig)) {
          const row = document.createElement('tr');
          
          // Format the value for display
          let displayValue = value;
          if (typeof value === 'object') {
            displayValue = JSON.stringify(value);
          } else if (typeof value === 'boolean') {
            displayValue = value ? 'Yes' : 'No';
          }
          
          row.innerHTML = `
            <td>${key}</td>
            <td>${displayValue}</td>
          `;
          
          tableBody.appendChild(row);
        }
      }
      
      // Load device channels
      const channels = await this.mqttClient.getMeshtasticDeviceChannels(deviceId);
      
      if (channels.success === false) {
        showToast('error', `Failed to load channels: ${channels.error}`);
        return;
      }
      
      // Display channels
      const channelDisplay = document.getElementById('channel-display');
      if (channelDisplay) {
        channelDisplay.innerHTML = '';
        
        if (channels.length === 0) {
          channelDisplay.innerHTML = '<div class="alert alert-info">No channels available</div>';
          return;
        }
        
        // Create tabs for each channel
        const tabsNav = document.createElement('ul');
        tabsNav.className = 'nav nav-tabs';
        tabsNav.setAttribute('role', 'tablist');
        
        const tabContent = document.createElement('div');
        tabContent.className = 'tab-content mt-3';
        
        channels.forEach((channel, index) => {
          // Create tab nav item
          const tabNavItem = document.createElement('li');
          tabNavItem.className = 'nav-item';
          tabNavItem.innerHTML = `
            <button class="nav-link ${index === 0 ? 'active' : ''}" 
                    id="channel-${index}-tab" 
                    data-bs-toggle="tab" 
                    data-bs-target="#channel-${index}-content" 
                    type="button" 
                    role="tab"
                    aria-controls="channel-${index}-content" 
                    aria-selected="${index === 0 ? 'true' : 'false'}">
              Channel ${index}
            </button>
          `;
          
          tabsNav.appendChild(tabNavItem);
          
          // Create tab content
          const tabPane = document.createElement('div');
          tabPane.className = `tab-pane fade ${index === 0 ? 'show active' : ''}`;
          tabPane.id = `channel-${index}-content`;
          tabPane.setAttribute('role', 'tabpanel');
          tabPane.setAttribute('aria-labelledby', `channel-${index}-tab`);
          
          // Format the channel data
          const channelData = JSON.stringify(channel, null, 2);
          
          tabPane.innerHTML = `
            <div class="card">
              <div class="card-body">
                <h5 class="card-title">Channel ${index} Settings</h5>
                <pre class="bg-light p-3 rounded"><code>${channelData}</code></pre>
                <button class="btn btn-sm btn-primary edit-channel-btn" data-channel-index="${index}">
                  Edit Channel
                </button>
              </div>
            </div>
          `;
          
          tabContent.appendChild(tabPane);
        });
        
        channelDisplay.appendChild(tabsNav);
        channelDisplay.appendChild(tabContent);
        
        // Add event listeners for edit channel buttons
        const editButtons = channelDisplay.querySelectorAll('.edit-channel-btn');
        editButtons.forEach(button => {
          button.addEventListener('click', () => {
            const channelIndex = button.getAttribute('data-channel-index');
            document.getElementById('channel-index').value = channelIndex;
            document.getElementById('channel-settings').value = JSON.stringify(channels[channelIndex], null, 2);
            
            // Scroll to the channel form
            this.channelFormElement.scrollIntoView({ behavior: 'smooth' });
          });
        });
      }
    } catch (error) {
      console.error('Error loading device configuration:', error);
      showToast('error', 'Failed to load device configuration');
    }
  }
}

/**
 * Show a toast notification
 * @param {string} type - Type of toast ('success', 'error', 'info', 'warning')
 * @param {string} message - Message to display
 */
function showToast(type, message) {
  const toastContainer = document.getElementById('toast-container');
  if (!toastContainer) return;
  
  const toast = document.createElement('div');
  toast.className = `toast align-items-center text-white bg-${type === 'error' ? 'danger' : type}`;
  toast.setAttribute('role', 'alert');
  toast.setAttribute('aria-live', 'assertive');
  toast.setAttribute('aria-atomic', 'true');
  
  toast.innerHTML = `
    <div class="d-flex">
      <div class="toast-body">
        ${message}
      </div>
      <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>
    </div>
  `;
  
  toastContainer.appendChild(toast);
  
  const bsToast = new bootstrap.Toast(toast);
  bsToast.show();
  
  // Remove toast after it's hidden
  toast.addEventListener('hidden.bs.toast', () => {
    toast.remove();
  });
} 
# Operations and Maintenance Guide for Meshtastic MQTT Bridge

## System Overview

The Meshtastic MQTT Bridge consists of several components:

1. **MQTT Broker** - Mosquitto MQTT broker handling message routing
2. **Backend API** - FastAPI application handling HTTP and WebSocket connections
3. **Meshtastic Integration** - Component for managing Meshtastic node data
4. **Message Queue** - System for handling message delivery with retry logic
5. **Frontend UI** - Web interface for visualization and interaction

## Setup and Installation

### Prerequisites

- Python 3.8 or higher
- Node.js 14 or higher (for frontend)
- Meshtastic compatible hardware (for testing with real devices)

### Installation

1. Clone the repository
2. Install backend dependencies: `pip install -r backend/requirements.txt`
3. Install frontend dependencies: `cd frontend && npm install`

For Windows systems, a setup script is provided:
```
setup.sh
```

## Regular Maintenance Tasks

### Database Maintenance

The system uses SQLite databases for message persistence and node data storage. Regular maintenance includes:

1. **Backup Database Files**

   ```bash
   # Backup databases
   mkdir -p backups/$(date +%Y-%m-%d)
   cp mqtt_data/meshtastic_nodes.db backups/$(date +%Y-%m-%d)/
   cp mqtt_messages.db backups/$(date +%Y-%m-%d)/
   ```

2. **Database Pruning**

   The system automatically prunes old messages, but you may want to manually vacuum the database:

   ```bash
   sqlite3 mqtt_data/meshtastic_nodes.db "VACUUM;"
   sqlite3 mqtt_messages.db "VACUUM;"
   ```

### Log Rotation

The system generates logs that should be rotated regularly:

1. **Configure Log Rotation**

   Create a logrotate configuration:

   ```bash
   # /etc/logrotate.d/meshtastic-mqtt
   /var/log/meshtastic-mqtt/*.log {
       daily
       rotate 7
       compress
       delaycompress
       missingok
       notifempty
       create 0640 meshtastic-mqtt adm
   }
   ```

2. **Manual Log Cleanup**

   ```bash
   # Remove logs older than 30 days
   find /var/log/meshtastic-mqtt -name "*.log" -type f -mtime +30 -delete
   ```

### Monitoring

1. **Check Service Status**

   ```bash
   # For systemd installations
   systemctl status MeshtasticMQTTBackend
   systemctl status MeshtasticMQTTFrontend
   
   # For Windows services
   sc query MeshtasticMQTTBackend
   sc query MeshtasticMQTTFrontend
   ```

2. **API Health Check**

   ```bash
   curl http://localhost:8000/broker_status
   ```

3. **Check MQTT Broker Status**

   ```bash
   mosquitto_sub -h localhost -t '$SYS/#' -v
   ```

4. **Check Node Status**

   Access the API endpoint to check node status:

   ```bash
   curl http://localhost:8000/meshtastic/stats
   ```

## Troubleshooting

### Common Issues

1. **MQTT Connection Failures**

   - Check MQTT broker is running: `systemctl status mosquitto`
   - Verify credentials in configuration
   - Check network connectivity
   - Verify ports are open (default 1883 for MQTT, 8883 for MQTT over TLS)

   Resolution:
   ```bash
   # Restart MQTT broker
   systemctl restart mosquitto
   
   # Check MQTT broker logs
   journalctl -u mosquitto -f
   ```

2. **Database Errors**

   - Check file permissions on database files
   - Ensure SQLite is installed
   - Verify disk space available

   Resolution:
   ```bash
   # Fix permissions
   chown -R meshtastic-mqtt:meshtastic-mqtt mqtt_data/
   chmod 644 mqtt_data/*.db
   
   # Check disk space
   df -h
   ```

3. **API Not Responding**

   - Check if the service is running
   - Verify port availability
   - Check for error logs

   Resolution:
   ```bash
   # Restart API service
   systemctl restart MeshtasticMQTTBackend
   
   # Check logs
   journalctl -u MeshtasticMQTTBackend -f
   ```

4. **Meshtastic Nodes Not Appearing**

   - Check MQTT topic subscriptions
   - Verify Meshtastic device is publishing to the correct topics
   - Check if the Meshtastic integration is properly initialized

   Resolution:
   ```bash
   # Monitor MQTT messages
   mosquitto_sub -h localhost -t 'msh/#' -v
   
   # Check integration logs
   grep "Meshtastic" /var/log/meshtastic-mqtt/backend.log
   ```

## Scaling and Performance

### Handling High Message Volume

1. **Adjust Queue Settings**

   Edit `.env` or configuration to increase queue capacity:

   ```
   MAX_QUEUE_SIZE=5000
   QUEUE_WORKER_COUNT=4
   ```

2. **Monitor Resources**

   Monitor CPU, memory, and disk usage during peak loads:

   ```bash
   # Install monitoring tools
   apt-get install sysstat
   
   # Monitor system resources
   vmstat 5
   iostat -x 5
   ```

3. **Database Optimization**

   For high message volume, consider these optimizations:

   ```sql
   -- Add indices for faster lookups
   sqlite3 mqtt_data/meshtastic_nodes.db "CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes(last_seen);"
   sqlite3 mqtt_data/meshtastic_nodes.db "CREATE INDEX IF NOT EXISTS idx_nodes_group ON nodes(group_name);"
   ```

## Backup and Restore

### Backup Procedure

1. **Full System Backup**

   ```bash
   # Create backup directory
   mkdir -p /backup/meshtastic-mqtt/$(date +%Y-%m-%d)
   
   # Backup configuration
   cp .env /backup/meshtastic-mqtt/$(date +%Y-%m-%d)/
   
   # Backup databases
   cp -r mqtt_data/ /backup/meshtastic-mqtt/$(date +%Y-%m-%d)/
   cp mqtt_messages.db /backup/meshtastic-mqtt/$(date +%Y-%m-%d)/
   
   # Backup configurations
   cp -r mosquitto/ /backup/meshtastic-mqtt/$(date +%Y-%m-%d)/
   ```

2. **Automated Backup Script**

   Create a script for automated backups:

   ```bash
   #!/bin/bash
   # backup_meshtastic_mqtt.sh
   BACKUP_DIR="/backup/meshtastic-mqtt/$(date +%Y-%m-%d)"
   mkdir -p $BACKUP_DIR
   
   # Stop services
   systemctl stop MeshtasticMQTTBackend
   systemctl stop MeshtasticMQTTFrontend
   
   # Backup files
   cp -r /path/to/meshtastic-mqtt/* $BACKUP_DIR/
   
   # Start services
   systemctl start MeshtasticMQTTBackend
   systemctl start MeshtasticMQTTFrontend
   
   # Clean old backups (keep last 7 days)
   find /backup/meshtastic-mqtt/ -type d -mtime +7 -exec rm -rf {} \;
   ```

### Restore Procedure

1. **Full System Restore**

   ```bash
   # Stop services
   systemctl stop MeshtasticMQTTBackend
   systemctl stop MeshtasticMQTTFrontend
   
   # Restore configuration
   cp /backup/meshtastic-mqtt/2023-01-01/.env /path/to/meshtastic-mqtt/
   
   # Restore databases
   cp -r /backup/meshtastic-mqtt/2023-01-01/mqtt_data/ /path/to/meshtastic-mqtt/
   cp /backup/meshtastic-mqtt/2023-01-01/mqtt_messages.db /path/to/meshtastic-mqtt/
   
   # Restore configurations
   cp -r /backup/meshtastic-mqtt/2023-01-01/mosquitto/ /path/to/meshtastic-mqtt/
   
   # Start services
   systemctl start MeshtasticMQTTBackend
   systemctl start MeshtasticMQTTFrontend
   ```

## Security Hardening

1. **Enable TLS for MQTT**

   Update Mosquitto configuration (`mosquitto/mosquitto.conf`):

   ```
   # TLS Configuration
   listener 8883
   cafile /etc/mosquitto/ca_certificates/ca.crt
   certfile /etc/mosquitto/certs/server.crt
   keyfile /etc/mosquitto/certs/server.key
   ```

2. **Configure Authentication**

   Set up user authentication in Mosquitto:

   ```bash
   # Create password file
   mosquitto_passwd -c /etc/mosquitto/passwd meshtastic
   
   # Update configuration
   echo "password_file /etc/mosquitto/passwd" >> /etc/mosquitto/mosquitto.conf
   ```

3. **Set Up Firewall Rules**

   ```bash
   # Allow only necessary ports
   ufw allow 8000/tcp  # API
   ufw allow 5000/tcp  # Frontend
   ufw allow 1883/tcp  # MQTT
   ufw allow 8883/tcp  # MQTT over TLS
   ```

4. **Regular Security Updates**

   ```bash
   # Update system
   apt update && apt upgrade -y
   
   # Update Python dependencies
   pip install --upgrade -r backend/requirements.txt
   
   # Update Node.js dependencies
   cd frontend && npm update
   ```

## System Recovery

### Disaster Recovery

In case of catastrophic failure:

1. **Reinstall the System**

   Follow the installation steps to reinstall the system.

2. **Restore from Backup**

   Use the restoration procedure above to restore from the most recent backup.

3. **Verify Functionality**

   ```bash
   # Check services are running
   systemctl status MeshtasticMQTTBackend
   systemctl status MeshtasticMQTTFrontend
   
   # Check API connectivity
   curl http://localhost:8000/broker_status
   
   # Check MQTT connectivity
   mosquitto_sub -h localhost -t 'msh/#' -v -c 1
   ```

## Configuration Reference

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MQTT_BROKER_HOST` | MQTT broker hostname | `localhost` |
| `MQTT_BROKER_PORT` | MQTT broker port | `1883` |
| `MQTT_CLIENT_ID` | Client ID for MQTT connection | `meshtastic_mqtt_bridge` |
| `MQTT_USERNAME` | Username for MQTT authentication | `""` |
| `MQTT_PASSWORD` | Password for MQTT authentication | `""` |
| `MQTT_USE_TLS` | Enable TLS for MQTT connection | `false` |
| `MQTT_KEEPALIVE` | Keepalive interval in seconds | `60` |
| `MQTT_QOS` | Default QoS level for messages | `0` |
| `PERSISTENCE_ENABLED` | Enable message persistence | `true` |
| `PERSISTENCE_PATH` | Path for persisted messages | `mqtt_messages.db` |
| `MAX_QUEUE_SIZE` | Maximum message queue size | `1000` |
| `QUEUE_WORKER_COUNT` | Number of queue workers | `2` |
| `MAX_RETRIES` | Maximum retry attempts | `3` |
| `API_HOST` | Host to bind the API server | `0.0.0.0` |
| `API_PORT` | Port for the API server | `8000` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `CORS_ORIGINS` | Allowed CORS origins | `*` |
| `MESHTASTIC_DB_PATH` | Path to Meshtastic nodes database | `mqtt_data/meshtastic_nodes.db` |
| `NODE_EXPIRATION_SECONDS` | Node expiration time in seconds | `3600` |

## Upgrade Procedure

1. **Backup Current System**

   Follow the backup procedure above.

2. **Update Code**

   ```bash
   # Pull latest code
   git pull origin main
   
   # Install updated dependencies
   pip install -r backend/requirements.txt
   cd frontend && npm install
   ```

3. **Database Migrations**

   For major updates, database migrations may be necessary:

   ```bash
   # Check if migration script exists
   if [ -f "migrations/migrate.py" ]; then
     python migrations/migrate.py
   fi
   ```

4. **Restart Services**

   ```bash
   systemctl restart MeshtasticMQTTBackend
   systemctl restart MeshtasticMQTTFrontend
   ```

5. **Verify Upgrade**

   ```bash
   # Check API version
   curl http://localhost:8000/version
   
   # Check system functionality
   curl http://localhost:8000/broker_status
   ```

## Contact and Support

For issues, questions, or contributions:

- GitHub Repository: [GitHub Link]
- Documentation: [Docs Link]
- Issue Tracker: [Issues Link]

## License and Attribution

This project is licensed under [LICENSE].

Powered by Meshtastic™ https://meshtastic.org/ 
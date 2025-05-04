@echo on
:: Open a command prompt that remains open
cmd /k "echo Starting Mosquitto MQTT Broker...
:: Start Mosquitto service
net stop mosquitto
mosquitto -c "C:\0_repos\ML Stuff\meshBroker\mosquitto\mosquitto.conf" -v
net start mosquitto
pause

:: Print service status
sc query mosquitto | findstr "STATE""
pause
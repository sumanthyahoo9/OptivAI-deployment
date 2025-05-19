# Create and manage devices
from device_abstraction_opcua import DeviceFactory
from protocol_specific_clients.opcua_client import create_opcua_client

client = await create_opcua_client("opc.tcp://localhost:4840")
await client.connect()
motor = DeviceFactory.create_device('motor', 'MOT001', 'Chiller Motor', client)
pump = DeviceFactory.create_device('pump', 'PMP001', 'Cooling Pump', client)

# Read all parameters
motor_readings = await motor.read_all_parameters()
print(f"Motor speed: {motor_readings['speed'].value} RPM")

# Control operations with safety checks
await motor.set_speed(1800)  # Validates against temp/vibration limits
await pump.set_speed(75)     # Checks suction pressure before allowing high speed

# Monitor device health
status = motor.get_device_status()
if status['state'] == 'fault':
    print(f"Motor fault: {status['fault_code']}")
    for alarm_id, alarm in status['active_alarms'].items():
        print(f"Alarm: {alarm['message']} ({alarm['severity']})")
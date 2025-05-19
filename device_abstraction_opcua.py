"""
Industrial Device/Equipment Abstraction Layer
Provides high-level abstractions for common manufacturing equipment
Handles device-specific logic, state management, and control operations
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod

# Import our OPC-UA modules
from protocol_specific_clients.opcua_client import OPCUAClient, create_opcua_client

logger = logging.getLogger(__name__)

# ============================================================================
# Base Models and Enums
# ============================================================================

class DeviceState(str, Enum):
    """Common device states"""
    UNKNOWN = "unknown"
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    FAULT = "fault"
    MAINTENANCE = "maintenance"
    MANUAL = "manual"

class AlarmSeverity(str, Enum):
    """Alarm severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class DeviceAlarm:
    """Represents a device alarm"""
    alarm_id: str
    message: str
    severity: AlarmSeverity
    timestamp: datetime
    active: bool = True
    acknowledged: bool = False

@dataclass
class DeviceParameter:
    """Configuration for a device parameter"""
    node_id: str
    name: str
    unit: str = ""
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    deadband: float = 0.0
    read_only: bool = True

@dataclass
class DeviceReading:
    """Standardized device reading"""
    parameter_name: str
    value: Union[float, int, bool, str]
    unit: str
    timestamp: datetime
    quality: str = "Good"
    alarm_state: Optional[str] = None

# ============================================================================
# Base Device Class
# ============================================================================

class BaseDevice(ABC):
    """Abstract base class for all industrial devices"""
    
    def __init__(
        self,
        device_id: str,
        device_name: str,
        opcua_client: OPCUAClient,
        parameters: Dict[str, DeviceParameter]
    ):
        self.device_id = device_id
        self.device_name = device_name
        self.opcua_client = opcua_client
        self.parameters = parameters
        
        # Device state
        self.state = DeviceState.UNKNOWN
        self.last_state_change = datetime.utcnow()
        self.fault_code: Optional[str] = None
        
        # Readings cache
        self.current_readings: Dict[str, DeviceReading] = {}
        self.last_reading_time: Optional[datetime] = None
        
        # Alarms
        self.active_alarms: Dict[str, DeviceAlarm] = {}
        
        # Statistics
        self.stats = {
            'total_reads': 0,
            'successful_reads': 0,
            'total_writes': 0,
            'successful_writes': 0,
            'uptime_hours': 0.0,
            'fault_count': 0
        }
        
        self.logger = logging.getLogger(f"{__name__}.{device_name}")
    
    async def read_all_parameters(self) -> Dict[str, DeviceReading]:
        """Read all device parameters"""
        try:
            # Prepare node IDs for batch read
            node_ids = [param.node_id for param in self.parameters.values()]
            param_names = list(self.parameters.keys())
            
            # Read all values at once
            readings = await self.opcua_client.read_multiple_nodes(node_ids)
            
            # Process readings
            device_readings = {}
            for i, (param_name, opcua_reading) in enumerate(zip(param_names, readings)):
                if opcua_reading and opcua_reading.quality == "Good":
                    param = self.parameters[param_name]
                    
                    # Create device reading
                    reading = DeviceReading(
                        parameter_name=param_name,
                        value=opcua_reading.value,
                        unit=param.unit,
                        timestamp=opcua_reading.timestamp,
                        quality=opcua_reading.quality
                    )
                    
                    # Apply device-specific processing
                    reading = await self._process_reading(param_name, reading)
                    
                    device_readings[param_name] = reading
                    self.current_readings[param_name] = reading
                else:
                    self.logger.warning(f"Failed to read parameter: {param_name}")
            
            self.last_reading_time = datetime.utcnow()
            self.stats['total_reads'] += 1
            self.stats['successful_reads'] += len(device_readings)
            
            # Update device state based on readings
            await self._update_device_state(device_readings)
            
            return device_readings
            
        except Exception as e:
            self.logger.error(f"Failed to read device parameters: {e}")
            return {}
    
    async def read_parameter(self, parameter_name: str) -> Optional[DeviceReading]:
        """Read a single parameter"""
        try:
            if parameter_name not in self.parameters:
                self.logger.error(f"Unknown parameter: {parameter_name}")
                return None
            
            param = self.parameters[parameter_name]
            opcua_reading = await self.opcua_client.read_node(param.node_id)
            
            if opcua_reading and opcua_reading.quality == "Good":
                reading = DeviceReading(
                    parameter_name=parameter_name,
                    value=opcua_reading.value,
                    unit=param.unit,
                    timestamp=opcua_reading.timestamp,
                    quality=opcua_reading.quality
                )
                
                # Apply device-specific processing
                reading = await self._process_reading(parameter_name, reading)
                
                self.current_readings[parameter_name] = reading
                return reading
            
        except Exception as e:
            self.logger.error(f"Failed to read parameter {parameter_name}: {e}")
        
        return None
    
    async def write_parameter(self, parameter_name: str, value: Any) -> bool:
        """Write a value to a parameter"""
        try:
            if parameter_name not in self.parameters:
                self.logger.error(f"Unknown parameter: {parameter_name}")
                return False
            
            param = self.parameters[parameter_name]
            
            if param.read_only:
                self.logger.error(f"Parameter {parameter_name} is read-only")
                return False
            
            # Validate value
            if not await self._validate_write_value(parameter_name, value):
                return False
            
            # Apply device-specific value processing
            processed_value = await self._process_write_value(parameter_name, value)
            
            # Write to OPC-UA
            result = await self.opcua_client.write_node(param.node_id, processed_value)
            
            self.stats['total_writes'] += 1
            if result.success:
                self.stats['successful_writes'] += 1
                self.logger.info(f"Successfully wrote {parameter_name} = {value}")
                return True
            else:
                self.logger.error(f"Failed to write {parameter_name}: {result.error_message}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to write parameter {parameter_name}: {e}")
            return False
    
    async def _validate_write_value(self, parameter_name: str, value: Any) -> bool:
        """Validate a write value against parameter constraints"""
        param = self.parameters[parameter_name]
        
        # Check numeric ranges
        if isinstance(value, (int, float)):
            if param.min_value is not None and value < param.min_value:
                self.logger.error(f"Value {value} below minimum {param.min_value}")
                return False
            if param.max_value is not None and value > param.max_value:
                self.logger.error(f"Value {value} above maximum {param.max_value}")
                return False
        
        # Device-specific validation
        return await self._validate_device_specific(parameter_name, value)
    
    @abstractmethod
    async def _process_reading(self, parameter_name: str, reading: DeviceReading) -> DeviceReading:
        """Process a reading (device-specific logic)"""
        pass
    
    @abstractmethod
    async def _process_write_value(self, parameter_name: str, value: Any) -> Any:
        """Process a write value (device-specific logic)"""
        pass
    
    @abstractmethod
    async def _validate_device_specific(self, parameter_name: str, value: Any) -> bool:
        """Device-specific validation logic"""
        pass
    
    @abstractmethod
    async def _update_device_state(self, readings: Dict[str, DeviceReading]):
        """Update device state based on readings"""
        pass
    
    @abstractmethod
    async def start(self) -> bool:
        """Start the device"""
        pass
    
    @abstractmethod
    async def stop(self) -> bool:
        """Stop the device"""
        pass
    
    def get_device_status(self) -> Dict[str, Any]:
        """Get comprehensive device status"""
        return {
            'device_id': self.device_id,
            'device_name': self.device_name,
            'state': self.state.value,
            'last_state_change': self.last_state_change.isoformat(),
            'fault_code': self.fault_code,
            'last_reading_time': self.last_reading_time.isoformat() if self.last_reading_time else None,
            'active_alarms': {k: {
                'message': v.message,
                'severity': v.severity.value,
                'timestamp': v.timestamp.isoformat(),
                'acknowledged': v.acknowledged
            } for k, v in self.active_alarms.items()},
            'current_readings': {k: {
                'value': v.value,
                'unit': v.unit,
                'timestamp': v.timestamp.isoformat(),
                'quality': v.quality
            } for k, v in self.current_readings.items()},
            'statistics': self.stats
        }

# ============================================================================
# Motor Device
# ============================================================================

class Motor(BaseDevice):
    """Represents an industrial motor"""
    
    def __init__(self, device_id: str, device_name: str, opcua_client: OPCUAClient):
        # Define motor-specific parameters
        parameters = {
            'speed': DeviceParameter("ns=2;i=1001", "Speed", "RPM", 0, 3600, deadband=1.0),
            'current': DeviceParameter("ns=2;i=1002", "Current", "A", 0, 100, deadband=0.1),
            'torque': DeviceParameter("ns=2;i=1003", "Torque", "Nm", 0, 1000),
            'temperature': DeviceParameter("ns=2;i=1004", "Temperature", "°C", -10, 120),
            'vibration': DeviceParameter("ns=2;i=1005", "Vibration", "mm/s", 0, 50),
            'status': DeviceParameter("ns=2;i=1006", "Status", "", read_only=True),
            'setpoint': DeviceParameter("ns=2;i=1007", "Speed Setpoint", "RPM", 0, 3600, read_only=False),
            'enable': DeviceParameter("ns=2;i=1008", "Enable", "", read_only=False)
        }
        
        super().__init__(device_id, device_name, opcua_client, parameters)
        
        # Motor-specific attributes
        self.rated_power = 100.0  # kW
        self.efficiency = 0.95
        self.power_factor = 0.85
    
    async def _process_reading(self, parameter_name: str, reading: DeviceReading) -> DeviceReading:
        """Process motor-specific readings"""
        # Calculate power consumption
        if parameter_name == 'current':
            voltage = 400  # V (could be another parameter)
            power = (reading.value * voltage * self.power_factor * 1.732) / 1000  # kW
            # You could add this as a calculated parameter
        
        # Check for alarm conditions
        if parameter_name == 'temperature' and reading.value > 100:
            alarm = DeviceAlarm(
                alarm_id=f"{self.device_id}_temp_high",
                message=f"Motor temperature high: {reading.value}°C",
                severity=AlarmSeverity.HIGH,
                timestamp=reading.timestamp
            )
            self.active_alarms[alarm.alarm_id] = alarm
            reading.alarm_state = "high_temperature"
        
        if parameter_name == 'vibration' and reading.value > 30:
            alarm = DeviceAlarm(
                alarm_id=f"{self.device_id}_vib_high",
                message=f"Motor vibration high: {reading.value}mm/s",
                severity=AlarmSeverity.MEDIUM,
                timestamp=reading.timestamp
            )
            self.active_alarms[alarm.alarm_id] = alarm
            reading.alarm_state = "high_vibration"
        
        return reading
    
    async def _process_write_value(self, parameter_name: str, value: Any) -> Any:
        """Process motor write values"""
        # Apply ramping for speed setpoint changes
        if parameter_name == 'setpoint':
            current_speed = self.current_readings.get('speed')
            if current_speed:
                speed_diff = abs(value - current_speed.value)
                if speed_diff > 300:  # Large change, apply ramping
                    self.logger.info(f"Applying ramping for large speed change: {speed_diff} RPM")
                    # In real implementation, you might implement gradual ramping
        
        return value
    
    async def _validate_device_specific(self, parameter_name: str, value: Any) -> bool:
        """Motor-specific validation"""
        if parameter_name == 'setpoint':
            # Don't allow speed changes if motor is in fault
            if self.state == DeviceState.FAULT:
                self.logger.error("Cannot change setpoint while motor is in fault")
                return False
            
            # Check for safe operating range based on current conditions
            temp_reading = self.current_readings.get('temperature')
            if temp_reading and temp_reading.value > 90 and value > 1800:
                self.logger.error("High speed not allowed while temperature is elevated")
                return False
        
        return True
    
    async def _update_device_state(self, readings: Dict[str, DeviceReading]):
        """Update motor state based on readings"""
        old_state = self.state
        
        status_reading = readings.get('status')
        speed_reading = readings.get('speed')
        
        if status_reading:
            # Map status codes to device states
            status_value = status_reading.value
            if status_value == 0:
                self.state = DeviceState.STOPPED
                self.fault_code = None
            elif status_value == 1:
                self.state = DeviceState.RUNNING
                self.fault_code = None
            elif status_value == 2:
                self.state = DeviceState.STARTING
            elif status_value == 3:
                self.state = DeviceState.STOPPING
            elif status_value >= 100:  # Fault codes
                self.state = DeviceState.FAULT
                self.fault_code = f"Error_{status_value}"
                self.stats['fault_count'] += 1
        
        # Additional state logic based on speed
        if speed_reading:
            if self.state == DeviceState.RUNNING and speed_reading.value < 10:
                self.state = DeviceState.STOPPED
        
        # Track state changes
        if old_state != self.state:
            self.last_state_change = datetime.utcnow()
            self.logger.info(f"Motor state changed: {old_state.value} -> {self.state.value}")
    
    async def start(self) -> bool:
        """Start the motor"""
        try:
            self.logger.info("Starting motor...")
            
            # Check if already running
            if self.state == DeviceState.RUNNING:
                self.logger.info("Motor already running")
                return True
            
            # Check for fault conditions
            if self.state == DeviceState.FAULT:
                self.logger.error("Cannot start motor in fault state")
                return False
            
            # Enable the motor
            success = await self.write_parameter('enable', True)
            if success:
                # Wait for state change
                await asyncio.sleep(2)
                await self.read_all_parameters()
                
                if self.state in [DeviceState.STARTING, DeviceState.RUNNING]:
                    self.logger.info("Motor start command successful")
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to start motor: {e}")
            return False
    
    async def stop(self) -> bool:
        """Stop the motor"""
        try:
            self.logger.info("Stopping motor...")
            
            # Check if already stopped
            if self.state == DeviceState.STOPPED:
                self.logger.info("Motor already stopped")
                return True
            
            # Disable the motor
            success = await self.write_parameter('enable', False)
            if success:
                # Wait for state change
                await asyncio.sleep(2)
                await self.read_all_parameters()
                
                if self.state in [DeviceState.STOPPING, DeviceState.STOPPED]:
                    self.logger.info("Motor stop command successful")
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to stop motor: {e}")
            return False
    
    async def set_speed(self, speed_rpm: float) -> bool:
        """Set motor speed setpoint"""
        return await self.write_parameter('setpoint', speed_rpm)
    
    def get_efficiency(self) -> Optional[float]:
        """Calculate current motor efficiency"""
        speed_reading = self.current_readings.get('speed')
        current_reading = self.current_readings.get('current')
        
        if speed_reading and current_reading and speed_reading.value > 0:
            # Simplified efficiency calculation
            load_factor = current_reading.value / 80.0  # Assuming 80A full load
            # Efficiency curve (simplified)
            efficiency = 0.95 * (1 - 0.1 * (1 - load_factor)**2)
            return min(efficiency, 0.95)
        
        return None

# ============================================================================
# Pump Device
# ============================================================================

class Pump(BaseDevice):
    """Represents an industrial pump"""
    
    def __init__(self, device_id: str, device_name: str, opcua_client: OPCUAClient):
        parameters = {
            'flow_rate': DeviceParameter("ns=2;i=2001", "Flow Rate", "m³/h", 0, 500),
            'pressure': DeviceParameter("ns=2;i=2002", "Pressure", "bar", 0, 20),
            'suction_pressure': DeviceParameter("ns=2;i=2003", "Suction Pressure", "bar", -1, 5),
            'power': DeviceParameter("ns=2;i=2004", "Power", "kW", 0, 200),
            'temperature': DeviceParameter("ns=2;i=2005", "Temperature", "°C"),
            'status': DeviceParameter("ns=2;i=2006", "Status", "", read_only=True),
            'speed_setpoint': DeviceParameter("ns=2;i=2007", "Speed Setpoint", "%", 0, 100, read_only=False),
            'enable': DeviceParameter("ns=2;i=2008", "Enable", "", read_only=False)
        }
        
        super().__init__(device_id, device_name, opcua_client, parameters)
        
        # Pump-specific attributes
        self.rated_flow = 400.0  # m³/h
        self.rated_head = 15.0   # bar
    
    async def _process_reading(self, parameter_name: str, reading: DeviceReading) -> DeviceReading:
        """Process pump-specific readings"""
        # Check for cavitation (low suction pressure)
        if parameter_name == 'suction_pressure' and reading.value < 0.5:
            alarm = DeviceAlarm(
                alarm_id=f"{self.device_id}_cavitation",
                message=f"Pump cavitation risk: suction pressure {reading.value} bar",
                severity=AlarmSeverity.HIGH,
                timestamp=reading.timestamp
            )
            self.active_alarms[alarm.alarm_id] = alarm
            reading.alarm_state = "cavitation_risk"
        
        # Check for dry running (no flow but pump running)
        if parameter_name == 'flow_rate':
            speed_reading = self.current_readings.get('speed_setpoint')
            if (speed_reading and speed_reading.value > 30 and 
                reading.value < 10 and self.state == DeviceState.RUNNING):
                alarm = DeviceAlarm(
                    alarm_id=f"{self.device_id}_dry_run",
                    message="Pump dry running detected",
                    severity=AlarmSeverity.CRITICAL,
                    timestamp=reading.timestamp
                )
                self.active_alarms[alarm.alarm_id] = alarm
                reading.alarm_state = "dry_running"
        
        return reading
    
    async def _process_write_value(self, parameter_name: str, value: Any) -> Any:
        """Process pump write values"""
        # Apply soft start for speed changes
        if parameter_name == 'speed_setpoint':
            current_setpoint = self.current_readings.get('speed_setpoint')
            if current_setpoint and abs(value - current_setpoint.value) > 50:
                self.logger.info("Applying soft start for large speed change")
        
        return value
    
    async def _validate_device_specific(self, parameter_name: str, value: Any) -> bool:
        """Pump-specific validation"""
        if parameter_name == 'speed_setpoint':
            # Don't allow high speed if suction pressure is low
            suction_reading = self.current_readings.get('suction_pressure')
            if suction_reading and suction_reading.value < 1.0 and value > 50:
                self.logger.error("High speed not allowed with low suction pressure")
                return False
        
        return True
    
    async def _update_device_state(self, readings: Dict[str, DeviceReading]):
        """Update pump state based on readings"""
        old_state = self.state
        
        status_reading = readings.get('status')
        flow_reading = readings.get('flow_rate')
        
        if status_reading:
            status_value = status_reading.value
            if status_value == 0:
                self.state = DeviceState.STOPPED
            elif status_value == 1:
                self.state = DeviceState.RUNNING
            elif status_value >= 100:
                self.state = DeviceState.FAULT
                self.fault_code = f"Pump_Error_{status_value}"
        
        # Check for dry running fault
        if (self.state == DeviceState.RUNNING and flow_reading and 
            flow_reading.value < 5):
            # Could transition to fault state if dry running persists
            pass
        
        if old_state != self.state:
            self.last_state_change = datetime.utcnow()
            self.logger.info(f"Pump state changed: {old_state.value} -> {self.state.value}")
    
    async def start(self) -> bool:
        """Start the pump"""
        try:
            # Check suction pressure before starting
            suction_reading = await self.read_parameter('suction_pressure')
            if suction_reading and suction_reading.value < 0.5:
                self.logger.error("Cannot start pump: insufficient suction pressure")
                return False
            
            return await self.write_parameter('enable', True)
        except Exception as e:
            self.logger.error(f"Failed to start pump: {e}")
            return False
    
    async def stop(self) -> bool:
        """Stop the pump"""
        try:
            return await self.write_parameter('enable', False)
        except Exception as e:
            self.logger.error(f"Failed to stop pump: {e}")
            return False
    
    async def set_speed(self, speed_percent: float) -> bool:
        """Set pump speed as percentage"""
        return await self.write_parameter('speed_setpoint', speed_percent)

# ============================================================================
# Device Factory
# ============================================================================

class DeviceFactory:
    """Factory for creating device instances"""
    
    device_types = {
        'motor': Motor,
        'pump': Pump,
        # Add more device types as needed
    }
    
    @classmethod
    def create_device(
        self,
        device_type: str,
        device_id: str,
        device_name: str,
        opcua_client: OPCUAClient,
        **kwargs
    ) -> Optional[BaseDevice]:
        """Create a device instance"""
        if device_type not in self.device_types:
            logger.error(f"Unknown device type: {device_type}")
            return None
        
        device_class = self.device_types[device_type]
        return device_class(device_id, device_name, opcua_client, **kwargs)

# ============================================================================
# Device Manager
# ============================================================================

class DeviceManager:
    """Manages multiple devices"""
    
    def __init__(self, opcua_client: OPCUAClient):
        self.opcua_client = opcua_client
        self.devices: Dict[str, BaseDevice] = {}
        self.logger = logging.getLogger(__name__ + ".Manager")
    
    def add_device(self, device: BaseDevice):
        """Add a device to management"""
        self.devices[device.device_id] = device
        self.logger.info(f"Added device: {device.device_name}")
    
    def remove_device(self, device_id: str):
        """Remove a device from management"""
        if device_id in self.devices:
            device = self.devices.pop(device_id)
            self.logger.info(f"Removed device: {device.device_name}")
    
    async def read_all_devices(self) -> Dict[str, Dict[str, DeviceReading]]:
        """Read all parameters from all devices"""
        results = {}
        
        for device_id, device in self.devices.items():
            try:
                readings = await device.read_all_parameters()
                results[device_id] = readings
            except Exception as e:
                self.logger.error(f"Failed to read device {device_id}: {e}")
                results[device_id] = {}
        
        return results
    
    async def start_all_devices(self) -> Dict[str, bool]:
        """Start all devices"""
        results = {}
        
        for device_id, device in self.devices.items():
            try:
                success = await device.start()
                results[device_id] = success
            except Exception as e:
                self.logger.error(f"Failed to start device {device_id}: {e}")
                results[device_id] = False
        
        return results
    
    async def stop_all_devices(self) -> Dict[str, bool]:
        """Stop all devices"""
        results = {}
        
        for device_id, device in self.devices.items():
            try:
                success = await device.stop()
                results[device_id] = success
            except Exception as e:
                self.logger.error(f"Failed to stop device {device_id}: {e}")
                results[device_id] = False
        
        return results
    
    def get_device_statuses(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all devices"""
        return {
            device_id: device.get_device_status()
            for device_id, device in self.devices.items()
        }
    
    def get_device(self, device_id: str) -> Optional[BaseDevice]:
        """Get a specific device"""
        return self.devices.get(device_id)

# ============================================================================
# Example Usage
# ============================================================================

async def main():
    """Example usage of the device abstraction layer"""
    
    client = await create_opcua_client("opc.tcp://localhost:4840")
    await client.connect()
    
    try:
        # Create device manager
        device_manager = DeviceManager(client)
        
        # Create devices
        motor1 = DeviceFactory.create_device(
            'motor', 'MOT001', 'Compressor Motor', client
        )
        pump1 = DeviceFactory.create_device(
            'pump', 'PMP001', 'Cooling Water Pump', client
        )
        
        # Add to manager
        device_manager.add_device(motor1)
        device_manager.add_device(pump1)
        
        # Read all devices
        all_readings = await device_manager.read_all_devices()
        print(f"Device readings: {all_readings}")
        
        # Control devices
        await motor1.set_speed(1800)
        await pump1.set_speed(75)
        
        # Get device statuses
        statuses = device_manager.get_device_statuses()
        print(f"Device statuses: {statuses}")
        
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
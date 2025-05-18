"""
Data models define the contract between all components
They establish how sensor data, control commands, and system status are structured
Once you have models, everything else (storage, queuing, API) follows naturally
Validate the models against real building data early
"""
from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field, validator
from uuid import UUID, uuid4


# Enums for standardized values
class ProtocolType(str, Enum):
    """
    Protocol Client Type
    """
    BACNET = "bacnet"
    MODBUS = "modbus"
    OPC_UA = "opc_ua"
    PROPRIETARY = "proprietary"
    DIRECT_SENSOR = "direct_sensor"


class DeviceType(str, Enum):
    """
    Device Type
    """
    SENSOR = "sensor"
    ACTUATOR = "actuator"
    CONTROLLER = "controller"
    BMS = "bms"
    HVAC_UNIT = "hvac_unit"


class DataType(str, Enum):
    """
    Data type
    """
    TEMPERATURE = "temperature"
    HUMIDITY = "humidity"
    PRESSURE = "pressure"
    CO2 = "co2"
    OCCUPANCY = "occupancy"
    SETPOINT = "setpoint"
    VALVE_POSITION = "valve_position"
    FAN_SPEED = "fan_speed"
    FLOW_RATE = "flow_rate"
    POWER = "power"
    STATUS = "status"


class DataQuality(str, Enum):
    GOOD = "good"
    UNCERTAIN = "uncertain"
    BAD = "bad"
    MAINTENANCE = "maintenance"
    OFFLINE = "offline"


class CommandType(str, Enum):
    """
    Command type for the RL Agent
    """
    SET_SETPOINT = "set_setpoint"
    OVERRIDE = "override"
    RESET = "reset"
    SCHEDULE = "schedule"
    EMERGENCY_STOP = "emergency_stop"


# Base Models
class TimestampedData(BaseModel):
    """Base model for all timestamped data"""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source_device_id: str
    quality: DataQuality = DataQuality.GOOD


class DeviceIdentifier(BaseModel):
    """Standardized device identification"""
    device_id: str = Field(..., description="Unique device identifier")
    device_name: Optional[str] = Field(None, description="Human-readable device name")
    device_type: DeviceType
    protocol: ProtocolType
    location: Optional[str] = Field(None, description="Physical location (e.g., 'Floor 2, Zone A')")
    zone_id: Optional[str] = Field(None, description="Associated zone identifier")
    
    # Protocol-specific addressing
    bacnet_address: Optional[Dict[str, Any]] = Field(None, description="BACnet object identifier")
    modbus_address: Optional[Dict[str, Any]] = Field(None, description="Modbus register info")
    opc_address: Optional[str] = Field(None, description="OPC node identifier")


# Sensor Data Models
class SensorReading(TimestampedData):
    """Individual sensor reading"""
    data_type: DataType
    value: Union[float, int, bool, str]
    unit: Optional[str] = Field(None, description="Unit of measurement (e.g., 'celsius', 'percent')")
    min_value: Optional[float] = Field(None, description="Expected minimum value")
    max_value: Optional[float] = Field(None, description="Expected maximum value")
    
    @validator('value', pre=True)
    def validate_numeric_range(cls, v, values):
        """Validate numeric values are within expected range"""
        if isinstance(v, (int, float)):
            min_val = values.get('min_value')
            max_val = values.get('max_value')
            
            if min_val is not None and v < min_val:
                raise ValueError(f"Value {v} below minimum {min_val}")
            if max_val is not None and v > max_val:
                raise ValueError(f"Value {v} above maximum {max_val}")
        return v


class ZoneReading(BaseModel):
    """Aggregated readings for a specific zone"""
    zone_id: str
    zone_name: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    # Common zone measurements
    temperature: Optional[SensorReading] = None
    humidity: Optional[SensorReading] = None
    co2_level: Optional[SensorReading] = None
    occupancy_count: Optional[SensorReading] = None
    
    # Setpoints
    heating_setpoint: Optional[SensorReading] = None
    cooling_setpoint: Optional[SensorReading] = None
    
    # HVAC status
    hvac_status: Optional[SensorReading] = None
    fan_speed: Optional[SensorReading] = None
    
    # Additional sensor readings
    additional_sensors: Dict[str, SensorReading] = Field(default_factory=dict)


# Control Command Models
class ControlCommand(BaseModel):
    """Command to control a device"""
    command_id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    target_device_id: str
    command_type: CommandType
    
    # Command parameters
    target_value: Optional[Union[float, int, bool, str]] = None
    duration: Optional[int] = Field(None, description="Command duration in seconds")
    priority: int = Field(1, ge=1, le=10, description="Command priority (1=lowest, 10=highest)")
    
    # Source information
    source: str = Field("rl_agent", description="Source of the command")
    reason: Optional[str] = Field(None, description="Reason for the command")
    
    # Safety constraints
    safety_override: bool = Field(False, description="Whether this overrides safety systems")
    emergency: bool = Field(False, description="Emergency command flag")


class ControlResponse(BaseModel):
    """Response to a control command"""
    command_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    status: str = Field(..., regex="^(accepted|rejected|executing|completed|failed)$")
    message: Optional[str] = None
    actual_value: Optional[Union[float, int, bool, str]] = None
    error_code: Optional[str] = None


# System Health Models
class DeviceHealth(BaseModel):
    """Health status of a device"""
    device_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    online: bool = True
    last_communication: datetime
    communication_errors: int = 0
    data_quality_score: float = Field(1.0, ge=0.0, le=1.0)
    alerts: List[str] = Field(default_factory=list)


class SystemHealth(BaseModel):
    """Overall system health"""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    total_devices: int
    online_devices: int
    offline_devices: int
    devices_with_errors: int
    overall_health_score: float = Field(ge=0.0, le=1.0)
    critical_alerts: List[str] = Field(default_factory=list)


# RL Agent Integration Models
class ObservationSpace(BaseModel):
    """Current state observation for RL agent"""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    # Zone conditions (what the agent observes)
    zones: List[ZoneReading]
    
    # External conditions
    outdoor_temperature: Optional[float] = None
    outdoor_humidity: Optional[float] = None
    wind_speed: Optional[float] = None
    solar_radiation: Optional[float] = None
    
    # Time features
    hour_of_day: int = Field(ge=0, le=23)
    day_of_week: int = Field(ge=0, le=6)
    day_of_year: int = Field(ge=1, le=366)
    
    # System status
    total_power_consumption: Optional[float] = None
    system_health: Optional[float] = Field(None, ge=0.0, le=1.0)


class ActionSpace(BaseModel):
    """Actions the RL agent can take"""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    # Primary actions - setpoint adjustments
    heating_setpoint_adjustments: Dict[str, float] = Field(
        default_factory=dict, 
        description="Zone ID -> setpoint change"
    )
    cooling_setpoint_adjustments: Dict[str, float] = Field(
        default_factory=dict,
        description="Zone ID -> setpoint change"
    )
    
    # Secondary actions - direct control
    fan_speed_commands: Dict[str, float] = Field(
        default_factory=dict,
        description="Device ID -> fan speed (0-1)"
    )
    valve_position_commands: Dict[str, float] = Field(
        default_factory=dict,
        description="Device ID -> valve position (0-1)"
    )
    
    # Meta information
    confidence: float = Field(1.0, ge=0.0, le=1.0, description="Agent's confidence in actions")
    exploration_mode: bool = Field(False, description="Whether agent is exploring")


# Configuration Models
class DeviceConfiguration(BaseModel):
    """Configuration for a building device"""
    device: DeviceIdentifier
    polling_interval: int = Field(60, gt=0, description="Polling interval in seconds")
    enabled: bool = True
    
    # Protocol-specific configuration
    protocol_config: Dict[str, Any] = Field(default_factory=dict)
    
    # Data validation rules
    validation_rules: Dict[str, Any] = Field(default_factory=dict)
    
    # Alert thresholds
    alert_thresholds: Dict[str, Dict[str, float]] = Field(default_factory=dict)


class ZoneConfiguration(BaseModel):
    """Configuration for a building zone"""
    zone_id: str
    zone_name: str
    zone_type: str = Field("office", description="Type of zone (office, conference, lobby, etc.)")
    
    # Physical properties
    floor_area: Optional[float] = Field(None, gt=0, description="Floor area in square meters")
    volume: Optional[float] = Field(None, gt=0, description="Volume in cubic meters")
    occupancy_capacity: Optional[int] = Field(None, gt=0)
    
    # Comfort settings
    comfort_temperature_range: Dict[str, float] = Field(
        default_factory=lambda: {"min": 20.0, "max": 26.0}
    )
    comfort_humidity_range: Dict[str, float] = Field(
        default_factory=lambda: {"min": 30.0, "max": 70.0}
    )
    
    # Associated devices
    temperature_sensors: List[str] = Field(default_factory=list)
    humidity_sensors: List[str] = Field(default_factory=list)
    thermostats: List[str] = Field(default_factory=list)
    hvac_units: List[str] = Field(default_factory=list)


class SystemConfiguration(BaseModel):
    """Overall system configuration"""
    system_id: str
    system_name: str
    building_name: Optional[str] = None
    
    # Zones and devices
    zones: List[ZoneConfiguration]
    devices: List[DeviceConfiguration]
    
    # Global settings
    global_polling_interval: int = Field(60, gt=0)
    data_retention_days: int = Field(365, gt=0)
    
    # RL agent settings
    rl_agent_enabled: bool = True
    rl_action_interval: int = Field(900, gt=0, description="RL action interval in seconds")
    
    # Safety settings
    safety_mode_enabled: bool = True
    emergency_override_enabled: bool = True


# Message Queue Models
class MessagePayload(BaseModel):
    """Standard message format for the queue"""
    message_id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    message_type: str = Field(..., description="Type of message (sensor_data, control_command, etc.)")
    source: str = Field(..., description="Source system/component")
    destination: Optional[str] = Field(None, description="Target system/component")
    priority: int = Field(1, ge=1, le=10)
    
    # Actual message content
    payload: Dict[str, Any] = Field(..., description="The actual message data")
    
    # Processing metadata
    retry_count: int = Field(0, ge=0)
    max_retries: int = Field(3, ge=0)
    expires_at: Optional[datetime] = None


# API Response Models
class StandardResponse(BaseModel):
    """Standard API response format"""
    success: bool
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    message: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    errors: List[str] = Field(default_factory=list)


class PaginatedResponse(StandardResponse):
    """Paginated response for large datasets"""
    total_count: int
    page: int = Field(ge=1)
    page_size: int = Field(ge=1, le=1000)
    total_pages: int
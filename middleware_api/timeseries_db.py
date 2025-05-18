"""
Complete Time Series Database Implementation for HVAC RL System
This module provides comprehensive time series data storage and retrieval
using InfluxDB for HVAC building control with RL agent integration.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Union, AsyncGenerator
from dataclasses import dataclass, asdict
from enum import Enum
import json
import numpy as np
import pandas as pd

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from influxdb_client.domain.write_precision import WritePrecision
from influxdb_client.client.flux_table import FluxTable
import aiohttp
from pydantic import BaseModel, Field
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# Data Models and Enums
# ============================================================================

class SensorType(str, Enum):
    """Types of sensors in the HVAC system"""
    TEMPERATURE = "temperature"
    HUMIDITY = "humidity"
    CO2 = "co2"
    PRESSURE = "pressure"
    FLOW_RATE = "flow_rate"
    POWER_CONSUMPTION = "power_consumption"
    OCCUPANCY = "occupancy"
    AIR_QUALITY = "air_quality"
    VIBRATION = "vibration"
    LIGHT_LEVEL = "light_level"

class DeviceStatus(str, Enum):
    """Status of HVAC devices"""
    ONLINE = "online"
    OFFLINE = "offline"
    MAINTENANCE = "maintenance"
    ERROR = "error"
    UNKNOWN = "unknown"

class ZoneType(str, Enum):
    """Types of building zones"""
    OFFICE = "office"
    CONFERENCE_ROOM = "conference_room"
    LOBBY = "lobby"
    CORRIDOR = "corridor"
    UTILITY = "utility"
    STORAGE = "storage"
    SERVER_ROOM = "server_room"

@dataclass
class SensorReading:
    """Individual sensor reading data"""
    sensor_id: str
    sensor_type: SensorType
    value: float
    unit: str
    zone_id: str
    timestamp: datetime = None
    device_id: Optional[str] = None
    quality: float = 1.0  # Data quality score 0-1
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()

@dataclass
class ZoneConditions:
    """Aggregated zone environmental conditions"""
    zone_id: str
    temperature: float
    humidity: float
    co2_level: Optional[float] = None
    occupancy_count: Optional[int] = None
    pressure: Optional[float] = None
    air_quality_index: Optional[float] = None
    timestamp: datetime = None
    comfort_score: Optional[float] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()

@dataclass
class DeviceHealth:
    """Device health and status information"""
    device_id: str
    device_type: str
    status: DeviceStatus
    uptime_hours: float
    error_count: int
    last_maintenance: Optional[datetime] = None
    performance_score: float = 1.0
    energy_efficiency: Optional[float] = None
    timestamp: datetime = None
    alerts: Optional[List[str]] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()

@dataclass
class RLObservation:
    """RL Agent observation data"""
    observation_id: str
    zone_id: str
    agent_id: str
    state_vector: List[float]
    reward: Optional[float] = None
    done: bool = False
    episode_id: Optional[str] = None
    step_number: Optional[int] = None
    timestamp: datetime = None
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()

@dataclass
class RLAction:
    """RL Agent action data"""
    action_id: str
    zone_id: str
    agent_id: str
    action_vector: List[float]
    action_type: str  # e.g., "setpoint_adjustment", "damper_control"
    confidence: float = 1.0
    episode_id: Optional[str] = None
    step_number: Optional[int] = None
    timestamp: datetime = None
    execution_result: Optional[str] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()

@dataclass
class EnergyConsumption:
    """Energy consumption data"""
    meter_id: str
    zone_id: Optional[str]
    device_id: Optional[str]
    consumption_kwh: float
    demand_kw: float
    cost_usd: Optional[float] = None
    tariff_period: Optional[str] = None
    efficiency_ratio: Optional[float] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()

# ============================================================================
# Time Series Store Implementation
# ============================================================================

class TimeSeriesStore:
    """InfluxDB-based time series data store for HVAC RL system"""
    
    def __init__(
        self,
        url: str,
        token: str,
        org: str,
        bucket: str,
        timeout: int = 10000,
        enable_gzip: bool = True
    ):
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.timeout = timeout
        self.enable_gzip = enable_gzip
        
        # Initialize InfluxDB client
        self.client = InfluxDBClient(
            url=url,
            token=token,
            org=org,
            timeout=timeout,
            enable_gzip=enable_gzip
        )
        
        self.write_api = self.client.write_api(write_options=ASYNCHRONOUS)
        self.query_api = self.client.query_api()
        self.delete_api = self.client.delete_api()
        
        logger.info(f"Initialized TimeSeriesStore for bucket: {bucket}")
    
    async def health_check(self) -> bool:
        """Check if InfluxDB is healthy and accessible"""
        try:
            health = self.client.health()
            return health.status == "pass"
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    # ========================================================================
    # Write Operations
    # ========================================================================
    
    async def write_sensor_data(self, reading: SensorReading) -> None:
        """Write sensor reading to time series database"""
        try:
            point = (
                Point("sensor_reading")
                .tag("sensor_id", reading.sensor_id)
                .tag("sensor_type", reading.sensor_type.value)
                .tag("zone_id", reading.zone_id)
                .tag("unit", reading.unit)
            )
            
            if reading.device_id:
                point = point.tag("device_id", reading.device_id)
            
            point = (
                point
                .field("value", reading.value)
                .field("quality", reading.quality)
                .time(reading.timestamp, WritePrecision.MS)
            )
            
            if reading.metadata:
                for key, value in reading.metadata.items():
                    if isinstance(value, (int, float)):
                        point = point.field(f"meta_{key}", value)
                    else:
                        point = point.tag(f"meta_{key}", str(value))
            
            await self._write_point(point)
            logger.debug(f"Wrote sensor reading: {reading.sensor_id}")
            
        except Exception as e:
            logger.error(f"Failed to write sensor data: {e}")
            raise
    
    async def write_zone_conditions(self, conditions: ZoneConditions) -> None:
        """Write zone conditions to time series database"""
        try:
            point = (
                Point("zone_conditions")
                .tag("zone_id", conditions.zone_id)
                .field("temperature", conditions.temperature)
                .field("humidity", conditions.humidity)
                .time(conditions.timestamp, WritePrecision.MS)
            )
            
            # Add optional fields
            optional_fields = {
                "co2_level": conditions.co2_level,
                "occupancy_count": conditions.occupancy_count,
                "pressure": conditions.pressure,
                "air_quality_index": conditions.air_quality_index,
                "comfort_score": conditions.comfort_score
            }
            
            for field_name, value in optional_fields.items():
                if value is not None:
                    point = point.field(field_name, value)
            
            await self._write_point(point)
            logger.debug(f"Wrote zone conditions: {conditions.zone_id}")
            
        except Exception as e:
            logger.error(f"Failed to write zone conditions: {e}")
            raise
    
    async def write_device_health(self, health: DeviceHealth) -> None:
        """Write device health data to time series database"""
        try:
            point = (
                Point("device_health")
                .tag("device_id", health.device_id)
                .tag("device_type", health.device_type)
                .tag("status", health.status.value)
                .field("uptime_hours", health.uptime_hours)
                .field("error_count", health.error_count)
                .field("performance_score", health.performance_score)
                .time(health.timestamp, WritePrecision.MS)
            )
            
            if health.energy_efficiency is not None:
                point = point.field("energy_efficiency", health.energy_efficiency)
            
            if health.last_maintenance:
                point = point.field("last_maintenance", health.last_maintenance.timestamp())
            
            if health.alerts:
                point = point.field("alert_count", len(health.alerts))
                point = point.tag("alerts", json.dumps(health.alerts))
            
            await self._write_point(point)
            logger.debug(f"Wrote device health: {health.device_id}")
            
        except Exception as e:
            logger.error(f"Failed to write device health: {e}")
            raise
    
    async def write_rl_observation(self, observation: RLObservation) -> None:
        """Write RL observation to time series database"""
        try:
            point = (
                Point("rl_observation")
                .tag("observation_id", observation.observation_id)
                .tag("zone_id", observation.zone_id)
                .tag("agent_id", observation.agent_id)
                .field("done", observation.done)
                .time(observation.timestamp, WritePrecision.MS)
            )
            
            # Store state vector as individual fields
            for i, value in enumerate(observation.state_vector):
                point = point.field(f"state_{i}", value)
            
            # Add optional fields
            if observation.reward is not None:
                point = point.field("reward", observation.reward)
            if observation.episode_id:
                point = point.tag("episode_id", observation.episode_id)
            if observation.step_number is not None:
                point = point.field("step_number", observation.step_number)
            
            if observation.metadata:
                for key, value in observation.metadata.items():
                    if isinstance(value, (int, float)):
                        point = point.field(f"meta_{key}", value)
                    else:
                        point = point.tag(f"meta_{key}", str(value))
            
            await self._write_point(point)
            logger.debug(f"Wrote RL observation: {observation.observation_id}")
            
        except Exception as e:
            logger.error(f"Failed to write RL observation: {e}")
            raise
    
    async def write_rl_action(self, action: RLAction) -> None:
        """Write RL action to time series database"""
        try:
            point = (
                Point("rl_action")
                .tag("action_id", action.action_id)
                .tag("zone_id", action.zone_id)
                .tag("agent_id", action.agent_id)
                .tag("action_type", action.action_type)
                .field("confidence", action.confidence)
                .time(action.timestamp, WritePrecision.MS)
            )
            
            # Store action vector as individual fields
            for i, value in enumerate(action.action_vector):
                point = point.field(f"action_{i}", value)
            
            # Add optional fields
            if action.episode_id:
                point = point.tag("episode_id", action.episode_id)
            if action.step_number is not None:
                point = point.field("step_number", action.step_number)
            if action.execution_result:
                point = point.tag("execution_result", action.execution_result)
            
            await self._write_point(point)
            logger.debug(f"Wrote RL action: {action.action_id}")
            
        except Exception as e:
            logger.error(f"Failed to write RL action: {e}")
            raise
    
    async def write_energy_consumption(self, consumption: EnergyConsumption) -> None:
        """Write energy consumption data to time series database"""
        try:
            point = (
                Point("energy_consumption")
                .tag("meter_id", consumption.meter_id)
                .field("consumption_kwh", consumption.consumption_kwh)
                .field("demand_kw", consumption.demand_kw)
                .time(consumption.timestamp, WritePrecision.MS)
            )
            
            if consumption.zone_id:
                point = point.tag("zone_id", consumption.zone_id)
            if consumption.device_id:
                point = point.tag("device_id", consumption.device_id)
            
            # Add optional fields
            optional_fields = {
                "cost_usd": consumption.cost_usd,
                "efficiency_ratio": consumption.efficiency_ratio
            }
            
            for field_name, value in optional_fields.items():
                if value is not None:
                    point = point.field(field_name, value)
            
            if consumption.tariff_period:
                point = point.tag("tariff_period", consumption.tariff_period)
            
            await self._write_point(point)
            logger.debug(f"Wrote energy consumption: {consumption.meter_id}")
            
        except Exception as e:
            logger.error(f"Failed to write energy consumption: {e}")
            raise
    
    async def _write_point(self, point: Point) -> None:
        """Internal method to write a point to InfluxDB"""
        try:
            self.write_api.write(bucket=self.bucket, record=point)
        except Exception as e:
            logger.error(f"Failed to write point to InfluxDB: {e}")
            raise
    
    # ========================================================================
    # Query Operations
    # ========================================================================
    
    async def query_sensor_data(
        self,
        sensor_ids: Optional[List[str]] = None,
        sensor_types: Optional[List[SensorType]] = None,
        zone_ids: Optional[List[str]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        aggregation_window: Optional[str] = None,
        aggregation_fn: str = "mean"
    ) -> pd.DataFrame:
        """Query sensor data with optional filtering and aggregation"""
        
        # Build query
        query = f'from(bucket: "{self.bucket}") |> range('
        
        if start_time:
            query += f'start: {int(start_time.timestamp())}'
        else:
            query += 'start: -24h'
        
        if end_time:
            query += f', stop: {int(end_time.timestamp())}'
        
        query += ') |> filter(fn: (r) => r._measurement == "sensor_reading")'
        
        # Add filters
        if sensor_ids:
            sensor_filter = ' or '.join([f'r.sensor_id == "{sid}"' for sid in sensor_ids])
            query += f' |> filter(fn: (r) => {sensor_filter})'
        
        if sensor_types:
            type_filter = ' or '.join([f'r.sensor_type == "{st.value}"' for st in sensor_types])
            query += f' |> filter(fn: (r) => {type_filter})'
        
        if zone_ids:
            zone_filter = ' or '.join([f'r.zone_id == "{zid}"' for zid in zone_ids])
            query += f' |> filter(fn: (r) => {zone_filter})'
        
        query += ' |> filter(fn: (r) => r._field == "value")'
        
        # Add aggregation if specified
        if aggregation_window:
            query += f' |> aggregateWindow(every: {aggregation_window}, fn: {aggregation_fn})'
        
        try:
            tables = self.query_api.query(query)
            return self._tables_to_dataframe(tables)
        except Exception as e:
            logger.error(f"Failed to query sensor data: {e}")
            raise
    
    async def query_zone_conditions(
        self,
        zone_ids: Optional[List[str]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        fields: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Query zone conditions data"""
        
        query = f'from(bucket: "{self.bucket}") |> range('
        
        if start_time:
            query += f'start: {int(start_time.timestamp())}'
        else:
            query += 'start: -24h'
        
        if end_time:
            query += f', stop: {int(end_time.timestamp())}'
        
        query += ') |> filter(fn: (r) => r._measurement == "zone_conditions")'
        
        if zone_ids:
            zone_filter = ' or '.join([f'r.zone_id == "{zid}"' for zid in zone_ids])
            query += f' |> filter(fn: (r) => {zone_filter})'
        
        if fields:
            field_filter = ' or '.join([f'r._field == "{field}"' for field in fields])
            query += f' |> filter(fn: (r) => {field_filter})'
        
        try:
            tables = self.query_api.query(query)
            return self._tables_to_dataframe(tables)
        except Exception as e:
            logger.error(f"Failed to query zone conditions: {e}")
            raise
    
    async def query_device_health(
        self,
        device_ids: Optional[List[str]] = None,
        device_types: Optional[List[str]] = None,
        statuses: Optional[List[DeviceStatus]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> pd.DataFrame:
        """Query device health data"""
        
        query = f'from(bucket: "{self.bucket}") |> range('
        
        if start_time:
            query += f'start: {int(start_time.timestamp())}'
        else:
            query += 'start: -24h'
        
        if end_time:
            query += f', stop: {int(end_time.timestamp())}'
        
        query += ') |> filter(fn: (r) => r._measurement == "device_health")'
        
        if device_ids:
            device_filter = ' or '.join([f'r.device_id == "{did}"' for did in device_ids])
            query += f' |> filter(fn: (r) => {device_filter})'
        
        if device_types:
            type_filter = ' or '.join([f'r.device_type == "{dt}"' for dt in device_types])
            query += f' |> filter(fn: (r) => {type_filter})'
        
        if statuses:
            status_filter = ' or '.join([f'r.status == "{s.value}"' for s in statuses])
            query += f' |> filter(fn: (r) => {status_filter})'
        
        try:
            tables = self.query_api.query(query)
            return self._tables_to_dataframe(tables)
        except Exception as e:
            logger.error(f"Failed to query device health: {e}")
            raise
    
    async def query_rl_training_data(
        self,
        agent_ids: Optional[List[str]] = None,
        zone_ids: Optional[List[str]] = None,
        episode_ids: Optional[List[str]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        include_actions: bool = True,
        include_observations: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """Query RL training data (observations and actions)"""
        
        result = {}
        
        if include_observations:
            obs_query = f'from(bucket: "{self.bucket}") |> range('
            
            if start_time:
                obs_query += f'start: {int(start_time.timestamp())}'
            else:
                obs_query += 'start: -24h'
            
            if end_time:
                obs_query += f', stop: {int(end_time.timestamp())}'
            
            obs_query += ') |> filter(fn: (r) => r._measurement == "rl_observation")'
            
            # Add filters
            if agent_ids:
                agent_filter = ' or '.join([f'r.agent_id == "{aid}"' for aid in agent_ids])
                obs_query += f' |> filter(fn: (r) => {agent_filter})'
            
            if zone_ids:
                zone_filter = ' or '.join([f'r.zone_id == "{zid}"' for zid in zone_ids])
                obs_query += f' |> filter(fn: (r) => {zone_filter})'
            
            if episode_ids:
                episode_filter = ' or '.join([f'r.episode_id == "{eid}"' for eid in episode_ids])
                obs_query += f' |> filter(fn: (r) => {episode_filter})'
            
            try:
                tables = self.query_api.query(obs_query)
                result['observations'] = self._tables_to_dataframe(tables)
            except Exception as e:
                logger.error(f"Failed to query RL observations: {e}")
                raise
        
        if include_actions:
            action_query = f'from(bucket: "{self.bucket}") |> range('
            
            if start_time:
                action_query += f'start: {int(start_time.timestamp())}'
            else:
                action_query += 'start: -24h'
            
            if end_time:
                action_query += f', stop: {int(end_time.timestamp())}'
            
            action_query += ') |> filter(fn: (r) => r._measurement == "rl_action")'
            
            # Add filters (same as observations)
            if agent_ids:
                agent_filter = ' or '.join([f'r.agent_id == "{aid}"' for aid in agent_ids])
                action_query += f' |> filter(fn: (r) => {agent_filter})'
            
            if zone_ids:
                zone_filter = ' or '.join([f'r.zone_id == "{zid}"' for zid in zone_ids])
                action_query += f' |> filter(fn: (r) => {zone_filter})'
            
            if episode_ids:
                episode_filter = ' or '.join([f'r.episode_id == "{eid}"' for eid in episode_ids])
                action_query += f' |> filter(fn: (r) => {episode_filter})'
            
            try:
                tables = self.query_api.query(action_query)
                result['actions'] = self._tables_to_dataframe(tables)
            except Exception as e:
                logger.error(f"Failed to query RL actions: {e}")
                raise
        
        return result
    
    async def query_energy_consumption(
        self,
        meter_ids: Optional[List[str]] = None,
        zone_ids: Optional[List[str]] = None,
        device_ids: Optional[List[str]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        aggregation_window: Optional[str] = None
    ) -> pd.DataFrame:
        """Query energy consumption data"""
        
        query = f'from(bucket: "{self.bucket}") |> range('
        
        if start_time:
            query += f'start: {int(start_time.timestamp())}'
        else:
            query += 'start: -24h'
        
        if end_time:
            query += f', stop: {int(end_time.timestamp())}'
        
        query += ') |> filter(fn: (r) => r._measurement == "energy_consumption")'
        
        # Add filters
        if meter_ids:
            meter_filter = ' or '.join([f'r.meter_id == "{mid}"' for mid in meter_ids])
            query += f' |> filter(fn: (r) => {meter_filter})'
        
        if zone_ids:
            zone_filter = ' or '.join([f'r.zone_id == "{zid}"' for zid in zone_ids])
            query += f' |> filter(fn: (r) => {zone_filter})'
        
        if device_ids:
            device_filter = ' or '.join([f'r.device_id == "{did}"' for did in device_ids])
            query += f' |> filter(fn: (r) => {device_filter})'
        
        # Add aggregation if specified
        if aggregation_window:
            query += f' |> aggregateWindow(every: {aggregation_window}, fn: sum)'
        
        try:
            tables = self.query_api.query(query)
            return self._tables_to_dataframe(tables)
        except Exception as e:
            logger.error(f"Failed to query energy consumption: {e}")
            raise
    
    async def get_latest_zone_conditions(self, zone_id: str) -> Optional[Dict[str, Any]]:
        """Get the most recent conditions for a specific zone"""
        
        query = f'''
        from(bucket: "{self.bucket}")
          |> range(start: -1h)
          |> filter(fn: (r) => r._measurement == "zone_conditions")
          |> filter(fn: (r) => r.zone_id == "{zone_id}")
          |> last()
        '''
        
        try:
            tables = self.query_api.query(query)
            df = self._tables_to_dataframe(tables)
            
            if df.empty:
                return None
            
            # Convert to dictionary format
            latest_conditions = {}
            for _, row in df.iterrows():
                latest_conditions[row['_field']] = row['_value']
                latest_conditions['timestamp'] = row['_time']
                latest_conditions['zone_id'] = row['zone_id']
            
            return latest_conditions
        except Exception as e:
            logger.error(f"Failed to get latest zone conditions: {e}")
            return None
    
    async def get_system_overview(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get system-wide overview statistics"""
        
        if not start_time:
            start_time = datetime.utcnow() - timedelta(hours=24)
        if not end_time:
            end_time = datetime.utcnow()
        
        overview = {}
        
        try:
            # Get zone count and average conditions
            zone_query = f'''
            from(bucket: "{self.bucket}")
              |> range(start: {int(start_time.timestamp())}, stop: {int(end_time.timestamp())})
              |> filter(fn: (r) => r._measurement == "zone_conditions")
              |> filter(fn: (r) => r._field == "temperature")
              |> group(columns: ["zone_id"])
              |> mean()
              |> group()
              |> count()
            '''
            
            tables = self.query_api.query(zone_query)
            zone_df = self._tables_to_dataframe(tables)
            overview['active_zones'] = len(zone_df) if not zone_df.empty else 0
            
            # Get device health summary
            device_query = f'''
            from(bucket: "{self.bucket}")
              |> range(start: {int(start_time.timestamp())}, stop: {int(end_time.timestamp())})
              |> filter(fn: (r) => r._measurement == "device_health")
              |> filter(fn: (r) => r._field == "performance_score")
              |> last()
              |> group(columns: ["status"])
              |> count()
            '''
            
            tables = self.query_api.query(device_query)
            device_df = self._tables_to_dataframe(tables)
            
            overview['device_status'] = {}
            for _, row in device_df.iterrows():
                overview['device_status'][row['status']] = int(row['_value'])
            
            # Get total energy consumption
            energy_query = f'''
            from(bucket: "{self.bucket}")
              |> range(start: {int(start_time.timestamp())}, stop: {int(end_time.timestamp())})
              |> filter(fn: (r) => r._measurement == "energy_consumption")
              |> filter(fn: (r) => r._field == "consumption_kwh")
              |> sum()
            '''
            
            tables = self.query_api.query(energy_query)
            energy_df = self._tables_to_dataframe(tables)
            
            if not energy_df.empty:
                overview['total_energy_kwh'] = float(energy_df['_value'].sum())
            else:
                overview['total_energy_kwh'] = 0.0
            
            # Get RL training statistics
            rl_query = f'''
            from(bucket: "{self.bucket}")
              |> range(start: {int(start_time.timestamp())}, stop: {int(end_time.timestamp())})
              |> filter(fn: (r) => r._measurement == "rl_observation")
              |> filter(fn: (r) => r._field == "reward")
              |> mean()
            '''
            
            tables = self.query_api.query(rl_query)
            rl_df = self._tables_to_dataframe(tables)
            
            if not rl_df.empty:
                overview['avg_rl_reward'] = float(rl_df['_value'].mean())
            else:
                overview['avg_rl_reward'] = None
            
            overview['query_period'] = {
                'start': start_time.isoformat(),
                'end': end_time.isoformat()
            }
            
            return overview
            
        except Exception as e:
            logger.error(f"Failed to get system overview: {e}")
            return {}
    
    def _tables_to_dataframe(self, tables: List[FluxTable]) -> pd.DataFrame:
        """Convert InfluxDB tables to pandas DataFrame"""
        if not tables:
            return pd.DataFrame()
        
        data = []
        for table in tables:
            for record in table.records:
                row = {
                    '_time': record.get_time(),
                    '_measurement': record.get_measurement(),
                    '_field': record.get_field(),
                    '_value': record.get_value()
                }
                
                # Add all tags
                for key, value in record.values.items():
                    if key not in ['_time', '_measurement', '_field', '_value', 'result', 'table']:
                        row[key] = value
                
                data.append(row)
        
        return pd.DataFrame(data)
    
    # ========================================================================
    # Data Management Operations
    # ========================================================================
    
    async def delete_data(
        self,
        measurement: str,
        start: datetime,
        stop: datetime,
        predicate: Optional[str] = None
    ) -> None:
        """Delete data from InfluxDB"""
        try:
            if predicate:
                full_predicate = f'_measurement="{measurement}" AND {predicate}'
            else:
                full_predicate = f'_measurement="{measurement}"'
            
            self.delete_api.delete(
                start,
                stop,
                predicate=full_predicate,
                bucket=self.bucket,
                org=self.org
            )
            
            logger.info(f"Deleted data from {measurement} between {start} and {stop}")
            
        except Exception as e:
            logger.error(f"Failed to delete data: {e}")
            raise
    
    async def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics for the bucket"""
        try:
            # Query bucket usage
            query = f'''
            from(bucket: "{self.bucket}")
              |> range(start: -30d)
              |> group()
              |> count()
            '''
            
            tables = self.query_api.query(query)
            df = self._tables_to_dataframe(tables)
            
            total_points = df['_value'].sum() if not df.empty else 0
            
            # Get measurements breakdown
            measurement_query = f'''
            from(bucket: "{self.bucket}")
              |> range(start: -7d)
              |> group(columns: ["_measurement"])
              |> count()
              |> group()
            '''
            
            tables = self.query_api.query(measurement_query)
            measurement_df = self._tables_to_dataframe(tables)
            
            measurements = {}
            for _, row in measurement_df.iterrows():
                measurements[row['_measurement']] = int(row['_value'])
            
            return {
                'total_points_30d': int(total_points),
                'measurements_7d': measurements,
                'bucket': self.bucket,
                'organization': self.org
            }
            
        except Exception as e:
            logger.error(f"Failed to get storage stats: {e}")
            return {}
    
    async def create_retention_policy(
        self,
        name: str,
        duration: timedelta,
        replication: int = 1
    ) -> None:
        """Create a retention policy for automatic data cleanup"""
        # Note: InfluxDB 2.x uses retention policies differently than 1.x
        # This would require bucket configuration or using tasks for cleanup
        logger.warning("Retention policies in InfluxDB 2.x require bucket-level configuration")
    
    async def close(self) -> None:
        """Close InfluxDB client connection"""
        try:
            self.client.close()
            logger.info("Closed InfluxDB client connection")
        except Exception as e:
            logger.error(f"Error closing InfluxDB client: {e}")

# ============================================================================
# Data Manager - High-level Operations
# ============================================================================

class HVACDataManager:
    """High-level data manager for HVAC RL system operations"""
    
    def __init__(self, store: TimeSeriesStore):
        self.store = store
        self.logger = logging.getLogger(__name__ + ".HVACDataManager")
    
    async def initialize(self) -> None:
        """Initialize the data manager"""
        health = await self.store.health_check()
        if not health:
            raise RuntimeError("InfluxDB health check failed")
        
        self.logger.info("HVACDataManager initialized successfully")
    
    # ========================================================================
    # Batch Operations
    # ========================================================================
    
    async def batch_write_sensor_data(self, readings: List[SensorReading]) -> None:
        """Write multiple sensor readings in batch"""
        try:
            tasks = [self.store.write_sensor_data(reading) for reading in readings]
            await asyncio.gather(*tasks)
            self.logger.info(f"Batch wrote {len(readings)} sensor readings")
        except Exception as e:
            self.logger.error(f"Failed to batch write sensor data: {e}")
            raise
    
    async def batch_write_zone_data(self, conditions_list: List[ZoneConditions]) -> None:
        """Write multiple zone conditions in batch"""
        try:
            tasks = [self.store.write_zone_conditions(conditions) for conditions in conditions_list]
            await asyncio.gather(*tasks)
            self.logger.info(f"Batch wrote {len(conditions_list)} zone condition records")
        except Exception as e:
            self.logger.error(f"Failed to batch write zone data: {e}")
            raise
    
    async def batch_write_rl_data(
        self,
        observations: List[RLObservation],
        actions: List[RLAction]
    ) -> None:
        """Write RL observations and actions in batch"""
        try:
            tasks = []
            tasks.extend([self.store.write_rl_observation(obs) for obs in observations])
            tasks.extend([self.store.write_rl_action(action) for action in actions])
            
            await asyncio.gather(*tasks)
            self.logger.info(f"Batch wrote {len(observations)} observations and {len(actions)} actions")
        except Exception as e:
            self.logger.error(f"Failed to batch write RL data: {e}")
            raise
    
    # ========================================================================
    # Analytics and Aggregations
    # ========================================================================
    
    async def get_zone_performance_summary(
        self,
        zone_id: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """Get comprehensive performance summary for a zone"""
        
        try:
            # Get zone conditions
            conditions_df = await self.store.query_zone_conditions(
                zone_ids=[zone_id],
                start_time=start_time,
                end_time=end_time
            )
            
            # Get energy consumption
            energy_df = await self.store.query_energy_consumption(
                zone_ids=[zone_id],
                start_time=start_time,
                end_time=end_time
            )
            
            # Get RL performance
            rl_data = await self.store.query_rl_training_data(
                zone_ids=[zone_id],
                start_time=start_time,
                end_time=end_time
            )
            
            summary = {
                'zone_id': zone_id,
                'period': {
                    'start': start_time.isoformat(),
                    'end': end_time.isoformat()
                }
            }
            
            # Analyze conditions
            if not conditions_df.empty:
                temp_data = conditions_df[conditions_df['_field'] == 'temperature']
                humidity_data = conditions_df[conditions_df['_field'] == 'humidity']
                
                summary['temperature'] = {
                    'avg': float(temp_data['_value'].mean()) if not temp_data.empty else None,
                    'min': float(temp_data['_value'].min()) if not temp_data.empty else None,
                    'max': float(temp_data['_value'].max()) if not temp_data.empty else None,
                    'std': float(temp_data['_value'].std()) if not temp_data.empty else None
                }
                
                summary['humidity'] = {
                    'avg': float(humidity_data['_value'].mean()) if not humidity_data.empty else None,
                    'min': float(humidity_data['_value'].min()) if not humidity_data.empty else None,
                    'max': float(humidity_data['_value'].max()) if not humidity_data.empty else None,
                    'std': float(humidity_data['_value'].std()) if not humidity_data.empty else None
                }
            
            # Analyze energy consumption
            if not energy_df.empty:
                consumption_data = energy_df[energy_df['_field'] == 'consumption_kwh']
                demand_data = energy_df[energy_df['_field'] == 'demand_kw']
                
                summary['energy'] = {
                    'total_consumption_kwh': float(consumption_data['_value'].sum()) if not consumption_data.empty else 0.0,
                    'avg_demand_kw': float(demand_data['_value'].mean()) if not demand_data.empty else None,
                    'peak_demand_kw': float(demand_data['_value'].max()) if not demand_data.empty else None
                }
            
            # Analyze RL performance
            if 'observations' in rl_data and not rl_data['observations'].empty:
                obs_df = rl_data['observations']
                reward_data = obs_df[obs_df['_field'] == 'reward']
                
                summary['rl_performance'] = {
                    'avg_reward': float(reward_data['_value'].mean()) if not reward_data.empty else None,
                    'total_episodes': len(obs_df['episode_id'].unique()) if 'episode_id' in obs_df.columns else None,
                    'total_steps': len(obs_df) if not obs_df.empty else 0
                }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Failed to get zone performance summary: {e}")
            return {}
    
    async def get_system_efficiency_metrics(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """Calculate system-wide efficiency metrics"""
        
        try:
            # Get all energy consumption data
            energy_df = await self.store.query_energy_consumption(
                start_time=start_time,
                end_time=end_time
            )
            
            # Get all zone conditions
            conditions_df = await self.store.query_zone_conditions(
                start_time=start_time,
                end_time=end_time
            )
            
            metrics = {
                'period': {
                    'start': start_time.isoformat(),
                    'end': end_time.isoformat()
                }
            }
            
            if not energy_df.empty:
                consumption_data = energy_df[energy_df['_field'] == 'consumption_kwh']
                demand_data = energy_df[energy_df['_field'] == 'demand_kw']
                
                # Total energy metrics
                total_consumption = consumption_data['_value'].sum()
                avg_demand = demand_data['_value'].mean()
                peak_demand = demand_data['_value'].max()
                
                metrics['energy'] = {
                    'total_consumption_kwh': float(total_consumption),
                    'average_demand_kw': float(avg_demand) if not pd.isna(avg_demand) else None,
                    'peak_demand_kw': float(peak_demand) if not pd.isna(peak_demand) else None
                }
                
                # Calculate energy per zone
                zone_consumption = consumption_data.groupby('zone_id')['_value'].sum().to_dict()
                metrics['zone_consumption'] = {k: float(v) for k, v in zone_consumption.items()}
                
                # Energy efficiency ratio (if available)
                if 'efficiency_ratio' in energy_df['_field'].values:
                    efficiency_data = energy_df[energy_df['_field'] == 'efficiency_ratio']
                    metrics['average_efficiency'] = float(efficiency_data['_value'].mean())
            
            if not conditions_df.empty:
                # Calculate comfort metrics
                temp_data = conditions_df[conditions_df['_field'] == 'temperature']
                humidity_data = conditions_df[conditions_df['_field'] == 'humidity']
                
                # Comfort statistics by zone
                temp_by_zone = temp_data.groupby('zone_id')['_value'].agg(['mean', 'std']).to_dict()
                humidity_by_zone = humidity_data.groupby('zone_id')['_value'].agg(['mean', 'std']).to_dict()
                
                metrics['comfort'] = {
                    'temperature_by_zone': temp_by_zone,
                    'humidity_by_zone': humidity_by_zone
                }
                
                # Overall comfort score (simplified calculation)
                # Assume comfort range: 20-26°C for temperature, 30-60% for humidity
                temp_comfort = temp_data[(temp_data['_value'] >= 20) & (temp_data['_value'] <= 26)]
                humidity_comfort = humidity_data[(humidity_data['_value'] >= 30) & (humidity_data['_value'] <= 60)]
                
                temp_comfort_ratio = len(temp_comfort) / len(temp_data) if len(temp_data) > 0 else 0
                humidity_comfort_ratio = len(humidity_comfort) / len(humidity_data) if len(humidity_data) > 0 else 0
                
                metrics['comfort_score'] = {
                    'temperature_comfort_ratio': float(temp_comfort_ratio),
                    'humidity_comfort_ratio': float(humidity_comfort_ratio),
                    'overall_comfort_score': float((temp_comfort_ratio + humidity_comfort_ratio) / 2)
                }
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Failed to calculate efficiency metrics: {e}")
            return {}
    
    async def get_zone_dashboard_data(self, zone_id: str) -> Dict[str, Any]:
        """Get real-time dashboard data for a specific zone"""
        
        try:
            # Get current conditions
            current_conditions = await self.store.get_latest_zone_conditions(zone_id)
            
            # Get last 24h trends
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=24)
            
            # Temperature and humidity trends
            conditions_df = await self.store.query_zone_conditions(
                zone_ids=[zone_id],
                start_time=start_time,
                end_time=end_time,
                fields=['temperature', 'humidity']
            )
            
            # Energy consumption last 24h
            energy_df = await self.store.query_energy_consumption(
                zone_ids=[zone_id],
                start_time=start_time,
                end_time=end_time
            )
            
            # Recent RL performance
            rl_data = await self.store.query_rl_training_data(
                zone_ids=[zone_id],
                start_time=start_time,
                end_time=end_time,
                include_observations=True,
                include_actions=False
            )
            
            dashboard = {
                'zone_id': zone_id,
                'timestamp': datetime.utcnow().isoformat(),
                'current_conditions': current_conditions or {}
            }
            
            # Process trends
            if not conditions_df.empty:
                temp_data = conditions_df[conditions_df['_field'] == 'temperature']
                humidity_data = conditions_df[conditions_df['_field'] == 'humidity']
                
                dashboard['trends'] = {
                    'temperature': [
                        {'time': row['_time'].isoformat(), 'value': row['_value']}
                        for _, row in temp_data.iterrows()
                    ],
                    'humidity': [
                        {'time': row['_time'].isoformat(), 'value': row['_value']}
                        for _, row in humidity_data.iterrows()
                    ]
                }
            else:
                dashboard['trends'] = {'temperature': [], 'humidity': []}
            
            # Process energy data
            if not energy_df.empty:
                consumption_data = energy_df[energy_df['_field'] == 'consumption_kwh']
                
                dashboard['energy_24h'] = {
                    'total_consumption_kwh': float(consumption_data['_value'].sum()),
                    'consumption_trend': [
                        {'time': row['_time'].isoformat(), 'value': row['_value']}
                        for _, row in consumption_data.iterrows()
                    ]
                }
            else:
                dashboard['energy_24h'] = {'total_consumption_kwh': 0.0, 'consumption_trend': []}
            
            # Process RL data
            if 'observations' in rl_data and not rl_data['observations'].empty:
                obs_df = rl_data['observations']
                reward_data = obs_df[obs_df['_field'] == 'reward']
                
                if not reward_data.empty:
                    dashboard['rl_performance'] = {
                        'avg_reward_24h': float(reward_data['_value'].mean()),
                        'reward_trend': [
                            {'time': row['_time'].isoformat(), 'value': row['_value']}
                            for _, row in reward_data.iterrows()
                        ]
                    }
                else:
                    dashboard['rl_performance'] = {'avg_reward_24h': None, 'reward_trend': []}
            else:
                dashboard['rl_performance'] = {'avg_reward_24h': None, 'reward_trend': []}
            
            return dashboard
            
        except Exception as e:
            self.logger.error(f"Failed to get zone dashboard data: {e}")
            return {'zone_id': zone_id, 'error': str(e)}
    
    # ========================================================================
    # Data Export and Backup
    # ========================================================================
    
    async def export_zone_data(
        self,
        zone_id: str,
        start_time: datetime,
        end_time: datetime,
        export_format: str = 'csv'
    ) -> str:
        """Export zone data to file"""
        
        try:
            # Get all data for the zone
            conditions_df = await self.store.query_zone_conditions(
                zone_ids=[zone_id],
                start_time=start_time,
                end_time=end_time
            )
            
            energy_df = await self.store.query_energy_consumption(
                zone_ids=[zone_id],
                start_time=start_time,
                end_time=end_time
            )
            
            rl_data = await self.store.query_rl_training_data(
                zone_ids=[zone_id],
                start_time=start_time,
                end_time=end_time
            )
            
            # Create filename
            filename = f"zone_{zone_id}_{start_time.strftime('%Y%m%d')}_{end_time.strftime('%Y%m%d')}"
            
            if export_format.lower() == 'csv':
                # Export as separate CSV files
                if not conditions_df.empty:
                    conditions_df.to_csv(f"{filename}_conditions.csv", index=False)
                
                if not energy_df.empty:
                    energy_df.to_csv(f"{filename}_energy.csv", index=False)
                
                if 'observations' in rl_data and not rl_data['observations'].empty:
                    rl_data['observations'].to_csv(f"{filename}_rl_observations.csv", index=False)
                
                if 'actions' in rl_data and not rl_data['actions'].empty:
                    rl_data['actions'].to_csv(f"{filename}_rl_actions.csv", index=False)
                
                self.logger.info(f"Exported zone data to CSV files: {filename}")
                return filename
            
            elif export_format.lower() == 'json':
                # Export as JSON
                export_data = {
                    'zone_id': zone_id,
                    'export_period': {
                        'start': start_time.isoformat(),
                        'end': end_time.isoformat()
                    },
                    'conditions': conditions_df.to_dict('records') if not conditions_df.empty else [],
                    'energy': energy_df.to_dict('records') if not energy_df.empty else [],
                    'rl_observations': rl_data.get('observations', pd.DataFrame()).to_dict('records'),
                    'rl_actions': rl_data.get('actions', pd.DataFrame()).to_dict('records')
                }
                
                json_filename = f"{filename}.json"
                with open(json_filename, 'w') as f:
                    json.dump(export_data, f, indent=2, default=str)
                
                self.logger.info(f"Exported zone data to JSON: {json_filename}")
                return json_filename
            
            else:
                raise ValueError(f"Unsupported export format: {export_format}")
                
        except Exception as e:
            self.logger.error(f"Failed to export zone data: {e}")
            raise
    
    async def backup_system_data(
        self,
        backup_path: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> str:
        """Create a full system backup"""
        
        if not start_time:
            start_time = datetime.utcnow() - timedelta(days=30)
        if not end_time:
            end_time = datetime.utcnow()
        
        try:
            # Create backup directory
            backup_dir = f"{backup_path}/hvac_backup_{int(datetime.utcnow().timestamp())}"
            import os
            os.makedirs(backup_dir, exist_ok=True)
            
            # Get all measurements
            measurements = ['sensor_reading', 'zone_conditions', 'device_health', 
                          'rl_observation', 'rl_action', 'energy_consumption']
            
            for measurement in measurements:
                try:
                    # Query each measurement
                    query = f'''
                    from(bucket: "{self.store.bucket}")
                      |> range(start: {int(start_time.timestamp())}, stop: {int(end_time.timestamp())})
                      |> filter(fn: (r) => r._measurement == "{measurement}")
                    '''
                    
                    tables = self.store.query_api.query(query)
                    df = self.store._tables_to_dataframe(tables)
                    
                    if not df.empty:
                        backup_file = f"{backup_dir}/{measurement}.csv"
                        df.to_csv(backup_file, index=False)
                        self.logger.info(f"Backed up {measurement}: {len(df)} records")
                
                except Exception as e:
                    self.logger.warning(f"Failed to backup {measurement}: {e}")
            
            # Create backup metadata
            metadata = {
                'backup_timestamp': datetime.utcnow().isoformat(),
                'data_period': {
                    'start': start_time.isoformat(),
                    'end': end_time.isoformat()
                },
                'bucket': self.store.bucket,
                'organization': self.store.org
            }
            
            with open(f"{backup_dir}/metadata.json", 'w') as f:
                json.dump(metadata, f, indent=2)
            
            self.logger.info(f"System backup completed: {backup_dir}")
            return backup_dir
            
        except Exception as e:
            self.logger.error(f"Failed to create system backup: {e}")
            raise

# ============================================================================
# Factory Functions and Utilities
# ============================================================================

async def create_time_series_store(
    url: str,
    token: str,
    org: str,
    bucket: str,
    **kwargs
) -> TimeSeriesStore:
    """Factory function to create and initialize a time series store"""
    
    store = TimeSeriesStore(url, token, org, bucket, **kwargs)
    
    # Verify connection
    if not await store.health_check():
        raise RuntimeError(f"Failed to connect to InfluxDB at {url}")
    
    logger.info(f"Successfully created TimeSeriesStore for bucket '{bucket}'")
    return store

async def prepare_rl_training_data(
    store: TimeSeriesStore,
    start_time: datetime,
    end_time: datetime,
    zone_ids: Optional[List[str]] = None,
    agent_ids: Optional[List[str]] = None,
    resample_frequency: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """Prepare RL training data with optional resampling"""
    
    try:
        # Get RL training data
        rl_data = await store.query_rl_training_data(
            agent_ids=agent_ids,
            zone_ids=zone_ids,
            start_time=start_time,
            end_time=end_time
        )
        
        processed_data = {}
        
        for data_type, df in rl_data.items():
            if df.empty:
                processed_data[data_type] = df
                continue
            
            # Convert time column to datetime
            df['_time'] = pd.to_datetime(df['_time'])
            df.set_index('_time', inplace=True)
            
            # Resample if requested
            if resample_frequency:
                # Pivot to get fields as columns
                pivot_df = df.pivot_table(
                    index=df.index,
                    columns='_field',
                    values='_value',
                    aggfunc='mean'
                )
                
                # Resample
                resampled_df = pivot_df.resample(resample_frequency).mean()
                processed_data[data_type] = resampled_df
            else:
                processed_data[data_type] = df
        
        return processed_data
        
    except Exception as e:
        logger.error(f"Failed to prepare RL training data: {e}")
        raise

# ============================================================================
# Context Manager for Resource Management
# ============================================================================

@asynccontextmanager
async def managed_time_series_store(url: str, token: str, org: str, bucket: str):
    """Context manager for automatic resource cleanup"""
    store = None
    try:
        store = await create_time_series_store(url, token, org, bucket)
        yield store
    finally:
        if store:
            await store.close()

# ============================================================================
# Example Usage and Testing
# ============================================================================

async def main():
    """Example usage of the time series database"""
    
    # Configuration
    INFLUXDB_URL = "http://localhost:8086"
    INFLUXDB_TOKEN = "your-token-here"
    INFLUXDB_ORG = "hvac-org"
    INFLUXDB_BUCKET = "hvac-data"
    
    async with managed_time_series_store(
        INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET
    ) as store:
        
        # Create data manager
        data_manager = HVACDataManager(store)
        await data_manager.initialize()
        
        # Example: Write sensor data
        sensor_reading = SensorReading(
            sensor_id="temp_001",
            sensor_type=SensorType.TEMPERATURE,
            value=23.5,
            unit="celsius",
            zone_id="zone_1",
            device_id="hvac_unit_1"
        )
        
        await store.write_sensor_data(sensor_reading)
        
        # Example: Write zone conditions
        zone_conditions = ZoneConditions(
            zone_id="zone_1",
            temperature=23.5,
            humidity=45.0,
            co2_level=400.0,
            occupancy_count=5
        )
        
        await store.write_zone_conditions(zone_conditions)
        
        # Example: Query data
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=24)
        
        sensor_data = await store.query_sensor_data(
            zone_ids=["zone_1"],
            start_time=start_time,
            end_time=end_time
        )
        
        print(f"Retrieved {len(sensor_data)} sensor readings")
        
        # Example: Get dashboard data
        dashboard_data = await data_manager.get_zone_dashboard_data("zone_1")
        print(f"Dashboard data: {len(dashboard_data)} items")
        
        # Example: System overview
        overview = await store.get_system_overview()
        print(f"System overview: {overview}")

if __name__ == "__main__":
    asyncio.run(main())
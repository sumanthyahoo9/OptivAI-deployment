"""
OPC-UA Data Mapping and Transformation Module
Converts raw OPC-UA data into standardized formats
Handles semantic mapping, unit conversion, and data quality assessment
Core Components:
UnitConverter for automatic conversions (Fahrenheit↔Celsius, PSI↔Bar, etc.)
QualityAssessor that validates data ranges and freshness
OPCUADataMapper that transforms raw node data into semantic tags
BatchDataProcessor for efficient bulk transformations
MappingConfigBuilder with preset configurations for HVAC/manufacturing
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum

from protocol_specific_clients.opcua_client import OPCUAReading

logger = logging.getLogger(__name__)

# ============================================================================
# Data Models
# ============================================================================

class DataQuality(str, Enum):
    """Standardized data quality levels"""
    GOOD = "good"
    UNCERTAIN = "uncertain"
    BAD = "bad"
    STALE = "stale"
    INVALID = "invalid"

@dataclass
class DataPoint:
    """Standardized data point"""
    tag_name: str
    value: Union[float, int, bool, str]
    unit: str
    timestamp: datetime
    quality: DataQuality
    source_node: str
    metadata: Dict[str, Any] = None

@dataclass
class MappingRule:
    """Mapping rule for OPC-UA node to semantic tag"""
    node_id: str
    tag_name: str
    description: str
    unit_from: str
    unit_to: str
    conversion_factor: float = 1.0
    conversion_offset: float = 0.0
    value_filter: Optional[Dict[str, Any]] = None
    quality_checks: List[str] = None

# ============================================================================
# Unit Conversion
# ============================================================================

class UnitConverter:
    """Handles unit conversions between different measurement systems"""
    
    # Conversion factors to base units
    CONVERSIONS = {
        # Temperature
        'celsius': {'to_base': lambda x: x, 'from_base': lambda x: x},
        'fahrenheit': {'to_base': lambda x: (x - 32) * 5/9, 'from_base': lambda x: x * 9/5 + 32},
        'kelvin': {'to_base': lambda x: x - 273.15, 'from_base': lambda x: x + 273.15},
        
        # Pressure
        'bar': {'to_base': lambda x: x, 'from_base': lambda x: x},
        'psi': {'to_base': lambda x: x * 0.0689476, 'from_base': lambda x: x / 0.0689476},
        'kpa': {'to_base': lambda x: x / 100, 'from_base': lambda x: x * 100},
        
        # Flow
        'm3/h': {'to_base': lambda x: x, 'from_base': lambda x: x},
        'l/min': {'to_base': lambda x: x * 0.06, 'from_base': lambda x: x / 0.06},
        'gpm': {'to_base': lambda x: x * 0.227124, 'from_base': lambda x: x / 0.227124},
        
        # Power
        'kw': {'to_base': lambda x: x, 'from_base': lambda x: x},
        'hp': {'to_base': lambda x: x * 0.745699, 'from_base': lambda x: x / 0.745699},
        'btu/h': {'to_base': lambda x: x * 0.000293071, 'from_base': lambda x: x / 0.000293071},
    }
    
    @classmethod
    def convert(cls, value: float, from_unit: str, to_unit: str) -> float:
        """Convert value between units"""
        try:
            from_unit = from_unit.lower()
            to_unit = to_unit.lower()
            
            if from_unit == to_unit:
                return value
            
            # Convert to base unit, then to target unit
            if from_unit in cls.CONVERSIONS and to_unit in cls.CONVERSIONS:
                base_value = cls.CONVERSIONS[from_unit]['to_base'](value)
                target_value = cls.CONVERSIONS[to_unit]['from_base'](base_value)
                return target_value
            else:
                logger.warning(f"Unknown unit conversion: {from_unit} to {to_unit}")
                return value
                
        except Exception as e:
            logger.error(f"Unit conversion error: {e}")
            return value

# ============================================================================
# Data Quality Assessment
# ============================================================================

class QualityAssessor:
    """Assesses and filters data quality"""
    
    @staticmethod
    def assess_quality(reading: OPCUAReading, rules: List[str] = None) -> DataQuality:
        """Assess data quality based on reading and rules"""
        # Check OPC-UA status
        if reading.quality != "Good":
            return DataQuality.BAD
        
        # Check timestamp age
        age = (datetime.utcnow() - reading.timestamp).total_seconds()
        if age > 300:  # 5 minutes
            return DataQuality.STALE
        
        # Apply custom quality rules
        if rules:
            for rule in rules:
                if not QualityAssessor._apply_quality_rule(reading, rule):
                    return DataQuality.UNCERTAIN
        
        return DataQuality.GOOD
    
    @staticmethod
    def _apply_quality_rule(reading: OPCUAReading, rule: str) -> bool:
        """Apply a specific quality rule"""
        try:
            # Simple rule examples
            if rule == "non_negative" and isinstance(reading.value, (int, float)):
                return reading.value >= 0
            elif rule == "realistic_temperature" and isinstance(reading.value, (int, float)):
                return -50 <= reading.value <= 200
            elif rule == "realistic_pressure" and isinstance(reading.value, (int, float)):
                return 0 <= reading.value <= 100
            elif rule == "non_null":
                return reading.value is not None
            else:
                return True
        except:
            return False

# ============================================================================
# Data Mapper
# ============================================================================

class OPCUADataMapper:
    """Maps and transforms OPC-UA data to standardized format"""
    
    def __init__(self, mapping_config: Dict[str, Any] = None):
        self.mapping_rules: Dict[str, MappingRule] = {}
        self.load_mapping_config(mapping_config or {})
        
    def load_mapping_config(self, config: Dict[str, Any]):
        """Load mapping configuration"""
        for node_id, rule_config in config.items():
            rule = MappingRule(
                node_id=node_id,
                tag_name=rule_config.get('tag_name', f"tag_{node_id}"),
                description=rule_config.get('description', ''),
                unit_from=rule_config.get('unit_from', ''),
                unit_to=rule_config.get('unit_to', ''),
                conversion_factor=rule_config.get('conversion_factor', 1.0),
                conversion_offset=rule_config.get('conversion_offset', 0.0),
                value_filter=rule_config.get('value_filter'),
                quality_checks=rule_config.get('quality_checks', [])
            )
            self.mapping_rules[node_id] = rule
    
    def add_mapping_rule(self, rule: MappingRule):
        """Add a mapping rule"""
        self.mapping_rules[rule.node_id] = rule
    
    def transform_reading(self, reading: OPCUAReading) -> Optional[DataPoint]:
        """Transform a single OPC-UA reading to standardized data point"""
        try:
            # Get mapping rule
            rule = self.mapping_rules.get(reading.node_id)
            if not rule:
                # Create default mapping
                rule = MappingRule(
                    node_id=reading.node_id,
                    tag_name=f"tag_{reading.node_id.replace(';', '_').replace(':', '_')}",
                    description=f"Auto-mapped from {reading.node_id}",
                    unit_from='',
                    unit_to=''
                )
            
            # Apply value filter if configured
            if rule.value_filter and not self._apply_value_filter(reading.value, rule.value_filter):
                logger.debug(f"Value filtered out for {reading.node_id}: {reading.value}")
                return None
            
            # Convert value
            converted_value = self._convert_value(reading.value, rule)
            
            # Assess quality
            quality = QualityAssessor.assess_quality(reading, rule.quality_checks)
            
            # Create standardized data point
            data_point = DataPoint(
                tag_name=rule.tag_name,
                value=converted_value,
                unit=rule.unit_to or rule.unit_from,
                timestamp=reading.timestamp,
                quality=quality,
                source_node=reading.node_id,
                metadata={
                    'description': rule.description,
                    'original_value': reading.value,
                    'opcua_status': reading.status_code,
                    'server_timestamp': reading.server_timestamp,
                    'source_timestamp': reading.source_timestamp
                }
            )
            
            return data_point
            
        except Exception as e:
            logger.error(f"Failed to transform reading {reading.node_id}: {e}")
            return None
    
    def transform_readings(self, readings: List[OPCUAReading]) -> List[DataPoint]:
        """Transform multiple OPC-UA readings"""
        data_points = []
        
        for reading in readings:
            data_point = self.transform_reading(reading)
            if data_point:
                data_points.append(data_point)
        
        return data_points
    
    def _convert_value(self, value: Any, rule: MappingRule) -> Any:
        """Convert value according to mapping rule"""
        if not isinstance(value, (int, float)):
            return value
        
        # Apply linear conversion (factor and offset)
        converted = (value * rule.conversion_factor) + rule.conversion_offset
        
        # Apply unit conversion if needed
        if rule.unit_from and rule.unit_to and rule.unit_from != rule.unit_to:
            converted = UnitConverter.convert(converted, rule.unit_from, rule.unit_to)
        
        return converted
    
    def _apply_value_filter(self, value: Any, filter_config: Dict[str, Any]) -> bool:
        """Apply value filter based on configuration"""
        try:
            if 'min' in filter_config and isinstance(value, (int, float)):
                if value < filter_config['min']:
                    return False
            
            if 'max' in filter_config and isinstance(value, (int, float)):
                if value > filter_config['max']:
                    return False
            
            if 'exclude_values' in filter_config:
                if value in filter_config['exclude_values']:
                    return False
            
            if 'include_values' in filter_config:
                if value not in filter_config['include_values']:
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error applying value filter: {e}")
            return True

# ============================================================================
# Batch Processor
# ============================================================================

class BatchDataProcessor:
    """Processes batches of OPC-UA data for efficiency"""
    
    def __init__(self, mapper: OPCUADataMapper):
        self.mapper = mapper
        self.batch_size = 100
        self.processing_stats = {
            'total_processed': 0,
            'successful_transforms': 0,
            'filtered_values': 0,
            'quality_issues': 0
        }
    
    async def process_batch(self, readings: List[OPCUAReading]) -> Dict[str, List[DataPoint]]:
        """Process a batch of readings and categorize by quality"""
        results = {
            'good': [],
            'uncertain': [],
            'bad': [],
            'stale': [],
            'invalid': []
        }
        
        # Transform all readings
        data_points = self.mapper.transform_readings(readings)
        
        # Categorize by quality
        for point in data_points:
            results[point.quality.value].append(point)
            
            # Update statistics
            if point.quality == DataQuality.GOOD:
                self.processing_stats['successful_transforms'] += 1
            else:
                self.processing_stats['quality_issues'] += 1
        
        self.processing_stats['total_processed'] += len(readings)
        
        return results
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        total = self.processing_stats['total_processed']
        success_rate = (self.processing_stats['successful_transforms'] / total * 100) if total > 0 else 0
        
        return {
            **self.processing_stats,
            'success_rate_percent': round(success_rate, 2)
        }

# ============================================================================
# Configuration Builder
# ============================================================================

class MappingConfigBuilder:
    """Builds mapping configurations for common scenarios"""
    
    @staticmethod
    def create_hvac_config() -> Dict[str, Any]:
        """Create standard HVAC mapping configuration"""
        return {
            "ns=2;i=1001": {
                "tag_name": "chiller_supply_temp",
                "description": "Chiller supply water temperature",
                "unit_from": "celsius",
                "unit_to": "celsius",
                "quality_checks": ["realistic_temperature", "non_null"]
            },
            "ns=2;i=1002": {
                "tag_name": "chiller_return_temp", 
                "description": "Chiller return water temperature",
                "unit_from": "celsius",
                "unit_to": "celsius",
                "quality_checks": ["realistic_temperature"]
            },
            "ns=2;i=1003": {
                "tag_name": "chiller_power",
                "description": "Chiller power consumption",
                "unit_from": "kw",
                "unit_to": "kw",
                "quality_checks": ["non_negative"]
            },
            "ns=2;i=1004": {
                "tag_name": "pump_flow",
                "description": "Chilled water pump flow rate",
                "unit_from": "m3/h",
                "unit_to": "m3/h",
                "quality_checks": ["non_negative"],
                "value_filter": {"min": 0, "max": 1000}
            }
        }
    
    @staticmethod
    def create_manufacturing_config() -> Dict[str, Any]:
        """Create standard manufacturing mapping configuration"""
        return {
            "ns=2;i=2001": {
                "tag_name": "motor_speed",
                "description": "Production motor speed",
                "unit_from": "rpm",
                "unit_to": "rpm",
                "quality_checks": ["non_negative"]
            },
            "ns=2;i=2002": {
                "tag_name": "motor_power",
                "description": "Motor power consumption",
                "unit_from": "kw",
                "unit_to": "kw",
                "quality_checks": ["non_negative"]
            },
            "ns=2;i=2003": {
                "tag_name": "ambient_temp",
                "description": "Factory ambient temperature",
                "unit_from": "fahrenheit",
                "unit_to": "celsius",
                "quality_checks": ["realistic_temperature"]
            }
        }

# ============================================================================
# Example Usage
# ============================================================================

async def main():
    """Example usage of data mapping and transformation"""
    
    # Create mapper with HVAC configuration
    config = MappingConfigBuilder.create_hvac_config()
    mapper = OPCUADataMapper(config)
    
    # Example OPC-UA readings
    readings = [
        OPCUAReading(
            node_id="ns=2;i=1001",
            value=7.5,
            timestamp=datetime.utcnow(),
            status_code="Good",
            quality="Good"
        ),
        OPCUAReading(
            node_id="ns=2;i=1003", 
            value=250.0,
            timestamp=datetime.utcnow(),
            status_code="Good",
            quality="Good"
        )
    ]
    
    # Transform readings
    data_points = mapper.transform_readings(readings)
    
    for point in data_points:
        print(f"Tag: {point.tag_name}, Value: {point.value} {point.unit}, Quality: {point.quality}")
    
    # Batch processing
    processor = BatchDataProcessor(mapper)
    results = await processor.process_batch(readings)
    
    print(f"Good quality points: {len(results['good'])}")
    print(f"Processing stats: {processor.get_processing_stats()}")

if __name__ == "__main__":
    asyncio.run(main())
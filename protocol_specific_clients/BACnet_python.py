#!/usr/bin/env python3
"""
BACnet Client for Reading/Writing Building Automation Data
Uses bacpypes library for robust BACnet communication
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import json

# BACpypes imports
from bacpypes.core import run, stop, deferred
from bacpypes.pdu import Address
from bacpypes.app import BIPSimpleApplication
from bacpypes.service.device import LocalDeviceObject
from bacpypes.service.object import ReadWritePropertyServices
from bacpypes.primitivedata import Real, CharacterString, Boolean, Unsigned
from bacpypes.constructeddata import ArrayOf
from bacpypes.apdu import (
    ReadPropertyRequest, ReadPropertyACK,
    WritePropertyRequest, WritePropertyACK,
    ReadPropertyMultipleRequest, ReadPropertyMultipleACK,
    SubscribeCOVRequest, ConfirmedCOVNotificationRequest
)
from bacpypes.object import (
    AnalogInputObject, AnalogOutputObject, AnalogValueObject,
    BinaryInputObject, BinaryOutputObject, BinaryValueObject,
    MultiStateInputObject, MultiStateOutputObject, MultiStateValueObject
)

@dataclass
class BACnetPoint:
    """Represents a BACnet data point"""
    device_id: int
    object_type: str
    object_instance: int
    property_name: str = "presentValue"
    description: str = ""
    units: str = ""
    writeable: bool = False

@dataclass
class BACnetReading:
    """Represents a reading from a BACnet point"""
    point: BACnetPoint
    value: Any
    timestamp: datetime
    quality: str = "good"
    error: Optional[str] = None

class BACnetClient(BIPSimpleApplication, ReadWritePropertyServices):
    """
    Comprehensive BACnet client for building automation systems
    """
    
    def __init__(self, local_device_config: Dict, interface: str = None):
        """
        Initialize BACnet client
        
        Args:
            local_device_config: Local device configuration
            interface: Network interface (e.g., "192.168.1.100/24")
        """
        # Configure local device
        self.local_device = LocalDeviceObject(**local_device_config)
        
        # Initialize BACnet application
        super().__init__(self.local_device, interface)
        
        # Client state
        self.devices = {}  # Cache of discovered devices
        self.subscriptions = {}  # COV subscriptions
        self.request_queue = asyncio.Queue()
        self.response_handlers = {}
        
        # Logging
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Start the client
        self._running = False
        
    async def start(self):
        """Start the BACnet client"""
        self._running = True
        self.logger.info("Starting BACnet client...")
        
        # Start processing requests
        asyncio.create_task(self._process_requests())
        
        # Start BACnet core (this runs the BACnet stack)
        deferred(run)
        
    async def stop(self):
        """Stop the BACnet client"""
        self._running = False
        self.logger.info("Stopping BACnet client...")
        stop()
        
    async def discover_devices(self, timeout: float = 30.0) -> List[Dict]:
        """
        Discover BACnet devices on the network using Who-Is
        
        Returns:
            List of discovered device information
        """
        self.logger.info("Starting device discovery...")
        
        discovered_devices = []
        
        # Send Who-Is broadcast
        request = WhoIsRequest()
        request.pduDestination = Address("255.255.255.255")  # Broadcast
        
        # Set up response handler
        discovery_responses = []
        
        def handle_iam(iocb):
            if iocb.ioResponse:
                device_info = self._parse_iam_response(iocb.ioResponse)
                if device_info:
                    discovery_responses.append(device_info)
        
        # Send request and wait for responses
        iocb = IOCB(request)
        iocb.add_callback(handle_iam)
        self.request(iocb)
        
        # Wait for responses
        await asyncio.sleep(timeout)
        
        # Process discovered devices
        for device_info in discovery_responses:
            device_details = await self._get_device_details(device_info)
            discovered_devices.append(device_details)
            
        self.logger.info(f"Discovered {len(discovered_devices)} devices")
        return discovered_devices
    
    def _parse_iam_response(self, response) -> Optional[Dict]:
        """Parse I-Am response to extract device information"""
        try:
            device_instance = response.iAmDeviceIdentifier[1]
            ip_address = str(response.pduSource)
            
            return {
                'device_instance': device_instance,
                'ip_address': ip_address,
                'max_apdu_length': response.maxAPDULengthAccepted,
                'segmentation_supported': response.segmentationSupported,
                'vendor_id': response.vendorID
            }
        except Exception as e:
            self.logger.error(f"Error parsing I-Am response: {e}")
            return None
    
    async def _get_device_details(self, device_info: Dict) -> Dict:
        """Get detailed information about a device"""
        device_instance = device_info['device_instance']
        device_address = device_info['ip_address']
        
        # Read standard device properties
        properties_to_read = [
            'objectName', 'vendorName', 'modelName', 'firmwareRevision',
            'applicationSoftwareVersion', 'description', 'location'
        ]
        
        device_details = device_info.copy()
        
        for prop in properties_to_read:
            try:
                value = await self.read_property(
                    device_address, 
                    'device', 
                    device_instance, 
                    prop
                )
                device_details[prop] = value
            except Exception as e:
                self.logger.warning(f"Could not read {prop} from device {device_instance}: {e}")
                device_details[prop] = None
        
        # Cache device info
        self.devices[device_instance] = device_details
        
        return device_details
    
    async def read_property(self, 
                           device_address: str, 
                           object_type: str, 
                           object_instance: int, 
                           property_name: str,
                           array_index: Optional[int] = None) -> Any:
        """
        Read a property from a BACnet object
        
        Args:
            device_address: Device IP address or instance
            object_type: BACnet object type (e.g., 'analogInput', 'device')
            object_instance: Object instance number
            property_name: Property to read (e.g., 'presentValue', 'objectName')
            array_index: Array index if reading array property
            
        Returns:
            Property value
        """
        try:
            # Create read property request
            request = ReadPropertyRequest(
                objectIdentifier=(object_type, object_instance),
                propertyIdentifier=property_name
            )
            
            if array_index is not None:
                request.propertyArrayIndex = array_index
            
            # Set destination
            if isinstance(device_address, str) and '.' in device_address:
                # IP address format
                request.pduDestination = Address(device_address)
            else:
                # Device instance format - look up in cache
                if int(device_address) in self.devices:
                    ip_addr = self.devices[int(device_address)]['ip_address']
                    request.pduDestination = Address(ip_addr)
                else:
                    raise ValueError(f"Device {device_address} not found in cache")
            
            # Send request and get response
            iocb = IOCB(request)
            self.request(iocb)
            
            # Wait for response with timeout
            await asyncio.wait_for(iocb.wait(), timeout=10.0)
            
            if iocb.ioError:
                raise Exception(f"BACnet error: {iocb.ioError}")
            
            # Extract value from response
            if isinstance(iocb.ioResponse, ReadPropertyACK):
                return self._extract_property_value(iocb.ioResponse.propertyValue)
            else:
                raise Exception(f"Unexpected response type: {type(iocb.ioResponse)}")
                
        except Exception as e:
            self.logger.error(f"Error reading property {property_name}: {e}")
            raise
    
    async def write_property(self, 
                            device_address: str, 
                            object_type: str, 
                            object_instance: int, 
                            property_name: str,
                            value: Any,
                            priority: int = 16) -> bool:
        """
        Write a property to a BACnet object
        
        Args:
            device_address: Device IP address or instance
            object_type: BACnet object type
            object_instance: Object instance number
            property_name: Property to write
            value: Value to write
            priority: Write priority (1-16, lower is higher priority)
            
        Returns:
            True if successful
        """
        try:
            # Create write property request
            request = WritePropertyRequest(
                objectIdentifier=(object_type, object_instance),
                propertyIdentifier=property_name
            )
            
            # Set property value based on type
            if isinstance(value, (int, float)):
                request.propertyValue = Real(float(value))
            elif isinstance(value, str):
                request.propertyValue = CharacterString(value)
            elif isinstance(value, bool):
                request.propertyValue = Boolean(value)
            else:
                # Try to automatically determine type
                request.propertyValue = value
            
            # Set priority for present value writes
            if property_name.lower() == 'presentvalue':
                request.priority = priority
            
            # Set destination
            if isinstance(device_address, str) and '.' in device_address:
                request.pduDestination = Address(device_address)
            else:
                if int(device_address) in self.devices:
                    ip_addr = self.devices[int(device_address)]['ip_address']
                    request.pduDestination = Address(ip_addr)
                else:
                    raise ValueError(f"Device {device_address} not found")
            
            # Send request
            iocb = IOCB(request)
            self.request(iocb)
            
            # Wait for response
            await asyncio.wait_for(iocb.wait(), timeout=10.0)
            
            if iocb.ioError:
                raise Exception(f"BACnet error: {iocb.ioError}")
            
            if isinstance(iocb.ioResponse, WritePropertyACK):
                self.logger.info(f"Successfully wrote {value} to {object_type}:{object_instance}.{property_name}")
                return True
            else:
                raise Exception(f"Unexpected response: {type(iocb.ioResponse)}")
                
        except Exception as e:
            self.logger.error(f"Error writing property {property_name}: {e}")
            return False
    
    async def read_multiple_properties(self, reads: List[tuple]) -> List[BACnetReading]:
        """
        Read multiple properties in a single request (more efficient)
        
        Args:
            reads: List of (device_address, object_type, object_instance, property_name) tuples
            
        Returns:
            List of BACnetReading objects
        """
        # Group reads by device for efficiency
        device_reads = {}
        for device_address, object_type, object_instance, property_name in reads:
            if device_address not in device_reads:
                device_reads[device_address] = []
            device_reads[device_address].append((object_type, object_instance, property_name))
        
        all_readings = []
        
        for device_address, device_read_list in device_reads.items():
            try:
                # Create ReadPropertyMultiple request
                request = ReadPropertyMultipleRequest()
                
                # Set destination
                if '.' in device_address:
                    request.pduDestination = Address(device_address)
                else:
                    ip_addr = self.devices[int(device_address)]['ip_address']
                    request.pduDestination = Address(ip_addr)
                
                # Build read access specification
                read_access_specs = []
                for object_type, object_instance, property_name in device_read_list:
                    read_access_spec = ReadAccessSpecification(
                        objectIdentifier=(object_type, object_instance),
                        listOfPropertyReferences=[PropertyReference(propertyIdentifier=property_name)]
                    )
                    read_access_specs.append(read_access_spec)
                
                request.listOfReadAccessSpecs = read_access_specs
                
                # Send request
                iocb = IOCB(request)
                self.request(iocb)
                await asyncio.wait_for(iocb.wait(), timeout=15.0)
                
                if iocb.ioError:
                    raise Exception(f"BACnet error: {iocb.ioError}")
                
                # Parse response
                if isinstance(iocb.ioResponse, ReadPropertyMultipleACK):
                    device_readings = self._parse_read_multiple_response(
                        iocb.ioResponse, device_address, device_read_list
                    )
                    all_readings.extend(device_readings)
                    
            except Exception as e:
                self.logger.error(f"Error reading multiple properties from {device_address}: {e}")
                # Create error readings for failed device
                for object_type, object_instance, property_name in device_read_list:
                    point = BACnetPoint(
                        device_id=device_address,
                        object_type=object_type,
                        object_instance=object_instance,
                        property_name=property_name
                    )
                    reading = BACnetReading(
                        point=point,
                        value=None,
                        timestamp=datetime.now(),
                        quality="error",
                        error=str(e)
                    )
                    all_readings.append(reading)
        
        return all_readings
    
    def _parse_read_multiple_response(self, response, device_address: str, expected_reads: List) -> List[BACnetReading]:
        """Parse ReadPropertyMultiple response"""
        readings = []
        
        try:
            for i, read_access_result in enumerate(response.listOfReadAccessResults):
                object_type, object_instance, property_name = expected_reads[i]
                
                point = BACnetPoint(
                    device_id=device_address,
                    object_type=object_type,
                    object_instance=object_instance,
                    property_name=property_name
                )
                
                if read_access_result.listOfResults:
                    property_result = read_access_result.listOfResults[0]
                    
                    if hasattr(property_result, 'propertyValue'):
                        value = self._extract_property_value(property_result.propertyValue)
                        reading = BACnetReading(
                            point=point,
                            value=value,
                            timestamp=datetime.now(),
                            quality="good"
                        )
                    else:
                        # Property access error
                        error_code = getattr(property_result, 'propertyAccessError', 'Unknown error')
                        reading = BACnetReading(
                            point=point,
                            value=None,
                            timestamp=datetime.now(),
                            quality="error",
                            error=str(error_code)
                        )
                else:
                    reading = BACnetReading(
                        point=point,
                        value=None,
                        timestamp=datetime.now(),
                        quality="error",
                        error="No results returned"
                    )
                
                readings.append(reading)
                
        except Exception as e:
            self.logger.error(f"Error parsing ReadPropertyMultiple response: {e}")
        
        return readings
    
    def _extract_property_value(self, property_value) -> Any:
        """Extract the actual value from a BACnet property value"""
        try:
            if hasattr(property_value, 'value'):
                return property_value.value
            else:
                # For primitive types
                return property_value
        except Exception as e:
            self.logger.error(f"Error extracting property value: {e}")
            return None
    
    async def subscribe_cov(self, 
                           device_address: str, 
                           object_type: str, 
                           object_instance: int,
                           callback,
                           confirmed: bool = True,
                           lifetime: int = 300) -> bool:
        """
        Subscribe to Change of Value (COV) notifications
        
        Args:
            device_address: Device address
            object_type: Object type to monitor
            object_instance: Object instance
            callback: Function to call when value changes
            confirmed: Use confirmed notifications
            lifetime: Subscription lifetime in seconds
            
        Returns:
            True if subscription successful
        """
        try:
            # Create subscription request
            request = SubscribeCOVRequest(
                subscriberProcessIdentifier=self._get_next_process_id(),
                monitoredObjectIdentifier=(object_type, object_instance),
                issueConfirmedNotifications=confirmed,
                lifetime=lifetime
            )
            
            # Set destination
            if '.' in device_address:
                request.pduDestination = Address(device_address)
            else:
                ip_addr = self.devices[int(device_address)]['ip_address']
                request.pduDestination = Address(ip_addr)
            
            # Send subscription request
            iocb = IOCB(request)
            self.request(iocb)
            await asyncio.wait_for(iocb.wait(), timeout=10.0)
            
            if iocb.ioError:
                raise Exception(f"COV subscription error: {iocb.ioError}")
            
            # Store subscription info
            sub_key = f"{device_address}:{object_type}:{object_instance}"
            self.subscriptions[sub_key] = {
                'callback': callback,
                'confirmed': confirmed,
                'lifetime': lifetime,
                'subscription_time': datetime.now()
            }
            
            self.logger.info(f"COV subscription successful for {sub_key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error subscribing to COV: {e}")
            return False
    
    def _get_next_process_id(self) -> int:
        """Get next available process ID for subscriptions"""
        # Simple incrementing counter
        if not hasattr(self, '_process_id_counter'):
            self._process_id_counter = 1
        else:
            self._process_id_counter += 1
        return self._process_id_counter
    
    def indication(self, apdu):
        """Handle incoming BACnet indications (like COV notifications)"""
        if isinstance(apdu, ConfirmedCOVNotificationRequest):
            self._handle_cov_notification(apdu)
            
        # Call parent indication handler
        super().indication(apdu)
    
    def _handle_cov_notification(self, notification):
        """Handle incoming COV notification"""
        try:
            device_instance = notification.initiatingDeviceIdentifier[1]
            object_type = notification.monitoredObjectIdentifier[0]
            object_instance = notification.monitoredObjectIdentifier[1]
            
            # Find subscription
            sub_key = f"{device_instance}:{object_type}:{object_instance}"
            if sub_key in self.subscriptions:
                subscription = self.subscriptions[sub_key]
                
                # Extract current value from notification
                current_value = None
                if notification.listOfValues:
                    for prop_value in notification.listOfValues:
                        if prop_value.propertyIdentifier == 'presentValue':
                            current_value = self._extract_property_value(prop_value.value)
                            break
                
                # Call callback with the new value
                if subscription['callback']:
                    try:
                        subscription['callback'](sub_key, current_value, datetime.now())
                    except Exception as callback_error:
                        self.logger.error(f"Error in COV callback: {callback_error}")
                        
        except Exception as e:
            self.logger.error(f"Error handling COV notification: {e}")
    
    async def _process_requests(self):
        """Process queued requests asynchronously"""
        while self._running:
            try:
                # This is a placeholder for any async request processing
                await asyncio.sleep(0.1)
            except Exception as e:
                self.logger.error(f"Error processing requests: {e}")

# Example usage and integration class
class BACnetHVACInterface:
    """
    High-level interface for HVAC control via BACnet
    Designed to integrate with RL agents
    """
    
    def __init__(self, config_file: str):
        with open(config_file, 'r') as f:
            self.config = json.load(f)
        
        # Initialize BACnet client
        self.client = BACnetClient(
            local_device_config=self.config['local_device'],
            interface=self.config.get('interface')
        )
        
        # Map RL agent observations to BACnet points
        self.observation_points = self._load_observation_points()
        self.action_points = self._load_action_points()
        
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _load_observation_points(self) -> List[BACnetPoint]:
        """Load observation points from configuration"""
        points = []
        for point_config in self.config.get('observation_points', []):
            point = BACnetPoint(**point_config)
            points.append(point)
        return points
    
    def _load_action_points(self) -> List[BACnetPoint]:
        """Load action points from configuration"""
        points = []
        for point_config in self.config.get('action_points', []):
            point = BACnetPoint(**point_config)
            points.append(point)
        return points
    
    async def start(self):
        """Start the BACnet interface"""
        await self.client.start()
        
        # Discover devices if not configured
        if not self.client.devices:
            await self.client.discover_devices()
    
    async def stop(self):
        """Stop the BACnet interface"""
        await self.client.stop()
    
    async def get_observations(self) -> Dict[str, float]:
        """
        Get current observations for RL agent
        
        Returns:
            Dictionary mapping observation names to values
        """
        # Build read requests
        reads = []
        for point in self.observation_points:
            reads.append((
                point.device_id,
                point.object_type,
                point.object_instance,
                point.property_name
            ))
        
        # Read all points
        readings = await self.client.read_multiple_properties(reads)
        
        # Convert to observation dictionary
        observations = {}
        for i, reading in enumerate(readings):
            point = self.observation_points[i]
            obs_name = f"{point.object_type}_{point.object_instance}_{point.property_name}"
            
            if reading.quality == "good" and reading.value is not None:
                observations[obs_name] = float(reading.value)
            else:
                # Use last known value or default
                self.logger.warning(f"Bad reading for {obs_name}: {reading.error}")
                observations[obs_name] = 0.0  # Or use cached value
        
        return observations
    
    async def execute_actions(self, actions: Dict[str, float]) -> bool:
        """
        Execute actions from RL agent
        
        Args:
            actions: Dictionary mapping action names to values
            
        Returns:
            True if all actions executed successfully
        """
        success = True
        
        for action_name, value in actions.items():
            # Find corresponding action point
            action_point = None
            for point in self.action_points:
                point_name = f"{point.object_type}_{point.object_instance}_{point.property_name}"
                if point_name == action_name:
                    action_point = point
                    break
            
            if action_point:
                write_success = await self.client.write_property(
                    action_point.device_id,
                    action_point.object_type,
                    action_point.object_instance,
                    action_point.property_name,
                    value
                )
                if not write_success:
                    success = False
                    self.logger.error(f"Failed to write action {action_name}")
            else:
                self.logger.error(f"Action point not found: {action_name}")
                success = False
        
        return success

# Configuration example
def create_config_example():
    """Create an example configuration file"""
    config = {
        "local_device": {
            "objectName": "RL Agent BACnet Client",
            "objectIdentifier": ("device", 599),
            "description": "Reinforcement Learning HVAC Controller",
            "vendorIdentifier": 999,
            "vendorName": "RL Systems Inc"
        },
        "interface": "192.168.1.100/24",
        "observation_points": [
            {
                "device_id": 1001,
                "object_type": "analogInput",
                "object_instance": 1,
                "property_name": "presentValue",
                "description": "Zone Temperature",
                "units": "degreesCelsius"
            },
            {
                "device_id": 1001,
                "object_type": "analogInput", 
                "object_instance": 2,
                "property_name": "presentValue",
                "description": "Zone Humidity",
                "units": "percent"
            }
        ],
        "action_points": [
            {
                "device_id": 1001,
                "object_type": "analogOutput",
                "object_instance": 1,
                "property_name": "presentValue",
                "description": "Cooling Setpoint",
                "units": "degreesCelsius",
                "writeable": True
            },
            {
                "device_id": 1001,
                "object_type": "analogOutput",
                "object_instance": 2,
                "property_name": "presentValue", 
                "description": "Heating Setpoint",
                "units": "degreesCelsius",
                "writeable": True
            }
        ]
    }
    
    with open('bacnet_config.json', 'w') as f:
        json.dump(config, f, indent=2)
    
    print("Created bacnet_config.json")

# Example usage
async def main():
    # Create example config
    create_config_example()
    
    # Initialize interface
    interface = BACnetHVACInterface('bacnet_config.json')
    
    try:
        # Start interface
        await interface.start()
        
        # Example: Get observations
        observations = await interface.get_observations()
        print(f"Current observations: {observations}")
        
        # Example: Execute actions  
        actions = {
            "analogOutput_1_presentValue": 24.0,  # Cooling setpoint
            "analogOutput_2_presentValue": 20.0   # Heating setpoint
        }
        success = await interface.execute_actions(actions)
        print(f"Actions executed successfully: {success}")
        
        # Keep running for a while
        await asyncio.sleep(60)
        
    finally:
        await interface.stop()

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the example
    asyncio.run(main())
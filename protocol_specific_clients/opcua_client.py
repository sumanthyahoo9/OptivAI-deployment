"""
Core OPC-UA Client Module for Industrial Manufacturing Integration
Handles connection management, security, and basic read/write operations
Designed for RL agent integration in energy optimization systems
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
from dataclasses import dataclass
from enum import Enum
import json
import ssl
from pathlib import Path

# OPC-UA imports
from asyncua import Client, Node, ua
from asyncua.crypto.security_policies import SecurityPolicy
from asyncua.ua.uaerrors import BadUserAccessDenied, BadConnectionClosed, BadSessionClosed
from asyncua.common.subscription import Subscription
from asyncua.common.node import Node

# Pydantic for data validation (matching your existing infrastructure)
from pydantic import BaseModel, Field, validator
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# Data Models and Enums
# ============================================================================

class SecurityMode(str, Enum):
    """OPC-UA Security Modes"""
    NONE = "None"
    SIGN = "Sign" 
    SIGN_AND_ENCRYPT = "SignAndEncrypt"

class MessageSecurityMode(str, Enum):
    """OPC-UA Message Security Modes"""
    NONE = "None"
    BASIC128RSA15 = "Basic128Rsa15"
    BASIC256 = "Basic256"
    BASIC256SHA256 = "Basic256Sha256"

class AuthenticationType(str, Enum):
    """Authentication types"""
    ANONYMOUS = "anonymous"
    USERNAME_PASSWORD = "username_password"
    CERTIFICATE = "certificate"

class NodeDataType(str, Enum):
    """Common OPC-UA data types"""
    BOOLEAN = "Boolean"
    BYTE = "Byte"
    INT16 = "Int16"
    INT32 = "Int32"
    INT64 = "Int64"
    UINT16 = "UInt16"
    UINT32 = "UInt32"
    UINT64 = "UInt64"
    FLOAT = "Float"
    DOUBLE = "Double"
    STRING = "String"
    DATETIME = "DateTime"
    GUID = "Guid"

class ConnectionStatus(str, Enum):
    """Connection status enumeration"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"

@dataclass
class ServerEndpoint:
    """OPC-UA Server endpoint configuration"""
    url: str
    server_name: str = ""
    description: str = ""
    security_mode: SecurityMode = SecurityMode.NONE
    security_policy: MessageSecurityMode = MessageSecurityMode.NONE
    
class ClientConfiguration(BaseModel):
    """OPC-UA Client configuration"""
    client_name: str = "RL_HVAC_Client"
    application_uri: str = "urn:hvac:rl:client"
    product_name: str = "HVAC RL OPC-UA Client"
    
    # Connection settings
    session_timeout: int = 60000  # milliseconds
    secure_channel_timeout: int = 60000  # milliseconds
    request_timeout: float = 30.0  # seconds
    
    # Security settings
    certificate_path: Optional[str] = None
    private_key_path: Optional[str] = None
    trusted_certs_path: Optional[str] = None
    
    # Authentication
    auth_type: AuthenticationType = AuthenticationType.ANONYMOUS
    username: Optional[str] = None
    password: Optional[str] = None
    
    # Connection behavior
    auto_reconnect: bool = True
    reconnect_delay: float = 5.0  # seconds
    max_reconnect_attempts: int = -1  # -1 = infinite
    keep_alive_interval: int = 5000  # milliseconds
    
    @validator('certificate_path', 'private_key_path', 'trusted_certs_path')
    def validate_paths(cls, v):
        """
        Ensure the path exists
        """
        if v and not Path(v).exists():
            raise ValueError(f"Path does not exist: {v}")
        return v

@dataclass
class OPCUAReading:
    """Represents a reading from an OPC-UA node"""
    node_id: str
    value: Any
    timestamp: datetime
    status_code: str
    server_timestamp: Optional[datetime] = None
    source_timestamp: Optional[datetime] = None
    quality: str = "Good"
    data_type: Optional[str] = None

@dataclass 
class OPCUAWriteResult:
    """Result of an OPC-UA write operation"""
    node_id: str
    success: bool
    status_code: str
    timestamp: datetime
    error_message: Optional[str] = None

# ============================================================================
# Core OPC-UA Client Implementation
# ============================================================================

class OPCUAClient:
    """
    Core OPC-UA client for industrial manufacturing integration
    Handles connection management, security, and basic operations
    """
    
    def __init__(
        self,
        endpoint: ServerEndpoint,
        config: Optional[ClientConfiguration] = None
    ):
        self.endpoint = endpoint
        self.config = config or ClientConfiguration()
        
        # Client state
        self.client: Optional[Client] = None
        self.status = ConnectionStatus.DISCONNECTED
        self.last_connection_time: Optional[datetime] = None
        self.last_error: Optional[str] = None
        self.connection_attempts = 0
        
        # Node cache for performance
        self.node_cache: Dict[str, Node] = {}
        
        # Monitoring and statistics
        self.stats = {
            'total_reads': 0,
            'total_writes': 0,
            'successful_reads': 0,
            'successful_writes': 0,
            'connection_count': 0,
            'last_activity': None
        }
        
        # Background tasks
        self._reconnect_task: Optional[asyncio.Task] = None
        self._keepalive_task: Optional[asyncio.Task] = None
        self._running = False
        
        # Logger
        self.logger = logging.getLogger(f"{__name__}.{self.endpoint.server_name}")
        
    async def initialize(self) -> bool:
        """Initialize the OPC-UA client"""
        try:
            self.logger.info(f"Initializing OPC-UA client for {self.endpoint.url}")
            
            # Create client instance
            self.client = Client(url=self.endpoint.url)
            
            # Configure client
            await self._configure_client()
            
            # Configure security if needed
            if self.endpoint.security_mode != SecurityMode.NONE:
                await self._configure_security()
            
            self.logger.info("OPC-UA client initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize OPC-UA client: {e}")
            self.last_error = str(e)
            return False
    
    async def _configure_client(self) -> None:
        """Configure the OPC-UA client with application settings"""
        if not self.client:
            raise RuntimeError("Client not initialized")
        
        # Set application description
        self.client.name = self.config.client_name
        self.client.description = f"RL HVAC Client for {self.endpoint.server_name}"
        self.client.application_uri = self.config.application_uri
        self.client.product_uri = self.config.application_uri
        
        # Set session parameters
        self.client.session_timeout = self.config.session_timeout
        self.client.secure_channel_timeout = self.config.secure_channel_timeout
        self.client.request_timeout = self.config.request_timeout
        
        # Set keepalive
        self.client.keepalive = self.config.keep_alive_interval
        
    async def _configure_security(self) -> None:
        """Configure OPC-UA security settings"""
        if not self.client:
            raise RuntimeError("Client not initialized")
        
        try:
            # Load certificates if provided
            if self.config.certificate_path and self.config.private_key_path:
                await self.client.load_client_certificate(
                    self.config.certificate_path,
                    self.config.private_key_path
                )
                self.logger.info("Client certificate loaded")
            
            # Set trusted certificates path
            if self.config.trusted_certs_path:
                await self.client.load_trusted_certs(self.config.trusted_certs_path)
                self.logger.info("Trusted certificates loaded")
            
            # Set security policy
            security_string = f"{self.endpoint.security_policy.value}_{self.endpoint.security_mode.value}"
            await self.client.set_security_string(security_string)
            
            self.logger.info(f"Security configured: {security_string}")
            
        except Exception as e:
            self.logger.error(f"Failed to configure security: {e}")
            raise
    
    async def connect(self) -> bool:
        """Connect to the OPC-UA server"""
        if self.status == ConnectionStatus.CONNECTED:
            self.logger.warning("Already connected")
            return True
        
        try:
            self.status = ConnectionStatus.CONNECTING
            self.connection_attempts += 1
            
            self.logger.info(f"Connecting to OPC-UA server: {self.endpoint.url}")
            
            # Connect to server
            await self.client.connect()
            
            # Authenticate if required
            if self.config.auth_type == AuthenticationType.USERNAME_PASSWORD:
                if self.config.username and self.config.password:
                    await self.client.set_user(self.config.username, self.config.password)
                    self.logger.info("Authenticated with username/password")
                else:
                    raise ValueError("Username/password required but not provided")
            
            # Update status
            self.status = ConnectionStatus.CONNECTED
            self.last_connection_time = datetime.utcnow()
            self.stats['connection_count'] += 1
            self.last_error = None
            
            # Start background tasks
            self._running = True
            if self.config.auto_reconnect:
                self._keepalive_task = asyncio.create_task(self._keepalive_loop())
            
            self.logger.info("Successfully connected to OPC-UA server")
            return True
            
        except Exception as e:
            self.status = ConnectionStatus.ERROR
            self.last_error = str(e)
            self.logger.error(f"Failed to connect to OPC-UA server: {e}")
            
            # Start reconnection if auto-reconnect is enabled
            if self.config.auto_reconnect and not self._reconnect_task:
                self._reconnect_task = asyncio.create_task(self._reconnect_loop())
            
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from the OPC-UA server"""
        try:
            self._running = False
            
            # Cancel background tasks
            if self._keepalive_task:
                self._keepalive_task.cancel()
                try:
                    await self._keepalive_task
                except asyncio.CancelledError:
                    pass
                self._keepalive_task = None
            
            if self._reconnect_task:
                self._reconnect_task.cancel()
                try:
                    await self._reconnect_task
                except asyncio.CancelledError:
                    pass
                self._reconnect_task = None
            
            # Disconnect from server
            if self.client and self.status == ConnectionStatus.CONNECTED:
                await self.client.disconnect()
                self.logger.info("Disconnected from OPC-UA server")
            
            self.status = ConnectionStatus.DISCONNECTED
            self.node_cache.clear()
            
        except Exception as e:
            self.logger.error(f"Error during disconnect: {e}")
    
    async def _reconnect_loop(self) -> None:
        """Background task for automatic reconnection"""
        reconnect_attempts = 0
        
        while self._running and self.config.auto_reconnect:
            if self.status != ConnectionStatus.CONNECTED:
                try:
                    self.status = ConnectionStatus.RECONNECTING
                    reconnect_attempts += 1
                    
                    self.logger.info(f"Reconnection attempt {reconnect_attempts}")
                    
                    # Check if we should stop trying
                    if (self.config.max_reconnect_attempts > 0 and 
                        reconnect_attempts > self.config.max_reconnect_attempts):
                        self.logger.error("Maximum reconnection attempts reached")
                        break
                    
                    # Attempt to reconnect
                    if await self.connect():
                        self.logger.info("Reconnection successful")
                        reconnect_attempts = 0
                        break
                    
                except Exception as e:
                    self.logger.error(f"Reconnection attempt failed: {e}")
                
                # Wait before next attempt
                await asyncio.sleep(self.config.reconnect_delay)
            else:
                break
    
    async def _keepalive_loop(self) -> None:
        """Background task for connection keepalive"""
        while self._running:
            try:
                if self.status == ConnectionStatus.CONNECTED:
                    # Read server status to verify connection
                    server_state = await self.client.get_server_node().read_attribute(ua.AttributeIds.Value)
                    self.stats['last_activity'] = datetime.utcnow()
                
                await asyncio.sleep(self.config.keep_alive_interval / 1000.0)
                
            except Exception as e:
                self.logger.warning(f"Keepalive failed: {e}")
                self.status = ConnectionStatus.ERROR
                self.last_error = str(e)
                
                # Trigger reconnection
                if self.config.auto_reconnect and not self._reconnect_task:
                    self._reconnect_task = asyncio.create_task(self._reconnect_loop())
                
                break
    
    # ========================================================================
    # Node Operations
    # ========================================================================
    
    async def get_node(self, node_id: str) -> Optional[Node]:
        """Get a node by its identifier, with caching"""
        try:
            # Check cache first
            if node_id in self.node_cache:
                return self.node_cache[node_id]
            
            # Get node from server
            node = self.client.get_node(node_id)
            
            # Verify node exists
            await node.read_browse_name()
            
            # Cache the node
            self.node_cache[node_id] = node
            
            return node
            
        except Exception as e:
            self.logger.error(f"Failed to get node {node_id}: {e}")
            return None
    
    async def browse_nodes(self, parent_node_id: str = "i=85") -> List[Dict[str, Any]]:
        """Browse child nodes of a parent node"""
        try:
            parent_node = await self.get_node(parent_node_id)
            if not parent_node:
                return []
            
            children = await parent_node.get_children()
            node_info = []
            
            for child in children:
                try:
                    info = {
                        'node_id': child.nodeid.to_string(),
                        'browse_name': (await child.read_browse_name()).Name,
                        'display_name': (await child.read_display_name()).Text,
                        'node_class': (await child.read_node_class()).name,
                        'data_type': None,
                        'access_level': None
                    }
                    
                    # Get additional info for variables
                    if info['node_class'] == 'Variable':
                        try:
                            data_type = await child.read_data_type()
                            info['data_type'] = data_type.Identifier
                            
                            access_level = await child.read_attribute(ua.AttributeIds.AccessLevel)
                            info['access_level'] = access_level.Value
                        except:
                            pass
                    
                    node_info.append(info)
                    
                except Exception as e:
                    self.logger.warning(f"Failed to read node info: {e}")
                    continue
            
            self.logger.debug(f"Browsed {len(node_info)} nodes under {parent_node_id}")
            return node_info
            
        except Exception as e:
            self.logger.error(f"Failed to browse nodes: {e}")
            return []
    
    async def read_node(self, node_id: str) -> Optional[OPCUAReading]:
        """Read a single node value"""
        try:
            self.stats['total_reads'] += 1
            
            node = await self.get_node(node_id)
            if not node:
                return None
            
            # Read the value
            data_value = await node.read_value()
            
            # Read timestamps if available
            timestamps = await node.read_data_value()
            
            reading = OPCUAReading(
                node_id=node_id,
                value=data_value,
                timestamp=datetime.utcnow(),
                status_code=timestamps.StatusCode.name,
                server_timestamp=timestamps.ServerTimestamp,
                source_timestamp=timestamps.SourceTimestamp,
                quality="Good" if timestamps.StatusCode.is_good() else "Bad"
            )
            
            # Try to get data type
            try:
                data_type = await node.read_data_type()
                reading.data_type = str(data_type.Identifier)
            except:
                pass
            
            self.stats['successful_reads'] += 1
            self.stats['last_activity'] = datetime.utcnow()
            
            return reading
            
        except Exception as e:
            self.logger.error(f"Failed to read node {node_id}: {e}")
            return None
    
    async def read_multiple_nodes(self, node_ids: List[str]) -> List[OPCUAReading]:
        """Read multiple nodes efficiently"""
        readings = []
        
        try:
            # Get all nodes
            nodes = []
            valid_node_ids = []
            
            for node_id in node_ids:
                node = await self.get_node(node_id)
                if node:
                    nodes.append(node)
                    valid_node_ids.append(node_id)
            
            if not nodes:
                return readings
            
            # Read all values at once
            data_values = await self.client.read_values(nodes)
            
            # Process results
            for i, (node_id, data_value) in enumerate(zip(valid_node_ids, data_values)):
                try:
                    reading = OPCUAReading(
                        node_id=node_id,
                        value=data_value.Value,
                        timestamp=datetime.utcnow(),
                        status_code=data_value.StatusCode.name,
                        server_timestamp=data_value.ServerTimestamp,
                        source_timestamp=data_value.SourceTimestamp,
                        quality="Good" if data_value.StatusCode.is_good() else "Bad"
                    )
                    readings.append(reading)
                    
                except Exception as e:
                    self.logger.warning(f"Failed to process reading for {node_id}: {e}")
            
            self.stats['total_reads'] += len(node_ids)
            self.stats['successful_reads'] += len(readings)
            self.stats['last_activity'] = datetime.utcnow()
            
        except Exception as e:
            self.logger.error(f"Failed to read multiple nodes: {e}")
        
        return readings
    
    async def write_node(self, node_id: str, value: Any, data_type: Optional[str] = None) -> OPCUAWriteResult:
        """Write a value to a node"""
        try:
            self.stats['total_writes'] += 1
            
            node = await self.get_node(node_id)
            if not node:
                return OPCUAWriteResult(
                    node_id=node_id,
                    success=False,
                    status_code="BadNodeIdUnknown",
                    timestamp=datetime.utcnow(),
                    error_message="Node not found"
                )
            
            # Convert value to appropriate OPC-UA type if needed
            if data_type:
                value = self._convert_to_opcua_type(value, data_type)
            
            # Write the value
            status_code = await node.write_value(value)
            
            result = OPCUAWriteResult(
                node_id=node_id,
                success=status_code.is_good(),
                status_code=status_code.name,
                timestamp=datetime.utcnow()
            )
            
            if result.success:
                self.stats['successful_writes'] += 1
            else:
                result.error_message = f"Write failed with status: {status_code.name}"
            
            self.stats['last_activity'] = datetime.utcnow()
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to write to node {node_id}: {e}")
            return OPCUAWriteResult(
                node_id=node_id,
                success=False,
                status_code="BadUnexpectedError",
                timestamp=datetime.utcnow(),
                error_message=str(e)
            )
    
    async def write_multiple_nodes(self, writes: List[Tuple[str, Any, Optional[str]]]) -> List[OPCUAWriteResult]:
        """Write values to multiple nodes"""
        results = []
        
        try:
            # Prepare nodes and values
            nodes = []
            values = []
            node_ids = []
            
            for node_id, value, data_type in writes:
                node = await self.get_node(node_id)
                if node:
                    nodes.append(node)
                    # Convert value if data type specified
                    if data_type:
                        value = self._convert_to_opcua_type(value, data_type)
                    values.append(value)
                    node_ids.append(node_id)
                else:
                    # Add failed result for missing node
                    results.append(OPCUAWriteResult(
                        node_id=node_id,
                        success=False,
                        status_code="BadNodeIdUnknown",
                        timestamp=datetime.utcnow(),
                        error_message="Node not found"
                    ))
            
            if not nodes:
                return results
            
            # Write all values at once
            status_codes = await self.client.write_values(nodes, values)
            
            # Process results
            for node_id, status_code in zip(node_ids, status_codes):
                result = OPCUAWriteResult(
                    node_id=node_id,
                    success=status_code.is_good(),
                    status_code=status_code.name,
                    timestamp=datetime.utcnow()
                )
                
                if not result.success:
                    result.error_message = f"Write failed with status: {status_code.name}"
                
                results.append(result)
            
            self.stats['total_writes'] += len(writes)
            self.stats['successful_writes'] += sum(1 for r in results if r.success)
            self.stats['last_activity'] = datetime.utcnow()
            
        except Exception as e:
            self.logger.error(f"Failed to write multiple nodes: {e}")
            # Add error results for any remaining writes
            for node_id, _, _ in writes[len(results):]:
                results.append(OPCUAWriteResult(
                    node_id=node_id,
                    success=False,
                    status_code="BadUnexpectedError",
                    timestamp=datetime.utcnow(),
                    error_message=str(e)
                ))
        
        return results
    
    def _convert_to_opcua_type(self, value: Any, data_type: str) -> Any:
        """Convert Python value to appropriate OPC-UA type"""
        try:
            if data_type == NodeDataType.BOOLEAN:
                return bool(value)
            elif data_type in [NodeDataType.BYTE, NodeDataType.INT16, NodeDataType.INT32, NodeDataType.INT64]:
                return int(value)
            elif data_type in [NodeDataType.UINT16, NodeDataType.UINT32, NodeDataType.UINT64]:
                return abs(int(value))
            elif data_type in [NodeDataType.FLOAT, NodeDataType.DOUBLE]:
                return float(value)
            elif data_type == NodeDataType.STRING:
                return str(value)
            elif data_type == NodeDataType.DATETIME:
                if isinstance(value, datetime):
                    return value
                else:
                    return datetime.fromisoformat(str(value))
            else:
                return value
        except Exception as e:
            self.logger.warning(f"Failed to convert value {value} to type {data_type}: {e}")
            return value
    
    # ========================================================================
    # Status and Monitoring
    # ========================================================================
    
    def get_connection_status(self) -> Dict[str, Any]:
        """Get current connection status and statistics"""
        return {
            'endpoint': self.endpoint.url,
            'server_name': self.endpoint.server_name,
            'status': self.status.value,
            'last_connection_time': self.last_connection_time.isoformat() if self.last_connection_time else None,
            'last_error': self.last_error,
            'connection_attempts': self.connection_attempts,
            'stats': self.stats.copy(),
            'cached_nodes': len(self.node_cache)
        }
    
    def is_connected(self) -> bool:
        """Check if client is connected"""
        return self.status == ConnectionStatus.CONNECTED
    
    async def test_connection(self) -> Tuple[bool, Optional[str]]:
        """Test the connection by reading server status"""
        try:
            if not self.is_connected():
                return False, "Not connected"
            
            # Try to read server state
            server_node = self.client.get_server_node()
            state = await server_node.read_attribute(ua.AttributeIds.Value)
            
            return True, f"Server state: {state.Value}"
            
        except Exception as e:
            return False, str(e)
    
    async def get_server_info(self) -> Dict[str, Any]:
        """Get server information"""
        try:
            if not self.is_connected():
                return {'error': 'Not connected'}
            
            # Get server object
            server_node = self.client.get_server_node()
            
            # Read server attributes
            info = {
                'server_uri': self.endpoint.url,
                'state': None,
                'build_info': {},
                'namespaces': [],
                'endpoints': []
            }
            
            # Server state
            try:
                state = await server_node.read_attribute(ua.AttributeIds.Value)
                info['state'] = state.Value
            except:
                pass
            
            # Build info
            try:
                build_info_node = await server_node.get_child("Server", "ServerStatus", "BuildInfo")
                build_info = {}
                
                for attr in ['ProductUri', 'ManufacturerName', 'ProductName', 'SoftwareVersion']:
                    try:
                        child = await build_info_node.get_child(attr)
                        value = await child.read_value()
                        build_info[attr.lower()] = value
                    except:
                        pass
                
                info['build_info'] = build_info
            except:
                pass
            
            # Namespaces
            try:
                ns_array = await self.client.get_namespace_array()
                info['namespaces'] = ns_array
            except:
                pass
            
            return info
            
        except Exception as e:
            self.logger.error(f"Failed to get server info: {e}")
            return {'error': str(e)}

# ============================================================================
# Context Manager and Factory Functions
# ============================================================================

@asynccontextmanager
async def managed_opcua_client(endpoint: ServerEndpoint, config: Optional[ClientConfiguration] = None):
    """Context manager for automatic connection management"""
    client = OPCUAClient(endpoint, config)
    
    try:
        if await client.initialize():
            if await client.connect():
                yield client
            else:
                raise RuntimeError("Failed to connect to OPC-UA server")
        else:
            raise RuntimeError("Failed to initialize OPC-UA client")
    finally:
        await client.disconnect()

async def create_opcua_client(
    url: str,
    server_name: str = "",
    security_mode: SecurityMode = SecurityMode.NONE,
    config: Optional[ClientConfiguration] = None
) -> OPCUAClient:
    """Factory function to create and initialize OPC-UA client"""
    
    endpoint = ServerEndpoint(
        url=url,
        server_name=server_name or f"Server_{url.split('/')[-1]}",
        security_mode=security_mode
    )
    
    client = OPCUAClient(endpoint, config)
    
    if not await client.initialize():
        raise RuntimeError(f"Failed to initialize OPC-UA client for {url}")
    
    return client

# ============================================================================
# Example Usage
# ============================================================================

async def main():
    """Example usage of the OPC-UA client"""
    
    # Create client configuration
    config = ClientConfiguration(
        client_name="HVAC_RL_Client",
        auto_reconnect=True,
        keep_alive_interval=5000
    )
    
    # Create endpoint
    endpoint = ServerEndpoint(
        url="opc.tcp://localhost:4840",
        server_name="Local_OPC_Server",
        security_mode=SecurityMode.NONE
    )
    
    # Use context manager for automatic cleanup
    async with managed_opcua_client(endpoint, config) as client:
        
        # Test connection
        connected, status = await client.test_connection()
        print(f"Connection test: {connected}, {status}")
        
        # Get server info
        server_info = await client.get_server_info()
        print(f"Server info: {server_info}")
        
        # Browse nodes
        nodes = await client.browse_nodes()
        print(f"Found {len(nodes)} nodes")
        
        # Read some values (example node IDs)
        node_ids = ["ns=2;i=2", "ns=2;i=3", "ns=2;i=4"]
        readings = await client.read_multiple_nodes(node_ids)
        
        for reading in readings:
            print(f"Node {reading.node_id}: {reading.value} ({reading.quality})")
        
        # Write a value (example)
        write_result = await client.write_node("ns=2;i=2", 25.5, NodeDataType.DOUBLE)
        print(f"Write result: {write_result.success}")
        
        # Get connection status
        status = client.get_connection_status()
        print(f"Connection status: {status}")

if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
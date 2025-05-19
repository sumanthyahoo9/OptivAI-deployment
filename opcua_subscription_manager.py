"""
OPC-UA Data Subscription Manager
Handles real-time subscriptions for continuous monitoring
Integrates with message queue for RL agent data flow
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass
from enum import Enum

from asyncua import Client, Node, ua
from asyncua.common.subscription import Subscription
from asyncua.common.node import Node

# Import our core client and data models
from protocol_specific_clients.opcua_client import ( 
    OPCUAClient, OPCUAReading,
    create_opcua_client
    )

# Configure logging
logger = logging.getLogger(__name__)

# ============================================================================
# Data Models and Enums
# ============================================================================

class SubscriptionStatus(str, Enum):
    """Subscription status"""
    INACTIVE = "inactive"
    ACTIVE = "active"
    ERROR = "error"
    RECONNECTING = "reconnecting"

@dataclass
class SubscriptionConfig:
    """Configuration for OPC-UA subscription"""
    node_id: str
    display_name: str = ""
    min_interval: float = 100.0  # milliseconds
    deadband_type: str = "absolute"  # absolute, percent, none
    deadband_value: float = 0.0
    discard_oldest: bool = True
    queue_size: int = 10

@dataclass
class SubscriptionInfo:
    """Information about an active subscription"""
    config: SubscriptionConfig
    handle: int
    status: SubscriptionStatus
    last_value: Any = None
    last_update: Optional[datetime] = None
    error_count: int = 0
    total_notifications: int = 0

# ============================================================================
# Subscription Event Handler
# ============================================================================

class SubscriptionHandler:
    """Handles OPC-UA subscription notifications"""
    
    def __init__(self, subscription_manager):
        self.subscription_manager = subscription_manager
        self.logger = logging.getLogger(__name__ + ".Handler")
    
    def datachange_notification(self, node: Node, val, data):
        """Handle data change notifications"""
        try:
            node_id = node.nodeid.to_string()
            
            # Create reading object
            reading = OPCUAReading(
                node_id=node_id,
                value=val,
                timestamp=datetime.utcnow(),
                status_code="Good",
                server_timestamp=getattr(data, 'ServerTimestamp', None),
                source_timestamp=getattr(data, 'SourceTimestamp', None)
            )
            
            # Process the notification
            asyncio.create_task(
                self.subscription_manager._process_notification(node_id, reading)
            )
            
        except Exception as e:
            self.logger.error(f"Error in datachange_notification: {e}")
    
    def event_notification(self, event):
        """Handle event notifications"""
        self.logger.debug(f"Event notification: {event}")

# ============================================================================
# Subscription Manager
# ============================================================================

class OPCUASubscriptionManager:
    """Manages OPC-UA subscriptions for real-time data monitoring"""
    
    def __init__(
        self,
        opcua_client: OPCUAClient,
        publishing_interval: float = 500.0,  # milliseconds
        max_notifications_per_publish: int = 0,  # 0 = no limit
        priority: int = 0,
        lifetime_count: int = 10000,
        max_keepalive_count: int = 3000
    ):
        self.opcua_client = opcua_client
        self.publishing_interval = publishing_interval
        self.max_notifications_per_publish = max_notifications_per_publish
        self.priority = priority
        self.lifetime_count = lifetime_count
        self.max_keepalive_count = max_keepalive_count
        
        # Subscription state
        self.subscription: Optional[Subscription] = None
        self.subscriptions: Dict[str, SubscriptionInfo] = {}
        self.handler = SubscriptionHandler(self)
        
        # Callbacks
        self.data_callback: Optional[Callable] = None
        self.error_callback: Optional[Callable] = None
        
        # Statistics
        self.stats = {
            'total_subscriptions': 0,
            'active_subscriptions': 0,
            'total_notifications': 0,
            'last_notification': None,
            'subscription_errors': 0
        }
        
        # Background tasks
        self._monitoring_task: Optional[asyncio.Task] = None
        self._running = False
        
        self.logger = logging.getLogger(__name__ + ".Manager")
    
    async def initialize(self) -> bool:
        """Initialize the subscription manager"""
        try:
            if not self.opcua_client.is_connected():
                self.logger.error("OPC-UA client not connected")
                return False
            
            # Create subscription
            self.subscription = await self.opcua_client.client.create_subscription(
                period=self.publishing_interval,
                handler=self.handler,
                publishing=True,
                max_notif_per_publish=self.max_notifications_per_publish,
                priority=self.priority,
                lifetime=self.lifetime_count,
                max_keepalive=self.max_keepalive_count
            )
            
            self.logger.info(f"Subscription created with ID: {self.subscription.subscription_id}")
            
            # Start monitoring task
            self._running = True
            self._monitoring_task = asyncio.create_task(self._monitor_subscriptions())
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize subscription manager: {e}")
            return False
    
    async def shutdown(self):
        """Shutdown the subscription manager"""
        try:
            self._running = False
            
            # Cancel monitoring task
            if self._monitoring_task:
                self._monitoring_task.cancel()
                try:
                    await self._monitoring_task
                except asyncio.CancelledError:
                    pass
            
            # Remove all subscriptions
            await self.remove_all_subscriptions()
            
            # Delete subscription
            if self.subscription:
                await self.subscription.delete()
                self.subscription = None
            
            self.logger.info("Subscription manager shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
    
    def set_data_callback(self, callback: Callable[[str, OPCUAReading], None]):
        """Set callback for data notifications"""
        self.data_callback = callback
    
    def set_error_callback(self, callback: Callable[[str, str], None]):
        """Set callback for subscription errors"""
        self.error_callback = callback
    
    async def subscribe_to_node(self, config: SubscriptionConfig) -> bool:
        """Subscribe to a single node"""
        try:
            if not self.subscription:
                self.logger.error("Subscription manager not initialized")
                return False
            
            # Check if already subscribed
            if config.node_id in self.subscriptions:
                self.logger.warning(f"Already subscribed to {config.node_id}")
                return True
            
            # Get the node
            node = await self.opcua_client.get_node(config.node_id)
            if not node:
                self.logger.error(f"Node not found: {config.node_id}")
                return False
            
            # Create monitoring parameters
            moni_params = ua.MonitoringParameters()
            moni_params.RequestedPublishingInterval = config.min_interval
            moni_params.RequestedLifetimeCount = self.lifetime_count
            moni_params.RequestedMaxKeepAliveCount = self.max_keepalive_count
            moni_params.MaxNotificationsPerPublish = 0
            moni_params.Priority = self.priority
            moni_params.DiscardOldest = config.discard_oldest
            moni_params.QueueSize = config.queue_size
            
            # Set deadband filter if specified
            if config.deadband_value > 0:
                if config.deadband_type == "absolute":
                    deadband = ua.DataChangeFilter()
                    deadband.Trigger = ua.DataChangeTrigger.StatusValue
                    deadband.DeadbandType = ua.DeadbandType.Absolute
                    deadband.DeadbandValue = config.deadband_value
                    moni_params.Filter = deadband
                elif config.deadband_type == "percent":
                    deadband = ua.DataChangeFilter()
                    deadband.Trigger = ua.DataChangeTrigger.StatusValue
                    deadband.DeadbandType = ua.DeadbandType.Percent
                    deadband.DeadbandValue = config.deadband_value
                    moni_params.Filter = deadband
            
            # Subscribe to the node
            handle = await self.subscription.subscribe_data_change(node, moni_params)
            
            # Store subscription info
            sub_info = SubscriptionInfo(
                config=config,
                handle=handle,
                status=SubscriptionStatus.ACTIVE
            )
            self.subscriptions[config.node_id] = sub_info
            
            # Update statistics
            self.stats['total_subscriptions'] += 1
            self.stats['active_subscriptions'] = len(
                [s for s in self.subscriptions.values() if s.status == SubscriptionStatus.ACTIVE]
            )
            
            self.logger.info(f"Subscribed to node: {config.node_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to subscribe to node {config.node_id}: {e}")
            return False
    
    async def subscribe_to_multiple_nodes(self, configs: List[SubscriptionConfig]) -> Dict[str, bool]:
        """Subscribe to multiple nodes"""
        results = {}
        
        for config in configs:
            success = await self.subscribe_to_node(config)
            results[config.node_id] = success
        
        self.logger.info(f"Subscribed to {sum(results.values())}/{len(configs)} nodes")
        return results
    
    async def unsubscribe_from_node(self, node_id: str) -> bool:
        """Unsubscribe from a node"""
        try:
            if node_id not in self.subscriptions:
                self.logger.warning(f"No subscription found for {node_id}")
                return True
            
            sub_info = self.subscriptions[node_id]
            
            # Unsubscribe from the node
            await self.subscription.unsubscribe(sub_info.handle)
            
            # Remove from subscriptions
            del self.subscriptions[node_id]
            
            # Update statistics
            self.stats['active_subscriptions'] = len(
                [s for s in self.subscriptions.values() if s.status == SubscriptionStatus.ACTIVE]
            )
            
            self.logger.info(f"Unsubscribed from node: {node_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to unsubscribe from node {node_id}: {e}")
            return False
    
    async def remove_all_subscriptions(self):
        """Remove all subscriptions"""
        node_ids = list(self.subscriptions.keys())
        
        for node_id in node_ids:
            await self.unsubscribe_from_node(node_id)
        
        self.logger.info("All subscriptions removed")
    
    async def _process_notification(self, node_id: str, reading: OPCUAReading):
        """Process a data change notification"""
        try:
            # Update subscription info
            if node_id in self.subscriptions:
                sub_info = self.subscriptions[node_id]
                sub_info.last_value = reading.value
                sub_info.last_update = reading.timestamp
                sub_info.total_notifications += 1
                sub_info.status = SubscriptionStatus.ACTIVE
            
            # Update global statistics
            self.stats['total_notifications'] += 1
            self.stats['last_notification'] = reading.timestamp
            
            # Call data callback if set
            if self.data_callback:
                try:
                    await self.data_callback(node_id, reading)
                except Exception as e:
                    self.logger.error(f"Error in data callback: {e}")
            
            self.logger.debug(f"Processed notification for {node_id}: {reading.value}")
            
        except Exception as e:
            self.logger.error(f"Error processing notification for {node_id}: {e}")
    
    async def _monitor_subscriptions(self):
        """Background task to monitor subscription health"""
        while self._running:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                current_time = datetime.utcnow()
                stale_threshold = timedelta(minutes=5)
                
                # Check for stale subscriptions
                for node_id, sub_info in self.subscriptions.items():
                    if sub_info.last_update:
                        time_since_update = current_time - sub_info.last_update
                        
                        if time_since_update > stale_threshold:
                            self.logger.warning(f"Stale subscription detected: {node_id}")
                            sub_info.status = SubscriptionStatus.ERROR
                            sub_info.error_count += 1
                            
                            # Call error callback if set
                            if self.error_callback:
                                try:
                                    await self.error_callback(node_id, "Stale subscription")
                                except Exception as e:
                                    self.logger.error(f"Error in error callback: {e}")
                
                # Update statistics
                self.stats['active_subscriptions'] = len(
                    [s for s in self.subscriptions.values() if s.status == SubscriptionStatus.ACTIVE]
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in subscription monitoring: {e}")
    
    def get_subscription_status(self) -> Dict[str, Any]:
        """Get current subscription status"""
        subscription_details = {}
        
        for node_id, sub_info in self.subscriptions.items():
            subscription_details[node_id] = {
                'status': sub_info.status.value,
                'last_value': sub_info.last_value,
                'last_update': sub_info.last_update.isoformat() if sub_info.last_update else None,
                'error_count': sub_info.error_count,
                'total_notifications': sub_info.total_notifications,
                'config': {
                    'display_name': sub_info.config.display_name,
                    'min_interval': sub_info.config.min_interval,
                    'deadband_value': sub_info.config.deadband_value
                }
            }
        
        return {
            'subscription_id': self.subscription.subscription_id if self.subscription else None,
            'publishing_interval': self.publishing_interval,
            'statistics': self.stats,
            'subscriptions': subscription_details
        }
    
    def get_subscription_list(self) -> List[str]:
        """Get list of subscribed node IDs"""
        return list(self.subscriptions.keys())

# ============================================================================
# Integration with Message Queue
# ============================================================================

class MessageQueueIntegration:
    """Integrates subscription manager with message queue"""
    
    def __init__(self, subscription_manager: OPCUASubscriptionManager, message_queue):
        self.subscription_manager = subscription_manager
        self.message_queue = message_queue
        self.logger = logging.getLogger(__name__ + ".MQIntegration")
        
        # Set up callbacks
        subscription_manager.set_data_callback(self._handle_data_notification)
        subscription_manager.set_error_callback(self._handle_error_notification)
    
    async def _handle_data_notification(self, node_id: str, reading: OPCUAReading):
        """Handle data notifications by publishing to message queue"""
        try:
            # Convert reading to dictionary for message queue
            message_data = {
                'node_id': reading.node_id,
                'value': reading.value,
                'timestamp': reading.timestamp.isoformat(),
                'status_code': reading.status_code,
                'quality': reading.quality,
                'source': 'opcua_subscription',
                'server_timestamp': reading.server_timestamp.isoformat() if reading.server_timestamp else None,
                'source_timestamp': reading.source_timestamp.isoformat() if reading.source_timestamp else None
            }
            
            # Publish to sensor data queue
            await self.message_queue.publish_sensor_data(message_data, priority=1)
            
            self.logger.debug(f"Published OPC-UA data to queue: {node_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to publish data to queue: {e}")
    
    async def _handle_error_notification(self, node_id: str, error_message: str):
        """Handle error notifications"""
        try:
            # Publish error to alerts queue
            alert_data = {
                'type': 'opcua_subscription_error',
                'node_id': node_id,
                'message': error_message,
                'timestamp': datetime.utcnow().isoformat(),
                'severity': 'warning'
            }
            
            await self.message_queue.publish(
                queue_type='alerts',
                payload=alert_data,
                priority=8  # High priority for errors
            )
            
            self.logger.info(f"Published subscription error alert: {node_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to publish error alert: {e}")

# ============================================================================
# Example Usage
# ============================================================================

async def main():
    """Example usage of the subscription manager"""
    
    # Create and connect OPC-UA client
    client = await create_opcua_client("opc.tcp://localhost:4840")
    await client.connect()
    
    try:
        # Create subscription manager
        sub_manager = OPCUASubscriptionManager(
            opcua_client=client,
            publishing_interval=1000  # 1 second
        )
        
        # Initialize
        if await sub_manager.initialize():
            
            # Configure subscriptions
            configs = [
                SubscriptionConfig(
                    node_id="ns=2;i=1001",
                    display_name="Motor Speed",
                    min_interval=500,
                    deadband_type="absolute",
                    deadband_value=0.1
                ),
                SubscriptionConfig(
                    node_id="ns=2;i=1002",
                    display_name="Temperature",
                    min_interval=1000,
                    deadband_type="absolute",
                    deadband_value=0.5
                ),
                SubscriptionConfig(
                    node_id="ns=2;i=1003",
                    display_name="Pressure",
                    min_interval=1000
                )
            ]
            
            # Subscribe to nodes
            results = await sub_manager.subscribe_to_multiple_nodes(configs)
            print(f"Subscription results: {results}")
            
            # Let it run for a while
            await asyncio.sleep(30)
            
            # Get status
            status = sub_manager.get_subscription_status()
            print(f"Subscription status: {status}")
            
        # Shutdown
        await sub_manager.shutdown()
        
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
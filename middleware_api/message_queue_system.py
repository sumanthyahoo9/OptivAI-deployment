"""
This provides robust, asynchronous message handling for the HVAC middleware.
"""
import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any
from enum import Enum
import redis.asyncio as redis
from pydantic import BaseModel
import uuid

# Import our data models
from data_models_python import MessagePayload, SensorReading, ControlCommand, ControlResponse


class QueueType(str, Enum):
    """Types of message queues"""
    SENSOR_DATA = "sensor_data"
    CONTROL_COMMANDS = "control_commands"
    CONTROL_RESPONSES = "control_responses"
    SYSTEM_HEALTH = "system_health"
    RL_OBSERVATIONS = "rl_observations"
    RL_ACTIONS = "rl_actions"
    ALERTS = "alerts"
    DEAD_LETTER = "dead_letter"


class MessageStatus(str, Enum):
    """Message processing status"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD_LETTER = "dead_letter"


class QueueConfig(BaseModel):
    """Configuration for a message queue"""
    queue_name: str
    max_size: int = 10000
    ttl_seconds: int = 3600  # Time to live
    retry_delay_seconds: int = 30
    max_retries: int = 3
    priority_enabled: bool = True
    dlq_enabled: bool = True  # Dead letter queue


class MessageProcessor:
    """Base class for message processors"""
    
    async def process(self, message: MessagePayload) -> bool:
        """
        Process a message. Return True if successful, False to retry.
        Raise exception for immediate dead letter.
        """
        raise NotImplementedError
    
    async def on_retry(self, message: MessagePayload, attempt: int) -> None:
        """Called when a message is being retried"""
        pass
    
    async def on_dead_letter(self, message: MessagePayload, reason: str) -> None:
        """Called when a message is moved to dead letter queue"""
        pass


class RedisMessageQueue:
    """Redis-based message queue with advanced features"""
    
    def __init__(self, 
                 redis_url: str = "redis://localhost:6379",
                 default_config: Optional[QueueConfig] = None):
        self.redis_url = redis_url
        self.redis_client: Optional[redis.Redis] = None
        self.queues: Dict[str, QueueConfig] = {}
        self.processors: Dict[str, MessageProcessor] = {}
        self.running = False
        self.worker_tasks: List[asyncio.Task] = []
        self.default_config = default_config or QueueConfig(queue_name="default")
        
        # Logging
        self.logger = logging.getLogger(__name__)
    
    async def connect(self):
        """Connect to Redis"""
        try:
            self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
            await self.redis_client.ping()
            self.logger.info("Connected to Redis successfully")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from Redis"""
        if self.redis_client:
            await self.redis_client.close()
            self.logger.info("Disconnected from Redis")
    
    def register_queue(self, queue_type: QueueType, config: Optional[QueueConfig] = None):
        """Register a new queue with configuration"""
        queue_config = config or QueueConfig(queue_name=queue_type.value)
        queue_config.queue_name = queue_type.value
        self.queues[queue_type.value] = queue_config
        self.logger.info(f"Registered queue: {queue_type.value}")
    
    def register_processor(self, queue_type: QueueType, processor: MessageProcessor):
        """Register a message processor for a queue"""
        self.processors[queue_type.value] = processor
        self.logger.info(f"Registered processor for queue: {queue_type.value}")
    
    async def publish(self, queue_type: QueueType, payload: Dict[str, Any], 
                     priority: int = 1, expires_in: Optional[int] = None) -> str:
        """Publish a message to a queue"""
        if not self.redis_client:
            raise RuntimeError("Not connected to Redis")
        
        # Create message
        message = MessagePayload(
            message_type=queue_type.value,
            source="middleware",
            priority=priority,
            payload=payload,
            expires_at=datetime.utcnow() + timedelta(seconds=expires_in) if expires_in else None
        )
        
        # Serialize message
        message_data = {
            "message": message.json(),
            "status": MessageStatus.PENDING.value,
            "created_at": datetime.utcnow().isoformat(),
            "attempts": 0
        }
        
        # Add to queue based on priority
        queue_name = self._get_queue_name(queue_type)
        if self.queues.get(queue_type.value, self.default_config).priority_enabled:
            # Use priority queue (sorted set)
            score = self._calculate_priority_score(priority, datetime.utcnow())
            await self.redis_client.zadd(f"{queue_name}:priority", {json.dumps(message_data): score})
        else:
            # Use simple FIFO queue
            await self.redis_client.lpush(queue_name, json.dumps(message_data))
        
        # Set TTL for the message
        queue_config = self.queues.get(queue_type.value, self.default_config)
        await self.redis_client.expire(f"{queue_name}:message:{message.message_id}", queue_config.ttl_seconds)
        
        self.logger.debug(f"Published message {message.message_id} to {queue_type.value}")
        return message.message_id
    
    async def publish_sensor_data(self, sensor_reading: SensorReading, priority: int = 1) -> str:
        """Convenience method to publish sensor data"""
        return await self.publish(
            QueueType.SENSOR_DATA,
            sensor_reading.dict(),
            priority=priority
        )
    
    async def publish_control_command(self, command: ControlCommand, priority: int = 5) -> str:
        """Convenience method to publish control commands"""
        return await self.publish(
            QueueType.CONTROL_COMMANDS,
            command.dict(),
            priority=priority
        )
    
    async def publish_control_response(self, response: ControlResponse, priority: int = 3) -> str:
        """Convenience method to publish control responses"""
        return await self.publish(
            QueueType.CONTROL_RESPONSES,
            response.dict(),
            priority=priority
        )
    
    async def start_workers(self, num_workers: int = 3):
        """Start worker tasks to process messages"""
        if self.running:
            return
        
        self.running = True
        self.logger.info(f"Starting {num_workers} worker tasks")
        
        for i in range(num_workers):
            task = asyncio.create_task(self._worker_loop(f"worker-{i}"))
            self.worker_tasks.append(task)
    
    async def stop_workers(self):
        """Stop all worker tasks"""
        if not self.running:
            return
        
        self.running = False
        self.logger.info("Stopping worker tasks")
        
        # Cancel all tasks
        for task in self.worker_tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        self.worker_tasks.clear()
    
    async def _worker_loop(self, worker_id: str):
        """Main worker loop to process messages"""
        self.logger.info(f"Worker {worker_id} started")
        
        while self.running:
            try:
                # Try to get a message from any queue
                message_data = await self._get_next_message()
                
                if message_data:
                    await self._process_message(worker_id, message_data)
                else:
                    # No messages, wait before checking again
                    await asyncio.sleep(1)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(5)  # Wait before retrying
        
        self.logger.info(f"Worker {worker_id} stopped")
    
    async def _get_next_message(self) -> Optional[Dict[str, Any]]:
        """Get the next message from any queue"""
        for queue_name in self.queues:
            queue_config = self.queues[queue_name]
            redis_queue_name = self._get_queue_name(QueueType(queue_name))
            
            if queue_config.priority_enabled:
                # Get from priority queue (lowest score first)
                result = await self.redis_client.zpopmin(f"{redis_queue_name}:priority")
                if result:
                    message_json, _ = result[0]
                    return json.loads(message_json)
            else:
                # Get from FIFO queue
                result = await self.redis_client.rpop(redis_queue_name)
                if result:
                    return json.loads(result)
        
        return None
    
    async def _process_message(self, worker_id: str, message_data: Dict[str, Any]):
        """Process a single message"""
        try:
            # Parse message
            message = MessagePayload.parse_raw(message_data["message"])
            queue_type = QueueType(message.message_type)
            
            # Check if message has expired
            if message.expires_at and datetime.utcnow() > message.expires_at:
                self.logger.warning(f"Message {message.message_id} expired, moving to DLQ")
                await self._move_to_dead_letter(message, "expired")
                return
            
            # Update status to processing
            await self._update_message_status(message, MessageStatus.PROCESSING)
            
            # Get processor
            processor = self.processors.get(queue_type.value)
            if not processor:
                self.logger.warning(f"No processor for queue {queue_type.value}")
                await self._move_to_dead_letter(message, "no_processor")
                return
            
            # Process message
            success = await processor.process(message)
            
            if success:
                await self._update_message_status(message, MessageStatus.COMPLETED)
                self.logger.debug(f"Worker {worker_id} processed message {message.message_id}")
            else:
                # Handle retry
                await self._handle_retry(message, "processor_returned_false")
                
        except Exception as e:
            message = MessagePayload.parse_raw(message_data["message"])
            self.logger.error(f"Worker {worker_id} failed to process message {message.message_id}: {e}")
            await self._handle_retry(message, str(e))
    
    async def _handle_retry(self, message: MessagePayload, reason: str):
        """Handle message retry logic"""
        queue_config = self.queues.get(message.message_type, self.default_config)
        
        if message.retry_count >= message.max_retries:
            await self._move_to_dead_letter(message, f"max_retries_exceeded: {reason}")
            return
        
        # Increment retry count
        message.retry_count += 1
        await self._update_message_status(message, MessageStatus.RETRYING)
        
        # Call processor retry hook
        processor = self.processors.get(message.message_type)
        if processor:
            await processor.on_retry(message, message.retry_count)
        
        # Re-queue with delay
        await asyncio.sleep(queue_config.retry_delay_seconds)
        await self.publish(
            QueueType(message.message_type),
            message.payload,
            priority=message.priority
        )
        
        self.logger.info(f"Retrying message {message.message_id} (attempt {message.retry_count})")
    
    async def _move_to_dead_letter(self, message: MessagePayload, reason: str):
        """Move message to dead letter queue"""
        dlq_message = {
            "original_message": message.dict(),
            "reason": reason,
            "moved_at": datetime.utcnow().isoformat()
        }
        
        await self.redis_client.lpush("dlq", json.dumps(dlq_message))
        await self._update_message_status(message, MessageStatus.DEAD_LETTER)
        
        # Call processor dead letter hook
        processor = self.processors.get(message.message_type)
        if processor:
            await processor.on_dead_letter(message, reason)
        
        self.logger.warning(f"Moved message {message.message_id} to DLQ: {reason}")
    
    async def _update_message_status(self, message: MessagePayload, status: MessageStatus):
        """Update message status in Redis"""
        key = f"message_status:{message.message_id}"
        await self.redis_client.hset(key, "status", status.value)
        await self.redis_client.hset(key, "updated_at", datetime.utcnow().isoformat())
        await self.redis_client.expire(key, 86400)  # 24 hour TTL for status
    
    def _get_queue_name(self, queue_type: QueueType) -> str:
        """Get Redis queue name for queue type"""
        return f"hvac_queue:{queue_type.value}"
    
    def _calculate_priority_score(self, priority: int, timestamp: datetime) -> float:
        """Calculate priority score (lower = higher priority)"""
        # Higher priority values get lower scores
        # Add timestamp to maintain FIFO order within same priority
        time_component = timestamp.timestamp() / 1000000  # Small time component
        return (10 - priority) + time_component
    
    async def get_queue_stats(self) -> Dict[str, Dict[str, int]]:
        """Get statistics for all queues"""
        stats = {}
        
        for queue_name in self.queues:
            redis_queue_name = self._get_queue_name(QueueType(queue_name))
            queue_config = self.queues[queue_name]
            
            if queue_config.priority_enabled:
                pending_count = await self.redis_client.zcard(f"{redis_queue_name}:priority")
            else:
                pending_count = await self.redis_client.llen(redis_queue_name)
            
            dlq_count = await self.redis_client.llen("dlq")
            
            stats[queue_name] = {
                "pending": pending_count,
                "dead_letter": dlq_count
            }
        
        return stats
    
    async def clear_queue(self, queue_type: QueueType):
        """Clear all messages from a queue"""
        redis_queue_name = self._get_queue_name(queue_type)
        queue_config = self.queues.get(queue_type.value, self.default_config)
        
        if queue_config.priority_enabled:
            await self.redis_client.delete(f"{redis_queue_name}:priority")
        else:
            await self.redis_client.delete(redis_queue_name)
        
        self.logger.info(f"Cleared queue: {queue_type.value}")
    
    async def clear_dead_letter_queue(self):
        """Clear the dead letter queue"""
        await self.redis_client.delete("dlq")
        self.logger.info("Cleared dead letter queue")


# Example processors
class SensorDataProcessor(MessageProcessor):
    """Processes sensor data messages"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    async def process(self, message: MessagePayload) -> bool:
        try:
            # Parse sensor reading
            sensor_data = SensorReading(**message.payload)
            
            # Validate data quality
            if sensor_data.quality.value not in ["good", "uncertain"]:
                self.logger.warning(f"Poor quality sensor data: {sensor_data.quality}")
                return False
            
            # Store in time series database (placeholder)
            await self._store_sensor_data(sensor_data)
            
            # Trigger any alerts if needed
            await self._check_alerts(sensor_data)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to process sensor data: {e}")
            return False
    
    async def _store_sensor_data(self, sensor_data: SensorReading):
        """Store sensor data (placeholder - implement with InfluxDB)"""
        # This would store data in InfluxDB
        pass
    
    async def _check_alerts(self, sensor_data: SensorReading):
        """Check for alert conditions"""
        # This would check thresholds and trigger alerts
        pass


class ControlCommandProcessor(MessageProcessor):
    """Processes control commands"""
    
    def __init__(self, device_controllers: Dict[str, Any]):
        self.device_controllers = device_controllers
        self.logger = logging.getLogger(__name__)
    
    async def process(self, message: MessagePayload) -> bool:
        try:
            # Parse control command
            command = ControlCommand(**message.payload)
            
            # Get device controller
            controller = self.device_controllers.get(command.target_device_id)
            if not controller:
                self.logger.error(f"No controller for device: {command.target_device_id}")
                return False
            
            # Execute command
            success = await self._execute_command(controller, command)
            
            # Send response
            await self._send_response(command, success)
            
            return success
            
        except Exception as e:
            self.logger.error(f"Failed to process control command: {e}")
            return False
    
    async def _execute_command(self, controller: Any, command: ControlCommand) -> bool:
        """Execute the control command"""
        # This would interface with actual device controllers
        # Return True if successful, False if failed
        return True  # Placeholder
    
    async def _send_response(self, command: ControlCommand, success: bool):
        """Send command response"""
        # This would send a response back through the queue
        pass


# Factory function to create a configured message queue
async def create_message_queue(redis_url: str = "redis://localhost:6379") -> RedisMessageQueue:
    """Create and configure a message queue with standard queues"""
    queue = RedisMessageQueue(redis_url)
    await queue.connect()
    
    # Register standard queues
    queue.register_queue(QueueType.SENSOR_DATA, QueueConfig(
        queue_name=QueueType.SENSOR_DATA.value,
        max_size=50000,
        ttl_seconds=3600,
        priority_enabled=False  # Sensor data doesn't need priority
    ))
    
    queue.register_queue(QueueType.CONTROL_COMMANDS, QueueConfig(
        queue_name=QueueType.CONTROL_COMMANDS.value,
        max_size=10000,
        ttl_seconds=300,  # Commands expire quickly
        priority_enabled=True,  # Commands need priority
        retry_delay_seconds=10
    ))
    
    queue.register_queue(QueueType.CONTROL_RESPONSES, QueueConfig(
        queue_name=QueueType.CONTROL_RESPONSES.value,
        max_size=10000,
        ttl_seconds=1800,
        priority_enabled=False
    ))
    
    queue.register_queue(QueueType.RL_OBSERVATIONS, QueueConfig(
        queue_name=QueueType.RL_OBSERVATIONS.value,
        max_size=10000,
        ttl_seconds=300,
        priority_enabled=False
    ))
    
    queue.register_queue(QueueType.RL_ACTIONS, QueueConfig(
        queue_name=QueueType.RL_ACTIONS.value,
        max_size=10000,
        ttl_seconds=300,
        priority_enabled=True,  # RL actions need priority
        retry_delay_seconds=5
    ))
    
    queue.register_queue(QueueType.ALERTS, QueueConfig(
        queue_name=QueueType.ALERTS.value,
        max_size=5000,
        ttl_seconds=86400,  # Keep alerts for 24 hours
        priority_enabled=True,
        max_retries=5
    ))
    
    return queue
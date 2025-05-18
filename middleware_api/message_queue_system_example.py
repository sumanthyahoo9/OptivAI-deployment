"""
Examples of using the message queue system
"""
queue = await create_message_queue("redis://localhost:6379")

# Register processors
queue.register_processor(QueueType.SENSOR_DATA, SensorDataProcessor())
queue.register_processor(QueueType.CONTROL_COMMANDS, ControlCommandProcessor(device_controllers))

# Start workers
await queue.start_workers(num_workers=5)

# Publish messages
sensor_reading = SensorReading(...)
await queue.publish_sensor_data(sensor_reading)

# Get queue statistics
stats = await queue.get_queue_stats()
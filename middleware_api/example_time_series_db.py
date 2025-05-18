# Initialize
store = await create_time_series_store(
   url="http://influxdb:8086",
   token="your-token",
   org="hvac-company", 
   bucket="building_data"
)

data_manager = HVACDataManager(store)
await data_manager.initialize()

# Write sensor data
sensor_reading = SensorReading(
   sensor_id="temp_sensor_01",
   sensor_type=SensorType.TEMPERATURE,
   value=23.5,
   unit="celsius",
   zone_id="office_zone_1"
)
await store.write_sensor_data(sensor_reading)

# Write RL training data
observation = RLObservation(
   observation_id="obs_001",
   zone_id="office_zone_1", 
   agent_id="ppo_agent_v1",
   state_vector=[23.5, 45.0, 400.0, 0.8],
   reward=-0.15,
   episode_id="episode_100"
)
await store.write_rl_observation(observation)

# Query for analytics
performance = await data_manager.get_zone_performance_summary(
   zone_id="office_zone_1",
   start_time=datetime(2024, 1, 1),
   end_time=datetime(2024, 12, 31)
)

# Get real-time dashboard
dashboard = await data_manager.get_zone_dashboard_data("office_zone_1")

# Prepare training data
training_data = await prepare_rl_training_data(
   store,
   start_time=datetime(2024, 1, 1),
   end_time=datetime(2024, 12, 31),
   zone_ids=["office_zone_1", "office_zone_2"],
   resample_frequency="15T"  # 15-minute intervals
)
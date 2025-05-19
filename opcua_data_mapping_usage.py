# Configure mapping
from datetime import datetime
from opcua_data_mapping import OPCUADataMapper, OPCUAReading
config = {
    "ns=2;i=1001": {
        "tag_name": "chiller_temp",
        "unit_from": "fahrenheit", 
        "unit_to": "celsius",
        "quality_checks": ["realistic_temperature"]
    }
}
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

mapper = OPCUADataMapper(config)
data_points = mapper.transform_readings(eadings)

# Result: Raw OPC-UA data becomes standardized tags
# "ns=2;i=1001" -> "chiller_temp: 7.5°C (Good quality)"
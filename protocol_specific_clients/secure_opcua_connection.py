# With security
from opcua_client import  (
    ServerEndpoint, ClientConfiguration,
    OPCUAClient, SecurityMode,
    AuthenticationType, MessageSecurityMode,
    NodeDataType
)

endpoint = ServerEndpoint(
    url="opc.tcp://secure-server:4840",
    security_mode=SecurityMode.SIGN_AND_ENCRYPT,
    security_policy=MessageSecurityMode.BASIC256SHA256
)

config = ClientConfiguration(
    auth_type=AuthenticationType.USERNAME_PASSWORD,
    username="operator",
    password="password123",
    certificate_path="/path/to/client.crt",
    private_key_path="/path/to/client.key"
)

client = OPCUAClient(endpoint, config)

# Reading manufacturing data
# Read multiple sensors at once
node_ids = [
    "ns=2;i=1001",  # Motor speed
    "ns=2;i=1002",  # Temperature
    "ns=2;i=1003",  # Pressure
    "ns=2;i=1004"   # Flow rate
]

readings = await client.read_multiple_nodes(node_ids)
for reading in readings:
    if reading.quality == "Good":
        print(f"Sensor {reading.node_id}: {reading.value}")

# Writing control commands
# Write setpoints
writes = [
    ("ns=2;i=2001", 1800.0, NodeDataType.DOUBLE),  # Motor setpoint
    ("ns=2;i=2002", 75.0, NodeDataType.DOUBLE),    # Temperature setpoint
    ("ns=2;i=2003", True, NodeDataType.BOOLEAN)    # Enable pump
]

results = await client.write_multiple_nodes(writes)
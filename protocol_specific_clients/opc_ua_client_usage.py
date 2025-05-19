# Simple connection
from opcua_client import create_opcua_client


client = await create_opcua_client("opc.tcp://192.168.1.100:4840")
await client.connect()
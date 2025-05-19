# Set up subscriptions for manufacturing equipment
from opcua_subscription_manager import SubscriptionConfig

configs = [
    SubscriptionConfig("ns=2;i=1001", "Motor_RPM", deadband_value=0.1),
    SubscriptionConfig("ns=2;i=1002", "Chiller_Temp", deadband_value=0.5),
    SubscriptionConfig("ns=2;i=1003", "Power_Consumption", min_interval=500)
]

await sub_manager.subscribe_to_multiple_nodes(configs)
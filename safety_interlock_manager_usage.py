"""
Using the safety interlock manager
"""
from safety_interlock_manager import SafetyInterlockManager
# Validate RL agent action before execution
safety_manager = SafetyInterlockManager()
allowed, reasons = await safety_manager.validate_action(
    "start", "pump_001", {'suction_pressure': 0.3}
)

if not allowed:
    print(f"Action blocked: {reasons}")
    # Action: ['Interlock violated: Pump cannot start without adequate suction pressure']

# Emergency conditions trigger automatic responses
# Low suction pressure → automatically stops pump
# Critical temperature → emergency stop all equipment

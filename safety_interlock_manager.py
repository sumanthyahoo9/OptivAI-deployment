"""
Safety and Interlock Manager for Industrial Equipment
Validates control actions against safety constraints
Implements emergency stops and manual override detection
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

# ============================================================================
# Data Models and Enums
# ============================================================================

class SafetyLevel(str, Enum):
    """Safety criticality levels"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"

class InterLockStatus(str, Enum):
    """Interlock status"""
    SATISFIED = "satisfied"
    VIOLATED = "violated"
    BYPASSED = "bypassed"
    DISABLED = "disabled"

@dataclass
class SafetyConstraint:
    """Defines a safety constraint"""
    constraint_id: str
    description: str
    condition: str  # e.g., "temperature < 80"
    safety_level: SafetyLevel
    affected_devices: List[str]
    auto_action: Optional[str] = None  # e.g., "stop_pump"
    enable_bypass: bool = False

@dataclass
class InterLock:
    """Defines an equipment interlock"""
    interlock_id: str
    description: str
    source_device: str
    target_device: str
    condition: str
    action_blocked: str  # e.g., "start"
    status: InterLockStatus = InterLockStatus.SATISFIED

@dataclass
class SafetyViolation:
    """Records a safety violation"""
    violation_id: str
    constraint_id: str
    timestamp: datetime
    description: str
    safety_level: SafetyLevel
    current_value: Any
    threshold_value: Any
    auto_action_taken: Optional[str] = None
    acknowledged: bool = False

# ============================================================================
# Safety State Manager
# ============================================================================

class SafetyStateManager:
    """Manages overall safety state of the system"""
    
    def __init__(self):
        self.overall_state = SafetyLevel.INFO
        self.emergency_stop_active = False
        self.manual_override_active = False
        self.safety_violations: Dict[str, SafetyViolation] = {}
        self.bypassed_constraints: List[str] = []
        
    def update_safety_state(self, violations: List[SafetyViolation]):
        """Update overall safety state based on violations"""
        if not violations:
            self.overall_state = SafetyLevel.INFO
            return
        
        # Determine highest severity
        max_level = max(v.safety_level for v in violations)
        self.overall_state = max_level
        
        # Check for emergency conditions
        emergency_violations = [v for v in violations if v.safety_level == SafetyLevel.EMERGENCY]
        if emergency_violations and not self.emergency_stop_active:
            self.trigger_emergency_stop("Safety violation detected")
    
    def trigger_emergency_stop(self, reason: str):
        """Trigger emergency stop"""
        self.emergency_stop_active = True
        logger.critical(f"EMERGENCY STOP ACTIVATED: {reason}")
    
    def reset_emergency_stop(self):
        """Reset emergency stop (requires manual intervention)"""
        self.emergency_stop_active = False
        logger.info("Emergency stop reset")
    
    def set_manual_override(self, active: bool, operator: str = ""):
        """Set manual override state"""
        self.manual_override_active = active
        logger.info(f"Manual override {'activated' if active else 'deactivated'} by {operator}")

# ============================================================================
# Safety Rule Engine
# ============================================================================

class SafetyRuleEngine:
    """Evaluates safety rules against current system state"""
    
    def __init__(self):
        self.constraints: Dict[str, SafetyConstraint] = {}
        self.interlocks: Dict[str, InterLock] = {}
        self.current_values: Dict[str, Any] = {}
        
    def add_constraint(self, constraint: SafetyConstraint):
        """Add a safety constraint"""
        self.constraints[constraint.constraint_id] = constraint
        logger.info(f"Added safety constraint: {constraint.description}")
    
    def add_interlock(self, interlock: InterLock):
        """Add an equipment interlock"""
        self.interlocks[interlock.interlock_id] = interlock
        logger.info(f"Added interlock: {interlock.description}")
    
    def update_values(self, values: Dict[str, Any]):
        """Update current system values"""
        self.current_values.update(values)
    
    def evaluate_constraints(self) -> List[SafetyViolation]:
        """Evaluate all safety constraints"""
        violations = []
        
        for constraint_id, constraint in self.constraints.items():
            # Skip bypassed constraints
            if constraint_id in self.bypassed_constraints:
                continue
            
            try:
                # Simple condition evaluation (in production, use safe eval or parser)
                condition_result = self._evaluate_condition(constraint.condition)
                
                if not condition_result:
                    violation = SafetyViolation(
                        violation_id=f"viol_{constraint_id}_{int(datetime.utcnow().timestamp())}",
                        constraint_id=constraint_id,
                        timestamp=datetime.utcnow(),
                        description=f"Safety constraint violated: {constraint.description}",
                        safety_level=constraint.safety_level,
                        current_value=self._get_constraint_value(constraint.condition),
                        threshold_value=self._get_constraint_threshold(constraint.condition)
                    )
                    violations.append(violation)
                    
            except Exception as e:
                logger.error(f"Error evaluating constraint {constraint_id}: {e}")
        
        return violations
    
    def evaluate_interlocks(self, action: str, device_id: str) -> List[str]:
        """Check if action is blocked by interlocks"""
        blocked_reasons = []
        
        for interlock_id, interlock in self.interlocks.items():
            if (interlock.target_device == device_id and 
                interlock.action_blocked == action and
                interlock.status == InterLockStatus.SATISFIED):
                
                try:
                    # Evaluate interlock condition
                    if not self._evaluate_condition(interlock.condition):
                        interlock.status = InterLockStatus.VIOLATED
                        blocked_reasons.append(f"Interlock violated: {interlock.description}")
                        
                except Exception as e:
                    logger.error(f"Error evaluating interlock {interlock_id}: {e}")
        
        return blocked_reasons
    
    def _evaluate_condition(self, condition: str) -> bool:
        """Evaluate a condition string safely"""
        # Simple condition parser for common cases
        # In production, use a proper expression parser
        
        for var_name, value in self.current_values.items():
            condition = condition.replace(var_name, str(value))
        
        # Basic operators
        try:
            # WARNING: This is unsafe for production - use a proper parser
            if ' < ' in condition:
                left, right = condition.split(' < ')
                return float(left.strip()) < float(right.strip())
            elif ' > ' in condition:
                left, right = condition.split(' > ')
                return float(left.strip()) > float(right.strip())
            elif ' <= ' in condition:
                left, right = condition.split(' <= ')
                return float(left.strip()) <= float(right.strip())
            elif ' >= ' in condition:
                left, right = condition.split(' >= ')
                return float(left.strip()) >= float(right.strip())
            elif ' == ' in condition:
                left, right = condition.split(' == ')
                return str(left.strip()) == str(right.strip())
            elif ' != ' in condition:
                left, right = condition.split(' != ')
                return str(left.strip()) != str(right.strip())
            else:
                # Boolean condition
                return bool(eval(condition, {"__builtins__": {}}, self.current_values))
        except:
            logger.error(f"Failed to evaluate condition: {condition}")
            return True  # Default to safe (constraint satisfied)
    
    def _get_constraint_value(self, condition: str) -> Any:
        """Extract current value from condition"""
        for var_name, value in self.current_values.items():
            if var_name in condition:
                return value
        return None
    
    def _get_constraint_threshold(self, condition: str) -> Any:
        """Extract threshold value from condition"""
        operators = [' < ', ' > ', ' <= ', ' >= ', ' == ', ' != ']
        for op in operators:
            if op in condition:
                return condition.split(op)[1].strip()
        return None

# ============================================================================
# Safety and Interlock Manager
# ============================================================================

class SafetyInterlockManager:
    """Main safety manager coordinating all safety functions"""
    
    def __init__(self):
        self.state_manager = SafetyStateManager()
        self.rule_engine = SafetyRuleEngine()
        self.action_callbacks: Dict[str, Callable] = {}
        self.alert_callback: Optional[Callable] = None
        
        # Statistics
        self.stats = {
            'total_checks': 0,
            'violations_detected': 0,
            'actions_blocked': 0,
            'emergency_stops': 0,
            'last_violation': None
        }
    
    def register_action_callback(self, action: str, callback: Callable):
        """Register callback for automatic safety actions"""
        self.action_callbacks[action] = callback
        logger.info(f"Registered callback for action: {action}")
    
    def set_alert_callback(self, callback: Callable):
        """Set callback for safety alerts"""
        self.alert_callback = callback
    
    async def validate_action(self, action: str, device_id: str, parameters: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate if an action is safe to execute"""
        self.stats['total_checks'] += 1
        
        # Check emergency stop
        if self.state_manager.emergency_stop_active:
            return False, ["Emergency stop active - all actions blocked"]
        
        # Check manual override
        if self.state_manager.manual_override_active:
            logger.info(f"Action allowed under manual override: {action} on {device_id}")
            return True, []
        
        # Update current values for evaluation
        self.rule_engine.update_values(parameters)
        
        # Check interlocks
        interlock_violations = self.rule_engine.evaluate_interlocks(action, device_id)
        if interlock_violations:
            self.stats['actions_blocked'] += 1
            return False, interlock_violations
        
        # Check safety constraints
        safety_violations = self.rule_engine.evaluate_constraints()
        if safety_violations:
            self.stats['violations_detected'] += len(safety_violations)
            self.stats['last_violation'] = datetime.utcnow()
            
            # Update safety state
            self.state_manager.update_safety_state(safety_violations)
            
            # Handle critical violations
            critical_violations = [v for v in safety_violations if v.safety_level == SafetyLevel.CRITICAL]
            if critical_violations:
                self._handle_critical_violations(critical_violations)
                return False, [v.description for v in critical_violations]
            
            # Send alerts for non-critical violations
            await self._send_alerts(safety_violations)
        
        return True, []
    
    async def _handle_critical_violations(self, violations: List[SafetyViolation]):
        """Handle critical safety violations"""
        for violation in violations:
            # Execute automatic actions if configured
            constraint = self.rule_engine.constraints[violation.constraint_id]
            if constraint.auto_action:
                await self._execute_safety_action(constraint.auto_action, constraint.affected_devices)
                violation.auto_action_taken = constraint.auto_action
    
    async def _execute_safety_action(self, action: str, devices: List[str]):
        """Execute automatic safety action"""
        try:
            if action in self.action_callbacks:
                for device_id in devices:
                    await self.action_callbacks[action](device_id)
                    logger.info(f"Executed safety action '{action}' on device {device_id}")
            else:
                logger.warning(f"No callback registered for safety action: {action}")
        except Exception as e:
            logger.error(f"Failed to execute safety action {action}: {e}")
    
    async def _send_alerts(self, violations: List[SafetyViolation]):
        """Send safety alerts"""
        if self.alert_callback:
            try:
                await self.alert_callback(violations)
            except Exception as e:
                logger.error(f"Failed to send safety alerts: {e}")
    
    def get_safety_status(self) -> Dict[str, Any]:
        """Get comprehensive safety status"""
        return {
            'overall_state': self.state_manager.overall_state.value,
            'emergency_stop_active': self.state_manager.emergency_stop_active,
            'manual_override_active': self.state_manager.manual_override_active,
            'active_violations': len(self.state_manager.safety_violations),
            'bypassed_constraints': len(self.state_manager.bypassed_constraints),
            'interlock_status': {
                iid: interlock.status.value 
                for iid, interlock in self.rule_engine.interlocks.items()
            },
            'statistics': self.stats
        }
    
    # ========================================================================
    # Configuration Methods
    # ========================================================================
    
    def load_hvac_safety_config(self):
        """Load standard HVAC safety configuration"""
        # Temperature constraints
        self.rule_engine.add_constraint(SafetyConstraint(
            constraint_id="chiller_temp_high",
            description="Chiller supply temperature too high",
            condition="chiller_supply_temp < 15",
            safety_level=SafetyLevel.WARNING,
            affected_devices=["chiller_001"]
        ))
        
        self.rule_engine.add_constraint(SafetyConstraint(
            constraint_id="chiller_temp_critical",
            description="Chiller supply temperature critical",
            condition="chiller_supply_temp < 20",
            safety_level=SafetyLevel.CRITICAL,
            affected_devices=["chiller_001"],
            auto_action="emergency_stop"
        ))
        
        # Pressure constraints
        self.rule_engine.add_constraint(SafetyConstraint(
            constraint_id="low_suction_pressure",
            description="Pump suction pressure too low",
            condition="pump_suction_pressure > 0.5",
            safety_level=SafetyLevel.CRITICAL,
            affected_devices=["pump_001"],
            auto_action="stop_pump"
        ))
        
        # Interlocks
        self.rule_engine.add_interlock(InterLock(
            interlock_id="pump_suction_interlock",
            description="Pump cannot start without adequate suction pressure",
            source_device="pressure_sensor_001",
            target_device="pump_001",
            condition="pump_suction_pressure > 1.0",
            action_blocked="start"
        ))
        
        self.rule_engine.add_interlock(InterLock(
            interlock_id="chiller_flow_interlock",
            description="Chiller cannot start without water flow",
            source_device="flow_sensor_001",
            target_device="chiller_001",
            condition="chiller_flow > 50",
            action_blocked="start"
        ))

# ============================================================================
# Example Usage
# ============================================================================

async def main():
    """Example usage of safety and interlock manager"""
    
    # Create safety manager
    safety_manager = SafetyInterlockManager()
    
    # Load HVAC safety configuration
    safety_manager.load_hvac_safety_config()
    
    # Register safety action callbacks
    async def emergency_stop_callback(device_id: str):
        print(f"EMERGENCY STOP executed for {device_id}")
    
    async def stop_pump_callback(device_id: str):
        print(f"Pump {device_id} stopped by safety system")
    
    safety_manager.register_action_callback("emergency_stop", emergency_stop_callback)
    safety_manager.register_action_callback("stop_pump", stop_pump_callback)
    
    # Register alert callback
    async def safety_alert_callback(violations):
        for violation in violations:
            print(f"SAFETY ALERT: {violation.description}")
    
    safety_manager.set_alert_callback(safety_alert_callback)
    
    # Simulate system values
    system_values = {
        'chiller_supply_temp': 8.0,
        'pump_suction_pressure': 0.3,
        'chiller_flow': 80.0
    }
    
    # Validate actions
    allowed, reasons = await safety_manager.validate_action(
        "start", "pump_001", system_values
    )
    print(f"Pump start allowed: {allowed}, Reasons: {reasons}")
    
    # Get safety status
    status = safety_manager.get_safety_status()
    print(f"Safety status: {status}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
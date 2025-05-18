#!/usr/bin/env python3
"""
Building Automation Network Discovery Tool
Scans for BACnet, Modbus, and other building automation devices
"""

import socket
import struct
import time
import threading
import ipaddress
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
import json
import argparse
from datetime import datetime

# Try to import optional dependencies
try:
    import nmap
    HAS_NMAP = True
except ImportError:
    HAS_NMAP = False
    print("Warning: python-nmap not installed. Some features will be limited.")

@dataclass
class DiscoveredDevice:
    """Represents a discovered building automation device"""
    ip_address: str
    mac_address: str = ""
    hostname: str = ""
    device_type: str = ""
    protocol: str = ""
    vendor: str = ""
    device_id: str = ""
    object_name: str = ""
    description: str = ""
    services: List[Dict] = None
    last_seen: str = ""
    
    def __post_init__(self):
        if self.services is None:
            self.services = []
        if not self.last_seen:
            self.last_seen = datetime.now().isoformat()

class BuildingNetworkScanner:
    """Main scanner class for building automation networks"""
    
    def __init__(self, target_networks: List[str] = None):
        self.target_networks = target_networks or ['192.168.1.0/24']
        self.discovered_devices = []
        self.scan_results = {
            'bacnet_devices': [],
            'modbus_devices': [],
            'http_devices': [],
            'snmp_devices': [],
            'unknown_devices': []
        }
        
    def scan_network_range(self, network: str, timeout: float = 1.0) -> List[str]:
        """Scan a network range for live IP addresses"""
        print(f"Scanning network range: {network}")
        
        try:
            network_obj = ipaddress.ip_network(network, strict=False)
            live_ips = []
            
            # Use threading for faster scanning
            with ThreadPoolExecutor(max_workers=50) as executor:
                futures = {
                    executor.submit(self._ping_host, str(ip), timeout): str(ip) 
                    for ip in network_obj.hosts()
                }
                
                for future in as_completed(futures):
                    ip = futures[future]
                    try:
                        if future.result():
                            live_ips.append(ip)
                            print(f"  Found live host: {ip}")
                    except Exception as e:
                        print(f"  Error checking {ip}: {e}")
                        
            return live_ips
            
        except ValueError as e:
            print(f"Invalid network range {network}: {e}")
            return []
    
    def _ping_host(self, ip: str, timeout: float) -> bool:
        """Check if a host is reachable"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((ip, 80))  # Try common port
            sock.close()
            return result == 0
        except:
            # Try ICMP ping alternative using socket
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.settimeout(timeout)
                sock.connect((ip, 1))  # Dummy connection
                sock.close()
                return True
            except:
                return False
    
    def scan_bacnet_devices(self, target_ips: List[str]) -> List[DiscoveredDevice]:
        """Scan for BACnet devices using Who-Is broadcast"""
        print("Scanning for BACnet devices...")
        bacnet_devices = []
        
        # BACnet Who-Is message (simplified)
        # Real implementation would use bacpypes library
        who_is_message = self._create_bacnet_whois()
        
        for ip in target_ips:
            try:
                device = self._probe_bacnet_device(ip, who_is_message)
                if device:
                    bacnet_devices.append(device)
                    print(f"  Found BACnet device: {ip}")
            except Exception as e:
                pass  # Silent fail for non-BACnet devices
                
        return bacnet_devices
    
    def _create_bacnet_whois(self) -> bytes:
        """Create a BACnet Who-Is message"""
        # Simplified BACnet Who-Is packet
        # In production, use bacpypes library for proper packet construction
        bvlc_header = b'\x81\x0a\x00\x0c'  # BVLC header
        npdu_header = b'\x01\x20\xff\xff\x00\xff'  # NPDU header for broadcast
        apdu = b'\x10\x08'  # Who-Is service request
        return bvlc_header + npdu_header + apdu
    
    def _probe_bacnet_device(self, ip: str, who_is_message: bytes) -> Optional[DiscoveredDevice]:
        """Probe a single IP for BACnet response"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(2.0)
            sock.sendto(who_is_message, (ip, 47808))  # BACnet UDP port
            
            # Wait for I-Am response
            data, addr = sock.recvfrom(1024)
            sock.close()
            
            if len(data) > 10:  # Valid response
                device = DiscoveredDevice(
                    ip_address=ip,
                    protocol="BACnet",
                    device_type="BACnet Device"
                )
                
                # Parse basic device info from I-Am response
                # This is simplified - real parsing would extract device ID, etc.
                device.device_id = f"bacnet_{ip.replace('.', '_')}"
                return device
                
        except socket.timeout:
            pass
        except Exception as e:
            pass
            
        return None
    
    def scan_modbus_devices(self, target_ips: List[str]) -> List[DiscoveredDevice]:
        """Scan for Modbus TCP devices"""
        print("Scanning for Modbus devices...")
        modbus_devices = []
        
        for ip in target_ips:
            try:
                device = self._probe_modbus_device(ip)
                if device:
                    modbus_devices.append(device)
                    print(f"  Found Modbus device: {ip}")
            except Exception as e:
                pass
                
        return modbus_devices
    
    def _probe_modbus_device(self, ip: str) -> Optional[DiscoveredDevice]:
        """Probe a single IP for Modbus TCP"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            result = sock.connect_ex((ip, 502))  # Modbus TCP port
            
            if result == 0:
                # Send Modbus read request (Function Code 3 - Read Holding Registers)
                transaction_id = b'\x00\x01'
                protocol_id = b'\x00\x00'
                length = b'\x00\x06'
                unit_id = b'\x01'  # Slave ID 1
                function_code = b'\x03'  # Read Holding Registers
                start_address = b'\x00\x00'  # Starting at register 0
                num_registers = b'\x00\x01'  # Read 1 register
                
                modbus_request = (transaction_id + protocol_id + length + 
                                unit_id + function_code + start_address + num_registers)
                
                sock.send(modbus_request)
                response = sock.recv(1024)
                sock.close()
                
                if len(response) >= 9:  # Valid Modbus response
                    device = DiscoveredDevice(
                        ip_address=ip,
                        protocol="Modbus TCP",
                        device_type="Modbus Device"
                    )
                    device.device_id = f"modbus_{ip.replace('.', '_')}"
                    return device
            else:
                sock.close()
                
        except Exception as e:
            pass
            
        return None
    
    def scan_web_interfaces(self, target_ips: List[str]) -> List[DiscoveredDevice]:
        """Scan for web-based building management interfaces"""
        print("Scanning for web interfaces...")
        web_devices = []
        
        common_ports = [80, 443, 8080, 8443, 8000, 9000]
        
        for ip in target_ips:
            for port in common_ports:
                try:
                    device = self._probe_web_interface(ip, port)
                    if device:
                        web_devices.append(device)
                        print(f"  Found web interface: {ip}:{port}")
                        break  # Found one, move to next IP
                except Exception as e:
                    pass
                    
        return web_devices
    
    def _probe_web_interface(self, ip: str, port: int) -> Optional[DiscoveredDevice]:
        """Probe for web interface on specific port"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3.0)
            result = sock.connect_ex((ip, port))
            
            if result == 0:
                # Send HTTP HEAD request
                http_request = f"HEAD / HTTP/1.1\r\nHost: {ip}\r\n\r\n"
                sock.send(http_request.encode())
                response = sock.recv(1024).decode('utf-8', errors='ignore')
                sock.close()
                
                if 'HTTP/' in response:
                    device = DiscoveredDevice(
                        ip_address=ip,
                        protocol="HTTP/HTTPS",
                        device_type="Web Interface"
                    )
                    
                    # Extract server info
                    lines = response.split('\n')
                    for line in lines:
                        if line.lower().startswith('server:'):
                            device.description = line.split(':', 1)[1].strip()
                            break
                    
                    device.device_id = f"web_{ip.replace('.', '_')}_{port}"
                    device.services.append({
                        'type': 'http',
                        'port': port,
                        'protocol': 'https' if port in [443, 8443] else 'http'
                    })
                    return device
            else:
                sock.close()
                
        except Exception as e:
            pass
            
        return None
    
    def scan_snmp_devices(self, target_ips: List[str]) -> List[DiscoveredDevice]:
        """Scan for SNMP-enabled devices (building controllers often support SNMP)"""
        print("Scanning for SNMP devices...")
        snmp_devices = []
        
        # Common SNMP community strings
        communities = ['public', 'private', 'admin', 'manager']
        
        for ip in target_ips:
            for community in communities:
                try:
                    device = self._probe_snmp_device(ip, community)
                    if device:
                        snmp_devices.append(device)
                        print(f"  Found SNMP device: {ip} (community: {community})")
                        break  # Found one, move to next IP
                except Exception as e:
                    pass
                    
        return snmp_devices
    
    def _probe_snmp_device(self, ip: str, community: str) -> Optional[DiscoveredDevice]:
        """Probe for SNMP device with specific community string"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(2.0)
            
            # Simple SNMP GET request for system description (1.3.6.1.2.1.1.1.0)
            # This is a simplified implementation - real SNMP would use pysnmp
            snmp_request = self._create_snmp_get_request(community)
            
            sock.sendto(snmp_request, (ip, 161))  # SNMP port
            response, addr = sock.recvfrom(1024)
            sock.close()
            
            if len(response) > 10:  # Valid SNMP response
                device = DiscoveredDevice(
                    ip_address=ip,
                    protocol="SNMP",
                    device_type="SNMP Device"
                )
                device.device_id = f"snmp_{ip.replace('.', '_')}"
                device.description = f"SNMP community: {community}"
                return device
                
        except Exception as e:
            pass
            
        return None
    
    def _create_snmp_get_request(self, community: str) -> bytes:
        """Create a simple SNMP GET request"""
        # This is a very simplified SNMP packet
        # In production, use pysnmp library
        version = b'\x02\x01\x00'  # SNMPv1
        community_bytes = community.encode() + b'\x00'
        pdu_type = b'\xa0'  # GET request
        # ... (simplified for brevity)
        return b'\x30' + bytes([len(version + community_bytes) + 10]) + version + community_bytes
    
    def enhanced_port_scan(self, target_ips: List[str]) -> List[DiscoveredDevice]:
        """Enhanced port scanning for building automation protocols"""
        print("Performing enhanced port scan...")
        
        # Building automation specific ports
        ba_ports = {
            47808: "BACnet",
            502: "Modbus TCP",
            161: "SNMP",
            80: "HTTP",
            443: "HTTPS",
            10001: "Schneider Electric",
            1911: "Siemens S7",
            2222: "Tridium Niagara",
            4059: "LonWorks/IP",
            1962: "KNX/IP",
            6789: "Johnson Controls",
            8080: "Building Management",
            8443: "Secure Building Management"
        }
        
        enhanced_devices = []
        
        for ip in target_ips:
            device_services = []
            
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {
                    executor.submit(self._scan_port, ip, port): (port, protocol)
                    for port, protocol in ba_ports.items()
                }
                
                for future in as_completed(futures):
                    port, protocol = futures[future]
                    try:
                        if future.result():
                            device_services.append({
                                'port': port,
                                'protocol': protocol,
                                'status': 'open'
                            })
                    except Exception as e:
                        pass
            
            if device_services:
                device = DiscoveredDevice(
                    ip_address=ip,
                    protocol="Multiple",
                    device_type="Building Automation Device",
                    services=device_services
                )
                
                # Determine most likely device type
                if any(s['port'] == 47808 for s in device_services):
                    device.device_type = "BACnet Controller"
                elif any(s['port'] == 502 for s in device_services):
                    device.device_type = "Modbus Controller"
                elif any(s['port'] in [8080, 8443] for s in device_services):
                    device.device_type = "Building Management System"
                
                device.device_id = f"multi_{ip.replace('.', '_')}"
                enhanced_devices.append(device)
                print(f"  Found multi-protocol device: {ip} ({len(device_services)} services)")
        
        return enhanced_devices
    
    def _scan_port(self, ip: str, port: int, timeout: float = 1.0) -> bool:
        """Scan a single port on an IP address"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((ip, port))
            sock.close()
            return result == 0
        except:
            return False
    
    def discover_all(self) -> Dict:
        """Run comprehensive discovery across all target networks"""
        print("Starting comprehensive building automation network discovery...")
        print(f"Target networks: {', '.join(self.target_networks)}")
        
        # Step 1: Find live hosts
        all_live_ips = []
        for network in self.target_networks:
            live_ips = self.scan_network_range(network)
            all_live_ips.extend(live_ips)
        
        print(f"Found {len(all_live_ips)} live hosts total")
        
        if not all_live_ips:
            print("No live hosts found. Check network connectivity and target ranges.")
            return self.scan_results
        
        # Step 2: Protocol-specific scanning
        print("\nRunning protocol-specific scans...")
        
        # BACnet discovery
        bacnet_devices = self.scan_bacnet_devices(all_live_ips)
        self.scan_results['bacnet_devices'] = [device.__dict__ for device in bacnet_devices]
        
        # Modbus discovery
        modbus_devices = self.scan_modbus_devices(all_live_ips)
        self.scan_results['modbus_devices'] = [device.__dict__ for device in modbus_devices]
        
        # Web interface discovery
        web_devices = self.scan_web_interfaces(all_live_ips)
        self.scan_results['http_devices'] = [device.__dict__ for device in web_devices]
        
        # SNMP discovery
        snmp_devices = self.scan_snmp_devices(all_live_ips)
        self.scan_results['snmp_devices'] = [device.__dict__ for device in snmp_devices]
        
        # Enhanced port scanning
        enhanced_devices = self.enhanced_port_scan(all_live_ips)
        self.scan_results['unknown_devices'] = [device.__dict__ for device in enhanced_devices]
        
        # Combine all devices
        all_devices = (bacnet_devices + modbus_devices + web_devices + 
                      snmp_devices + enhanced_devices)
        
        self.discovered_devices = all_devices
        
        print("\nDiscovery complete!")
        print(f"  BACnet devices: {len(bacnet_devices)}")
        print(f"  Modbus devices: {len(modbus_devices)}")
        print(f"  Web interfaces: {len(web_devices)}")
        print(f"  SNMP devices: {len(snmp_devices)}")
        print(f"  Other BA devices: {len(enhanced_devices)}")
        print(f"  Total devices: {len(all_devices)}")
        
        return self.scan_results
    
    def save_results(self, filename: str = None):
        """Save discovery results to JSON file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"building_discovery_{timestamp}.json"
        
        # Add metadata
        results_with_metadata = {
            'scan_metadata': {
                'timestamp': datetime.now().isoformat(),
                'target_networks': self.target_networks,
                'total_devices_found': len(self.discovered_devices)
            },
            'discovered_devices': self.scan_results
        }
        
        with open(filename, 'w') as f:
            json.dump(results_with_metadata, f, indent=2)
        
        print(f"Results saved to: {filename}")
    
    def generate_report(self) -> str:
        """Generate a human-readable report"""
        report = ["Building Automation Network Discovery Report"]
        report.append("=" * 50)
        report.append(f"Scan completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Networks scanned: {', '.join(self.target_networks)}")
        report.append("")
        
        # Summary
        total_devices = len(self.discovered_devices)
        report.append(f"Total devices discovered: {total_devices}")
        report.append("")
        
        # By protocol
        for protocol_type, devices in self.scan_results.items():
            if devices:
                report.append(f"{protocol_type.replace('_', ' ').title()}: {len(devices)}")
                for device in devices:
                    ip = device['ip_address']
                    device_type = device.get('device_type', 'Unknown')
                    report.append(f"  - {ip}: {device_type}")
                report.append("")
        
        # Recommendations
        report.append("Recommendations:")
        report.append("-" * 20)
        
        if self.scan_results['bacnet_devices']:
            report.append("• BACnet devices found - consider using BACpypes for integration")
        
        if self.scan_results['modbus_devices']:
            report.append("• Modbus devices found - pymodbus library recommended")
        
        if self.scan_results['http_devices']:
            report.append("• Web interfaces found - API integration possible")
        
        if self.scan_results['snmp_devices']:
            report.append("• SNMP devices found - monitoring and data collection possible")
        
        return '\n'.join(report)

# Command line interface
def main():
    """
    Run the code to discover the network
    """
    parser = argparse.ArgumentParser(description='Building Automation Network Discovery Tool')
    parser.add_argument('--networks', '-n', nargs='+', 
                       default=['192.168.1.0/24'],
                       help='Network ranges to scan (e.g., 192.168.1.0/24)')
    parser.add_argument('--output', '-o', 
                       help='Output filename for results (JSON)')
    parser.add_argument('--report', '-r', action='store_true',
                       help='Generate human-readable report')
    parser.add_argument('--timeout', '-t', type=float, default=1.0,
                       help='Timeout for network probes (seconds)')
    
    args = parser.parse_args()
    
    # Create scanner
    scanner = BuildingNetworkScanner(target_networks=args.networks)
    
    # Run discovery
    results = scanner.discover_all()
    
    # Save results
    if args.output:
        scanner.save_results(args.output)
    else:
        scanner.save_results()
    
    # Generate report
    if args.report:
        report = scanner.generate_report()
        print("\n" + report)
        
        # Save report to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"building_discovery_report_{timestamp}.txt"
        with open(report_filename, 'w') as f:
            f.write(report)
        print(f"\nReport saved to: {report_filename}")

if __name__ == "__main__":
    main()
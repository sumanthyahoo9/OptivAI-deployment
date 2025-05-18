using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Newtonsoft.Json;
using System.Diagnostics;

namespace BuildingAutomationDiscovery
{
    /// <summary>
    /// Represents a discovered building automation device
    /// </summary>
    public class DiscoveredDevice
    {
        public string IpAddress { get; set; }
        public string MacAddress { get; set; } = "";
        public string Hostname { get; set; } = "";
        public string DeviceType { get; set; } = "";
        public string Protocol { get; set; } = "";
        public string Vendor { get; set; } = "";
        public string DeviceId { get; set; } = "";
        public string ObjectName { get; set; } = "";
        public string Description { get; set; } = "";
        public List<DeviceService> Services { get; set; } = new List<DeviceService>();
        public DateTime LastSeen { get; set; } = DateTime.Now;
    }

    /// <summary>
    /// Represents a service running on a discovered device
    /// </summary>
    public class DeviceService
    {
        public int Port { get; set; }
        public string Protocol { get; set; }
        public string Status { get; set; }
        public Dictionary<string, object> Properties { get; set; } = new Dictionary<string, object>();
    }

    /// <summary>
    /// Discovery scan results container
    /// </summary>
    public class ScanResults
    {
        public DateTime ScanTimestamp { get; set; } = DateTime.Now;
        public List<string> TargetNetworks { get; set; } = new List<string>();
        public List<DiscoveredDevice> BacnetDevices { get; set; } = new List<DiscoveredDevice>();
        public List<DiscoveredDevice> ModbusDevices { get; set; } = new List<DiscoveredDevice>();
        public List<DiscoveredDevice> HttpDevices { get; set; } = new List<DiscoveredDevice>();
        public List<DiscoveredDevice> SnmpDevices { get; set; } = new List<DiscoveredDevice>();
        public List<DiscoveredDevice> UnknownDevices { get; set; } = new List<DiscoveredDevice>();
        
        public int TotalDevicesFound => 
            BacnetDevices.Count + ModbusDevices.Count + HttpDevices.Count + 
            SnmpDevices.Count + UnknownDevices.Count;
    }

    /// <summary>
    /// Main building automation network scanner class
    /// </summary>
    public class BuildingNetworkScanner
    {
        private readonly List<string> _targetNetworks;
        private readonly int _maxConcurrency;
        private readonly int _timeoutMs;
        private ScanResults _scanResults;

        // Building automation specific ports
        private readonly Dictionary<int, string> _buildingAutomationPorts = new Dictionary<int, string>
        {
            { 47808, "BACnet" },
            { 502, "Modbus TCP" },
            { 161, "SNMP" },
            { 80, "HTTP" },
            { 443, "HTTPS" },
            { 10001, "Schneider Electric" },
            { 1911, "Siemens S7" },
            { 2222, "Tridium Niagara" },
            { 4059, "LonWorks/IP" },
            { 1962, "KNX/IP" },
            { 6789, "Johnson Controls" },
            { 8080, "Building Management" },
            { 8443, "Secure Building Management" }
        };

        public BuildingNetworkScanner(List<string> targetNetworks = null, int maxConcurrency = 50, int timeoutMs = 2000)
        {
            _targetNetworks = targetNetworks ?? new List<string> { "192.168.1.0/24" };
            _maxConcurrency = maxConcurrency;
            _timeoutMs = timeoutMs;
            _scanResults = new ScanResults { TargetNetworks = _targetNetworks };
        }

        /// <summary>
        /// Scan a network range for live IP addresses using ping
        /// </summary>
        public async Task<List<IPAddress>> ScanNetworkRangeAsync(string networkCidr)
        {
            Console.WriteLine($"Scanning network range: {networkCidr}");
            
            var liveIps = new List<IPAddress>();
            var ipRange = GetIpRange(networkCidr);
            
            // Use SemaphoreSlim to limit concurrency
            using var semaphore = new SemaphoreSlim(_maxConcurrency);
            var tasks = ipRange.Select(async ip =>
            {
                await semaphore.WaitAsync();
                try
                {
                    if (await PingHostAsync(ip))
                    {
                        lock (liveIps)
                        {
                            liveIps.Add(ip);
                            Console.WriteLine($"  Found live host: {ip}");
                        }
                    }
                }
                finally
                {
                    semaphore.Release();
                }
            });
            
            await Task.WhenAll(tasks);
            return liveIps;
        }

        /// <summary>
        /// Convert CIDR notation to IP address range
        /// </summary>
        private List<IPAddress> GetIpRange(string cidr)
        {
            var ips = new List<IPAddress>();
            var parts = cidr.Split('/');
            var baseIp = IPAddress.Parse(parts[0]);
            var prefixLength = int.Parse(parts[1]);
            
            var mask = (-1) << (32 - prefixLength);
            var maskBytes = BitConverter.GetBytes(mask);
            if (BitConverter.IsLittleEndian) Array.Reverse(maskBytes);
            
            var baseBytes = baseIp.GetAddressBytes();
            var networkBytes = new byte[4];
            for (int i = 0; i < 4; i++)
                networkBytes[i] = (byte)(baseBytes[i] & maskBytes[i]);
            
            var hostBits = 32 - prefixLength;
            var numHosts = (1 << hostBits) - 2; // Exclude network and broadcast
            
            for (int i = 1; i <= numHosts; i++)
            {
                var hostBytes = BitConverter.GetBytes(i);
                if (BitConverter.IsLittleEndian) Array.Reverse(hostBytes);
                
                var ipBytes = new byte[4];
                for (int j = 0; j < 4; j++)
                    ipBytes[j] = (byte)(networkBytes[j] | hostBytes[j]);
                
                ips.Add(new IPAddress(ipBytes));
            }
            
            return ips;
        }

        /// <summary>
        /// Ping a host to check if it's alive
        /// </summary>
        private async Task<bool> PingHostAsync(IPAddress ip)
        {
            try
            {
                using var ping = new Ping();
                var reply = await ping.SendPingAsync(ip, _timeoutMs);
                return reply.Status == IPStatus.Success;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Scan for BACnet devices using Who-Is broadcast
        /// </summary>
        public async Task<List<DiscoveredDevice>> ScanBacnetDevicesAsync(List<IPAddress> targetIps)
        {
            Console.WriteLine("Scanning for BACnet devices...");
            var bacnetDevices = new List<DiscoveredDevice>();
            
            var whoIsMessage = CreateBacnetWhoIsMessage();
            var tasks = targetIps.Select(async ip =>
            {
                var device = await ProbeBacnetDeviceAsync(ip, whoIsMessage);
                if (device != null)
                {
                    lock (bacnetDevices)
                    {
                        bacnetDevices.Add(device);
                        Console.WriteLine($"  Found BACnet device: {ip}");
                    }
                }
            });
            
            await Task.WhenAll(tasks);
            return bacnetDevices;
        }

        /// <summary>
        /// Create a BACnet Who-Is message
        /// </summary>
        private byte[] CreateBacnetWhoIsMessage()
        {
            // Simplified BACnet Who-Is packet
            // BVLC header: Type=0x81 (BACnet/IP), Function=0x0a (Broadcast), Length=0x000c
            var bvlcHeader = new byte[] { 0x81, 0x0a, 0x00, 0x0c };
            
            // NPDU header for broadcast
            var npduHeader = new byte[] { 0x01, 0x20, 0xff, 0xff, 0x00, 0xff };
            
            // APDU: Who-Is service request
            var apdu = new byte[] { 0x10, 0x08 };
            
            var message = new byte[bvlcHeader.Length + npduHeader.Length + apdu.Length];
            Array.Copy(bvlcHeader, 0, message, 0, bvlcHeader.Length);
            Array.Copy(npduHeader, 0, message, bvlcHeader.Length, npduHeader.Length);
            Array.Copy(apdu, 0, message, bvlcHeader.Length + npduHeader.Length, apdu.Length);
            
            return message;
        }

        /// <summary>
        /// Probe a single IP for BACnet response
        /// </summary>
        private async Task<DiscoveredDevice> ProbeBacnetDeviceAsync(IPAddress ip, byte[] whoIsMessage)
        {
            try
            {
                using var udpClient = new UdpClient();
                udpClient.Client.ReceiveTimeout = _timeoutMs;
                
                await udpClient.SendAsync(whoIsMessage, whoIsMessage.Length, new IPEndPoint(ip, 47808));
                
                // Wait for I-Am response
                var result = await udpClient.ReceiveAsync();
                
                if (result.Buffer.Length > 10)
                {
                    var device = new DiscoveredDevice
                    {
                        IpAddress = ip.ToString(),
                        Protocol = "BACnet",
                        DeviceType = "BACnet Device",
                        DeviceId = $"bacnet_{ip.ToString().Replace('.', '_')}"
                    };
                    
                    // Parse basic device info from I-Am response
                    ParseBacnetIAmResponse(device, result.Buffer);
                    return device;
                }
            }
            catch (Exception ex)
            {
                // Silent fail for non-BACnet devices
                Debug.WriteLine($"BACnet probe failed for {ip}: {ex.Message}");
            }
            
            return null;
        }

        /// <summary>
        /// Parse BACnet I-Am response to extract device information
        /// </summary>
        private void ParseBacnetIAmResponse(DiscoveredDevice device, byte[] response)
        {
            try
            {
                // Simplified parsing - in production use a proper BACnet library
                // This would extract device instance, vendor ID, object name, etc.
                
                if (response.Length >= 20)
                {
                    // Extract device instance (simplified)
                    var deviceInstance = BitConverter.ToUInt32(response, 12);
                    device.DeviceId = $"bacnet_device_{deviceInstance}";
                    device.Description = $"BACnet Device Instance: {deviceInstance}";
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error parsing BACnet response: {ex.Message}");
            }
        }

        /// <summary>
        /// Scan for Modbus TCP devices
        /// </summary>
        public async Task<List<DiscoveredDevice>> ScanModbusDevicesAsync(List<IPAddress> targetIps)
        {
            Console.WriteLine("Scanning for Modbus devices...");
            var modbusDevices = new List<DiscoveredDevice>();
            
            var tasks = targetIps.Select(async ip =>
            {
                var device = await ProbeModbusDeviceAsync(ip);
                if (device != null)
                {
                    lock (modbusDevices)
                    {
                        modbusDevices.Add(device);
                        Console.WriteLine($"  Found Modbus device: {ip}");
                    }
                }
            });
            
            await Task.WhenAll(tasks);
            return modbusDevices;
        }

        /// <summary>
        /// Probe a single IP for Modbus TCP
        /// </summary>
        private async Task<DiscoveredDevice> ProbeModbusDeviceAsync(IPAddress ip)
        {
            try
            {
                using var tcpClient = new TcpClient();
                await tcpClient.ConnectAsync(ip, 502);
                
                var stream = tcpClient.GetStream();
                
                // Send Modbus read request (Function Code 3 - Read Holding Registers)
                var modbusRequest = CreateModbusReadRequest();
                await stream.WriteAsync(modbusRequest, 0, modbusRequest.Length);
                
                var response = new byte[1024];
                var bytesRead = await stream.ReadAsync(response, 0, response.Length);
                
                if (bytesRead >= 9) // Valid Modbus response
                {
                    var device = new DiscoveredDevice
                    {
                        IpAddress = ip.ToString(),
                        Protocol = "Modbus TCP",
                        DeviceType = "Modbus Device",
                        DeviceId = $"modbus_{ip.ToString().Replace('.', '_')}"
                    };
                    
                    // Parse Modbus response for additional info
                    ParseModbusResponse(device, response, bytesRead);
                    return device;
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Modbus probe failed for {ip}: {ex.Message}");
            }
            
            return null;
        }

        /// <summary>
        /// Create a Modbus read request
        /// </summary>
        private byte[] CreateModbusReadRequest()
        {
            // Modbus TCP header + PDU
            var transactionId = new byte[] { 0x00, 0x01 };
            var protocolId = new byte[] { 0x00, 0x00 };
            var length = new byte[] { 0x00, 0x06 };
            var unitId = new byte[] { 0x01 }; // Slave ID 1
            var functionCode = new byte[] { 0x03 }; // Read Holding Registers
            var startAddress = new byte[] { 0x00, 0x00 }; // Starting at register 0
            var numRegisters = new byte[] { 0x00, 0x01 }; // Read 1 register
            
            var request = new byte[12];
            var offset = 0;
            Array.Copy(transactionId, 0, request, offset, 2); offset += 2;
            Array.Copy(protocolId, 0, request, offset, 2); offset += 2;
            Array.Copy(length, 0, request, offset, 2); offset += 2;
            Array.Copy(unitId, 0, request, offset, 1); offset += 1;
            Array.Copy(functionCode, 0, request, offset, 1); offset += 1;
            Array.Copy(startAddress, 0, request, offset, 2); offset += 2;
            Array.Copy(numRegisters, 0, request, offset, 2);
            
            return request;
        }

        /// <summary>
        /// Parse Modbus response for device information
        /// </summary>
        private void ParseModbusResponse(DiscoveredDevice device, byte[] response, int length)
        {
            try
            {
                if (length >= 9)
                {
                    var transactionId = (response[0] << 8) | response[1];
                    var functionCode = response[7];
                    
                    device.Description = $"Modbus Device - Function Code: {functionCode:X2}";
                    
                    // Add service information
                    device.Services.Add(new DeviceService
                    {
                        Port = 502,
                        Protocol = "Modbus TCP",
                        Status = "Open",
                        Properties = new Dictionary<string, object>
                        {
                            ["TransactionId"] = transactionId,
                            ["FunctionCode"] = functionCode
                        }
                    });
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error parsing Modbus response: {ex.Message}");
            }
        }

        /// <summary>
        /// Scan for web-based building management interfaces
        /// </summary>
        public async Task<List<DiscoveredDevice>> ScanWebInterfacesAsync(List<IPAddress> targetIps)
        {
            Console.WriteLine("Scanning for web interfaces...");
            var webDevices = new List<DiscoveredDevice>();
            var commonPorts = new[] { 80, 443, 8080, 8443, 8000, 9000 };
            
            foreach (var ip in targetIps)
            {
                foreach (var port in commonPorts)
                {
                    var device = await ProbeWebInterfaceAsync(ip, port);
                    if (device != null)
                    {
                        webDevices.Add(device);
                        Console.WriteLine($"  Found web interface: {ip}:{port}");
                        break; // Found one, move to next IP
                    }
                }
            }
            
            return webDevices;
        }

        /// <summary>
        /// Probe for web interface on specific port
        /// </summary>
        private async Task<DiscoveredDevice> ProbeWebInterfaceAsync(IPAddress ip, int port)
        {
            try
            {
                using var tcpClient = new TcpClient();
                await tcpClient.ConnectAsync(ip, port);
                
                var stream = tcpClient.GetStream();
                
                // Send HTTP HEAD request
                var httpRequest = $"HEAD / HTTP/1.1\r\nHost: {ip}\r\nConnection: close\r\n\r\n";
                var requestBytes = Encoding.ASCII.GetBytes(httpRequest);
                await stream.WriteAsync(requestBytes, 0, requestBytes.Length);
                
                var responseBuffer = new byte[1024];
                var bytesRead = await stream.ReadAsync(responseBuffer, 0, responseBuffer.Length);
                var response = Encoding.ASCII.GetString(responseBuffer, 0, bytesRead);
                
                if (response.Contains("HTTP/"))
                {
                    var device = new DiscoveredDevice
                    {
                        IpAddress = ip.ToString(),
                        Protocol = port == 443 || port == 8443 ? "HTTPS" : "HTTP",
                        DeviceType = "Web Interface",
                        DeviceId = $"web_{ip.ToString().Replace('.', '_')}_{port}"
                    };
                    
                    // Extract server information
                    ParseHttpResponse(device, response, port);
                    return device;
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Web probe failed for {ip}:{port}: {ex.Message}");
            }
            
            return null;
        }

        /// <summary>
        /// Parse HTTP response for server information
        /// </summary>
        private void ParseHttpResponse(DiscoveredDevice device, string response, int port)
        {
            var lines = response.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            
            foreach (var line in lines)
            {
                if (line.StartsWith("Server:", StringComparison.OrdinalIgnoreCase))
                {
                    device.Description = line.Substring(7).Trim();
                    break;
                }
            }
            
            device.Services.Add(new DeviceService
            {
                Port = port,
                Protocol = device.Protocol,
                Status = "Open",
                Properties = new Dictionary<string, object>
                {
                    ["ServerHeader"] = device.Description ?? "Unknown"
                }
            });
            
            // Try to identify building automation systems by server header
            if (device.Description?.ToLower().Contains("tridium") == true)
                device.DeviceType = "Tridium Niagara";
            else if (device.Description?.ToLower().Contains("johnson") == true)
                device.DeviceType = "Johnson Controls";
            else if (device.Description?.ToLower().Contains("schneider") == true)
                device.DeviceType = "Schneider Electric";
        }

        /// <summary>
        /// Enhanced port scanning for building automation protocols
        /// </summary>
        public async Task<List<DiscoveredDevice>> EnhancedPortScanAsync(List<IPAddress> targetIps)
        {
            Console.WriteLine("Performing enhanced port scan...");
            var enhancedDevices = new List<DiscoveredDevice>();
            
            foreach (var ip in targetIps)
            {
                var deviceServices = new List<DeviceService>();
                var tasks = _buildingAutomationPorts.Select(async kvp =>
                {
                    if (await ScanPortAsync(ip, kvp.Key))
                    {
                        lock (deviceServices)
                        {
                            deviceServices.Add(new DeviceService
                            {
                                Port = kvp.Key,
                                Protocol = kvp.Value,
                                Status = "Open"
                            });
                        }
                    }
                });
                
                await Task.WhenAll(tasks);
                
                if (deviceServices.Any())
                {
                    var device = new DiscoveredDevice
                    {
                        IpAddress = ip.ToString(),
                        Protocol = "Multiple",
                        DeviceType = DetermineDeviceType(deviceServices),
                        DeviceId = $"multi_{ip.ToString().Replace('.', '_')}",
                        Services = deviceServices
                    };
                    
                    enhancedDevices.Add(device);
                    Console.WriteLine($"  Found multi-protocol device: {ip} ({deviceServices.Count} services)");
                }
            }
            
            return enhancedDevices;
        }

        /// <summary>
        /// Scan a single port on an IP address
        /// </summary>
        private async Task<bool> ScanPortAsync(IPAddress ip, int port)
        {
            try
            {
                using var tcpClient = new TcpClient();
                var connectTask = tcpClient.ConnectAsync(ip, port);
                var timeoutTask = Task.Delay(_timeoutMs);
                
                var completedTask = await Task.WhenAny(connectTask, timeoutTask);
                
                if (completedTask == connectTask && tcpClient.Connected)
                {
                    return true;
                }
            }
            catch
            {
                // Port closed or filtered
            }
            
            return false;
        }

        /// <summary>
        /// Determine device type based on open services
        /// </summary>
        private string DetermineDeviceType(List<DeviceService> services)
        {
            if (services.Any(s => s.Port == 47808))
                return "BACnet Controller";
            if (services.Any(s => s.Port == 502))
                return "Modbus Controller";
            if (services.Any(s => s.Port == 2222))
                return "Tridium Niagara";
            if (services.Any(s => s.Port == 1911))
                return "Siemens S7 Controller";
            if (services.Any(s => s.Port == 8080 || s.Port == 8443))
                return "Building Management System";
            
            return "Building Automation Device";
        }

        /// <summary>
        /// Run comprehensive discovery across all target networks
        /// </summary>
        public async Task<ScanResults> DiscoverAllAsync()
        {
            Console.WriteLine("Starting comprehensive building automation network discovery...");
            Console.WriteLine($"Target networks: {string.Join(", ", _targetNetworks)}");
            
            // Step 1: Find live hosts
            var allLiveIps = new List<IPAddress>();
            foreach (var network in _targetNetworks)
            {
                var liveIps = await ScanNetworkRangeAsync(network);
                allLiveIps.AddRange(liveIps);
            }
            
            Console.WriteLine($"Found {allLiveIps.Count} live hosts total");
            
            if (!allLiveIps.Any())
            {
                Console.WriteLine("No live hosts found. Check network connectivity and target ranges.");
                return _scanResults;
            }
            
            // Step 2: Protocol-specific scanning
            Console.WriteLine("\nRunning protocol-specific scans...");
            
            // Run all scans in parallel
            var bacnetTask = ScanBacnetDevicesAsync(allLiveIps);
            var modbusTask = ScanModbusDevicesAsync(allLiveIps);
            var webTask = ScanWebInterfacesAsync(allLiveIps);
            var enhancedTask = EnhancedPortScanAsync(allLiveIps);
            
            await Task.WhenAll(bacnetTask, modbusTask, webTask, enhancedTask);
            
            _scanResults.BacnetDevices = await bacnetTask;
            _scanResults.ModbusDevices = await modbusTask;
            _scanResults.HttpDevices = await webTask;
            _scanResults.UnknownDevices = await enhancedTask;
            
            Console.WriteLine($"\nDiscovery complete!");
            Console.WriteLine($"  BACnet devices: {_scanResults.BacnetDevices.Count}");
            Console.WriteLine($"  Modbus devices: {_scanResults.ModbusDevices.Count}");
            Console.WriteLine($"  Web interfaces: {_scanResults.HttpDevices.Count}");
            Console.WriteLine($"  Other BA devices: {_scanResults.UnknownDevices.Count}");
            Console.WriteLine($"  Total devices: {_scanResults.TotalDevicesFound}");
            
            return _scanResults;
        }

        /// <summary>
        /// Save discovery results to JSON file
        /// </summary>
        public async Task SaveResultsAsync(string filename = null)
        {
            if (string.IsNullOrEmpty(filename))
            {
                var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                filename = $"building_discovery_{timestamp}.json";
            }
            
            var json = JsonConvert.SerializeObject(_scanResults, Formatting.Indented);
            await File.WriteAllTextAsync(filename, json);
            
            Console.WriteLine($"Results saved to: {filename}");
        }

        /// <summary>
        /// Generate a human-readable report
        /// </summary>
        public string GenerateReport()
        {
            var report = new StringBuilder();
            report.AppendLine("Building Automation Network Discovery Report");
            report.AppendLine(new string('=', 50));
            report.AppendLine($"Scan completed: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            report.AppendLine($"Networks scanned: {string.Join(", ", _targetNetworks)}");
            report.AppendLine();
            
            // Summary
            report.AppendLine($"Total devices discovered: {_scanResults.TotalDevicesFound}");
            report.AppendLine();
            
            // By protocol
            AddDevicesToReport(report, "BACnet Devices", _scanResults.BacnetDevices);
            AddDevicesToReport(report, "Modbus Devices", _scanResults.ModbusDevices);
            AddDevicesToReport(report, "Web Interfaces", _scanResults.HttpDevices);
            AddDevicesToReport(report, "Other Building Automation Devices", _scanResults.UnknownDevices);
            
            // Recommendations
            report.AppendLine("Recommendations:");
            report.AppendLine(new string('-', 20));
            
            if (_scanResults.BacnetDevices.Any())
                report.AppendLine("• BACnet devices found - consider using BACnet.Core for integration");
            
            if (_scanResults.ModbusDevices.Any())
                report.AppendLine("• Modbus devices found - NModbus4 library recommended");
            
            if (_scanResults.HttpDevices.Any())
                report.AppendLine("• Web interfaces found - API integration possible");
            
            return report.ToString();
        }

        /// <summary>
        /// Helper method to add devices to report
        /// </summary>
        private void AddDevicesToReport(StringBuilder report, string categoryName, List<DiscoveredDevice> devices)
        {
            if (devices.Any())
            {
                report.AppendLine($"{categoryName}: {devices.Count}");
                foreach (var device in devices)
                {
                    report.AppendLine($"  - {device.IpAddress}: {device.DeviceType}");
                    if (device.Services.Any())
                    {
                        foreach (var service in device.Services)
                        {
                            report.AppendLine($"    └─ Port {service.Port} ({service.Protocol})");
                        }
                    }
                }
                report.AppendLine();
            }
        }
    }

    /// <summary>
    /// Console application entry point
    /// </summary>
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Building Automation Network Discovery Tool (C#)");
            Console.WriteLine("================================================");
            
            // Parse command line arguments (simplified)
            var targetNetworks = new List<string> { "192.168.1.0/24" };
            var generateReport = false;
            string outputFile = null;
            
            // Simple argument parsing
            for (int i = 0; i < args.Length; i++)
            {
                switch (args[i].ToLower())
                {
                    case "--networks":
                    case "-n":
                        if (i + 1 < args.Length)
                        {
                            targetNetworks = args[++i].Split(',').ToList();
                        }
                        break;
                    case "--output":
                    case "-o":
                        if (i + 1 < args.Length)
                        {
                            outputFile = args[++i];
                        }
                        break;
                    case "--report":
                    case "-r":
                        generateReport = true;
                        break;
                    case "--help":
                    case "-h":
                        ShowHelp();
                        return;
                }
            }
            
            try
            {
                // Create and run scanner
                var scanner = new BuildingNetworkScanner(targetNetworks);
                var results = await scanner.DiscoverAllAsync();
                
                // Save results
                await scanner.SaveResultsAsync(outputFile);
                
                // Generate report if requested
                if (generateReport)
                {
                    var report = scanner.GenerateReport
                    // Generate report if requested
               if (generateReport)
               {
                   var report = scanner.GenerateReport();
                   Console.WriteLine("\n" + report);
                   
                   // Save report to file
                   var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                   var reportFilename = $"building_discovery_report_{timestamp}.txt";
                   await File.WriteAllTextAsync(reportFilename, report);
                   Console.WriteLine($"\nReport saved to: {reportFilename}");
               }
               
               Console.WriteLine("\nDiscovery completed successfully!");
           }
           catch (Exception ex)
           {
               Console.WriteLine($"Error during discovery: {ex.Message}");
               Console.WriteLine($"Stack trace: {ex.StackTrace}");
               Environment.Exit(1);
           }
           
           Console.WriteLine("Press any key to exit...");
           Console.ReadKey();
       }
       
       /// <summary>
       /// Display help information
       /// </summary>
       static void ShowHelp()
       {
           Console.WriteLine();
           Console.WriteLine("Usage: BuildingDiscovery.exe [options]");
           Console.WriteLine();
           Console.WriteLine("Options:");
           Console.WriteLine("  -n, --networks <networks>   Comma-separated list of networks to scan");
           Console.WriteLine("                              (e.g., 192.168.1.0/24,10.0.0.0/16)");
           Console.WriteLine("  -o, --output <filename>     Output filename for JSON results");
           Console.WriteLine("  -r, --report               Generate human-readable report");
           Console.WriteLine("  -h, --help                 Show this help message");
           Console.WriteLine();
           Console.WriteLine("Examples:");
           Console.WriteLine("  BuildingDiscovery.exe");
           Console.WriteLine("  BuildingDiscovery.exe -n 192.168.1.0/24 -r");
           Console.WriteLine("  BuildingDiscovery.exe -n 10.0.0.0/16,172.16.0.0/12 -o my_scan.json -r");
       }
   }
}
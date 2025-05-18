using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace BuildingAutomationDiscovery.Protocols
{
    /// <summary>
    /// Modbus-specific discovery and communication helper
    /// </summary>
    public class ModbusHelper
    {
        private const int MODBUS_PORT = 502;
        
        /// <summary>
        /// Discover Modbus devices by scanning for responsive slave IDs
        /// </summary>
        public static async Task<List<ModbusDevice>> DiscoverModbusDevicesAsync(
            IPAddress targetIp, 
            int timeoutMs = 2000)
        {
            var devices = new List<ModbusDevice>();
            
            // Test common slave IDs (1-247)
            var commonSlaveIds = new byte[] { 1, 2, 3, 4, 5, 10, 15, 20, 25, 50, 100, 247 };
            
            foreach (var slaveId in commonSlaveIds)
            {
                var device = await ProbeModbusSlaveAsync(targetIp, slaveId, timeoutMs);
                if (device != null)
                {
                    devices.Add(device);
                }
            }
            
            return devices;
        }
        
        /// <summary>
        /// Test if a specific Modbus slave ID responds
        /// </summary>
        private static async Task<ModbusDevice> ProbeModbusSlaveAsync(
            IPAddress ip, 
            byte slaveId, 
            int timeoutMs)
        {
            try
            {
                using var tcpClient = new TcpClient();
                await tcpClient.ConnectAsync(ip, MODBUS_PORT);
                
                var stream = tcpClient.GetStream();
                stream.ReadTimeout = timeoutMs;
                stream.WriteTimeout = timeoutMs;
                
                // Try reading holding registers (function code 3)
                var request = CreateReadHoldingRegistersRequest(slaveId, 0, 1);
                await stream.WriteAsync(request, 0, request.Length);
                
                var response = new byte[12];
                var bytesRead = await stream.ReadAsync(response, 0, response.Length);
                
                if (IsValidModbusResponse(response, bytesRead, slaveId))
                {
                    var device = new ModbusDevice
                    {
                        IpAddress = ip.ToString(),
                        SlaveId = slaveId,
                        LastSeen = DateTime.Now
                    };
                    
                    // Try to get additional device information
                    await EnrichModbusDeviceInfoAsync(stream, device);
                    
                    return device;
                }
            }
            catch (Exception ex)
            {
                // Device not responding for this slave ID
                Console.WriteLine($"Modbus probe failed for {ip}:{slaveId} - {ex.Message}");
            }
            
            return null;
        }
        
        /// <summary>
        /// Create a Modbus Read Holding Registers request
        /// </summary>
        private static byte[] CreateReadHoldingRegistersRequest(byte slaveId, ushort startAddress, ushort numRegisters)
        {
            var request = new byte[12];
            
            // Modbus TCP Header
            request[0] = 0x00; // Transaction ID (high byte)
            request[1] = 0x01; // Transaction ID (low byte)
            request[2] = 0x00; // Protocol ID (high byte)
            request[3] = 0x00; // Protocol ID (low byte)
            request[4] = 0x00; // Length (high byte)
            request[5] = 0x06; // Length (low byte)
            
            // Modbus PDU
            request[6] = slaveId; // Unit ID
            request[7] = 0x03; // Function Code: Read Holding Registers
            request[8] = (byte)(startAddress >> 8); // Start address (high byte)
            request[9] = (byte)(startAddress & 0xFF); // Start address (low byte)
            request[10] = (byte)(numRegisters >> 8); // Number of registers (high byte)
            request[11] = (byte)(numRegisters & 0xFF); // Number of registers (low byte)
            
            return request;
        }
        
        /// <summary>
        /// Validate Modbus response
        /// </summary>
        private static bool IsValidModbusResponse(byte[] response, int length, byte expectedSlaveId)
        {
            if (length < 9) return false;
            
            // Check unit ID
            if (response[6] != expectedSlaveId) return false;
            
            // Check function code (should be 0x03 for read holding registers)
            var functionCode = response[7];
            if (functionCode != 0x03 && functionCode != 0x83) return false; // 0x83 indicates exception
            
            // If it's an exception response, it's still a valid Modbus response
            return true;
        }
        
        /// <summary>
        /// Try to get additional device information using various Modbus function codes
        /// </summary>
        private static async Task EnrichModbusDeviceInfoAsync(NetworkStream stream, ModbusDevice device)
        {
            try
            {
                // Try Read Device Identification (function code 43, sub-function 14)
                var deviceIdRequest = CreateReadDeviceIdentificationRequest(device.SlaveId);
                await stream.WriteAsync(deviceIdRequest, 0, deviceIdRequest.Length);
                
                var response = new byte[256];
                var bytesRead = await stream.ReadAsync(response, 0, response.Length);
                
                ParseDeviceIdentificationResponse(response, bytesRead, device);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Could not get extended Modbus device info: {ex.Message}");
            }
        }
        
        /// <summary>
        /// Create a Read Device Identification request (MEI Type 43)
        /// </summary>
        private static byte[] CreateReadDeviceIdentificationRequest(byte slaveId)
        {
            var request = new byte[11];
            
            // Modbus TCP Header
            request[0] = 0x00; // Transaction ID (high byte)
            request[1] = 0x02; // Transaction ID (low byte)
            request[2] = 0x00; // Protocol ID (high byte)
            request[3] = 0x00; // Protocol ID (low byte)
            request[4] = 0x00; // Length (high byte)
            request[5] = 0x05; // Length (low byte)
            
            // Modbus PDU
            request[6] = slaveId; // Unit ID
            request[7] = 0x2B; // Function Code: Read/Write Multiple registers (MEI)
            request[8] = 0x0E; // MEI Type: Read Device Identification
            request[9] = 0x01; // Read Device ID code: Basic device identification
            request[10] = 0x00; // Object ID: Vendor Name
            
            return request;
        }
        
        /// <summary>
        /// Parse Read Device Identification response
        /// </summary>
        private static void ParseDeviceIdentificationResponse(byte[] response, int length, ModbusDevice device)
        {
            try
            {
                if (length < 13) return;
                
                // Skip Modbus TCP header and check function code
                if (response[7] != 0x2B || response[8] != 0x0E) return;
                
                int offset = 11; // Start after headers
                var numObjects = response[offset++];
                
                for (int i = 0; i < numObjects && offset < length; i++)
                {
                    if (offset + 2 >= length) break;
                    
                    var objectId = response[offset++];
                    var objectLength = response[offset++];
                    
                    if (offset + objectLength > length) break;
                    
                    var objectValue = Encoding.ASCII.GetString(response, offset, objectLength);
                    offset += objectLength;
                    
                    switch (objectId)
                    {
                        case 0x00: // Vendor Name
                            device.VendorName = objectValue;
                            break;
                        case 0x01: // Product Name
                            device.ProductName = objectValue;
                            break;
                        case 0x02: // Major/Minor Version
                            device.Version = objectValue;
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error parsing device identification: {ex.Message}");
            }
        }
    }
    
    /// <summary>
    /// Represents a discovered Modbus device
    /// </summary>
    public class ModbusDevice
    {
        public string IpAddress { get; set; }
        public byte SlaveId { get; set; }
        public string VendorName { get; set; }
        public string ProductName { get; set; }
        public string Version { get; set; }
        public DateTime LastSeen { get; set; }
        public List<ushort> SupportedFunctionCodes { get; set; } = new List<ushort>();
    }
}
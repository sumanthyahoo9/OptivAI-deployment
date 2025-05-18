using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace BuildingAutomationDiscovery.Protocols
{
    /// <summary>
    /// BACnet-specific discovery and communication helper
    /// </summary>
    public class BacnetHelper
    {
        private const int BACNET_PORT = 47808;
        
        /// <summary>
        /// Send a BACnet Who-Is broadcast and collect I-Am responses
        /// </summary>
        public static async Task<List<BacnetDevice>> DiscoverBacnetDevicesAsync(
            IPAddress networkAddress, 
            int timeoutMs = 5000)
        {
            var devices = new List<BacnetDevice>();
            
            try
            {
                using var udpClient = new UdpClient();
                udpClient.EnableBroadcast = true;
                
                // Create and send Who-Is broadcast
                var whoIsMessage = CreateWhoIsMessage();
                var broadcastEndpoint = new IPEndPoint(IPAddress.Broadcast, BACNET_PORT);
                
                await udpClient.SendAsync(whoIsMessage, whoIsMessage.Length, broadcastEndpoint);
                
                // Listen for I-Am responses
                var endTime = DateTime.Now.AddMilliseconds(timeoutMs);
                
                while (DateTime.Now < endTime)
                {
                    try
                    {
                        udpClient.Client.ReceiveTimeout = 1000; // 1 second timeout per receive
                        var result = await udpClient.ReceiveAsync();
                        
                        var device = ParseIAmMessage(result.Buffer, result.RemoteEndPoint);
                        if (device != null)
                        {
                            devices.Add(device);
                        }
                    }
                    catch (SocketException)
                    {
                        // Timeout or no more messages - continue
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"BACnet discovery error: {ex.Message}");
            }
            
            return devices;
        }
        
        /// <summary>
        /// Create a BACnet Who-Is message
        /// </summary>
        private static byte[] CreateWhoIsMessage()
        {
            var message = new List<byte>();
            
            // BVLC Header
            message.Add(0x81); // BVLL Type: BACnet/IP
            message.Add(0x0B); // BVLC Function: Original-Broadcast-NPDU
            message.AddRange(BitConverter.GetBytes((ushort)0x000C).Reverse()); // BVLC Length
            
            // NPDU Header
            message.Add(0x01); // NPDU Version
            message.Add(0x20); // NPDU Control (expecting reply, priority normal)
            message.AddRange(new byte[] { 0xFF, 0xFF }); // Destination network (global broadcast)
            message.Add(0x00); // Destination address length
            message.Add(0xFF); // Hop count
            
            // APDU - Who-Is service request
            message.Add(0x10); // PDU Type: Unconfirmed-REQ, PDU Flags
            message.Add(0x08); // Service Choice: Who-Is
            
            return message.ToArray();
        }
        
        /// <summary>
        /// Parse an I-Am message to extract device information
        /// </summary>
        private static BacnetDevice ParseIAmMessage(byte[] message, IPEndPoint remoteEndPoint)
        {
            try
            {
                if (message.Length < 16) return null;
                
                // Skip BVLC and NPDU headers (typically 10 bytes)
                int offset = 10;
                
                // Check for I-Am service (0x00)
                if (message[offset] != 0x10 || message[offset + 1] != 0x00)
                    return null;
                
                offset += 2;
                
                // Parse device instance
                uint deviceInstance = 0;
                if (offset + 4 < message.Length)
                {
                    // Context tag 0 - device identifier
                    if ((message[offset] & 0xF8) == 0xC0)
                    {
                        var length = message[offset] & 0x07;
                        if (length < 4) length = 4;
                        
                        deviceInstance = BitConverter.ToUInt32(
                            message.Skip(offset + 1).Take(4).Reverse().ToArray(), 0);
                        deviceInstance &= 0x3FFFFF; // Remove object type bits
                    }
                }
                
                return new BacnetDevice
                {
                    IpAddress = remoteEndPoint.Address.ToString(),
                    DeviceInstance = deviceInstance,
                    LastSeen = DateTime.Now
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error parsing I-Am message: {ex.Message}");
                return null;
            }
        }
        
        /// <summary>
        /// Read a BACnet property from a device
        /// </summary>
        public static async Task<string> ReadBacnetPropertyAsync(
            IPAddress deviceIp, 
            uint deviceInstance, 
            BacnetObjectType objectType, 
            uint objectInstance, 
            BacnetPropertyId propertyId)
        {
            try
            {
                using var udpClient = new UdpClient();
                
                var readPropertyMessage = CreateReadPropertyMessage(
                    deviceInstance, objectType, objectInstance, propertyId);
                
                var endpoint = new IPEndPoint(deviceIp, BACNET_PORT);
                await udpClient.SendAsync(readPropertyMessage, readPropertyMessage.Length, endpoint);
                
                udpClient.Client.ReceiveTimeout = 5000;
                var result = await udpClient.ReceiveAsync();
                
                return ParseReadPropertyResponse(result.Buffer);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error reading BACnet property: {ex.Message}");
                return null;
            }
        }
        
        private static byte[] CreateReadPropertyMessage(
            uint deviceInstance, 
            BacnetObjectType objectType, 
            uint objectInstance, 
            BacnetPropertyId propertyId)
        {
            // Simplified implementation - in production use a proper BACnet library
            var message = new List<byte>();
            
            // BVLC Header for unicast
            message.Add(0x81);
            message.Add(0x0A);
            message.AddRange(BitConverter.GetBytes((ushort)0x0019).Reverse());
            
            // NPDU Header
            message.Add(0x01);
            message.Add(0x04); // Expecting reply
            
            // APDU - Read Property Request
            message.Add(0x00); // Confirmed request
            message.Add(0x05); // Max APDU length
            message.Add(0x01); // Invoke ID
            message.Add(0x0C); // Service Choice: Read Property
            
            // Object identifier
            message.Add(0x0C);
            var objectId = ((uint)objectType << 22) | (objectInstance & 0x3FFFFF);
            message.AddRange(BitConverter.GetBytes(objectId).Reverse());
            
            // Property identifier
            message.Add(0x19);
            message.Add((byte)propertyId);
            
            return message.ToArray();
        }
        
        private static string ParseReadPropertyResponse(byte[] response)
        {
            // Simplified parsing - extract string value if present
            try
            {
                // Look for character string tag (0x75)
                for (int i = 0; i < response.Length - 1; i++)
                {
                    if (response[i] == 0x75)
                    {
                        var length = response[i + 1];
                        if (i + 2 + length < response.Length)
                        {
                            var encoding = response[i + 2]; // Character encoding
                            var stringBytes = response.Skip(i + 3).Take(length - 1).ToArray();
                            return Encoding.UTF8.GetString(stringBytes);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error parsing property response: {ex.Message}");
            }
            
            return null;
        }
    }
    
    /// <summary>
    /// Represents a discovered BACnet device
    /// </summary>
    public class BacnetDevice
    {
        public string IpAddress { get; set; }
        public uint DeviceInstance { get; set; }
        public string ObjectName { get; set; }
        public string VendorName { get; set; }
        public string ModelName { get; set; }
        public string ApplicationSoftwareVersion { get; set; }
        public DateTime LastSeen { get; set; }
    }
    
    /// <summary>
    /// BACnet object types
    /// </summary>
    public enum BacnetObjectType : uint
    {
        AnalogInput = 0,
        AnalogOutput = 1,
        AnalogValue = 2,
        BinaryInput = 3,
        BinaryOutput = 4,
        BinaryValue = 5,
        Device = 8,
        TrendLog = 20
    }
    
    /// <summary>
    /// BACnet property identifiers
    /// </summary>
    public enum BacnetPropertyId : byte
    {
        ObjectName = 77,
        VendorName = 121,
        ModelName = 70,
        ApplicationSoftwareVersion = 12,
        PresentValue = 85
    }
}
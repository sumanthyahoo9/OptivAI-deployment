using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.Net;
using System.IO.BACnet;
using System.IO.BACnet.Storage;
using Newtonsoft.Json;
using Microsoft.Extensions.Logging;

namespace BuildingAutomation.BACnet
{
    /// <summary>
    /// Represents a BACnet data point for reading or writing
    /// </summary>
    public class BACnetPoint
    {
        public uint DeviceId { get; set; }
        public BacnetObjectTypes ObjectType { get; set; }
        public uint ObjectInstance { get; set; }
        public BacnetPropertyIds Property { get; set; } = BacnetPropertyIds.PROP_PRESENT_VALUE;
        public string Description { get; set; } = "";
        public string Units { get; set; } = "";
        public bool Writeable { get; set; } = false;
    }

    /// <summary>
    /// Represents a reading from a BACnet point
    /// </summary>
    public class BACnetReading
    {
        public BACnetPoint Point { get; set; }
        public object Value { get; set; }
        public DateTime Timestamp { get; set; }
        public string Quality { get; set; } = "Good";
        public string Error { get; set; }
    }

    /// <summary>
    /// COV subscription information
    /// </summary>
    public class SubscriptionInfo
    {
        public BACnetPoint Point { get; set; }
        public uint ProcessId { get; set; }
        public bool Confirmed { get; set; }
        public DateTime LastRenewal { get; set; }
    }

    /// <summary>
    /// COV notification event arguments
    /// </summary>
    public class CovNotificationEventArgs : EventArgs
    {
        public BACnetPoint Point { get; set; }
        public object Value { get; set; }
        public DateTime Timestamp { get; set; }
    }

    /// <summary>
    /// Comprehensive BACnet client for building automation systems
    /// </summary>
    public class BACnetClient : IDisposable
    {
        private readonly BacnetClient _bacnetClient;
        private readonly DeviceStorage _deviceStorage;
        private readonly ILogger<BACnetClient> _logger;
        private readonly Dictionary<uint, BacnetAddress> _deviceAddresses;
        private readonly Dictionary<string, TaskCompletionSource<object>> _pendingRequests;
        private readonly SemaphoreSlim _requestSemaphore;
        private readonly Timer _discoveryTimer;
        
        // COV subscription management
        private readonly Dictionary<string, SubscriptionInfo> _subscriptions;
        private readonly Timer _subscriptionRenewalTimer;
        
        public event EventHandler<CovNotificationEventArgs> CovNotificationReceived;
        
        public BACnetClient(IPEndPoint localEndpoint, ILogger<BACnetClient> logger = null)
        {
            _logger = logger ?? NullLogger<BACnetClient>.Instance;
            _deviceAddresses = new Dictionary<uint, BacnetAddress>();
            _pendingRequests = new Dictionary<string, TaskCompletionSource<object>>();
            _subscriptions = new Dictionary<string, SubscriptionInfo>();
            _requestSemaphore = new SemaphoreSlim(10, 10); // Limit concurrent requests
            
            try
            {
                // Initialize BACnet client
                _bacnetClient = new BacnetClient(new BacnetIpUdpProtocolTransport(localEndpoint.Port, false));
                _deviceStorage = DeviceStorage.Load("DeviceStorage.xml");
                
                // Set up event handlers
                _bacnetClient.OnIam += OnIAmReceived;
                _bacnetClient.OnCOVNotification += OnCovNotificationReceived;
                _bacnetClient.OnReadPropertyResponse += OnReadPropertyResponse;
                _bacnetClient.OnWritePropertyResponse += OnWritePropertyResponse;
                
                // Start the client
                _bacnetClient.Start();
                
                // Set up periodic device discovery
                _discoveryTimer = new Timer(PerformDeviceDiscovery, null, TimeSpan.Zero, TimeSpan.FromMinutes(5));
                
                // Set up subscription renewal timer
                _subscriptionRenewalTimer = new Timer(RenewSubscriptions, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
                
                _logger.LogInformation($"BACnet client started on {localEndpoint}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize BACnet client");
                throw;
            }
        }

        /// <summary>
        /// Discover devices on the network
        /// </summary>
        public async Task<List<uint>> DiscoverDevicesAsync(TimeSpan? timeout = null)
        {
            timeout ??= TimeSpan.FromSeconds(10);
            var discoveredDevices = new List<uint>();
            
            try
            {
                _logger.LogInformation("Starting device discovery...");
                
                // Send WhoIs broadcast
                _bacnetClient.SendWhoIsBroadcast();
                
                // Wait for responses
                await Task.Delay(timeout.Value);
                
                discoveredDevices = _deviceAddresses.Keys.ToList();
                
                _logger.LogInformation($"Discovered {discoveredDevices.Count} devices");
                
                return discoveredDevices;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during device discovery");
                return discoveredDevices;
            }
        }

        /// <summary>
        /// Read a single BACnet point
        /// </summary>
        public async Task<BACnetReading> ReadPointAsync(BACnetPoint point, TimeSpan? timeout = null)
        {
            timeout ??= TimeSpan.FromSeconds(5);
            
            await _requestSemaphore.WaitAsync();
            
            try
            {
                if (!_deviceAddresses.TryGetValue(point.DeviceId, out var deviceAddress))
                {
                    await DiscoverDevicesAsync();
                    if (!_deviceAddresses.TryGetValue(point.DeviceId, out deviceAddress))
                    {
                        return new BACnetReading
                        {
                            Point = point,
                            Timestamp = DateTime.UtcNow,
                            Quality = "Bad",
                            Error = $"Device {point.DeviceId} not found"
                        };
                    }
                }

                var objectId = new BacnetObjectId(point.ObjectType, point.ObjectInstance);
                var requestId = Guid.NewGuid().ToString();
                var tcs = new TaskCompletionSource<object>();
                
                _pendingRequests[requestId] = tcs;
                
                try
                {
                    // Read property
                    if (!_bacnetClient.ReadPropertyRequest(
                        deviceAddress,
                        objectId,
                        point.Property,
                        out var values,
                        invokeId: (byte)Math.Abs(requestId.GetHashCode() % 256)))
                    {
                        return new BACnetReading
                        {
                            Point = point,
                            Timestamp = DateTime.UtcNow,
                            Quality = "Bad",
                            Error = "Read request failed"
                        };
                    }

                    // Wait for response or timeout
                    using var cts = new CancellationTokenSource(timeout.Value);
                    var result = await tcs.Task.WaitAsync(cts.Token);
                    
                    return new BACnetReading
                    {
                        Point = point,
                        Value = values?.FirstOrDefault()?.Value,
                        Timestamp = DateTime.UtcNow,
                        Quality = "Good"
                    };
                }
                catch (OperationCanceledException)
                {
                    return new BACnetReading
                    {
                        Point = point,
                        Timestamp = DateTime.UtcNow,
                        Quality = "Bad",
                        Error = "Read timeout"
                    };
                }
                finally
                {
                    _pendingRequests.Remove(requestId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error reading point {point.DeviceId}:{point.ObjectType}:{point.ObjectInstance}");
                return new BACnetReading
                {
                    Point = point,
                    Timestamp = DateTime.UtcNow,
                    Quality = "Bad",
                    Error = ex.Message
                };
            }
            finally
            {
                _requestSemaphore.Release();
            }
        }

        /// <summary>
        /// Read multiple BACnet points concurrently
        /// </summary>
        public async Task<List<BACnetReading>> ReadPointsAsync(List<BACnetPoint> points, TimeSpan? timeout = null, int? maxConcurrency = null)
        {
            maxConcurrency ??= 10;
            timeout ??= TimeSpan.FromSeconds(10);
            
            var semaphore = new SemaphoreSlim(maxConcurrency.Value, maxConcurrency.Value);
            var tasks = points.Select(async point =>
            {
                await semaphore.WaitAsync();
                try
                {
                    return await ReadPointAsync(point, timeout);
                }
                finally
                {
                    semaphore.Release();
                }
            });

            return (await Task.WhenAll(tasks)).ToList();
        }

        /// <summary>
        /// Write to a BACnet point
        /// </summary>
        public async Task<bool> WritePointAsync(BACnetPoint point, object value, BacnetApplicationTags valueType, TimeSpan? timeout = null)
        {
            timeout ??= TimeSpan.FromSeconds(5);
            
            await _requestSemaphore.WaitAsync();
            
            try
            {
                if (!_deviceAddresses.TryGetValue(point.DeviceId, out var deviceAddress))
                {
                    await DiscoverDevicesAsync();
                    if (!_deviceAddresses.TryGetValue(point.DeviceId, out deviceAddress))
                    {
                        _logger.LogError($"Device {point.DeviceId} not found for write operation");
                        return false;
                    }
                }

                var objectId = new BacnetObjectId(point.ObjectType, point.ObjectInstance);
                var bacnetValue = new BacnetValue(valueType, value);
                var values = new[] { bacnetValue };

                var requestId = Guid.NewGuid().ToString();
                var tcs = new TaskCompletionSource<object>();
                
                _pendingRequests[requestId] = tcs;
                
                try
                {
                    // Write property
                    if (!_bacnetClient.WritePropertyRequest(
                        deviceAddress,
                        objectId,
                        point.Property,
                        values,
                        invokeId: (byte)Math.Abs(requestId.GetHashCode() % 256)))
                    {
                        _logger.LogError("Write request failed");
                        return false;
                    }

                    // Wait for response or timeout
                    using var cts = new CancellationTokenSource(timeout.Value);
                    await tcs.Task.WaitAsync(cts.Token);
                    
                    _logger.LogInformation($"Successfully wrote {value} to {point.DeviceId}:{point.ObjectType}:{point.ObjectInstance}");
                    return true;
                }
                catch (OperationCanceledException)
                {
                    _logger.LogError("Write timeout");
                    return false;
                }
                finally
                {
                    _pendingRequests.Remove(requestId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error writing to point {point.DeviceId}:{point.ObjectType}:{point.ObjectInstance}");
                return false;
            }
            finally
            {
                _requestSemaphore.Release();
            }
        }

        /// <summary>
        /// Subscribe to COV notifications for a point
        /// </summary>
        public async Task<bool> SubscribeCovAsync(BACnetPoint point, uint processId = 1, bool confirmed = false)
        {
            try
            {
                if (!_deviceAddresses.TryGetValue(point.DeviceId, out var deviceAddress))
                {
                    await DiscoverDevicesAsync();
                    if (!_deviceAddresses.TryGetValue(point.DeviceId, out deviceAddress))
                    {
                        _logger.LogError($"Device {point.DeviceId} not found for COV subscription");
                        return false;
                    }
                }

                var objectId = new BacnetObjectId(point.ObjectType, point.ObjectInstance);
                var subscriptionKey = $"{point.DeviceId}:{point.ObjectType}:{point.ObjectInstance}";

                if (!_bacnetClient.SubscribeCOVRequest(
                    deviceAddress,
                    objectId,
                    processId,
                    confirmed,
                    false, // Cancel subscription
                    uint.MaxValue)) // Infinite lifetime
                {
                    _logger.LogError($"Failed to subscribe to COV for {subscriptionKey}");
                    return false;
                }

                _subscriptions[subscriptionKey] = new SubscriptionInfo
                {
                    Point = point,
                    ProcessId = processId,
                    Confirmed = confirmed,
                    LastRenewal = DateTime.UtcNow
                };

                _logger.LogInformation($"Successfully subscribed to COV for {subscriptionKey}");
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error subscribing to COV for point {point.DeviceId}:{point.ObjectType}:{point.ObjectInstance}");
                return false;
            }
        }

        /// <summary>
        /// Unsubscribe from COV notifications for a point
        /// </summary>
        public async Task<bool> UnsubscribeCovAsync(BACnetPoint point)
        {
            try
            {
                if (!_deviceAddresses.TryGetValue(point.DeviceId, out var deviceAddress))
                {
                    _logger.LogWarning($"Device {point.DeviceId} not found for COV unsubscription");
                    return false;
                }

                var objectId = new BacnetObjectId(point.ObjectType, point.ObjectInstance);
                var subscriptionKey = $"{point.DeviceId}:{point.ObjectType}:{point.ObjectInstance}";

                if (_subscriptions.TryGetValue(subscriptionKey, out var subscription))
                {
                    if (!_bacnetClient.SubscribeCOVRequest(
                        deviceAddress,
                        objectId,
                        subscription.ProcessId,
                        subscription.Confirmed,
                        true, // Cancel subscription
                        0))
                    {
                        _logger.LogError($"Failed to unsubscribe from COV for {subscriptionKey}");
                        return false;
                    }

                    _subscriptions.Remove(subscriptionKey);
                    _logger.LogInformation($"Successfully unsubscribed from COV for {subscriptionKey}");
                }

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error unsubscribing from COV for point {point.DeviceId}:{point.ObjectType}:{point.ObjectInstance}");
                return false;
            }
        }

        /// <summary>
        /// Get device information
        /// </summary>
        public async Task<DeviceInfo> GetDeviceInfoAsync(uint deviceId)
        {
            try
            {
                if (!_deviceAddresses.TryGetValue(deviceId, out var deviceAddress))
                {
                    await DiscoverDevicesAsync();
                    if (!_deviceAddresses.TryGetValue(deviceId, out deviceAddress))
                    {
                        return null;
                    }
                }

                var deviceObject = new BacnetObjectId(BacnetObjectTypes.OBJECT_DEVICE, deviceId);
                var deviceInfo = new DeviceInfo { DeviceId = deviceId };

                // Read device name
                if (_bacnetClient.ReadPropertyRequest(deviceAddress, deviceObject, BacnetPropertyIds.PROP_OBJECT_NAME, out var nameValues))
                {
                    deviceInfo.DeviceName = nameValues?.FirstOrDefault()?.Value?.ToString();
                }

                // Read device description
                if (_bacnetClient.ReadPropertyRequest(deviceAddress, deviceObject, BacnetPropertyIds.PROP_DESCRIPTION, out var descValues))
                {
                    deviceInfo.Description = descValues?.FirstOrDefault()?.Value?.ToString();
                }

                // Read vendor information
                if (_bacnetClient.ReadPropertyRequest(deviceAddress, deviceObject, BacnetPropertyIds.PROP_VENDOR_NAME, out var vendorValues))
                {
                    deviceInfo.VendorName = vendorValues?.FirstOrDefault()?.Value?.ToString();
                }

                // Read model name
                if (_bacnetClient.ReadPropertyRequest(deviceAddress, deviceObject, BacnetPropertyIds.PROP_MODEL_NAME, out var modelValues))
                {
                    deviceInfo.ModelName = modelValues?.FirstOrDefault()?.Value?.ToString();
                }

                // Read object list to get all objects in the device
                if (_bacnetClient.ReadPropertyRequest(deviceAddress, deviceObject, BacnetPropertyIds.PROP_OBJECT_LIST, out var objectListValues))
                {
                    deviceInfo.Objects = objectListValues?.Select(v => (BacnetObjectId)v.Value).ToList() ?? new List<BacnetObjectId>();
                }

                return deviceInfo;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error getting device info for device {deviceId}");
                return null;
            }
        }

        /// <summary>
        /// Event handlers
        /// </summary>
        private void OnIAmReceived(BacnetClient sender, BacnetAddress address, uint deviceId, uint maxApdu, BacnetSegmentations segmentation, ushort vendorId)
        {
            if (!_deviceAddresses.ContainsKey(deviceId))
            {
                _deviceAddresses[deviceId] = address;
                _logger.LogInformation($"Discovered device {deviceId} at {address}");
            }
        }

        private void OnCovNotificationReceived(BacnetClient sender, BacnetAddress address, byte invokeId, uint subscriberProcessIdentifier, BacnetObjectId initiatingDeviceIdentifier, BacnetObjectId monitoredObjectIdentifier, uint timeRemaining, IList<BacnetPropertyValue> values)
        {
            try
            {
                var deviceId = initiatingDeviceIdentifier.instance;
                var point = new BACnetPoint
                {
                    DeviceId = deviceId,
                    ObjectType = monitoredObjectIdentifier.type,
                    ObjectInstance = monitoredObjectIdentifier.instance
                };

                var presentValue = values?.FirstOrDefault(v => v.property.propertyIdentifier == (uint)BacnetPropertyIds.PROP_PRESENT_VALUE);
                
                var eventArgs = new CovNotificationEventArgs
                {
                    Point = point,
                    Value = presentValue?.value?.FirstOrDefault()?.Value,
                    Timestamp = DateTime.UtcNow
                };

                CovNotificationReceived?.Invoke(this, eventArgs);
                
                _logger.LogDebug($"COV notification received for {deviceId}:{monitoredObjectIdentifier.type}:{monitoredObjectIdentifier.instance}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing COV notification");
            }
        }

        private void OnReadPropertyResponse(BacnetClient sender, BacnetAddress address, byte invokeId, BacnetObjectId objectId, BacnetPropertyReference property, IList<BacnetValue> value, BacnetMaxSegments maxSegments)
        {
            try
            {
                var requestId = invokeId.ToString();
                if (_pendingRequests.TryGetValue(requestId, out var tcs))
                {
                    tcs.SetResult(value);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing read property response");
            }
        }

        private void OnWritePropertyResponse(BacnetClient sender, BacnetAddress address, byte invokeId, BacnetObjectId objectId, BacnetPropertyReference property)
        {
            try
            {
                var requestId = invokeId.ToString();
                if (_pendingRequests.TryGetValue(requestId, out var tcs))
                {
                    tcs.SetResult(true);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing write property response");
            }
        }

        /// <summary>
        /// Periodic tasks
        /// </summary>
        private void PerformDeviceDiscovery(object state)
        {
            try
            {
                _bacnetClient.SendWhoIsBroadcast();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during periodic device discovery");
            }
        }

        private void RenewSubscriptions(object state)
        {
            try
            {
                var now = DateTime.UtcNow;
                var expiredSubscriptions = _subscriptions.Values
                    .Where(s => now - s.LastRenewal > TimeSpan.FromHours(1))
                    .ToList();

                foreach (var subscription in expiredSubscriptions)
                {
                    _ = Task.Run(async () =>
                    {
                        await UnsubscribeCovAsync(subscription.Point);
                        await SubscribeCovAsync(subscription.Point, subscription.ProcessId, subscription.Confirmed);
                    });
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during subscription renewal");
            }
        }

        /// <summary>
        /// Disposal
        /// </summary>
        public void Dispose()
        {
            try
            {
                _discoveryTimer?.Dispose();
                _subscriptionRenewalTimer?.Dispose();
                
                // Unsubscribe from all COV notifications
                foreach (var subscription in _subscriptions.Values)
                {
                    _ = Task.Run(async () => await UnsubscribeCovAsync(subscription.Point));
                }
                
                _bacnetClient?.Dispose();
                _requestSemaphore?.Dispose();
                
                _logger.LogInformation("BACnet client disposed");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during disposal");
            }
        }
    }

    /// <summary>
    /// Device information class
    /// </summary>
    public class DeviceInfo
    {
        public uint DeviceId { get; set; }
        public string DeviceName { get; set; }
        public string Description { get; set; }
        public string VendorName { get; set; }
        public string ModelName { get; set; }
        public List<BacnetObjectId> Objects { get; set; } = new List<BacnetObjectId>();
    }

    /// <summary>
    /// Extension methods for easier operation
    /// </summary>
    public static class BACnetExtensions
    {
        /// <summary>
        /// Convert object value to strongly typed value
        /// </summary>
        public static T GetValue<T>(this BACnetReading reading)
        {
            if (reading?.Value == null) return default(T);
            
            try
            {
                return (T)Convert.ChangeType(reading.Value, typeof(T));
            }
            catch
            {
                return default(T);
            }
        }

        /// <summary>
        /// Create BACnet point from common HVAC parameters
        /// </summary>
        public static BACnetPoint CreateHvacPoint(uint deviceId, BacnetObjectTypes objectType, uint instance, string description = "")
        {
            return new BACnetPoint
            {
                DeviceId = deviceId,
                ObjectType = objectType,
                ObjectInstance = instance,
                Description = description,
                Property = BacnetPropertyIds.PROP_PRESENT_VALUE,
                Writeable = objectType == BacnetObjectTypes.OBJECT_ANALOG_OUTPUT || 
                           objectType == BacnetObjectTypes.OBJECT_BINARY_OUTPUT ||
                           objectType == BacnetObjectTypes.OBJECT_MULTI_STATE_OUTPUT
            };
        }
    }
}
// Initialize client
var client = new BACnetClient(new IPEndPoint(IPAddress.Any, 47808), logger);

// Discover devices
var devices = await client.DiscoverDevicesAsync();

// Read temperature setpoint
var tempPoint = BACnetExtensions.CreateHvacPoint(1001, BacnetObjectTypes.OBJECT_ANALOG_INPUT, 1, "Zone Temperature");
var reading = await client.ReadPointAsync(tempPoint);
var temperature = reading.GetValue<float>();

// Write to setpoint
var setpointPoint = BACnetExtensions.CreateHvacPoint(1001, BacnetObjectTypes.OBJECT_ANALOG_OUTPUT, 1, "Temperature Setpoint");
await client.WritePointAsync(setpointPoint, 22.5f, BacnetApplicationTags.BACNET_APPLICATION_TAG_REAL);

// Subscribe to notifications
await client.SubscribeCovAsync(tempPoint);
client.CovNotificationReceived += (sender, args) => {
    Console.WriteLine($"Temperature changed: {args.Value}");
};
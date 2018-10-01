![](images/HeaderPic.png "Microsoft Cloud Workshops")

<div class="MCWHeader1">
Internet of Things
</div>

<div class="MCWHeader2">
Hands-on lab step-by-step
</div>

<div class="MCWHeader3">
March 2018
</div>


Information in this document, including URL and other Internet Web site references, is subject to change without notice. Unless otherwise noted, the example companies, organizations, products, domain names, e-mail addresses, logos, people, places, and events depicted herein are fictitious, and no association with any real company, organization, product, domain name, e-mail address, logo, person, place or event is intended or should be inferred. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this document may be reproduced, stored in or introduced into a retrieval system, or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, or otherwise), or for any purpose, without the express written permission of Microsoft Corporation.

Microsoft may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this document. Except as expressly provided in any written license agreement from Microsoft, the furnishing of this document does not give you any license to these patents, trademarks, copyrights, or other intellectual property.

The names of manufacturers, products, or URLs are provided for informational purposes only and Microsoft makes no representations and warranties, either expressed, implied, or statutory, regarding these manufacturers or the use of the products with any Microsoft technologies. The inclusion of a manufacturer or product does not imply endorsement of Microsoft of the manufacturer or product. Links may be provided to third party sites. Such sites are not under the control of Microsoft and Microsoft is not responsible for the contents of any linked site or any link contained in a linked site, or any changes or updates to such sites. Microsoft is not responsible for webcasting or any other form of transmission received from any linked site. Microsoft is providing these links to you only as a convenience, and the inclusion of any link does not imply endorsement of Microsoft of the site or the products contained therein.
© 2018 Microsoft Corporation. All rights reserved.

Microsoft and the trademarks listed at https://www.microsoft.com/en-us/legal/intellectualproperty/Trademarks/Usage/General.aspx are trademarks of the Microsoft group of companies. All other trademarks are property of their respective owners.

**Contents**

<!-- TOC -->

- [Internet of Things hands-on lab step-by-step](#internet-of-things-hands-on-lab-step-by-step)
    - [Abstract and learning objectives](#abstract-and-learning-objectives)
    - [Overview](#overview)
    - [Requirements](#requirements)
    - [Exercise 1: Environment setup](#exercise-1-environment-setup)
        - [Task 1: Download and open the Smart Meter Simulator project](#task-1-download-and-open-the-smart-meter-simulator-project)
    - [Exercise 2: IoT Hub provisioning](#exercise-2-iot-hub-provisioning)
        - [Task 1: Provision an IoT Hub](#task-1-provision-an-iot-hub)
        - [Task 2: Configure the Smart Meter Simulator](#task-2-configure-the-smart-meter-simulator)
    - [Exercise 3: Completing the Smart Meter Simulator](#exercise-3-completing-the-smart-meter-simulator)
        - [Task 1: Implement device management with the IoT Hub](#task-1-implement-device-management-with-the-iot-hub)
        - [Task 2: Implement the communication of telemetry with the IoT Hub](#task-2-implement-the-communication-of-telemetry-with-the-iot-hub)
        - [Task 3: Verify device registration and telemetry](#task-3-verify-device-registration-and-telemetry)
    - [Exercise 4: Hot path data processing with Stream Analytics](#exercise-4-hot-path-data-processing-with-stream-analytics)
        - [Task 1: Create a Stream Analytics job for hot path processing to Power BI](#task-1-create-a-stream-analytics-job-for-hot-path-processing-to-power-bi)
        - [Task 2: Visualize hot data with Power BI](#task-2-visualize-hot-data-with-power-bi)
    - [Exercise 5: Cold path data processing with HDInsight Spark](#exercise-5-cold-path-data-processing-with-hdinsight-spark)
        - [Task 1: Create the Stream Analytics job for cold path processing](#task-1-create-the-stream-analytics-job-for-cold-path-processing)
        - [Task 2: Verify CSV files in blob storage](#task-2-verify-csv-files-in-blob-storage)
        - [Task 3: Update pandas version on the Spark cluster](#task-3-update-pandas-version-on-the-spark-cluster)
        - [Task 4: Process with Spark SQL](#task-4-process-with-spark-sql)
    - [Exercise 6: Reporting device outages with IoT Hub Operations Monitoring](#exercise-6-reporting-device-outages-with-iot-hub-operations-monitoring)
        - [Task 1: Enable verbose connection monitoring on the IoT Hub](#task-1-enable-verbose-connection-monitoring-on-the-iot-hub)
        - [Task 2: Collect device connection telemetry with the hot path Stream Analytics job](#task-2-collect-device-connection-telemetry-with-the-hot-path-stream-analytics-job)
        - [Task 3: Test the device outage notifications](#task-3-test-the-device-outage-notifications)
        - [Task 4: Visualize disconnected devices with Power BI](#task-4-visualize-disconnected-devices-with-power-bi)
    - [After the hands-on lab](#after-the-hands-on-lab)
        - [Task 1: Delete the resource group](#task-1-delete-the-resource-group)

<!-- /TOC -->

# Internet of Things hands-on lab step-by-step 

## Abstract and learning objectives

This workshop is designed to guide you through an implementation of an end-to-end IoT solution simulating high velocity data emitted from smart meters and analyzed in Azure. In this session, you will design a lambda architecture, filtering a subset of the telemetry data for real-time visualization on the hot path, and storing all the data in long-term storage for the cold path. After completing the package, you will be better able to implement device registration with the IoT Hub registry and visualize hot data with Power BI.

Learning objectives:

-   Implement a simulator sending telemetry from smart meters

-   Capture and process both hot and cold data using Stream Analytics and HDInsight with Spark

-   Visualize hot data with Power BI

## Overview

Fabrikam provides services and smart meters for enterprise energy (electrical power) management. Their "*You-Left-The-Light-On*" service enables the enterprise to understand their energy consumption.

In this hands-on lab, you will construct an end-to-end solution for an IoT scenario that includes device management; telemetry ingest; hot and cold path processing; and reporting.

## Requirements

1.  Microsoft Azure subscription must be pay-as-you-go or MSDN.

    -   Trial subscriptions will *not* work.

2.  A virtual machine configured with:

    -   Visual Studio Community 2017 or later

    -   Azure SDK 2.9 or later (Included with Visual Studio 2017)

3.  A running HDInsight Spark cluster (see [Before the Hands-on Lab](https://github.com/Microsoft/MCW-Internet-of-Things/blob/master/HOL/Setup.md#before-the-hands-on-lab)).


## Exercise 1: Environment setup

Duration: 10 minutes

Fabrikam has provided a Smart Meter Simulator that they use to simulate device registration, as well as the generation and transmission of telemetry data. They have asked you to use this as the starting point for integrating their smart meters with Azure.

### Task 1: Download and open the Smart Meter Simulator project

1.  Connect to your Lab VM, as was detailed in Before the Hands-on Lab, Task 4.

2.  From your Lab VM, download the Smart Meter Simulator starter project from the following URL: <https://bit.ly/2wMSwsH> (Note: the URL is case-sensitive)

3.  Unzip the contents to the folder C:\\SmartMeter\\.

4.  Open SmartMeterSimulator.sln with Visual Studio 2017.

5.  Sign in to Visual Studio or create an account, if prompted.

6.  If the Security Warning for SmartMeterSimulator window appears, uncheck *Ask me for every project in this solution*, and select **OK**.
    
    ![The SmartMeterSimulator Security Warning window has the option to \"Ask me for every project in this solution\" circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image28.png "SmartMeterSimulator Security Warning")

**Note:** If you attempt to build the solution at this point, you will see many build errors. This is intentional. You will correct these in the exercises that follow.

## Exercise 2: IoT Hub provisioning

Duration: 20 minutes

In your architecture design session with Fabrikam, it was agreed that you would use an Azure IoT Hub to manage both the device registration and telemetry ingest from the Smart Meter Simulator. Your team also identified the Microsoft provided Device Explorer project that Fabrikam can use to view the list and status of devices in the IoT Hub registry.

### Task 1: Provision an IoT Hub

In these steps, you will provision an instance of IoT Hub.

1.  In a browser, navigate to the Azure Portal (<https://portal.azure.com)>.

2.  Select **+New**, then select **Internet of Things**, and select **IoT Hub**.
    
    ![In the Azure Portal, new is selected in the left pane. In the right, New pane, under Azure Marketplace, Internet of Things is selected, and under Featured, IoT Hub is circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image29.png "Azure Portal, New pane")

3.  In the **IoT Hub** blade, enter the following:

    -   Name: Provide a name for your new IoT Hub, such as smartmeter-hub

    -   Pricing and scale tier: Select **F1** **Free**

    -   IoT Hub units: Set to **1** automatically when the F1 pricing tier is selected.

    -   Device-to-cloud partitions: Set to **2 partitions** automatically when the F1 pricing tier is selected.

    -   Subscription: Select the same subscription you've been using for previous resources in this lab.

    -   Resource group: Select Use existing, and select the **iot-hol** resource group you created previously.

    -   Location: Select the location you used previously.

        ![The IoT hub blade fields display with the previously mentioned settings.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image30.png "IoT hub blade")

    -   Select **Create**.

4.  When the IoT Hub deployment is completed, you will receive a notification in the Azure portal. Navigate to your new IoT Hub by selecting Go to resource in the notification.

    ![In the Notifications section, under Deployment succeeded, the Go to resource button is selected.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image31.png "Notifications section")

5.  From the IoT Hub's Overview blade, select **Shared access policies** under Settings on the left-hand menu.
    
    ![In the Overview blade settings section, Shared access policies is circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image32.png "Overview blade, settings section")

6.  Select **iothubowner** policy.
    
    ![Under Policy, iothubowner is circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image33.png "Policy section")

7.  In the iothubowner blade, select the **Copy** button to the right of the **Connection string - primary key** field. You will be pasting the connection string value into a TextBox's Text property value in the next Task.
    
    ![The iothubowner blade fields display with the previously mentioned settings. Under Shared access keys, the Connection string???primary key and its corresponding copy button are circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image34.png "iothubowner blade")

### Task 2: Configure the Smart Meter Simulator

If you want to save this connection string with your project (in case you stop debugging or otherwise close the simulator), you can set this as the default text for the text box. Follow these steps to configure the connection string:

1.  Return to Visual Studio on your Lab VM.

2.  In the Solution Explorer, double-click **MainForm.cs** to open it. (If the Solution Explorer is not in the upper left corner of your Visual Studio instance, you can find it under the View menu in Visual Studio.)

    ![In the Visual Studio Solution Explorer window, SmartMeterSimulator is expanded, and under it, MainForm.cs is circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image35.png "Visual Studio Solution Explorer")

3.  In the Windows Forms designer surface, click the **IoT Hub Connection String TextBox** to select it.
    
    ![The Windows Form designer surface is opened to the MainForm.cs tab. The IoT Hub Connection String is circled, but is empty.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image36.png "Windows Form designer surface")

4.  In the Properties panel, scroll until you see the **Text** property. Paste your IoT Hub connection string value copied in step 7 of the previous task into the value for the Text property. (If the properties window is not visible below the Solution Explorer, right-click the TextBox, and select **Properties**.)\
    
    ![In the Properties panel, the Text property is circled, and is set to HostName=smartmeter-hub.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image37.png "Solution Explorer")

5.  Your connection string should now be present every time you run the Smart Meter Simulator.
    
    ![The Windows Form designer surface is opened to the MainForm.cs tab. The IoT Hub Connection String now displays.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image38.png "IoT Hub Connection String dialog")

## Exercise 3: Completing the Smart Meter Simulator

Duration: 60 minutes

Fabrikam has left you a partially completed sample in the form of the Smart Meter Simulator solution. You will need to complete the missing lines of code that deal with device registration management and device telemetry transmission that communicate with your IoT Hub.

### Task 1: Implement device management with the IoT Hub

1.  In Visual Studio on your Lab VM, use Solution Explorer to open the file **DeviceManager.cs**.

2.  From the Visual Studio **View** menu, click **Task List**.

    ![On the Visual Studio View menu, Task List is selected.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image39.png "Visual Studio View menu")

3.  There you will see a list of TODO tasks, where each task represents one line of code that needs to be completed. Copy the code in **bold** below the corresponding TODO item in the completed code that follows.

4.  The following code represents the completed tasks in DeviceManager.cs:
    ```
    class DeviceManager
    {
        static string connectionString;
        static RegistryManager registryManager;

        public static string HostName { get; set; }

        public static void IotHubConnect(string cnString)
        {
            connectionString = cnString;

            //TODO: 1.Create an instance of RegistryManager from connectionString
            registryManager = RegistryManager.CreateFromConnectionString(connectionString);

            var builder = IotHubConnectionStringBuilder.Create(cnString);

            HostName = builder.HostName;
        }
            
        /// <summary>
        /// Register a single device with the IoT hub. The device is initially registered in a
        /// disabled state.
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="deviceId"></param>
        /// <returns></returns>
        public async static Task<string> RegisterDevicesAsync(string connectionString, string deviceId)
        {
            //Make sure we're connected
            if (registryManager == null)
                IotHubConnect(connectionString);

            //TODO: 2.Create new device
            Device device = new Device(deviceId);
                    
            //TODO: 3.Initialize device with a status of Disabled
            //Enabled in a subsequent step
            device.Status = DeviceStatus.Disabled;

            try
            {
                //TODO: 4.Register the new device
                device = await registryManager.AddDeviceAsync(device);
            }
            catch (Exception ex)
            {
                if (ex is DeviceAlreadyExistsException ||
                    ex.Message.Contains("DeviceAlreadyExists"))
                { 
                    //TODO: 5.Device already exists, get the registered device
                    device = await registryManager.GetDeviceAsync(deviceId);

                    //TODO: 6.Ensure the device is disabled until Activated later
                    device.Status = DeviceStatus.Disabled;

                    //TODO: 7.Update IoT Hubs with the device status change
                    await registryManager.UpdateDeviceAsync(device);
                }
                else
                {
                    MessageBox.Show($"An error occurred while registering one or more devices:\r\n{ex.Message}");
                }
            }

            //return the device key
            return device.Authentication.SymmetricKey.PrimaryKey;
        }

        /// <summary>
        /// Activate an already registered device by changing its status to Enabled.
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="deviceId"></param>
        /// <param name="deviceKey"></param>
        /// <returns></returns>
        public async static Task<bool> ActivateDeviceAsync(string connectionString, string deviceId, string deviceKey)
        {
            //Server-side management function to enable the provisioned device 
            //to connect to IoT Hub after it has been installed locally. 
            //If device id device key are valid, Activate (enable) the device.

            //Make sure we're connected
            if (registryManager == null)
                IotHubConnect(connectionString);

            bool success = false;
            Device device;

            try
            { 
                //TODO: 8.Fetch the device
                device = await registryManager.GetDeviceAsync(deviceId);

                //TODO: 9.Verify the device keys match
                if (deviceKey == device.Authentication.SymmetricKey.PrimaryKey)
                { 
                    //TODO: 10.Enable the device
                    device.Status = DeviceStatus.Enabled;

                    //TODO: 11.Update IoT Hubs
                    await registryManager.UpdateDeviceAsync(device);

                    success = true;
                }
            }
            catch(Exception)
            {
                success = false;
            }

            return success;
        }

        /// <summary>
        /// Deactivate a single device in the IoT Hub registry.
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="deviceId"></param>
        /// <returns></returns>
        public async static Task<bool> DeactivateDeviceAsync(string connectionString, string deviceId)
        {
            //Make sure we're connected
            if (registryManager == null)
                IotHubConnect(connectionString);

            bool success = false;
            Device device;

            try
            {
                //TODO: 12.Lookup the device from the registry by deviceId
                device = await registryManager.GetDeviceAsync(deviceId);

                //TODO: 13.Disable the device
                device.Status = DeviceStatus.Disabled;

                //TODO: 14.Update the registry 
                await registryManager.UpdateDeviceAsync(device);

                success = true;
            }
            catch (Exception)
            {
                success = false;
            }

            return success;
        }

        /// <summary>
        /// Unregister a single device from the IoT Hub Registry
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="deviceId"></param>
        /// <returns></returns>
        public async static Task UnregisterDevicesAsync(string connectionString, string deviceId)
        {
            //Make sure we're connected
            if (registryManager == null)
                IotHubConnect(connectionString);

                //TODO: 15.Remove the device from the Registry
                await registryManager.RemoveDeviceAsync(deviceId);
        }

        /// <summary>
        /// Unregister all the devices managed by the Smart Meter Simulator
        /// </summary>
        /// <param name="connectionString"></param>
        /// <returns></returns>
        public async static Task UnregisterAllDevicesAsync(string connectionString)
        {
            //Make sure we're connected
            if (registryManager == null)
               IotHubConnect(connectionString);

            for(int i = 0; i <= 9; i++)
            {
                string deviceId = "Device" + i.ToString();

                //TODO: 16.Remove the device from the Registry
                await registryManager.RemoveDeviceAsync(deviceId);
            }
        }
    }
    ```
5.  Save DeviceManager.cs.

### Task 2: Implement the communication of telemetry with the IoT Hub

1.  Open Sensor.cs from the Solution Explorer, and complete the TODO items indicated within the code that are responsible for transmitting telemetry data to the IoT Hub.

2.  The following code shows the completed result:
    ```
    class Sensor
    {
        private DeviceClient _DeviceClient;
        private string _IotHubUri { get; set; }
        public string DeviceId { get; set; }
        public string DeviceKey { get; set; }
        public DeviceState State { get; set; }
        public string StatusWindow { get; set; }
        public double CurrentTemperature
        {
            get
            {
                double avgTemperature = 70;
                Random rand = new Random();
                 
                double currentTemperature = avgTemperature + rand.Next(-6, 6);

                if(currentTemperature <= 68)
                    TemperatureIndicator = SensorState.Cold;
                else if(currentTemperature > 68 && currentTemperature < 72)
                    TemperatureIndicator = SensorState.Normal;
                else if(currentTemperature >= 72)
                    TemperatureIndicator = SensorState.Hot;

                return currentTemperature;
            }
        }

        public SensorState TemperatureIndicator { get; set; }

        public Sensor(string iotHubUri, string deviceId, string deviceKey)
        {
            _IotHubUri = iotHubUri;
            DeviceId = deviceId;
            DeviceKey = deviceKey;
            State = DeviceState.Registered;
        }

        public void InstallDevice(string statusWindow)
        {
            StatusWindow = statusWindow;
            State = DeviceState.Installed;
        }

        /// <summary>
        /// Connect a device to the IoT Hub by instantiating a DeviceClient for that Device by Id and Key.
        /// </summary>
        public void ConnectDevice()
        {
            //TODO: 17. Connect the Device to Iot Hub by creating an instance of DeviceClient
            _DeviceClient = DeviceClient.Create(_IotHubUri, new DeviceAuthenticationWithRegistrySymmetricKey(DeviceId, DeviceKey));

            //Set the Device State to Ready
            State = DeviceState.Ready;
        }

        public void DisconnectDevice()
        {
            //Delete the local device client
            _DeviceClient = null;

            //Set the Device State to Activate
            State = DeviceState.Activated;
        }

        /// <summary>
        /// Send a message to the IoT Hub from the Smart Meter device
        /// </summary>
        public async void SendMessageAsync()
        {
            var telemetryDataPoint = new
            {
                id = DeviceId,
                time = DateTime.UtcNow.ToString("o"),
                temp = CurrentTemperature
            };

            //TODO: 18.Serialize the telemetryDataPoint to JSON
            var messageString = JsonConvert.SerializeObject(telemetryDataPoint);

            //TODO: 19.Encode the JSON string to ASCII as bytes and create new Message with the bytes
            var message = new Message(Encoding.ASCII.GetBytes(messageString));

            //TODO: 20.Send the message to the IoT Hub
    	 var sendEventAsync = _DeviceClient?.SendEventAsync(message);
            if (sendEventAsync != null) await sendEventAsync;
        }
    }
    ```
3.  Save Sensor.cs

### Task 3: Verify device registration and telemetry

1.  Run the Smart Meter Simulator, by clicking on the green Start button on the Visual Studio menu bar.
    
    ![The green Start button is circled on the Visual Studio menu bar.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image40.png "Visual Studio menu bar")

2.  Click **Register**. The windows within the building should turn from black to gray.
    
    ![In addition to the IoT Hub Connection String, the Smart Meter Simulator has two buildings with 10 windows. The color of the windows indicating the status of the devices. Currently, all windows are gray.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image41.png "Fabrikam Smart Meter Simulator")

3.  Click some of the windows. Each represents a device for which you want to simulate device installation. The selected windows should turn yellow.

    ![The Smart Meter Simulator now has three white windows, with the other seven remaining gray.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image42.png "Fabrikam Smart Meter Simulator")

4.  Click **Activate** to simulate changing the device status from disabled to enabled in the Iot Hub Registry. The selected windows should turn green.

    ![On the Smart Meter Simulator, the Activate button is circled, and the three white windows have now turned to green.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image43.png "Fabrikam Smart Meter Simulator")

5.  At this point, you have registered 10 devices (the gray windows) but activated only the ones you selected (in green). To view this list of devices, you will switch over to the Azure Portal, and open the IoT Hub you provisioned.

6.  From the IoT Hub blade, click the **IoT Devices** link under Explorers on the left-hand menu.
    
    ![On the IoT Hub blade, in the Explorers section, under Explorers, IoT Devices is circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image44.png "IoT Hub blade, Explorers section")

7.  You should see all 10 devices listed, with the ones that you activated having a status of **enabled**.
    
    ![Devices in the Device ID list have a status of either enabled or disabled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image45.png "Device ID list")

8.  Return to the **Smart Meter Simulator**.

9.  Click **Connect**. Within a few moments, you should begin to see activity as the windows change color indicating the smart meters are transmitting telemetry. The grid on the left will list each telemetry message transmitted and the simulated temperature value.

    ![On the Smart Meter Simulator, the Connect button is circled, and one of the green windows has now turned to blue. The current windows count is now seven gray, two green, and one blue.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image46.png "Fabrikam Smart Meter Simulator")

10. Allow the smart meter to continue to run. (Whenever you want to stop the transmission of telemetry, click the **Disconnect** button.)

## Exercise 4: Hot path data processing with Stream Analytics

Duration: 45 minutes

Fabrikam would like to visualize the "hot" data showing the average temperature reported by each device over a 5-minute window in Power BI.

### Task 1: Create a Stream Analytics job for hot path processing to Power BI

1.  In your browser, navigate to the **Azure Portal** (<https://portal.azure.com>).

2.  Select **+New**, **Data + Analytics**, then select **Stream Analytics job**.
    
    ![The Azure Portal has New, Data + Analytics, and Stream Analytics job circled. ](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image47.png "Azure Portal, New pane")

3.  On the New Stream Analytics Job blade, enter the following:

    -   Job Name: Enter **hot-stream**.

    -   Subscription: Choose the same subscription you have been using thus far.

    -   Resource Group: Choose the **iot-hol** Resource Group.

    -   Location: Choose the same Location as you have for your other resources.

    -   Hosting environment: Select **Cloud**.

        ![The New Stream Analytics Job blade fields display with the previously mentioned settings.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image48.png "New Stream Analytics Job blade")

    -   Select **Create** to provision the new Stream Analytics job.

4.  Once provisioned, navigate to your new Stream Analytics job in the portal.

5.  Select **Inputs** on the left-hand menu, under Job Topology.

    ![Under Job Topology, Inputs is selected.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image49.png "Job Topology section")

6.  On the Inputs blade, select **+Add stream input**, then select **IoT Hub** from the dropdown menu to add an input connected to your IoT Hub.
    
    ![Inputs blade, Add stream input, IoT Hub selected.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image50.png "Inputs blade IoT Hub")

7.  On the New Input blade, enter the following:

    -   Input Alias: Set the value to **temps**.

    -   Source Type: Choose **Data stream**.

    -   Source: Choose **IoT hub**.

    -   Import Option: Choose **Select IoT hub from your subscriptions**.

    -   IoT Hub: Select your existing IoT Hub, **smartmeter-hub**.

    -   Endpoint: Choose **Messaging**.

    -   Shared Access Policy Name: Set to **Service**.

    -   Consumer Group: Leave as **\$Default**.

    -   Event serialization format: Choose **JSON**.

    -   Encoding: Choose **UTF-8**.

    -   Event compression type: Leave set to **None**.

        ![New IoT Hub input form for Stream Analytics](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image51.png "IoT Hub blade")

    -   Select **Save**.

8.  Now, select **Outputs** from the left-hand menu, under Job Topology.

    ![Under Job Topology, Outputs is selected.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image52.png "Job Topology Section")

9.  In the Outputs blade, select **+Add**, then **Power BI** to add the output destination for the query.
    
    ![Select + Add, then Power BI within the Stream Analytics outputs blade](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image53.png "Add Power Bit")

10. On the New output blade, enter the following:

    -   Output alias: Set to **powerbi**.

    -   Select **Authorize** to authorize the connection to your Power BI account. When prompted in the popup window, enter the account credentials you used to create your Power BI account in [Before the Hands-on Lab, Task 1](#task-1-provision-power-bi).

        ![Power BI new output blade. Output alias is selected and contains powerbi. Authorize button is selected.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image54.png "Power BI new output blade")

    -   For the remaining Power BI settings, enter the following:

        i.  Group Workspace: Select the default, **My Workspace**.

        ii. Dataset Name: Set to **avgtemps**.

        iii. Table Name: Set to **avgtemps**.

        iv. Select **Save**.

        ![Power BI blade. Output alias is powerbi, dataset name is avgtemps, table name is avgtemps.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image55.png "Power BI blade ")

11. Next, select **Query** from the left-hand menu, under Job Topology.
    
    ![Under Job Topology, Query is selected.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image56.png "Job Topology Section")

12. In the query text box, paste the following query.

    **SELECT** **AVG(**temp**)** **AS** Average**,** id

    **INTO** powerbi

    **FROM** temps

    **GROUP** **BY** TumblingWindow**(minute,** 5**),** id

13. Select **Save,** and **Yes** when prompted with the confirmation.

    ![Save button](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image57.png "Save button")

14. Return to the Overview blade on your Stream Analytics job and select **Start**.

    ![The Start button is circled on the Overview blade.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image58.png "Overview blade start  button")

15. In the Start job blade, select **Now** (the job will start processing messages from the current point in time onward).
    
    ![The New button is selected on the Start job blade.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image59.png "Start job blade")

16. Select **Start**.

17. Allow your Stream Analytics Job a few minutes to start.

18. Once the Stream Analytics Job has successfully started, verify that you are showing a non-zero amount of **Input Events** on the **Monitoring** chart on the overview blade. You may need to reconnect your devices on the Smart Meter Simulator and let it run for a while to see the events.
    
    ![The Monitoring chart lists 397 input events, 0 output events, and 0 runtime errors.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image60.png "Monitoring chart")

### Task 2: Visualize hot data with Power BI

1.  Sign in to your Power BI subscription (<https://app.powerbi.com>) to see if data is being collected.

2.  Select **My Workspace** on the left-hand menu, then select the **Datasets tab**.
    
    ![On the Power BI window, My Workspace is circled in the left pane, and the Datasets tab is circled in the right pane.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image61.png "Power BI window")

3.  Under the Datasets list, select the avgtemps dataset. Search for the avgtemps dataset, if there are too many items in the dataset list.
    
    ![On the Datasets tab, avgtemps is circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image62.png "Datasets tab")

4.  Select the Create Report button under the Actions column.
    
    ![On the Datasets tab, under Actions, the Create Report button is circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image63.png "Datasets tab, Action column")

5.  On the Visualizations palette, select **Stacked** **Column Chart** to create a chart visualization.

    ![On the Visualizations palette, the stacked column chart icon is circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image64.png "Visualizations palette")

6.  In the Fields listing, select and drag the **id** field, and drop it into the **Axis** field.
    
    ![Under Fields, an arrow points from the id field under avgtemps, to the same id field now located in the Visualizations listing, under Axis.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image65.png "Visualizations and Fields")

7.  Select and drag the **average** field and drop it into the **value** field.
    
    ![Under Fields, an arrow points from the average field under avgtemps, to the same id field now located in the Visualizations listing, under Value.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image66.png "Visualizations and Fields")

8.  Now, set the Value to **Max of average**, by click the down arrow next to **average**, and select **Maximum**.
    
    ![On the Value drop-down list, Maximum is circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image67.png "Value drop-down list")

9.  Repeat steps 5-8, this time adding a Stacked Column Chart for **Min of average**. (You may need to click on any area of white space on the report designer surface to deselect the Max of average chart visualization.)
    
    ![Min of average is added under Value.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image68.png "Min of average")

10. Next, add a **table visualization**.

    ![On the Visualizations pallete, the table icon is circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image69.png "Visualizations pallete")

11. Set the values to **id** and **Average of average**, by dragging and dropping both fields in the Values field, then selecting the dropdown next to average, and selecting Average.

    ![ID and Average of average now display under Values.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image70.png "ID")

12. Save the report.

    ![Under File, Save is selected.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image71.png "Save option")

13. Enter the name "Average Temperatures," and click **Save**.
    
    ![The report name is set to Average Temperatures.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image72.png "Save your report")

14. Switch to **Reading View**.

    ![Reading View button](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image73.png "Reading View button")

15. Within the report, click one of the columns to see the data for just that device.
    
    ![The report window has two bar graphs: Max of average by id, and Min of average by id. both bar charts list data for Devide0, Device1, Device3, Device8, and Device9. Device1 is selected. On the right, a table displays data for Device1, with an Average of average value of 69.50.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image74.png "Report window")

## Exercise 5: Cold path data processing with HDInsight Spark

Duration: 60 minutes

Fabrikam would like to be able to capture all the "cold" data into scalable storage so that they can summarize it periodically using a Spark SQL query.

### Task 1: Create the Stream Analytics job for cold path processing

To capture all metrics for the cold path, set up another Stream Analytics job that will write all events to Blob storage for analyses by Spark running on HDInsight.

1.  In your browser, navigate to the **Azure Portal** (<https://portal.azure.com>).

2.  Select **+New**, **Data + Analytics**, then select **Stream Analytics job**.
    
    ![In the Azure Portal, in the left pane, New is selected. In the New pane, Data + Analytics and Stream Analytics job are circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image47.png "Azure Portal")

3.  On the New Stream Analytics Job blade, enter the following:

    -   Job Name: Enter **cold-stream**.

    -   Subscription: Choose the same subscription you have been using thus far.

    -   Resource Group: Choose the **iot-hol** Resource Group.

    -   Location: Choose the same Location as you have for your other resources.

    -   Hosting environment: Select **Cloud**.

        ![The New Stream Analytics Job blade fields display with the previously mentioned settings.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image75.png "New Stream Analytics Job blade")

    -   Select **Create**.

4.  Once provisioned, navigate to your new Stream Analytics job in the portal.

5.  Select **Inputs** on the left-hand menu, under Job Topology.

    ![Under Job Topology, Inputs is selected.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image49.png "Job Topology Section")

6.  On the Inputs blade, select **+Add**, then **IoT Hub** to add an input connected to your IoT Hub.
    
    ![Select + Add stream input, then select IoT Hub from the dropdown menu](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image50.png "Add stream input")

7.  On the New Input blade, enter the following:

    -   Input Alias: Set the value to **iothub**.

    -   Choose **Select IoT hub from your subscriptions**.

    -   IoT Hub: Select your existing IoT Hub, **smartmeter-hub**.

    -   Endpoint: Choose **Messaging**.

    -   Shared Access Policy Name: Set to **Service**.

    -   Consumer Group: Leave as **\$Default**.

    -   Event serialization format: Choose **JSON**.

    -   Encoding: Choose **UTF-8**.

    -   Event compression type: Leave set to **None**.

        ![Form for adding a new IoT Hub input to Stream Analytics](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image76.png "IOT hub new input blade")

    -   Select **Create**.

8.  Now, select **Outputs** from the left-hand menu, under Job Topology.

    ![Under Job Topology, Outputs is selected.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image52.png "Job Topology Section")

9.  In the Outputs blade, select **+Add**, then **Blob storage** to add the output destination for the query.
    
    ![Outputs blade add blob storage](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image77.png "Outputs blade add blob storage")

10. On the New output blade, enter the following:

    -   Output alias: Set to **blobs**.

    -   Choose **Select blob storage from your subscriptions**.

    -   Storage account: Choose the storage account name you used for HDInsight in Before the hands-on lab, Task 2, Step 5c.

    -   Container: Set to **iotcontainer**.

    -   Path pattern: Enter **smartmeters/{date}/{time}**.

    -   Date format: Select **YYYY/MM/DD**.

    -   Time format: Select **HH**.

    -   Event serialization formation: Select **CSV**.

    -   Delimiter: Select **comma (,)**.

    -   Encoding: Select **UTF-8**.

        ![Blob storage blade with storage account, path pattern and event serialzation format complted.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image78.png "Blob Storage blade")

    -   Select **Save**.

11. Next, select **Query** from the left-hand menu, under Job Topology.
    
    ![Under Job Topology, Query is selected.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image56.png "Job Topology Section")

12. In the query text box, paste the following query.
    ```
    SELECT
          *
    INTO 
          blobs
    FROM 
          iothub
    ```

13. Select **Save,** and **Yes** when prompted with the confirmation.

    ![Save button](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image57.png "Save button")

14. Return to the Overview blade on your Stream Analytics job, and select **Start**.

    ![Start button](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image58.png "Start button")

15. In the Start job blade, select **Now** (the job will start processing messages from the current point in time onward).
    
    ![Now button](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image59.png "Now button")

16. Select **Start**.

17. Allow your Stream Analytics Job a few minutes to start.

18. Once the Stream Analytics Job has successfully started, verify that you are showing a non-zero amount of **Input Events** on the **Monitoring** chart on the overview blade. You may need to reconnect your devices on the Smart Meter Simulator and let it run for a while to see the events.

    ![The Monitoring chart now lists 88 Input Events, 88 Output Events, and 0 runtime errors.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image79.png "Monitoring chart")

### Task 2: Verify CSV files in blob storage

In this task, we are going to verify that the CSV file is being written to blob storage. (Note, this can be done via Visual Studio, or using the Azure portal. For this lab, we will perform the task using Visual Studio.)

1.  Within Visual Studio on your Lab VM, select the **View** menu, then select **Cloud Explorer**.
    
    ![On the Visual Studio View menu, Cloud Explorer is circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image80.png "Visual Studio View menu")

2.  In **Cloud Explorer**, select Account Management, and connect to Microsoft Azure Subscription.
    
    ![The Cloud Explorer window displays.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image81.png "Cloud Explorer")

3.  If prompted, sign into your Azure account.

4.  Allow Cloud Explorer about 30 seconds to load your subscription resources.

5.  Expand your Azure account, and then expand **HDInsight**. It may take a few moments to load your storage accounts.

6.  Expand the Spark cluster you created for this lab.

7.  Expand the **Default Storage Account** used for your HDInsight cluster.

    ![In the Cloud Explorer window, under Resource Types, the following path is expanded: MSDN (kyle@)\\HDInsight\\iothandsonlab (SPARK)\\iotholstorage (Default Storage)\\iotcontainer (Default Container).](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image82.png "Cloud Explorer")

8.  Right-click the container named after your HDInsight cluster. Select **View Container**.
    
    ![View container is selected.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image83.png "View container")

9.  Verify files are being written to Blob storage and take note of the path to one of the files (the files should be located underneath the smartmeters folder).
    
    ![Under smartmeters, the path name displays.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image84.png "Path name")

### Task 3: Update pandas version on the Spark cluster

In this task you will connect SSH into your HDInsight cluster, and update the version of pandas that Jupyter Notebook uses. This task is to address errors that are displayed because the autovizwidget in Jupyter needs a later version of pandas that has the API module.

1.  In the Azure portal, navigate to the blade for your Spark Cluster, under your HDInsight Cluster.

2.  On the cluster's overview blade, select **Secure Shell (SSH)** from the toolbar.
    
    ![On the Overview blade toolbar, Secure Shell (SSH) is circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image85.png "Overview blade toolbar")

3.  On the SSH + Cluster login blade, select your cluster from the Hostname drop down, then select the **copy button** next to the text box.
    
    ![On the SSH + Cluster login blade, under Hostname, the cluster name is circled, as is the copy button next to the textbox.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image86.png "SSH + Cluster login blade")

4.  On your Lab VM, select your open Git Bash client (or open a new one if you closed it).

1.  At the command prompt, paste the SSH connection string you copied in step 3, above. When prompted about continuing, type "yes."
    
    ![The Git Bash Window displays the SSh connection string, and the prompt, \"Are you sure you want ot continue connecting?\" is circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image87.png "Git Bash Window")

6.  Enter your password, Password.1!!, when prompted, and press **Enter**.
    
    ![The Git Bash Window now has the prompt for the password circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image88.png "Git Bash Window")

7.  Once logged in, you will be presented with a command prompt, similar to the following:
    
    ![The Git Bash Window now displays with the following prompt: sshuser\@hn0-iothan:\~\$](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image89.png "Git Bash Window")

8.  At the command prompt, enter the following command to install the latest version of pandas, which has the API module required by the autovizwidget.
    ```
    sudo -HE /usr/bin/anaconda/bin/conda install pandas
    ```
9.  When prompted to Proceed, type "y."
    
    ![In the Git Bash Window, the following line is circled: Proceed (\[y\]n)? y](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image90.png "Git Bash Window")

### Task 4: Process with Spark SQL 

1.  Navigate to the blade for your Spark Cluster in the Azure Portal, under you HDInsight Cluster.

2.  Under Quick Links, click **Cluster Dashboard**.
    
    ![Under Quick links, the Cluster dashboard option is circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image91.png "Quick links")

3.  On the Cluster Dashboards blade, select **Jupyter Notebook**. If prompted, log in with admin credentials you provided when creating the cluster (username: **admin**, password: **Password.1!!**).
    
    ![On the Cluster Dashboards blade, the Jupyter Notebook option is circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image92.png "Cluster Dashboards blade")

4.  From the navigation bar in the Jupyter site, select **New** and then **Spark.**
    
    ![On the Jupyter site navigation bar, New is selected, and on its drop-down menu, Spark is selected. A pop-up message says, \"Create a new notebook with Spark."](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image93.png "Jupyter site navigation bar")

5.  In the first text area (referred to as a paragraph in notebook jargon), enter the following **Scala code** that will load, parse, and save your batch scored telemetry data as a table that you can later query using Spark SQL. Before executing, make sure to replace the highlighted text with the correct path to your telemetry file, noted previously in the Cloud Explorer in Visual Studio.
    ```
    import spark.implicits._

    val rawText = spark.read.text("wasb:///smartmeters/2017/08/20/19/1008078303_90a84f7aba614d1fa4688cbda1de3846_1.csv")
    case class SmartMeterMetrics(id:String,time:String,temp:Integer)
    val telemetryRDD = rawText.map(row => row.getString(0).split(",")).filter(s=>s(0) != "id").map(
        s => SmartMeterMetrics(s(0), s(1), s(2).toInt)
    )
    val telemetryDF = telemetryRDD.toDF()
    telemetryDF.write.saveAsTable("SmartMeters")
    ```

1.  Next, click the **Run** icon in the toolbar to execute this code and create the **SmartMeters** table
    
    . ![The Run icon is selected on the toolbar.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image94.png "Run icon")

7.  The block is finished running when the In\[\*\] changes to In\[1\]\
    
    ![In the top half of this screen capture, code displays. In the bottom half of the screen capture, a SmartMeters table displays.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image95.png "SmartMeters table")

    You may see the below message. You can proceed.
    
    ![The message states that the SmartMeters table already exists.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image96.png "Message")

8.  In the second cell, enter the following SQL query and run it.
    ```
    %%sql
    select id, count(*) as count, avg(temp) averageTemp from SmartMeters group by id order by id
    ```

    You will see a table like the following:

    ![A table with three columns displays: id, count, and average temp. Devices 0 through 6 have counts varying between 59 and 60, and average Temps between 69.0 and 69.9.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image97.png "Table")

9.  Next, create a table that will summarize the telemetry collected using the previous query. In a new paragraph, try running the following query:
    ```
    //query the table to create a summary result set
    val summary = spark.sql("select id, count(*) as count, avg(temp) averageTemp from SmartMeters group by id order by id")

    //save the new pre-computed view
    summary.write.saveAsTable("DeviceSummary")
    ```

10. Next, query from this summary table by executing the following query.
    ```
    %%sql
    select * from DeviceSummary
    ```

11. In the results, click the **Bar** button.

12. In the X dropdown, select **id**.

13. In the Y dropdown, select **averageTemp**.

14. In the Func dropdown, select **Avg**.

15. Check the box for **Log scale** **Y.**

16. Observe the results graphed as a column chart, where each column represents a device's average temperature.
    
    ![A bar chart displays, with columns displaying for devices 0, 1, 3, 8, and 9. The chart indicates that Devices 0 and 1 have lower average temperatures, and that Device3 has the highest average temperature. Above the chart, for Type, Bar is circled. Under Encoding, for X, id is selected. For Y, averageTemp is selected. for Func, Avg is selected. The checkbox for Log scale Y is selected. ](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image98.png "Column chart")

## Exercise 6: Reporting device outages with IoT Hub Operations Monitoring

Duration: 20 minutes

Fabrikam would like to be alerted when devices disconnect and fail to reconnect after a period. Since they are already using PowerBI to visualize hot data, they would like to see a list of any of these devices in a report.

### Task 1: Enable verbose connection monitoring on the IoT Hub

To keep track of device connects and disconnects, we first need to enable verbose connection monitoring.

1.  In your browser, navigate to the **Azure Portal** (<https://portal.azure.com)>.

2.  Open the IoT Hub you provisioned earlier, **smart-meter**.

3.  Under SETTINGS in the left-hand menu, click on **Operations monitoring**.
    
    ![Under Settings, Operations monitoring is selected.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image99.png "Settings section")

4.  Select **Verbose** for **Log events when a device connects or disconnects from the IoT Hub**.

    ![In the Monitoring categories section, Verbose is selected under Log events when a device connects or disconnects from the IoT Hub.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image100.png "Monitoring categories section")

5.  Click **Save**.

### Task 2: Collect device connection telemetry with the hot path Stream Analytics job

Now that the device connections are being logged, update your hot path Stream Analytics job (the first one you created) with a new input that ingests device telemetry from Operations Monitoring. Next, create a query that joins all connected and disconnected events with a DATEDIFF function that only returns devices with a disconnect event, but no reconnect event within 120 seconds. Output the events to Power BI.

1.  In your browser, navigate to the **Azure Portal** (<https://portal.azure.com)>.

2.  Open the **hot-stream** Stream Analytics job (the first one you created).

3.  Stop the job if it is currently running, from the Overview blade, by selecting **Stop**, then **Yes** when prompted.
    
    ![The Stop button is circled on the Overview blade top menu.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image101.png "Overview blade Stop button")

4.  Select **Inputs** on the left-hand menu, under Job Topology.

    ![Under Job Topology, Inputs is selected.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image49.png "Job Topology Section")

5.  On the Inputs blade, select **+Add**, then **IoT Hub** to add an input connected to your IoT Hub.
    
    ![Add new IoT Hub stream input to Stream Analytics](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image50.png "Job topolog add stream input hub")

6.  On the New Input blade, enter the following:

    -   Input Alias: Set the value to **connections**.

    -   Import Option: Choose **Select IoT hub from your subscriptions**.

    -   IoT Hub: Select your existing IoT Hub, **smartmeter-hub**.

    -   Endpoint: Choose **Operations monitoring**.

    -   Shared Access Policy Name: Set to **Service**.

    -   Consumer Group: Leave as **\$Default**.

    -   Event serialization format: Choose **JSON**.

    -   Encoding: Choose **UTF-8**.

    -   Event compression type: Leave set to **None**.

        ![Create new IoT Hub input form for Stream Analytics](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image102.png "IoT hub new input blade")

    -   Select **Save**.

7. Now, select **Outputs** from the left-hand menu, under Job Topology.

    ![Under Job Topology, Outputs is selected.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image52.png "Job Topology Section")

8. In the Outputs blade, select **+Add**, the **Power BI**, to add the output destination for the query.
    
    ![Select +Add, then Power BI within the outputs blade of your Stream Analytics instance](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image53.png "Outputs blade add Power BI")

9. On the New output blade, enter the following:

    -   Set the **Output alias** to **powerbi-outage**.

        ![Enter powerbi-outage for the output alias, then select Authorize to authenticate to your Power BI subscription](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image103.png "Poewr BI new output")

    -   Select **Authorize** under Authorize Connection.

    Follow the on-screen prompts to log on to your Power BI account.

    After authenticating, complete the remaining fields as follows:

    -   Group Workspace: Leave set to **My Workspace**.

    -   Dataset Name: Enter **deviceoutage**

    -   Table Name: Enter **deviceoutage**

    -   Select **Save**.

        ![Complete the form by entering deviceoutage as both the Dataset name and Table name. Select the My workspace Group workspace option](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image104.png "Power BI new input ")

10. Next, select **Query** from the left-hand menu, under Job Topology.
    
    ![Under Job Topology, Query is selected.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image56.png "Job Topology Section")

11. We will replace the hot path query, which selects the averages of the temperatures into the PowerBI output, with queries that perform the following:

    -   Select **device disconnection events**.

    -   Select **device connection events**.

    -   Join these two streams together using the Stream Analytics DATEDIFF operation on the LEFT JOIN, and then filter out any records where there was a match. This gives us devices that had a disconnect event, but no corresponding connect event within 120 seconds. Output to the Service Bus.

    -   Execute the original hot path query.

12. Replace the existing query with the following, and click **Save** in the **command bar** at the top. (Be sure to substitute in your output aliases and input aliases):
    ```
    WITH
    Disconnected AS (
    SELECT *
    FROM connections TIMESTAMP BY [Time]
    WHERE OperationName = 'deviceDisconnect'
        AND Category = 'Connections'
    ),
    Connected AS (
    SELECT *
    FROM connections TIMESTAMP BY [Time]
    WHERE OperationName = 'deviceConnect'
        AND Category = 'Connections'
    )

    SELECT Disconnected.DeviceId, Disconnected.Time
    INTO [powerbi-outage] 
    FROM Disconnected
    LEFT JOIN Connected 
        ON DATEDIFF(second, Disconnected, Connected) BETWEEN 0 AND 120
        AND Connected.deviceId = Disconnected.deviceId
    WHERE Connected.DeviceId IS NULL;

    SELECT AVG(temp) AS Average, id
    INTO powerbi
    FROM temps
    GROUP BY TumblingWindow(minute, 5), id;
    ```

13. Select **Save,** and **Yes** when prompted with the confirmation.

    ![Save button](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image57.png "Save button")

14. Return to the Overview blade on your Stream Analytics job, and select **Start**.

    ![The start button on the Overview blade is circled](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image58.png "Overview blade start button")

15. In the Start job blade, select **Now** (the job will start processing messages from the current point in time onward).
    
    ![Next to Job output start time, the Now button is selected.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image59.png "Now button")

16. Select **Start**.

17. Allow your Stream Analytics Job a few minutes to start.

### Task 3: Test the device outage notifications

Register and activate a few devices on the Smart Meter Simulator, then connect them. Deactivate them without reconnecting in order for them to show up in the device outage report we will create in the next task.

1.  Run the Smart Meter Simulator from Visual Studio.

2.  Click the **Register** button.
    
    ![Register button](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image105.png "Register button")

3.  Click on 3 of the windows to highlight them.
    
    ![In the SmartMeter Simulator, three white windows display.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image106.png "SmartMeter Simulator")

4.  Click the **Activate** button.
    
    ![Activate button](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image107.png "Activate button")

5.  Click the **Connect** button.
    
    ![Connect button](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image108.png "Connect button")

6.  After a few seconds, click **Disconnect**.
    
    ![Disconnect button](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image109.png "Disconnect button")

7.  Click **Unregister**.
    
    ![Unregister button](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image110.png "Unregister button")

### Task 4: Visualize disconnected devices with Power BI

1.  Log on to **Power BI** to see if data is being collected.

2.  As done previously, select My Workspace on the left-hand menu, then select the Datasets tab. A new dataset should appear, named **deviceoutage**. (It is starred to indicate it is new) If you do not see the dataset, you may need to connect your devices on the Smart Meter Simulator, then disconnect and unregister them and wait up to 5 minutes.
    
    ![On the Power BI window, My Workspace is circled in the left pane, and the Datasets tab is circled in the right pane. Under Name, deviceoutage is circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image111.png "Power BI window")

3.  Select the **Create report** icon under actions for the dataset.
    
    ![Next to deviceoutage, the Create report icon is circled.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image112.png "Create report icon")

4.  Add a **Table visualization**.

    ![The table icon is circled on the Visualizations palette.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image113.png "Visualizations palette")

5.  Select the **deviceid** and **time** fields, which will automatically be added to the table. You should see the Device Id of each of the devices you connected, and then disconnected for more than 2 minutes.
    
    ![In the Fields listing, under deviceoutage, deviceid and time are both selected.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image114.png "Fields listing")\
    
    ![DeviceId and Time results display for Device one, Device five, and Device 8.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image115.png "DeviceId and Time results")

6.  Save the report as **Disconnected Devices**.

    ![Screenshot of the Save this report option.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image116.png "Save this report option")

7.  Switch to **Reading View**.

    ![Reading View button](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image73.png "Reading View button")

8.  Within the report, click the column headers to sort by device or date. You may run a few more tests with the Smart Meter Simulator and periodically refresh the report to see new devices.

    ![This report has two columns: DeviceID, and Time. Data is sorted by time.](images/Hands-onlabstep-bystep-InternetofThingsimages/media/image117.png "Report")

## After the hands-on lab 

Duration: 10 minutes

In this exercise, attendees will deprovision any Azure resources that were created in support of the lab.

### Task 1: Delete the resource group

1.  Using the Azure portal, navigate to the Resource group you used throughout this hands-on lab by selecting **Resource groups** in the left menu.

2.  Search for the name of your research group, and select it from the list.

3.  Select **Delete** in the command bar, and confirm the deletion by re-typing the Resource group name, and selecting **Delete**.


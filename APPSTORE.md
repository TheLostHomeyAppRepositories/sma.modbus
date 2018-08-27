# Use Homey to monitor SMA inverters
This Homey app allows you to monitor your SMA inverter through the modbus protocol interface. Modbus is a serial communications protocol often used in connecting industrial electronic devices. The app currently supports reading Current Power AC, Daily Yield, Voltage and Total Yield.

## Supported devices
Below is a shortend list of supported inverters including the available connection types for each inverter. View the complete list of supported inverters in the second tab of this [Google sheets document](https://docs.google.com/spreadsheets/d/1VDGGXSl3RE10oLtm8JgIrXKaZEp1--tMOS92ZPGuF4g/edit?usp=sharing). Unfortunately the configuration for reading out the data of each of these converters seems to differ. I have only tested it with an older Sunny Boy 2500TL. Support for other inverters may need to be added seperately and might require some research. If you connect your inverter and you do not see correct values appear in the Homey device, please use the support topic to ask for support for your type of inverter.

| TYPE | CONNECTION | STATUS |
| ------ | ------ | ------ |
| Sunny Boy 2500TL | Retrofit Webconnect | Tested |
| Sunny Boy 1.5 / 2.5 | Webconnect or WLAN | Untested |
| Sunny Boy 3.0 / 3.6 / 4.0 / 5.0 | Webconnect or WLAN | Untested |
| Sunny Tripower 5/6/7/8/9/10/12000TL-20 | Webconnect | Untested |
| Sunny Tripower 15/20/25000TL-30 | Webconnect | Untested |
| Sunny Tripower Core1 (50KW) | Webconnect or Datamanager M | Untested |
| Sunny Tripower 60 + Inverter Manager | Ethernet | Untested |
| Sunny Tripower Peak1 + Inverter Manager | Ethernet | Untested |

## Instructions
For Homey to be able to communicate with your SMA inverter over the modbus protocol this needs to be enabled on the inverter. You can enable modbus communication using the [SMA Sunny Explorer](https://www.sma.de/en/products/monitoring-control/sunny-explorer.html) software. So download and install it, the installable is located under PC Software in the previous link. During installation it may ask you to install the .NET framework 3.5 as well. Now follow these steps.
1. Start Sunny Explorer and choose to create a new install (or load a previous one if you used Sunny Explorer before);
2. Select Speedwire as communication protocol, hit next and wait for Sunny Explorer to discover your inverter. Once found select your inverter (and remember it's IP address);
3. In the login screen select the "Installer" user. Enter the password, by default password this is 1111 but this might have been changed by your installer. In this case you will have to retrieve it from your installer;
4. Once logged in to your inverter wait for a moment for the data to load. Then click on your inverter in the left pane and after that on the settings tab. Click on the external communications menu item and click on edit. Now enable the option 'Modbus TCP Server' and if available and not already enabled the option 'Webconnect'. You can leave the port for the Modbus TCP Server to the default 502. If you change it here make sure you enter it correctly when adding the inverter in Homey. If you have saved your changes you can close Sunny Explorer.
5. Now go into Homey and add a new device. Select the inverter device from the SMA Inverters app and enter the IP address, Port and Polling Frequency. The polling frequency determines how often Homey reads out the inverter. The default 5 seconds should be good. Now add the device and if all went well your inverter is now being monitored by Homey.

## Support topic
For support please use the official support topic on the forum [here](https://community.athom.com/t/696).

## Changelog
### 2018-08-27 - v1.0.0
* NEW: initial release
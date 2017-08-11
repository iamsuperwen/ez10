#!/usr/bin/env python

# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.
"""
* Ctrl^C can't stop; Instead, use Ctrl^\ force to quit.
<source> https://askubuntu.com/questions/14155/what-to-do-when-ctrl-c-wont-kill-running-job/14158#14158?newreg=301affc49ce3468893d6283eb314ba87

* Change the absolute path! <line 22>

* RPY & Vel haven't done yet.  --20170811 tsai
"""

import rospy
from sensor_msgs.msg import Imu, NavSatFix
from std_msgs.msg import Float32
from geometry_msgs.msg import TwistStamped
from diagnostic_msgs.msg import DiagnosticArray, DiagnosticStatus, KeyValue

import random
import time
import sys
sys.path.append("/home/tsaichiawen/catkin_ws/src/ez10/src/iothub_depend")
import datetime

import iothub_client
from iothub_client import IoTHubClient, IoTHubClientError, IoTHubTransportProvider, IoTHubClientResult
from iothub_client import IoTHubMessage, IoTHubMessageDispositionResult, IoTHubError, DeviceMethodReturnValue
from iothub_client_args import get_iothub_opt, OptionError

# HTTP options
# Because it can poll "after 9 seconds" polls will happen effectively
# at ~10 seconds.
# Note that for scalabilty, the default value of minimumPollingTime
# is 25 minutes. For more information, see:
# https://azure.microsoft.com/documentation/articles/iot-hub-devguide/#messaging
TIMEOUT = 241000
MINIMUM_POLLING_TIME = 9

# messageTimeout - the maximum time in milliseconds until a message times out.
# The timeout period starts at IoTHubClient.send_event_async.
# By default, messages do not expire.
MESSAGE_TIMEOUT = 10000

RECEIVE_CONTEXT = 0
MESSAGE_COUNT = 1
RECEIVED_COUNT = 0
TWIN_CONTEXT = 0
SEND_REPORTED_STATE_CONTEXT = 0
METHOD_CONTEXT = 0

# global counters
RECEIVE_CALLBACKS = 0
SEND_CALLBACKS = 0
BLOB_CALLBACKS = 0
TWIN_CALLBACKS = 0
SEND_REPORTED_STATE_CALLBACKS = 0
METHOD_CALLBACKS = 0

# chose HTTP, AMQP or MQTT as transport protocol
PROTOCOL = IoTHubTransportProvider.MQTT

# String containing Hostname, Device Id & Device Key in the format:
CONNECTION_STRING = "HostName=ez10S1.azure-devices.net;DeviceId=PC0;SharedAccessKey=uC98sguRyI1ytjvMxDHqdVjHG2F2rOCdcLNyREjWfts="

MSG_TXT = "{\"deviceId\": \"PC0\",\"datetime\": \"%s\",\"x\": %.8f,\"y\": %.8f,\"z\": %.8f,\"yaw\": %.8f,\"gpsx\": %.8f,\"gpsy\": %.8f,\"temp\": %.4f,\"humid\": %.4f}" ##JSON Type!?

# some embedded platforms need certificate information

# global variables, saving sensor data
# === IMU ===
imuMsg = Imu()
rpyMsg = DiagnosticArray()
# === GPS ===
gpsMsg = NavSatFix()

# === TEMP & HUMID ===
tempMsg = Float32()
humidMsg = Float32()


def set_certificates(client):
    from iothub_client_cert import CERTIFICATES
    try:
        client.set_option("TrustedCerts", CERTIFICATES)
        print ( "set_option TrustedCerts successful" )
    except IoTHubClientError as iothub_client_error:
        print ( "set_option TrustedCerts failed (%s)" % iothub_client_error )


def receive_message_callback(message, counter):
    global RECEIVE_CALLBACKS
    message_buffer = message.get_bytearray()
    size = len(message_buffer)
    print ( "Received Message [%d]:" % counter )
    print ( "    Data: <<<%s>>> & Size=%d" % (message_buffer[:size].decode('utf-8'), size) )
    map_properties = message.properties()
    key_value_pair = map_properties.get_internals()
    print ( "    Properties: %s" % key_value_pair )
    counter += 1
    RECEIVE_CALLBACKS += 1
    print ( "    Total calls received: %d" % RECEIVE_CALLBACKS )
    return IoTHubMessageDispositionResult.ACCEPTED


def send_confirmation_callback(message, result, user_context):
    global SEND_CALLBACKS
    print ( "Confirmation[%d] received for message with result = %s" % (user_context, result) )
    map_properties = message.properties()
    print ( "    message_id: %s" % message.message_id )
    print ( "    correlation_id: %s" % message.correlation_id )
    key_value_pair = map_properties.get_internals()
    print ( "    Properties: %s" % key_value_pair )
    SEND_CALLBACKS += 1
    print ( "    Total calls confirmed: %d" % SEND_CALLBACKS )


def device_twin_callback(update_state, payload, user_context):
    global TWIN_CALLBACKS
    print ( "\nTwin callback called with:\nupdateStatus = %s\npayload = %s\ncontext = %s" % (update_state, payload, user_context) )
    TWIN_CALLBACKS += 1
    print ( "Total calls confirmed: %d\n" % TWIN_CALLBACKS )


def send_reported_state_callback(status_code, user_context):
    global SEND_REPORTED_STATE_CALLBACKS
    print ( "Confirmation for reported state received with:\nstatus_code = [%d]\ncontext = %s" % (status_code, user_context) )
    SEND_REPORTED_STATE_CALLBACKS += 1
    print ( "    Total calls confirmed: %d" % SEND_REPORTED_STATE_CALLBACKS )


def device_method_callback(method_name, payload, user_context):
    global METHOD_CALLBACKS
    print ( "\nMethod callback called with:\nmethodName = %s\npayload = %s\ncontext = %s" % (method_name, payload, user_context) )
    METHOD_CALLBACKS += 1
    print ( "Total calls confirmed: %d\n" % METHOD_CALLBACKS )
    device_method_return_value = DeviceMethodReturnValue()
    device_method_return_value.response = "{ \"Response\": \"This is the response from the device\" }"
    device_method_return_value.status = 200
    return device_method_return_value


def blob_upload_conf_callback(result, user_context):
    global BLOB_CALLBACKS
    print ( "Blob upload confirmation[%d] received for message with result = %s" % (user_context, result) )
    BLOB_CALLBACKS += 1
    print ( "    Total calls confirmed: %d" % BLOB_CALLBACKS )


def iothub_client_init():
    # prepare iothub client
    client = IoTHubClient(CONNECTION_STRING, PROTOCOL)
    if client.protocol == IoTHubTransportProvider.HTTP:
        client.set_option("timeout", TIMEOUT)
        client.set_option("MinimumPollingTime", MINIMUM_POLLING_TIME)
    # set the time until a message times out
    client.set_option("messageTimeout", MESSAGE_TIMEOUT)
    # some embedded platforms need certificate information
    set_certificates(client)
    # to enable MQTT logging set to 1
    if client.protocol == IoTHubTransportProvider.MQTT:
        client.set_option("logtrace", 0)
    client.set_message_callback(
        receive_message_callback, RECEIVE_CONTEXT)
    if client.protocol == IoTHubTransportProvider.MQTT or client.protocol == IoTHubTransportProvider.MQTT_WS:
        client.set_device_twin_callback(
            device_twin_callback, TWIN_CONTEXT)
        client.set_device_method_callback(
            device_method_callback, METHOD_CONTEXT)
    return client


def print_last_message_time(client):
    try:
        last_message = client.get_last_message_receive_time()
        print ( "Last Message: %s" % time.asctime(time.localtime(last_message)) )
        print ( "Actual time : %s" % time.asctime() )
    except IoTHubClientError as iothub_client_error:
        if iothub_client_error.args[0].result == IoTHubClientResult.INDEFINITE_TIME:
            print ( "No message received" )
        else:
            print ( iothub_client_error )


def usage():
    print ( "Usage: iothub_node.py -p <protocol> -c <connectionstring>" )
    print ( "    protocol        : <amqp, amqp_ws, http, mqtt, mqtt_ws>" )
    print ( "    connectionstring: <HostName=<host_name>;DeviceId=<device_id>;SharedAccessKey=<device_key>>" )

# ============================== Customize Down Here! ==============================
def push2iothub(push_rate):

    try:

        client = iothub_client_init()

        if client.protocol == IoTHubTransportProvider.MQTT:
            print ( "IoTHubClient is reporting state" )
            reported_state = "{\"newState\":\"standBy\"}"
            client.send_reported_state(reported_state, len(reported_state), send_reported_state_callback, SEND_REPORTED_STATE_CONTEXT)


        rate = rospy.Rate(push_rate)

        while True:
            global imuMsg, gpsMsg
            date_f = datetime.datetime.fromtimestamp(imuMsg.header.stamp.secs).strftime("%Y-%m-%d")
            time_f = datetime.datetime.fromtimestamp(imuMsg.header.stamp.secs).strftime("%X.") + str(int(float(imuMsg.header.stamp.nsecs)/(10**6)))

            datetime_f = date_f + "T" + time_f + "Z"

            GPSx = gpsMsg.latitude
            GPSy = gpsMsg.longitude
            #GPSz = gpsMsg.altitude

            IMUyaw = imuMsg.header.stamp.nsecs


            msg_txt_formatted = MSG_TXT % (
                datetime_f,
                imuMsg.linear_acceleration.x,
                imuMsg.linear_acceleration.y,
                imuMsg.linear_acceleration.z,
                IMUyaw,
                GPSx,
                GPSy,
                tempMsg.data,
                humidMsg.data)
                    
            print (msg_txt_formatted)
            
            message_counter = 0
            message = IoTHubMessage(msg_txt_formatted)
            # optional: assign ids
            #message.message_id = "header seq: %d" % imuMsg.header.seq
            #message.correlation_id = "header frameID: %s" % imuMsg.header.frame_id
            # optional: assign properties
            #prop_map = message.properties()
            #prop_map.add("temperatureAlert", 'true' if temperature > 28 else 'false')

            client.send_event_async(message, send_confirmation_callback, message_counter)
            print ( "IoTHubClient.send_event_async accepted message [%d] for transmission to IoT Hub." % imuMsg.header.seq )

            status = client.get_send_status()
            print ( "Send status: %s" % status )

            rate.sleep()


    except IoTHubError as iothub_error:
        print ( "Unexpected error %s from IoTHub" % iothub_error )
        return
    except KeyboardInterrupt:
        print ( "IoTHubClient sample stopped" )

    print_last_message_time(client)


def get_Imu(data):
    global imuMsg
    imuMsg = data

def get_RPY(data):  ##NOT YET
    global rpyMsg
    rpyMsg = data

    #print(len(rpyMsg.status.pop()))

def get_Gps(data):
    global gpsMsg
    gpsMsg = data

def get_Vel(data):  ##NOT YET
    global velMsg
    velMsg = data
    
    #print(str(velMsg))

def get_Temp(data):
    global tempMsg
    tempMsg = data

def get_Humid(data):
    global humidMsg
    humidMsg = data

if __name__ == '__main__':
    print ( "\nPython %s" % sys.version )
    print ( "IoT Hub Client for Python" )

    try:
        (CONNECTION_STRING, PROTOCOL) = get_iothub_opt(sys.argv[1:], CONNECTION_STRING, PROTOCOL)
    except OptionError as option_error:
        print ( option_error )
        usage()
        sys.exit(1)

    print ( "    Protocol %s" % PROTOCOL )
    print ( "    Connection string=%s" % CONNECTION_STRING )
    
    # === IMU ===
    rospy.Subscriber("imu", Imu, get_Imu)
    rospy.Subscriber("diagnostics", DiagnosticArray, get_RPY)
    
    # === GPS ===
    rospy.Subscriber("fix", NavSatFix, get_Gps) 
    rospy.Subscriber("vel", TwistStamped, get_Vel)
    
    # === TEMP & HUMID ===
    rospy.Subscriber("temperature", Float32, get_Temp)
    rospy.Subscriber("humidity", Float32, get_Humid)

    rospy.init_node('iothub_node')

    push2iothub(10)  #push_rate=10hz

    #rospy.spin()

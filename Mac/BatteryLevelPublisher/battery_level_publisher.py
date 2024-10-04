import argparse
import traceback
import inspect
import json
import time
import re
import sys
import logging
import subprocess
from datetime import datetime

from awscrt import io, mqtt
from awsiot import mqtt_connection_builder, iotshadow

logger_format = "%(levelname)-8s  %(asctime)s [%(filename)s:%(lineno)3d] %(message)s"

logger = logging.getLogger(__name__)
# set the logging handler
handler = logging.StreamHandler()
# set the logging format
handler.setFormatter(logging.Formatter(logger_format))
logger.addHandler(handler)
# set the logging level
logger.setLevel(logging.INFO)

class BatteryLevelPublisher:

  BASE_TOPIC = "data/"
  KEEP_ALIVE_SECS = 300
  DEFAULT_WAIT_TIME_SEC = 10
  SHADOW_WAIT_TIME_KEY = "wait_time_sec"
  QUALITY_OF_SERVICE = mqtt.QoS.AT_LEAST_ONCE
  ERROR_MESSAGE_FORMAT = "Exception in {function_name}: {message}\n{traceback}"

  def __init__(self) -> None:
    self.init_data = self.parse_arguments()
    self.wait_time_sec = self.DEFAULT_WAIT_TIME_SEC
    self.mqtt_connection = None
    self.shadow_client = None


  def run(self):
    try:

      ## log the init data
      for key, value in self.init_data.items():
        logger.info(f"{key}: {value}")

      # create the client_bootstrap object
      client_bootstrap = self.create_client_bootstrap()

      # create the MQTT connection
      self.mqtt_connection = self.create_mqtt_connection(client_bootstrap)

      # create the shadow client
      self.shadow_client = iotshadow.IotShadowClient(self.mqtt_connection)

      # set up the shadow subscriptions
      self.setup_shadow_subscriptions()

      # publish a message for the shadow document retrieval
      self.get_shadow_document()

      while True:
        # start sending the battery level
        self.publish_battery_level()

        # wait for the next iteration
        logger.info(f"Waiting for {self.wait_time_sec} seconds.")
        time.sleep(self.wait_time_sec)

    except Exception as e:
      logger.error(e)
      self.exit_program()


  def parse_arguments(self):
    """
    Check the arguments and return the parsed arguments.
    The received arguments are as follows:

    1. --device_name: str
    2. --endpoint: str
    3. --ca_file: str
    4. --cert_file: str
    5. --key_file: str
    """

    # parse the arguments
    parser = argparse.ArgumentParser(prog="battery_level_publisher")
    parser.add_argument("--device_name", required=True, type=str, help="[MUST] AWS IoT Core Thing name")
    parser.add_argument("--endpoint", required=True, type=str, help="[MUST] AWS IoT Core endpoint")
    parser.add_argument("--ca_file", required=True, type=str, help="[MUST] Path to the CA file")
    parser.add_argument("--cert_file", required=True, type=str, help="[MUST] Path to the certificate file")
    parser.add_argument("--key_file", required=True, type=str, help="[MUST] Path to the private key file")

    args = parser.parse_args()

    # check if the arguments are valid
    # define the pattern that the argument string should follow
    args_pattern = {
      "device_name": r'^[a-zA-Z0-9-_]+$',
      "endpoint": r'^[a-z0-9-]+\.iot\.[a-z0-9-]+\.amazonaws\.com$',
      "ca_file": r'^[a-zA-Z0-9/-]+\.pem$',
      "cert_file": r'^[a-zA-Z0-9/-]+-certificate.pem.crt$',
      "key_file": r'^[a-zA-Z0-9/-]+-private.pem.key$'
    }

    # check if the endpoint string is valid
    if not re.match(args_pattern["endpoint"], args.endpoint):
      logger.error(f"The endpoint '{args.endpoint}' is not in the correct format.")
      logger.error("The endpoint should be in the format '<prefix>.iot.<region>.amazonaws.com'.")
      sys.exit(1)

    # check if the CA file string is valid
    if not re.match(args_pattern["ca_file"], args.ca_file):
      logger.error(f"The CA file '{args.ca_file}' is not in the correct format.")
      logger.error("The CA file should be in the format '<filename>.pem'.")
      sys.exit(1)
    
    # check if the certificate file string is valid
    if not re.match(args_pattern["cert_file"], args.cert_file):
      logger.error(f"The certificate file '{args.cert_file}' is not in the correct format.")
      logger.error("The certificate file should be in the format '<filename>-certificate.pem.crt'.")
      sys.exit(1)

    # check if the private key file string is valid
    if not re.match(args_pattern["key_file"], args.key_file):
      logger.error(f"The private key file '{args.key_file}' is not in the correct format.")
      logger.error("The private key file should be in the format '<filename>-private.pem.key'.")
      sys.exit(1)

    # return the parsed arguments
    return {
      "device_name": args.device_name,
      "endpoint": args.endpoint,
      "ca_file": args.ca_file,
      "cert_file": args.cert_file,
      "key_file": args.key_file
    }


  def create_client_bootstrap(self):
    """
    Create the client bootstrap object.
    """

    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)

    return io.ClientBootstrap(event_loop_group, host_resolver)

  def create_mqtt_connection(self, client_bootstrap):
    """
    Create the MQTT connection.
    """

    # create the MQTT connection
    logger.info("Creating the MQTT connection.")
    try:
      mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=self.init_data["endpoint"],
        cert_filepath=self.init_data["cert_file"],
        pri_key_filepath=self.init_data["key_file"],
        ca_filepath=self.init_data["ca_file"],
        client_id=self.init_data["device_name"],
        client_bootstrap=client_bootstrap,
        clean_session=False,
        keep_alive_secs=self.KEEP_ALIVE_SECS
      )
      connected_future = mqtt_connection.connect()
      connected_future.result()
      logger.info("Connected to the AWS IoT Core")
      return mqtt_connection
    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)


  def setup_shadow_subscriptions(self):
    """
    Set up the shadow subscriptions.
    """

    logger.info("Setting up the shadow subscriptions.")

    # GetShadow Accepted/Rejected
    self.subscribe_to_get_shadow()

    # UpdateShadow Accepted/Rejected
    self.subscribe_to_update_shadow()

    # Delta Updated Event
    self.subscribe_to_delta_updated_event()

    logger.info("Shadow subscriptions set up successfully.")


  def subscribe_to_get_shadow(self):
    """
    Subscribe to the GetShadow topic.
    """

    try:
      mqtt_get_shadow_accepted_future, _ = self.shadow_client.subscribe_to_get_shadow_accepted(
        request=iotshadow.GetShadowSubscriptionRequest(
          thing_name=self.init_data["device_name"]
        ),
        qos=self.QUALITY_OF_SERVICE,
        callback=self.on_shadow_document_retrieval_accepted
      )

      mqtt_get_shadow_rejected_future, _ = self.shadow_client.subscribe_to_get_shadow_rejected(
        request=iotshadow.GetShadowSubscriptionRequest(
          thing_name=self.init_data["device_name"]
        ),
        qos=self.QUALITY_OF_SERVICE,
        callback=self.on_shadow_document_retrieval_rejected
      )

      mqtt_get_shadow_accepted_future.result()
      mqtt_get_shadow_rejected_future.result()
    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)


  def subscribe_to_update_shadow(self):
    """
    Subscribe to the UpdateShadow topic.
    """

    try:
      mqtt_update_shadow_accepted_future, _ = self.shadow_client.subscribe_to_update_shadow_accepted(
        request=iotshadow.UpdateShadowSubscriptionRequest(
          thing_name=self.init_data["device_name"]
        ),
        qos=self.QUALITY_OF_SERVICE,
        callback=self.on_shadow_document_updated_accepted
      )

      mqtt_update_shadow_rejected_future, _ = self.shadow_client.subscribe_to_update_shadow_rejected(
        request=iotshadow.UpdateShadowSubscriptionRequest(
          thing_name=self.init_data["device_name"]
        ),
        qos=self.QUALITY_OF_SERVICE,
        callback=self.on_shadow_document_updated_rejected
      )

      mqtt_update_shadow_accepted_future.result()
      mqtt_update_shadow_rejected_future.result()
    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)


  def subscribe_to_delta_updated_event(self):
    """
    Subscribe to the DeltaUpdated event.
    """

    logger.info("Received the shadow delta updated event.")
    try:
      mqtt_delta_updated_event_future, _ = self.shadow_client.subscribe_to_shadow_delta_updated_events(
        request=iotshadow.ShadowDeltaUpdatedSubscriptionRequest(
          thing_name=self.init_data["device_name"]
        ),
        qos=self.QUALITY_OF_SERVICE,
        callback=self.on_shadow_delta_updated
      )

      mqtt_delta_updated_event_future.result()
    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)


  def on_shadow_document_retrieval_accepted(self, response):
    """
    Callback when the shadow document retrieval request is accepted.
    """

    try:
      logger.info("Received the shadow document retrieval response.")

      if not response.state:
        logger.warning("The shadow document does not contain the state key. Resetting the wait_time_sec to the default value.")
        self.update_shadow_value(self.DEFAULT_WAIT_TIME_SEC)
        return

      if response.state.delta and (self.SHADOW_WAIT_TIME_KEY in response.state.delta):
        value = response.state.delta[self.SHADOW_WAIT_TIME_KEY]
        if value is None:
          logger.warning("The shadow document contains a null value. Resetting the wait_time_sec to the default value.")
          self.update_shadow_value(self.DEFAULT_WAIT_TIME_SEC)
        else:
          logger.info(f"Shadow document reports that desired value is {value}. Changing local value from {self.wait_time_sec} to {value}.")
          self.wait_time_sec = value
          self.update_shadow_value(self.wait_time_sec)
        return

      if response.state.desired and (self.SHADOW_WAIT_TIME_KEY in response.state.desired):
        value = response.state.desired[self.SHADOW_WAIT_TIME_KEY]
        if value is None:
          logger.warning("The shadow document contains a null value. Resetting the wait_time_sec to the default value.")
          self.update_shadow_value(self.DEFAULT_WAIT_TIME_SEC)
        else:
          logger.info(f"Shadow document reports that desired value is {value}. Changing local value from {self.wait_time_sec} to {value}.")
          self.wait_time_sec = value
          self.update_shadow_value(self.wait_time_sec)
        return

    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)


  def on_shadow_document_retrieval_rejected(self, error):
    """
    Callback when the shadow document retrieval request is rejected.
    """

    current_function = inspect.currentframe().f_code.co_name
    error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = error, traceback = traceback.format_exc())
    raise Exception(error_message)


  def on_shadow_document_updated_accepted(self, response):
    """
    Callback when the shadow document update request is accepted.
    """


  def on_shadow_document_updated_rejected(self, error):
    """
    Callback when the shadow document update request is rejected.
    """

    current_function = inspect.currentframe().f_code.co_name
    error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = error, traceback = traceback.format_exc())
    raise Exception(error_message)


  def on_shadow_delta_updated(self, delta):
    """
    Callback for the shadow delta updated events.
    """

    logger.debug(delta)
    try:
      if delta.state and (self.SHADOW_WAIT_TIME_KEY in delta.state):
        value = delta.state[self.SHADOW_WAIT_TIME_KEY]

        if value is None:
          logger.warning("The shadow delta contains a null value. Resetting the wait_time_sec to the default value.")
          self.update_shadow_value(self.DEFAULT_WAIT_TIME_SEC)
          return

        if not isinstance(value, int):
          logger.warning(f"The shadow delta contains a non-integer value: {value}. Resetting the wait_time_sec to the default value.")
          self.update_shadow_value(self.DEFAULT_WAIT_TIME_SEC)
          return

        logger.info(f"Delta reports that desired value is {value}. Changing local value from {self.wait_time_sec} to {value}.")
        self.wait_time_sec = value
        self.update_shadow_value(self.wait_time_sec)
      else:
        logger.warning("The shadow delta does not contain the wait_time_sec key.")
    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)


  def get_shadow_document(self):
    """
    Get the shadow document.
    """

    logger.info("Publishing a message for the shadow document retrieval.")
    try:
      self.shadow_client.publish_get_shadow(
        request = iotshadow.GetShadowRequest(
          thing_name = self.init_data["device_name"]
        ),
        qos=self.QUALITY_OF_SERVICE
      ).result()
      logger.info("Published a message for the shadow document retrieval.")
    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)


  def publish_battery_level(self):
    """
    Publish the battery level to the MQTT topic.
    """

    topic = self.BASE_TOPIC + self.init_data["device_name"]

    # get the battery level
    battery_level = self.get_battery_level()
    logger.info(f"Current battery level: {battery_level}")

    # publish the battery level to the MQTT topic
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    payload = {
      "device_name": self.init_data["device_name"],
      "timestamp": now, 
      "battery_level": battery_level
    }

    logger.info(f"Publishing the battery level to the topic '{topic}'.")
    try:
      mqtt_publish_battery_level_future, _ = self.mqtt_connection.publish(
        topic=topic,
        payload=json.dumps(payload),
        qos=self.QUALITY_OF_SERVICE
      )

      mqtt_publish_battery_level_future.result()
      logger.info(f"Published the battery level to the topic '{topic}'.")

    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)


  def get_battery_level(self):
    """
    Get the battery level.
    """

    # get the battery level
    command = '/usr/sbin/ioreg -c AppleSmartBattery -r -k CurrentCapacity | awk \'/\"CurrentCapacity\"/ {print $3}\''
    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    # return the battery level
    return result.stdout.strip()


  def update_shadow_value(self, desired_value):
    """
    Update the reported shadow value when the shadow delta updated event is received.
    """

    logger.info(f"Updating the shadow value to {desired_value}.")
    try:
      # update the shadow value
      new_state = iotshadow.ShadowState(
        reported={self.SHADOW_WAIT_TIME_KEY: desired_value}
      )

      # publish the updated shadow value
      mqtt_update_shadow_future = self.shadow_client.publish_update_shadow(
        request=iotshadow.UpdateShadowRequest(
          thing_name=self.init_data["device_name"],
          state=new_state
        ),
        qos=self.QUALITY_OF_SERVICE
      )
      # add a callback when the shadow update request is published
      mqtt_update_shadow_future.add_done_callback(self.on_publish_update_shadow)
    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)


  def on_publish_update_shadow(self, future):
    """
    Callback when the shadow update request is published.
    """

    try:
      future.result()
      logger.info("Published the shadow update request.")
    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)


  def exit_program(self):
    """
    Exit the program.
    """

    logger.info("Exiting the program.")

    if self.shadow_client:
      logger.info("Unsubscribing from the shadow topics.")
      self.shadow_client.unsubscribe("$aws/things/{}/shadow/get/accepted".format(self.init_data["device_name"]))
      self.shadow_client.unsubscribe("$aws/things/{}/shadow/get/rejected".format(self.init_data["device_name"]))
      self.shadow_client.unsubscribe("$aws/things/{}/shadow/update/accepted".format(self.init_data["device_name"]))
      self.shadow_client.unsubscribe("$aws/things/{}/shadow/update/rejected".format(self.init_data["device_name"]))
      self.shadow_client.unsubscribe("$aws/things/{}/shadow/update/delta".format(self.init_data["device_name"]))

    if self.mqtt_connection:
      logger.info("Disconnecting the MQTT connection.")
      self.mqtt_connection.disconnect()


if __name__ == "__main__":

  publisher = BatteryLevelPublisher()
  publisher.run()
import argparse
import logging
import sys
import time
import re
from datetime import datetime

from awscrt import mqtt

from mqtt_client import MQTTClient
from shadow_client import ShadowClient
from battery_sensor import BatterySensor

class BatteryLevelPublisher:

  BASE_TOPIC = "data/"
  ERROR_MESSAGE_FORMAT = "Exception in {function_name}: {message}\n{traceback}"

  def __init__(self):
    self.init_data = self.parse_arguments()
    self.topic = self.BASE_TOPIC + self.init_data["device_name"]
    self.QOS = mqtt.QoS.AT_LEAST_ONCE
    self.logger = None
    self.mqtt_client = None
    self.shadow_client = None


  def run(self):
    try:
      self.logger = self.set_logger()

      ## log the init data
      for key, value in self.init_data.items():
        self.logger.info(f"{key}: {value}")

      # connect to the MQTT broker
      self.mqtt_client = MQTTClient(
        logger = self.logger,
        error_message_format = self.ERROR_MESSAGE_FORMAT,
        qos = self.QOS,
        topic = self.topic,
        endpoint = self.init_data["endpoint"],
        cert_file = self.init_data["cert_file"],
        key_file = self.init_data["key_file"],
        ca_file = self.init_data["ca_file"],
        device_name = self.init_data["device_name"]
      )

      self.mqtt_client.connect()

      # create the shadow client
      self.shadow_client = ShadowClient(
        logger = self.logger,
        error_message_format = self.ERROR_MESSAGE_FORMAT,
        qos = self.QOS,
        device_name = self.init_data["device_name"],
        mqtt_connection = self.mqtt_client.get_connection()
      )
      # set up the shadow subscriptions
      self.shadow_client.setup_shadow_subscriptions()

      # publish a message for the shadow document retrieval
      self.shadow_client.get_shadow_document()

      while True:
        # start sending the battery level
        # get the battery level
        battery_level = BatterySensor(self.logger).get_battery_level()
        self.logger.info(f"Current battery level: {battery_level}")

        # publish the battery level to the MQTT topic
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        payload = {
          "device_name": self.init_data["device_name"],
          "timestamp": now, 
          "battery_level": battery_level
        }
        self.mqtt_client.publish(payload)

        # wait for the next iteration
        self.logger.info(f"Waiting for {self.shadow_client.wait_time_sec} seconds.")
        time.sleep(self.shadow_client.wait_time_sec)

    except Exception as e:
      self.logger.error(e)
      self.exit_program()


  def set_logger(self):
    """
    Set the logger.
    """

    logger_format = "%(levelname)-8s  %(asctime)s [%(filename)s:%(lineno)3d] %(message)s"
    logger = logging.getLogger(__name__)

    if not logger.handlers:
      # set the logging handler
      handler = logging.StreamHandler()
      # set the logging format
      handler.setFormatter(logging.Formatter(logger_format))
      logger.addHandler(handler)
      # set the logging level
      logger.setLevel(logging.INFO)
      logger.propagate = False

    return logger


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
      self.logger.error(f"The endpoint '{args.endpoint}' is not in the correct format.")
      self.logger.error("The endpoint should be in the format '<prefix>.iot.<region>.amazonaws.com'.")
      sys.exit(1)

    # check if the CA file string is valid
    if not re.match(args_pattern["ca_file"], args.ca_file):
      self.logger.error(f"The CA file '{args.ca_file}' is not in the correct format.")
      self.logger.error("The CA file should be in the format '<filename>.pem'.")
      sys.exit(1)
    
    # check if the certificate file string is valid
    if not re.match(args_pattern["cert_file"], args.cert_file):
      self.logger.error(f"The certificate file '{args.cert_file}' is not in the correct format.")
      self.logger.error("The certificate file should be in the format '<filename>-certificate.pem.crt'.")
      sys.exit(1)

    # check if the private key file string is valid
    if not re.match(args_pattern["key_file"], args.key_file):
      self.logger.error(f"The private key file '{args.key_file}' is not in the correct format.")
      self.logger.error("The private key file should be in the format '<filename>-private.pem.key'.")
      sys.exit(1)

    # return the parsed arguments
    return {
      "device_name": args.device_name,
      "endpoint": args.endpoint,
      "ca_file": args.ca_file,
      "cert_file": args.cert_file,
      "key_file": args.key_file
    }


  def exit_program(self):
    """
    Exit the program.
    """

    self.logger.info("Exiting the program.")


    if self.mqtt_client:
      self.logger.info("Disconnecting the MQTT connection.")
      self.mqtt_client.disconnect()
    else:
      self.logger.warning("No MQTT connection to disconnect.")


if __name__ == "__main__":

  publisher = BatteryLevelPublisher()
  publisher.run()

import inspect
import json
import traceback

from awsiot import mqtt_connection_builder
from awscrt import io

class MQTTClient:

  KEEP_ALIVE_SECS = 300

  def __init__(self, logger, error_message_format, qos, topic, endpoint, cert_file, key_file, ca_file, device_name):
    self.logger = logger
    self.ERROR_MESSAGE_FORMAT = error_message_format
    self.QOS = qos
    self.topic = topic
    self.endpoint = endpoint
    self.cert_file = cert_file
    self.key_file = key_file
    self.ca_file = ca_file
    self.device_name = device_name
    self.mqtt_connection = None


  def connect(self):
    """
    Connect to the MQTT broker.
    """

    # create the client bootstrap object
    client_bootstrap = self.create_client_bootstrap()

    # create the MQTT connection
    self.logger.info("Creating the MQTT connection.")
    try:
      self.mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint = self.endpoint,
        cert_filepath = self.cert_file,
        pri_key_filepath = self.key_file,
        ca_filepath = self.ca_file,
        client_id = self.device_name,
        client_bootstrap = client_bootstrap,
        clean_session = False,
        keep_alive_secs = self.KEEP_ALIVE_SECS
      )
      connected_future = self.mqtt_connection.connect()
      connected_future.result()
      self.logger.info("Connected to the AWS IoT Core")
    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)


  def create_client_bootstrap(self):
    """
    Create the client bootstrap object.
    """

    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)

    return io.ClientBootstrap(event_loop_group, host_resolver)


  def disconnect(self):
    """
    Disconnect from the MQTT broker.
    """

    self.logger.info("Disconnecting from the MQTT broker.")
    try:
      if self.mqtt_connection:
        self.mqtt_connection.disconnect()
        self.logger.info("Disconnected from the MQTT broker.")
    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)

  
  def publish(self, payload):
    """
    Publish a message to the MQTT broker.
    """

    self.logger.info(f"Publishing a message to the topic '{self.topic}'.")
    try:
      mqtt_publish_future, _ = self.mqtt_connection.publish(
        topic = self.topic,
        payload = json.dumps(payload),
        qos = self.QOS
      )

      mqtt_publish_future.result()
      self.logger.info(f"Published a message to the topic '{self.topic}'.")
    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)
  

  def get_connection(self):
    """
    Get the MQTT connection object.
    """

    if not self.mqtt_connection:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = "The MQTT connection object is not initialized.", traceback = "")
      raise Exception(error_message)

    return self.mqtt_connection
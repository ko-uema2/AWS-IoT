import json
import pytest
from unittest.mock import MagicMock, patch
from mqtt_client import MQTTClient

@pytest.fixture
def mqtt_client():
  """
  Creates and returns an instance of MQTTClient with predefined parameters for testing purposes.

  ### Mocks:
    - `logger`:
      A mock instance of the logger.

  ### Returns:
    - `MQTTClient`:
      An instance of MQTTClient configured with test parameters.
  """

  logger = MagicMock()
  return MQTTClient(
    logger=logger,
    error_message_format="Exception in {function_name}: {message}\n{traceback}",
    qos=1,
    topic="test_topic",
    endpoint="test_endpoint",
    cert_file="test_cert_file",
    key_file="test_key_file",
    ca_file="test_ca_file",
    device_name="test_device_name"
  )

def test_connect_success(mqtt_client):
  """
  Test the successful connection of the MQTT client.

  This test mocks the `mtls_from_path` method from the `awsiot.mqtt_connection_builder` module
  and the `create_client_bootstrap` method of the `mqtt_client` to simulate a successful connection to the AWS IoT Core.  
  It then verifies that the appropriate log messages are generated and that the `mtls_from_path` method is called with the correct parameters.

  ### Args:
    - `mqtt_client (MQTTClient)`:
      The MQTT client instance to be tested.

  ### Mocks:
    - `awsiot.mqtt_connection_builder.mtls_from_path`
    - `mqtt_client.create_client_bootstrap`

  ### Asserts:
    - The MQTT client's logger logs the following messages.
      1. `Creating the MQTT connection.`
      2. `Connected to the AWS IoT Core.`
    - The `mtls_from_path` method is called once with the specified parameters.
    - The `connect` method of the `mtls_from_path` object is called once.
  """

  with patch('awsiot.mqtt_connection_builder.mtls_from_path') as mock_mtls_from_path, \
    patch.object(mqtt_client, 'create_client_bootstrap', return_value=MagicMock()) as mock_create_client_bootstrap:
    mock_mtls_from_path.return_value.connect = MagicMock()
    mock_mtls_from_path.return_value.connect.return_value.result.return_value = None

    mqtt_client.connect()

    mqtt_client.logger.info.assert_any_call("Creating the MQTT connection.")
    mock_mtls_from_path.assert_called_once_with(
      endpoint="test_endpoint",
      cert_filepath="test_cert_file",
      pri_key_filepath="test_key_file",
      ca_filepath="test_ca_file",
      client_id="test_device_name",
      client_bootstrap=mock_create_client_bootstrap.return_value,
      clean_session=False,
      keep_alive_secs=MQTTClient.KEEP_ALIVE_SECS
    )
    mock_mtls_from_path.return_value.connect.assert_called_once()
    mqtt_client.logger.info.assert_any_call("Connected to the AWS IoT Core.")

def test_connect_failure(mqtt_client):
  """
  Test the failure scenario of the `connect` method in the MQTT client.

  This test mocks the `mtls_from_path` method to raise an exception, simulating a connection error.  
  It then verifies that the `connect` method of the `mqtt_client` raises an exception with the expected error message.  
  Additionally, it checks that the logger logs the expected message.

  ### Args:
    - `mqtt_client`:
      The MQTT client instance to be tested.
  
  ### Mocks: 
    - `mqtt_client.mqtt_connection_builder.mtls_from_path`

  ### Raises:
    - `Exception`:
      If the connection fails, an exception is expected to be raised.
  
  ### Asserts:
    - The MQTT client's logger logs the message.
      `Creating the MQTT connection.`
    - The `connect` method of the `mtls_from_path` object is called once.
    - The following error message is part of the raised exception's message.
      `Exception in connect: Connection error`
  """

  with patch('mqtt_client.mqtt_connection_builder.mtls_from_path') as mock_mtls_from_path:
    mock_mtls_from_path.return_value = MagicMock()
    mock_mtls_from_path.return_value.connect.side_effect = Exception("Connection error")

    with pytest.raises(Exception) as excinfo:
      mqtt_client.connect()
    
    current_function = "connect"
    expected_error_message = f"Exception in {current_function}: Connection error\n"

    assert expected_error_message in str(excinfo.value)
    mqtt_client.logger.info.assert_any_call("Creating the MQTT connection.")
    mock_mtls_from_path.return_value.connect.assert_called_once()

def test_disconnect_success(mqtt_client):
  """
  Test the successful disconnection of the MQTT client.

  This test mocks the `disconnect` method of the MQTT connection object to simulate a successful disconnection.  
  It then verifies that the appropriate log messages are generated and that the `disconnect` method of the MQTT connection is called.


  ### Args:
    - `mqtt_client`:
      The MQTT client instance to be tested.

  ### Mocks:
    - `mqtt_client.mqtt_connection`:
      A mock instance for simulating the MQTT disconnection process with AWS IoT Core.

  ### Asserts:
    - The logger logs the following messages.
      1. `Disconnecting from the MQTT broker.`
      2. `Disconnected from the MQTT broker.`
    - The `disconnect` method of the `mqtt_connection` object is called once.
  """

  mqtt_client.mqtt_connection = MagicMock()
  mqtt_client.disconnect()
  mqtt_client.logger.info.assert_any_call("Disconnecting from the MQTT broker.")
  mqtt_client.mqtt_connection.disconnect.assert_called_once()
  mqtt_client.logger.info.assert_any_call("Disconnected from the MQTT broker.")

def test_disconnect_failure(mqtt_client):
  """
  Test the failure scenario of the `disconnect` method in the MQTT client.

  This test mocks the `disconnect` method of the MQTT connection object to raise an exception,
  simulating a disconnection error.  
  It then verifies that the `disconnect` method of the MQTT client is called once and raises an exception with the expected error message.
  Additionally, it checks that the logger logs the expected message.

  ### Args:
    - `mqtt_client (MagicMock)`:
      The MQTT client instance to be tested.

  ### Mocks:
    - `mqtt_client.mqtt_connection`:
      A mock instance for simulating the MQTT disconnection process with AWS IoT Core.

  ### Raises:
    - `Exception`:
      If the `disconnect` method of the MQTT connection raises an exception.

  ### Asserts:
    - The MQTT client's `logger` logs the following message.
      `Disconnecting from the MQTT broker.`
    - The `disconnect` method of the `mqtt_connection` object is called once.
    - The following error message is part of the raised exception's message.
      `Exception in disconnect: Disconnect error`
  """

  mqtt_client.mqtt_connection = MagicMock()
  mqtt_client.mqtt_connection.disconnect.side_effect = Exception("Disconnect error")

  with pytest.raises(Exception) as excinfo:
    mqtt_client.disconnect()

  current_function = "disconnect"
  expected_error_message = f"Exception in {current_function}: Disconnect error\n"

  assert expected_error_message in str(excinfo.value)
  mqtt_client.logger.info.assert_any_call("Disconnecting from the MQTT broker.")
  mqtt_client.mqtt_connection.disconnect.assert_called_once()

def test_publish_success(mqtt_client):
  """
  Test the successful publishing of a message using the MQTT client.

  TODO:
  This test verifies that the MQTT client's `publish` method correctly publishes
  a message to the specified topic and logs the appropriate messages.

  ### Args:
    - `mqtt_client`:
      An instance of the MQTT client with mocked dependencies.
  
  ### Mocks:
    - `mqtt_client.mqtt_connection`:
      A mock instance for simulating the MQTT publish process with AWS IoT Core.
    - `mqtt_client.mqtt_connection.publish`:
      A mock instance of the `publish` method of the MQTT connection object.
  
  ### Asserts:
    - The logger logs the following messages.
      1. `Publishing a message to the topic 'test_topic'.`
      2. `Published a message to the topic 'test_topic'.`
    - The `publish` method of the `mqtt_connection` object is called once with the specified parameters.

  """

  mqtt_client.mqtt_connection = MagicMock()
  payload = {"key": "value"}
  mock_future = MagicMock()
  mqtt_client.mqtt_connection.publish.return_value = (mock_future, None)
  mock_future.result.return_value = None

  mqtt_client.publish(payload)
  mqtt_client.logger.info.assert_any_call("Publishing a message to the topic 'test_topic'.")
  mqtt_client.mqtt_connection.publish.assert_called_once_with(
    topic="test_topic",
    payload=json.dumps(payload),
    qos=1
  )
  mqtt_client.logger.info.assert_any_call("Published a message to the topic 'test_topic'.")

def test_publish_failure(mqtt_client):
  """
  Test the failure scenario of the `publish` method in the MQTT client.

  This test mocks the MQTT connection's publish method to raise an exception, simulating a publish failure.  
  It then verifies that the publish method of the mqtt_client raises an exception with the expected error message.  
  Additionally, it checks that the logger logs the expected message.

  ### Args:
    - `mqtt_client`:
      The MQTT client instance to be tested.

  ### Raises:
    - `Exception`:
      If the publish method does not raise the expected exception.
  
  ### Asserts:
    - The logger logs the following message.
      `Publishing a message to the topic 'test_topic'.`
    - The `publish` method of the `mqtt_connection` object is called once.
    - The following error message is part of the raised exception's message.
      `Exception in publish: Publish error`
  """

  mqtt_client.mqtt_connection = MagicMock()
  mqtt_client.mqtt_connection.publish.side_effect = Exception("Publish error")

  with pytest.raises(Exception) as excinfo:
    mqtt_client.publish({"key": "value"})

  current_function = "publish"
  expected_error_message = f"Exception in {current_function}: Publish error\n"

  assert expected_error_message in str(excinfo.value)
  mqtt_client.logger.info.assert_any_call("Publishing a message to the topic 'test_topic'.")
  mqtt_client.mqtt_connection.publish.assert_called_once()

def test_get_connection_success(mqtt_client):
  """
  Test the successful retrieval of the MQTT connection object.

  This test mocks the `mqtt_connection` attribute of the `mqtt_client` object to simulate a successful connection.
  It then verifies that the `get_connection` method returns the correct
  MQTT connection object when the connection is successfully established.

  ### Args:
    - `mqtt_client`:
      The MQTT client instance to be tested.

  ### Mocks:
    - `mqtt_client.mqtt_connection`:
      A mock instance for simulating the MQTT connection object.

  ### Asserts:
    - The returned connection is the same as the mocked `mqtt_connection`.
  """

  mqtt_client.mqtt_connection = MagicMock()
  connection = mqtt_client.get_connection()
  assert connection == mqtt_client.mqtt_connection

def test_get_connection_failure(mqtt_client):
  """
  Test the failure scenario of the `get_connection` method in the MQTT client.

  This test mocks the `mqtt_connection` attribute of the `mqtt_client` object to simulate a connection error.
  It then verifies that the `get_connection` method raises an exception with the expected error message.

  ### Args:
    - `mqtt_client`:
      The MQTT client instance to be tested.

  ### Mocks:
    - `mqtt_client.mqtt_connection`:
      A mock instance for simulating the MQTT connection object.

  ### Raises:
    - `Exception`:
      If the MQTT connection object is not initialized.

  ### Asserts:
    - The following error message is part of the raised exception's message.
      `Exception in get_connection: The MQTT connection object is not initialized.`
  """

  mqtt_client.mqtt_connection = None

  with pytest.raises(Exception) as excinfo:
    mqtt_client.get_connection()

  current_function = "get_connection"
  expected_error_message = f"Exception in {current_function}: The MQTT connection object is not initialized.\n"

  assert expected_error_message in str(excinfo.value)
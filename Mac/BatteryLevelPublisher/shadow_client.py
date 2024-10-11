import inspect
import traceback

from awsiot import iotshadow

class ShadowClient:

  SHADOW_WAIT_TIME_KEY = "wait_time_sec"
  DEFAULT_WAIT_TIME_SEC = 10

  def __init__(self, logger, error_message_format, qos, device_name, mqtt_connection):
    self.logger = logger
    self.ERROR_MESSAGE_FORMAT = error_message_format
    self.QOS = qos
    self.wait_time_sec = self.DEFAULT_WAIT_TIME_SEC
    self.device_name = device_name
    self.shadow_client = iotshadow.IotShadowClient(mqtt_connection)
  

  def setup_shadow_subscriptions(self):
    """
    Set up the shadow subscriptions.
    """

    self.logger.info("Setting up the shadow subscriptions.")

    # GetShadow Accepted/Rejected
    self.subscribe_to_get_shadow()

    # UpdateShadow Accepted/Rejected
    self.subscribe_to_update_shadow()

    # Delta Updated Event
    self.subscribe_to_delta_updated_event()

    self.logger.info("Shadow subscriptions set up successfully.")

  
  def subscribe_to_get_shadow(self):
    """
    Subscribe to the GetShadow topic.
    """

    try:
      mqtt_get_shadow_accepted_future, _ = self.shadow_client.subscribe_to_get_shadow_accepted(
        request = iotshadow.GetShadowSubscriptionRequest(
          thing_name = self.device_name
        ),
        qos = self.QOS,
        callback = self.on_shadow_document_retrieval_accepted
      )

      mqtt_get_shadow_rejected_future, _ = self.shadow_client.subscribe_to_get_shadow_rejected(
        request = iotshadow.GetShadowSubscriptionRequest(
          thing_name = self.device_name
        ),
        qos = self.QOS,
        callback = self.on_shadow_document_retrieval_rejected
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
        request = iotshadow.UpdateShadowSubscriptionRequest(
          thing_name = self.device_name
        ),
        qos = self.QOS,
        callback = self.on_shadow_document_updated_accepted
      )

      mqtt_update_shadow_rejected_future, _ = self.shadow_client.subscribe_to_update_shadow_rejected(
        request = iotshadow.UpdateShadowSubscriptionRequest(
          thing_name = self.device_name
        ),
        qos = self.QOS,
        callback = self.on_shadow_document_updated_rejected
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

    self.logger.info("Received the shadow delta updated event.")
    try:
      mqtt_delta_updated_event_future, _ = self.shadow_client.subscribe_to_shadow_delta_updated_events(
        request = iotshadow.ShadowDeltaUpdatedSubscriptionRequest(
          thing_name = self.device_name
        ),
        qos = self.QOS,
        callback = self.on_shadow_delta_updated
      )

      mqtt_delta_updated_event_future.result()
    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)

  
  def unsubscribe_from_shadow_topics(self):
    """
    Unsubscribe from the shadow topics.
    """

    try:
      self.logger.info("Unsubscribing from the shadow topics.")

      if self.shadow_client:
        self.logger.info("Unsubscribing from the shadow topics.")
        self.shadow_client.unsubscribe("$aws/things/{}/shadow/get/accepted".format(self.device_name))
        self.shadow_client.unsubscribe("$aws/things/{}/shadow/get/rejected".format(self.device_name))
        self.shadow_client.unsubscribe("$aws/things/{}/shadow/update/accepted".format(self.device_name))
        self.shadow_client.unsubscribe("$aws/things/{}/shadow/update/rejected".format(self.device_name))
        self.shadow_client.unsubscribe("$aws/things/{}/shadow/update/delta".format(self.device_name))
      else:
        self.logger.warning("Shadow client is not initialized. Skipping the unsubscription.")
      
      self.logger.info("Unsubscribed from the shadow topics.")

    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)

  
  def get_shadow_document(self):
    """
    Get the shadow document.
    """

    self.logger.info("Publishing a message for the shadow document retrieval.")
    try:
      self.shadow_client.publish_get_shadow(
        request = iotshadow.GetShadowRequest(
          thing_name = self.device_name
        ),
        qos = self.QOS
      ).result()
      self.logger.info("Published a message for the shadow document retrieval.")
    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)


  def update_shadow_value(self, desired_value):
    """
    Update the reported shadow value when the shadow delta updated event is received.
    """

    self.logger.info(f"Updating the shadow value to {desired_value}.")
    try:
      # update the shadow value
      new_state = iotshadow.ShadowState(
        reported = {self.SHADOW_WAIT_TIME_KEY: desired_value}
      )

      # publish the updated shadow value
      mqtt_update_shadow_future = self.shadow_client.publish_update_shadow(
        request = iotshadow.UpdateShadowRequest(
          thing_name = self.device_name,
          state = new_state
        ),
        qos = self.QOS
      )
      # add a callback when the shadow update request is published
      mqtt_update_shadow_future.add_done_callback(self.on_publish_update_shadow)
    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)


  def on_shadow_document_retrieval_accepted(self, response):
    """
    Callback when the shadow document retrieval request is accepted.
    """

    try:
      self.logger.info("Received the shadow document retrieval response.")

      if not response.state:
        self.logger.warning("The shadow document does not contain the state key. Resetting the wait_time_sec to the default value.")
        self.update_shadow_value(self.DEFAULT_WAIT_TIME_SEC)
        return

      if response.state.delta and (self.SHADOW_WAIT_TIME_KEY in response.state.delta):
        value = response.state.delta[self.SHADOW_WAIT_TIME_KEY]
        if value is None:
          self.logger.warning("The shadow document contains a null value. Resetting the wait_time_sec to the default value.")
          self.update_shadow_value(self.DEFAULT_WAIT_TIME_SEC)
        else:
          self.logger.info(f"Shadow document reports that desired value is {value}. Changing local value from {self.wait_time_sec} to {value}.")
          self.wait_time_sec = value
          self.update_shadow_value(self.wait_time_sec)
        return

      if response.state.desired and (self.SHADOW_WAIT_TIME_KEY in response.state.desired):
        value = response.state.desired[self.SHADOW_WAIT_TIME_KEY]
        if value is None:
          self.logger.warning("The shadow document contains a null value. Resetting the wait_time_sec to the default value.")
          self.update_shadow_value(self.DEFAULT_WAIT_TIME_SEC)
        else:
          self.logger.info(f"Shadow document reports that desired value is {value}. Changing local value from {self.wait_time_sec} to {value}.")
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

    self.logger.debug(delta)
    try:
      if delta.state and (self.SHADOW_WAIT_TIME_KEY in delta.state):
        value = delta.state[self.SHADOW_WAIT_TIME_KEY]

        if value is None:
          self.logger.warning("The shadow delta contains a null value. Resetting the wait_time_sec to the default value.")
          self.update_shadow_value(self.DEFAULT_WAIT_TIME_SEC)
          return

        if not isinstance(value, int):
          self.logger.warning(f"The shadow delta contains a non-integer value: {value}. Resetting the wait_time_sec to the default value.")
          self.update_shadow_value(self.DEFAULT_WAIT_TIME_SEC)
          return

        self.logger.info(f"Delta reports that desired value is {value}. Changing local value from {self.wait_time_sec} to {value}.")
        self.wait_time_sec = value
        self.update_shadow_value(self.wait_time_sec)
      else:
        self.logger.warning("The shadow delta does not contain the wait_time_sec key.")
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
      self.logger.info("Published the shadow update request.")
    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)
  
  def get_wait_time_sec(self):
    """
    Get the wait time in seconds.
    """

    return self.wait_time_sec
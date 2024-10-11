import inspect
import subprocess
import traceback

class BatterySensor:

  def __init__(self, logger):
    self.logger = logger


  def get_battery_level(self):
    """
    Get the battery level.
    """

    try:

      self.logger.info("Getting the battery level.")

      # get the battery level
      command = '/usr/sbin/ioreg -c AppleSmartBattery -r -k CurrentCapacity | awk \'/\"CurrentCapacity\"/ {print $3}\''
      result = subprocess.run(command, shell=True, capture_output=True, text=True)

      # return the battery level
      return int(result.stdout.strip())
    
    except Exception as e:
      current_function = inspect.currentframe().f_code.co_name
      error_message = self.ERROR_MESSAGE_FORMAT.format(function_name = current_function, message = e, traceback = traceback.format_exc())
      raise Exception(error_message)
import time
import functools


class Timer(object):
    """
    Timer using time.time() differences and print statements
    """
    def __init__(self):
        """ Initialize start and end time
        """
        self.start_time = None
        self.end_time = None

    def reset(self):
        """ Reinitialize start and end time
        :return: None
        """
        self.__init__()

    def start(self, message_str=""):
        """ Start timer
        :param message_str: message to print when timer is started
        :return: None
        """
        if message_str != "":
            print(message_str)
        if self.start_time is not None:
            print("WARNING: Timer re-starting without having been stopped.")
        self.start_time = time.time()

    def stop(self, message_str=""):
        """ Stop and reset timer
        :param message_str: message to print when timer is stopped
        :return: None
        """
        self.end_time = time.time()
        if self.start_time is None:
            print("ERROR: Timer stop() called before start().")
        else:
            if message_str != "":
                print("Done. {} | {:,.1f} seconds"
                      .format(message_str, self.end_time - self.start_time))
            else:
                print("Done. {:,.1f} seconds"
                      .format(self.end_time - self.start_time))
            self.reset()    # No real need to reset self.end_time in error case


def add_timer(func):
    """ Decorator that adds timing to any function
        Usage: add "@add_timer" (syntactic sugar) right above a function definition
    """
    @functools.wraps(func)  # Carry over original function metadata
    def wrapper(*args, **kwargs):
        """ Add Timer statements to the execution of given function """
        timer = Timer()
        timer.start(f"Timing {func.__name__}() with args {args} and kwargs {kwargs}...")
        func_ret = func(*args, **kwargs)
        timer.stop()
        return func_ret
    return wrapper

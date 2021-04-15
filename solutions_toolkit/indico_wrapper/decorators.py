from time import sleep
from functools import wraps
from indico import IndicoRequestError

ALLOWED_EXCEPTIONS = (ConnectionError, TimeoutError, OSError, IndicoRequestError)


def retry_request(func):
    @wraps(func)
    def wrapper_retry_request(*args, **kwargs):
        """
        note that retry_count, delay, and allowed exceptions are arguments that
        are meant to be user defined and should be passed to the function that
        is getting retried
        """
        retry_count = kwargs.get("retry_count", 5)
        delay = kwargs.get("delay", 30)
        allowed_exceptions = kwargs.get("allowed_exceptions", ALLOWED_EXCEPTIONS)
        for count in range(retry_count):
            try:
                result = func(*args, **kwargs)
                # if result:
                #     return result
                return result
            except allowed_exceptions as e:
                if count == retry_count - 1:
                    print(
                        "An exception has occurred on attempt {}. Please re-run the script at a later time.".format(
                            count + 1
                        )
                    )
                    raise (e)
                else:
                    print(e)
                    print(
                        "Attempt {} failed, retrying in {} seconds".format(
                            count + 1, delay
                        )
                    )
                    sleep(delay)

    return wrapper_retry_request

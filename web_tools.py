import requests


def safe_requests_get(api, n_failures=5):
    """ Loop requests.get() for an improved chance of success when using web APIs
    :param api: web API on which to use requests.get()
    :param n_failures: max number of failures before giving up
    :return: response from requests.get() or RuntimeError exception
    """
    failure_count = 0
    while failure_count < n_failures:
        try:
            resp = requests.get(api)
            break
        except ConnectionError:
            failure_count += 1
            print("safe_requests_get: failure #{failure_count} - ConnectionError.")
    else:
        raise RuntimeError(f"{api} query failed {failure_count} times in a row.")
    return resp

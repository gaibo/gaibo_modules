import requests


def safe_requests_get(url, n_failures=5, **kwargs):
    """ Loop requests.get() for an improved chance of success when using web APIs
    :param url: web API URL on which to use requests.get()
    :param n_failures: max number of failures before giving up
    :param kwargs: additional optional arguments for requests.get(); e.g. auth=('user', 'pass')
    :return: response from requests.get() or RuntimeError exception
    """
    failure_count = 0
    while failure_count < n_failures:
        try:
            resp = requests.get(url, **kwargs)
            break
        except ConnectionError:
            failure_count += 1
            print("safe_requests_get: failure #{failure_count} - ConnectionError.")
        except requests.exceptions.ChunkedEncodingError:
            failure_count += 1
            print("safe_requests_get: failure #{failure_count} - ChunkedEncodingError;\n"
                  "\taborted connection due to transmission time-out.")
    else:
        raise RuntimeError(f"{url} query failed {failure_count} times in a row.")
    return resp

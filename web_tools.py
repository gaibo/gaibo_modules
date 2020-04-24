import requests
import pathlib
import gzip
import shutil


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


def unzip_gz(zipped_file_dir, file_name, unzipped_file_dir=None, verbose=False):
    """ Unzip a gzipped (.gz) file
        NOTE: this function checks whether the unzipped file already exists,
              which both saves effort and ensures files are never overwritten
    :param zipped_file_dir: directory of zipped file
    :param file_name: name of zipped file (including full file extension)
    :param unzipped_file_dir: directory of unzipped file
    :param verbose: set True for explicit print statements
    :return: True if file was unzipped, False if no unzip was performed
    """
    if unzipped_file_dir is None:
        unzipped_file_dir = zipped_file_dir
    unzipped_full_name = unzipped_file_dir + file_name[:-3]     # Leave out '.gz'
    if not pathlib.Path(unzipped_full_name).exists():
        # Unzipped file does not exist - perform unzip
        zipped_full_name = zipped_file_dir + file_name
        with gzip.open(zipped_full_name, 'rb') as f_in:
            with open(unzipped_full_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        if verbose:
            print(f"Unzipped: {unzipped_full_name}")
        return True
    else:
        if verbose:
            print(f"No need; already unzipped: {unzipped_full_name}")
        return False    # No unzip needed

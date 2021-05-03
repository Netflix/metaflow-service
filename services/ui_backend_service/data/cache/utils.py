from metaflow.datatools.s3 import MetaflowS3AccessDenied, MetaflowS3Exception, MetaflowS3NotFound, MetaflowS3URLException, MetaflowException
from . import s3op
from metaflow.datatools.s3 import S3, get_s3_client, debug
from botocore.exceptions import NoCredentialsError, ClientError
from tempfile import NamedTemporaryFile
import subprocess
import sys
import os
import pickle
from gzip import GzipFile
from itertools import islice

from features import FEATURE_S3_DISABLE


def batchiter(it, batch_size):
    it = iter(it)
    while True:
        batch = list(islice(it, batch_size))
        if batch:
            yield batch
        else:
            break


def decode(path):
    "decodes a gzip+pickle compressed object from a file path"
    with GzipFile(path) as f:
        obj = pickle.load(f)
        return obj


# No-Retry S3 client


class MetaflowS3CredentialsMissing(MetaflowException):
    headline = 'could not locate s3 credentials'


if FEATURE_S3_DISABLE:
    class NoRetryS3(S3):
        def _read_many_files(self, op, prefixes, **options):
            raise MetaflowS3Exception("S3 disabled.")

        def _one_boto_op(self, op, url):
            raise MetaflowS3Exception("S3 disabled.")

        def _s3op_with_retries(self, mode, **options):
            return None, "S3 disabled."

else:
    class NoRetryS3(S3):
        '''Custom S3 class with no retries for quick failing.
        Base implementation is the metaflow library S3 client

        Used only for get() and get_many() operations.
        '''

        def _one_boto_op(self, op, url):
            error = ''
            tmp = NamedTemporaryFile(dir=self._tmpdir,
                                     prefix='metaflow.s3.one_file.',
                                     delete=False)
            try:
                s3, _ = get_s3_client()
                op(s3, tmp.name)
                return tmp.name
            except ClientError as err:
                error_code = s3op.normalize_client_error(err)
                if error_code == 404:
                    raise MetaflowS3NotFound(url)
                elif error_code == 403:
                    raise MetaflowS3AccessDenied(url)
                elif error_code == 'NoSuchBucket':
                    raise MetaflowS3URLException("Specified S3 bucket doesn't exist.")
                error = str(err)
            except NoCredentialsError as err:
                raise MetaflowS3CredentialsMissing(err)
            except Exception as ex:
                # TODO specific error message for out of disk space
                error = str(ex)
            os.unlink(tmp.name)
            raise MetaflowS3Exception("S3 operation failed.\n"
                                      "Key requested: %s\n"
                                      "Error: %s" % (url, error))

        def _s3op_with_retries(self, mode, **options):

            cmdline = [sys.executable, os.path.abspath(s3op.__file__), mode]
            for key, value in options.items():
                key = key.replace('_', '-')
                if isinstance(value, bool):
                    if value:
                        cmdline.append('--%s' % key)
                    else:
                        cmdline.append('--no-%s' % key)
                else:
                    cmdline.extend(('--%s' % key, value))

            with NamedTemporaryFile(dir=self._tmpdir,
                                    mode='wb+',
                                    delete=not debug.s3client,
                                    prefix='metaflow.s3op.stderr') as stderr:
                try:
                    debug.s3client_exec(cmdline)
                    stdout = subprocess.check_output(cmdline,
                                                     cwd=self._tmpdir,
                                                     stderr=stderr.file)
                    return stdout, None
                except subprocess.CalledProcessError as ex:
                    stderr.seek(0)
                    err_out = stderr.read().decode('utf-8', errors='replace')
                    stderr.seek(0)
                    if ex.returncode == s3op.ERROR_URL_NOT_FOUND:
                        raise MetaflowS3NotFound(err_out)
                    elif ex.returncode == s3op.ERROR_URL_ACCESS_DENIED:
                        raise MetaflowS3AccessDenied(err_out)
                    elif ex.returncode == s3op.ERROR_MISSING_CREDENTIALS:
                        raise MetaflowS3CredentialsMissing(err_out)

            return None, err_out

# Cache action stream output helpers


class StreamedCacheError(Exception):
    "Used for custom raises during cache action stream errors"
    pass


def progress_event_msg(number):
    "formatter for cache action progress stream messages"
    return {
        "type": "progress",
        "fraction": number
    }


def error_event_msg(msg, id, traceback=None):
    "formatter for cache action error stream messages"
    return {
        "type": "error",
        "message": msg,
        "id": id,
        "traceback": traceback
    }


def search_result_event_msg(results):
    "formatter for cache action search result message"
    return {
        "type": "result",
        "matches": results
    }

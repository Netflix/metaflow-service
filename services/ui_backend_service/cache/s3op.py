# Patched version of the s3op from metaflow.datatools.s3
# supports only get and get_many.
# adds specific error code for missing S3 credentials.
# and removes aws_retry decorator usage to fit backend usecase.

from metaflow.datatools.s3op import _populate_prefixes
from metaflow.datatools.s3op import verify_results, generate_local_path
from metaflow.datatools.s3op import S3Ops as _S3Ops
from metaflow.datatools.s3op import with_unit
from metaflow.datatools.s3op import worker, start_workers, process_urls
from metaflow.datatools.s3op import format_triplet, normalize_client_error
from metaflow.multicore_utils import parallel_map
from metaflow.util import url_quote, url_unquote
import math
import string
import sys
import os
from multiprocessing import Process, Queue
from collections import namedtuple
from itertools import starmap, chain, islice

try:
    # python2
    from urlparse import urlparse
    from Queue import Full as QueueFull
except:
    # python3
    from urllib.parse import urlparse
    from queue import Full as QueueFull

import click

# s3op can be launched as a stand-alone script. We must set
# PYTHONPATH for the parent Metaflow explicitly.
sys.path.insert(0,
                os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
# we use Metaflow's parallel_imap_unordered instead of
# multiprocessing.Pool because https://bugs.python.org/issue31886

NUM_WORKERS_DEFAULT = 2

S3Url = namedtuple('S3Url', ['bucket', 'path', 'url', 'local', 'prefix'])

# We use error codes instead of Exceptions, which are trickier to
# handle reliably in a multi-process world
ERROR_INVALID_URL = 4
ERROR_NOT_FULL_PATH = 5
ERROR_URL_NOT_FOUND = 6
ERROR_URL_ACCESS_DENIED = 7
ERROR_WORKER_EXCEPTION = 8
ERROR_VERIFY_FAILED = 9
ERROR_LOCAL_FILE_NOT_FOUND = 10
ERROR_MISSING_CREDENTIALS = 11


# S3 worker pool

# Utility functions


class S3Ops(_S3Ops):
    def get_size(self, url):
        self.reset_client()
        try:
            head = self.s3.head_object(Bucket=url.bucket, Key=url.path)
            return True, url, [(url, head['ContentLength'])]
        except NoCredentialsError as err:
            return False, url, ERROR_MISSING_CREDENTIALS
        except ClientError as err:
            error_code = normalize_client_error(err)
            if error_code == 404:
                return False, url, ERROR_URL_NOT_FOUND
            elif error_code == 403:
                return False, url, ERROR_URL_ACCESS_DENIED
            else:
                raise

    def list_prefix(self, prefix_url, delimiter=''):
        self.reset_client()
        url_base = 's3://%s/' % prefix_url.bucket
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            urls = []
            for page in paginator.paginate(Bucket=prefix_url.bucket,
                                           Prefix=prefix_url.path,
                                           Delimiter=delimiter):
                # note that an url may be both a prefix and an object
                # - the trailing slash is significant in S3
                if 'Contents' in page:
                    for key in page.get('Contents', []):
                        url = url_base + key['Key']
                        urlobj = S3Url(url=url,
                                       bucket=prefix_url.bucket,
                                       path=key['Key'],
                                       local=generate_local_path(url),
                                       prefix=prefix_url.url)
                        urls.append((urlobj, key['Size']))
                if 'CommonPrefixes' in page:
                    # we get CommonPrefixes if Delimiter is a non-empty string
                    for key in page.get('CommonPrefixes', []):
                        url = url_base + key['Prefix']
                        urlobj = S3Url(url=url,
                                       bucket=prefix_url.bucket,
                                       path=key['Prefix'],
                                       local=None,
                                       prefix=prefix_url.url)
                        urls.append((urlobj, None))
            return True, prefix_url, urls
        except self.s3.exceptions.NoSuchBucket:
            return False, prefix_url, ERROR_URL_NOT_FOUND
        except NoCredentialsError as err:
            return False, prefix_url, ERROR_MISSING_CREDENTIALS
        except ClientError as err:
            if err.response['Error']['Code'] == 'AccessDenied':
                return False, prefix_url, ERROR_URL_ACCESS_DENIED
            else:
                raise


# We want to reuse an s3 client instance over multiple operations.
# This is accomplished by op_ functions below.


def op_get_size(urls):
    s3 = S3Ops()
    return [s3.get_size(url) for url in urls]


def op_list_prefix(prefix_urls):
    s3 = S3Ops()
    return [s3.list_prefix(prefix) for prefix in prefix_urls]


def op_list_prefix_nonrecursive(prefix_urls):
    s3 = S3Ops()
    return [s3.list_prefix(prefix, delimiter='/') for prefix in prefix_urls]


def exit(exit_code, url):
    if exit_code == ERROR_INVALID_URL:
        msg = 'Invalid url: %s' % url.url
    elif exit_code == ERROR_NOT_FULL_PATH:
        msg = 'URL not a full path: %s' % url.url
    elif exit_code == ERROR_URL_NOT_FOUND:
        msg = 'URL not found: %s' % url.url
    elif exit_code == ERROR_URL_ACCESS_DENIED:
        msg = 'Access denied to URL: %s' % url.url
    elif exit_code == ERROR_WORKER_EXCEPTION:
        msg = 'Download failed'
    elif exit_code == ERROR_MISSING_CREDENTIALS:
        msg = 'Missing S3 Credentials'
    elif exit_code == ERROR_VERIFY_FAILED:
        msg = 'Verification failed for URL %s, local file %s'\
              % (url.url, url.local)
    elif exit_code == ERROR_LOCAL_FILE_NOT_FOUND:
        msg = 'Local file not found: %s' % url
    else:
        msg = 'Unknown error'
    print(msg, file=sys.stderr)
    sys.exit(exit_code)


def parallel_op(op, lst, num_workers):
    # parallel op divides work equally amongst num_workers
    # processes. This is a good strategy if the cost is
    # uniform over the units of work, e.g. op_get_size, which
    # is a single HEAD request to S3.
    #
    # This approach is less optimal with op_list_prefix where
    # the cost of S3 listing per prefix can vary drastically.
    # We could optimize this case by using a worker model with
    # a queue, like for downloads but the difference here is
    # that we need to return a value, which would require a
    # bit more work - something to consider if this turns out
    # to be a bottleneck.
    if lst:
        num = min(len(lst), num_workers)
        batch_size = math.ceil(len(lst) / float(num))
        batches = []
        it = iter(lst)
        while True:
            batch = list(islice(it, batch_size))
            if batch:
                batches.append(batch)
            else:
                break
        it = parallel_map(op, batches, max_parallel=num)
        for x in chain.from_iterable(it):
            yield x

# CLI


@click.group()
def cli():
    pass


@cli.command('list', help='List S3 objects')
@click.option('--inputs',
              type=click.Path(exists=True),
              help='Read input prefixes from the given file.')
@click.option('--num-workers',
              default=NUM_WORKERS_DEFAULT,
              show_default=True,
              help='Number of concurrent connections.')
@click.option('--recursive/--no-recursive',
              default=False,
              show_default=True,
              help='Download prefixes recursively.')
@click.argument('prefixes', nargs=-1)
def lst(prefixes,
        inputs=None,
        num_workers=None,
        recursive=None):

    urllist = []
    for prefix in _populate_prefixes(prefixes, inputs):
        src = urlparse(prefix)
        url = S3Url(url=prefix,
                    bucket=src.netloc,
                    path=src.path.lstrip('/'),
                    local=None,
                    prefix=prefix)
        if src.scheme != 's3':
            exit(ERROR_INVALID_URL, url)
        urllist.append(url)

    op = op_list_prefix if recursive else op_list_prefix_nonrecursive
    urls = []
    for success, prefix_url, ret in parallel_op(op, urllist, num_workers):
        if success:
            urls.extend(ret)
        else:
            exit(ret, prefix_url)

    for url, size in urls:
        if size is None:
            print(format_triplet(url.prefix, url.url))
        else:
            print(format_triplet(url.prefix, url.url, str(size)))


@cli.command(help='Download files from S3')
@click.option('--recursive/--no-recursive',
              default=False,
              show_default=True,
              help='Download prefixes recursively.')
@click.option('--num-workers',
              default=NUM_WORKERS_DEFAULT,
              show_default=True,
              help='Number of concurrent connections.')
@click.option('--inputs',
              type=click.Path(exists=True),
              help='Read input prefixes from the given file.')
@click.option('--verify/--no-verify',
              default=True,
              show_default=True,
              help='Verify that files were loaded correctly.')
@click.option('--allow-missing/--no-allow-missing',
              default=False,
              show_default=True,
              help='Do not exit if missing files are detected. '
                   'Implies --verify.')
@click.option('--verbose/--no-verbose',
              default=True,
              show_default=True,
              help='Print status information on stderr.')
@click.option('--listing/--no-listing',
              default=False,
              show_default=True,
              help='Print S3 URL -> local file mapping on stdout.')
@click.argument('prefixes', nargs=-1)
def get(prefixes,
        recursive=None,
        num_workers=None,
        inputs=None,
        verify=None,
        allow_missing=None,
        verbose=None,
        listing=None):

    if allow_missing:
        verify = True

    # Construct a list of URL (prefix) objects
    urllist = []
    for prefix in _populate_prefixes(prefixes, inputs):
        src = urlparse(prefix)
        url = S3Url(url=prefix,
                    bucket=src.netloc,
                    path=src.path.lstrip('/'),
                    local=generate_local_path(prefix),
                    prefix=prefix)
        if src.scheme != 's3':
            exit(ERROR_INVALID_URL, url)
        if not recursive and not src.path:
            exit(ERROR_NOT_FULL_PATH, url)
        urllist.append(url)

    # Construct a url->size mapping
    op = None
    if recursive:
        op = op_list_prefix
    elif verify or verbose:
        op = op_get_size
    if op:
        urls = []

        # NOTE - we must retain the order of prefixes requested
        # and the listing order returned by S3
        for success, prefix_url, ret in parallel_op(op, urllist, num_workers):
            if success:
                urls.extend(ret)
            elif ret == ERROR_URL_NOT_FOUND and allow_missing:
                urls.append((prefix_url, None))
            else:
                exit(ret, prefix_url)
    else:
        # pretend zero size since we don't need it for anything.
        # it can't be None though, to make sure the listing below
        # works correctly (None denotes a missing file)
        urls = [(prefix_url, 0) for prefix_url in urllist]

    # exclude the non-existent files from loading
    to_load = [(url, size) for url, size in urls if size is not None]
    process_urls('download', to_load, verbose, num_workers)
    # Postprocess
    if verify:
        verify_results(to_load, verbose=verbose)

    if listing:
        for url, size in urls:
            if size is None:
                print(format_triplet(url.url))
            else:
                print(format_triplet(url.prefix, url.url, url.local))


if __name__ == '__main__':
    from botocore.exceptions import ClientError, NoCredentialsError
    cli(auto_envvar_prefix='S3OP')

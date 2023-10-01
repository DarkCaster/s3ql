'''
storjs3.py - this file is part of S3QL.

Copyright © 2023 DarkCaster

This work can be distributed under the terms of the GNU GPLv3.
'''

import logging
import re
import base64
import time
import threading
import random

from typing import Any, BinaryIO, Dict, Optional
from ..logging import QuietError
from . import s3c

from s3ql.http import (
    HTTPConnection,
)

log = logging.getLogger(__name__)

OBJ_DATA_TRANSLATED_RE = re.compile(r'^(.*)(s3ql_data)/([0-9]+)$')
OBJ_OTHER_TRANSLATED_RE = re.compile(r'^(.*)(s3ql_other)/([a-zA-Z0-9_\-=]*)$')

OBJ_DATA_RE = re.compile(r'^(s3ql_data_)([0-9]+)$')
PFX_DATA = "s3ql_data_"

PFX_DATA_TRANSLATED = "s3ql_data/"
PFX_OTHER_TRANSLATED = "s3ql_other/"


def STR_ENCODE(key):
    return base64.urlsafe_b64encode(key.encode()).decode()


def STR_DECODE(b64key):
    return base64.urlsafe_b64decode(b64key.encode()).decode()


def GET_RANDOM_DELAY(base, mult):
    if mult < 1:
        mult = 1
    return base * random.uniform(1, mult)


# NOTE: as of S3QL 5.1.2 - HTTPConnection class seem to contain internal problem that may lead to filesystem crash
# when calling to "reset" method, it will open another connection right after disconnect, and if it fail - exception may not be handled properly
# when it called from inside Backend::is_temp_failure method.
# We are needed NOT to reinitialize connection in "reset" method with STORJ anyway,
# so fix it here until it changed in upstream
class STORJConnection(HTTPConnection):
    def __init__(self, hostname, port=None, ssl_context=None, proxy=None):
        super().__init__(hostname, port, ssl_context, proxy)

    def reset(self):
        self.disconnect()

# some (in)sane default for object retention periods and retries
LOCK_RETRY_INTERVAL = 1
GRACELOCK_INTERVAL = 30

class RetentionLock:

    def __init__(self):
        self.oplock = threading.Lock()
        self.writelocks = set()
        self.readlocks = dict()
        self.gracelocks = dict()

    def AquireRead(self, key):
        wait_time=0
        while True:
            # wait for retention time, calculated at previous step, if any
            if wait_time > 0:
                time.sleep(wait_time)
            self.oplock.acquire()
            mark = time.monotonic()
            try:
                # check key is not writelocked, set timer, start over if so
                if key in self.writelocks:
                    log.warning('trying to read key that is held by writelock: %s', key)
                    wait_time = LOCK_RETRY_INTERVAL + GET_RANDOM_DELAY(0.1,10)
                    continue
                # check key is not gracelocked, set timer, start over if so
                if key in self.gracelocks:
                    mark_end = self.gracelocks[key]
                    wait_time = mark_end - mark
                    if wait_time > 0:
                        wait_time = wait_time + GET_RANDOM_DELAY(0.1,10)
                        log.warning('trying to read key that is held by gracelock: %s, time left: %0.2f', key, wait_time)
                        continue
                # TODO perform housekeeping for gracelocks

                # increase key read counter
                try:
                    rcnt = self.readlocks[key]
                    self.readlocks[key] = rcnt + 1
                except KeyError:
                    self.readlocks[key] = 1
                return
            finally:
                self.oplock.release()

    def ReleaseRead(self, key):
        self.oplock.acquire()
        try:
            # decrease key read counter
            rcnt = self.readlocks[key] - 1
            if rcnt < 1:
                del self.readlocks[key]
            else:
                self.readlocks[key] = rcnt
        except KeyError:
            log.warning("key error while managing readlocks on read release")
        finally:
            self.oplock.release()

    def AquireWrite(self, key):
        wait_time=0
        while True:
            # wait for retention time, calculated at previous step if any
            if wait_time > 0:
                time.sleep(wait_time)
            self.oplock.acquire()
            mark = time.monotonic()
            try:
                # check key not readlocked, set timer, start over if so
                if key in self.readlocks:
                    log.warning('trying to write key that is held by readlock: %s', key)
                    wait_time = LOCK_RETRY_INTERVAL + GET_RANDOM_DELAY(0.1,10)
                    continue
                # check key not writelocked, set timer, start over if so
                if key in self.writelocks:
                    log.warning('trying to write key that is held by writelock: %s', key)
                    wait_time = LOCK_RETRY_INTERVAL + GET_RANDOM_DELAY(0.1,10)
                    continue
                # check key is not gracelocked, set timer, start over if so
                if key in self.gracelocks:
                    mark_end = self.gracelocks[key]
                    wait_time = mark_end - mark
                    if wait_time > 0:
                        wait_time = wait_time + GET_RANDOM_DELAY(0.1,10)
                        log.warning('trying to write key that is held by gracelock: %s, time left: %0.2f', key, wait_time)
                        continue
                # TODO: perform housekeeping for gracelocks
                # set writelock for this key
                self.writelocks.add(key)
                return
            finally:
                self.oplock.release()

    def ReleaseWrite(self, key):
        self.oplock.acquire()
        try:
            # remove writelock for this key
            self.writelocks.remove(key)
            # set gracelock for this key
            mark = time.monotonic() + GRACELOCK_INTERVAL
            self.gracelocks[key] = mark
        except KeyError:
            log.warning("key error while managing writelocks on write release")
        finally:
            self.oplock.release()


class Backend(s3c.Backend):
    """A backend for Storj S3 gateway-st/mt

    Uses some quirks for placing data/seq/metadata objects in the storj bucket.
    This is needed for gateway-st/gateway-mt limited ListObjectsV2 S3-API to work correctly
    with buckets that contains more than 100K objects.
    """

    def __init__(self, options):
        super().__init__(options)
        self.oplock = RetentionLock()

    def _translate_s3_key_to_storj(self, key):
        '''convert object key to the form suitable for use with storj s3 bucket'''
        # try to convert binary string key to string if needed
        if not isinstance(key, str):
            log.info('key is not a string: %s', key)
            key = '%s' % key
        # check whether key is already in storj format, needed if backend methods called again from backend reference from inner code
        # NOTE: not sure if it still needed after removal of ObjectR and ObjectW classes
        match = OBJ_DATA_TRANSLATED_RE.match(key)
        if match is not None:
            log.debug('skipping already translated data key: %s', key)
            return key
        match = OBJ_OTHER_TRANSLATED_RE.match(key)
        if match is not None:
            log.debug('skipping already translated key: %s', key)
            return key
        # match s3ql_data keys
        match = OBJ_DATA_RE.match(key)
        if match is not None:
            return PFX_DATA_TRANSLATED + match.group(2)
        # for all other cases:
        # base64-encode key to remove forward slashes and any possible match with "s3ql_data" regex and place it into s3ql_other prefix
        result = PFX_OTHER_TRANSLATED + STR_ENCODE(key)
        log.debug('translated s3 key %s: %s', key, result)
        return result

    def _translate_other_key_to_s3(self, key):
        '''convert object key from storj form to normal s3 form'''
        match = OBJ_OTHER_TRANSLATED_RE.match(key)
        if match is not None:
            result = STR_DECODE(match.group(3))
            log.debug('translated storj key %s: %s', key, result)
            return result
        raise RuntimeError(f'Failed to translate key to s3 form: {key}')

    def _translate_data_key_to_s3(self, key):
        '''convert object key from storj form to normal s3 form'''
        match = OBJ_DATA_TRANSLATED_RE.match(key)
        if match is not None:
            result = PFX_DATA + match.group(3)
            # log.debug('translated storj key %s: %s', key, result)
            return result
        raise RuntimeError(f'Failed to translate data key to s3 form: {key}')

    # we are using our STORJConnection wrapper with some minor fixes and debugging
    def _get_conn(self):
        conn = STORJConnection(
            self.hostname, self.port, proxy=self.proxy, ssl_context=self.ssl_context
        )
        return conn

    def list(self, prefix=''):
        # try to convert binary string key to string if needed
        if not isinstance(prefix, str):
            log.info('filter prefix is not a string: %s', prefix)
            prefix = '%s' % prefix
        log.debug('list requested for prefix: %s', prefix)
        # list s3ql_data segments for any partial s3ql_data_ or empty searches
        if PFX_DATA.startswith(prefix):
            log.debug('running list for %s sub-prefix', PFX_DATA_TRANSLATED)
            inner_list = super().list(PFX_DATA_TRANSLATED)
            for el in inner_list:
                yield self._translate_data_key_to_s3(el)
        # iterate over s3ql_other store, if search prefix not exactly "s3ql_data_"
        if prefix != PFX_DATA:
            # get inner list generator for s3ql_other/ prefix
            log.debug('running list for %s sub-prefix with manual filtering', PFX_OTHER_TRANSLATED)
            inner_list = super().list(PFX_OTHER_TRANSLATED)
            # translate keys for s3 form and filter against requested prefix manually
            for el in inner_list:
                el_t = self._translate_other_key_to_s3(el)
                if not el_t.startswith(prefix):
                    continue
                yield el_t

    def delete(self, key):
        key_t = self._translate_s3_key_to_storj(key)
        try:
            self.oplock.AquireWrite(key_t)
            return super().delete(key_t)
        finally:
            self.oplock.ReleaseWrite(key_t)

    def lookup(self, key):
        key_t = self._translate_s3_key_to_storj(key)
        try:
            self.oplock.AquireRead(key_t)
            return super().lookup(key_t)
        finally:
            self.oplock.ReleaseRead(key_t)

    def get_size(self, key):
        key_t = self._translate_s3_key_to_storj(key)
        try:
            self.oplock.AquireRead(key_t)
            return super().get_size(key_t)
        finally:
            self.oplock.ReleaseRead(key_t)

    def readinto_fh(self, key: str, fh: BinaryIO):
        key_t = self._translate_s3_key_to_storj(key)
        try:
            self.oplock.AquireRead(key_t)
            return super().readinto_fh(key_t, fh)
        finally:
            self.oplock.ReleaseRead(key_t)

    def write_fh(
        self,
        key: str,
        fh: BinaryIO,
        metadata: Optional[Dict[str, Any]] = None,
        len_: Optional[int] = None,
    ):
        key_t = self._translate_s3_key_to_storj(key)
        try:
            self.oplock.AquireWrite(key_t)
            return super().write_fh(key_t, fh, metadata, len_)
        finally:
            self.oplock.ReleaseWrite(key_t)

    def __str__(self):
        return 'storjs3://%s/%s/%s' % (self.hostname, self.bucket_name, self.prefix)

    # improve error handling for STORJ backend to make it more resilent for errors during peak hours
    def is_temp_failure(self, exc):
        result = False
        try:
            result = super().is_temp_failure(exc)
            if result == True:
                log.info('S3 error, exception: %s, %s', type(exc).__name__, exc)
            else:
                log.info('S3 failed, exception: %s, %s', type(exc).__name__, exc)
        except Exception as e:
            log.warning(
                'Parent is_temp_failure call failed, exception: %s, %s', type(e).__name__, e
            )
            # make such error to be always temporary
            result = True
        # Disconnect from STORJ backend on any error, in order to always create new connection right before next request.
        # It seem that STORJ S3 gateway does not like to reuse HTTP connection even after legitimate HTTP error responses.
        # For example 429 response with long retry timeout followed after that - makes currently established HTTP/TLS connection dormant,
        # and it silently closed on remote side long before the next request, causing fail on retry
        self.conn.disconnect()
        return result

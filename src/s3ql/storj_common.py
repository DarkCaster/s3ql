import logging
import threading
import random
import time


log = logging.getLogger(__name__)


def GetConsistencyLock():
    if 'CONSISTENCY_LOCK' not in globals():
        globals()['CONSISTENCY_LOCK'] = ConsistencyLock()
    return globals()['CONSISTENCY_LOCK']


# some sane default for object retention periods and retries
LOCK_RETRY_BASE = 0.5
LOCK_RETRY_EXTRA = 1
GRACE_BASE_TIME = 20
GRACE_EXTRA_TIME = 20


class ConsistencyLock:
    '''
    Some sort of read-write lock that tracks objects by it's key
    not allowing to read and write in parallel to the same object,
    but allowing to read same object same time in multiple threads.
    It also add extra consistency delay to read and write operations
    after previous write operation to the same object
    '''

    def __init__(self):
        self.oplock = threading.Lock()
        self.writelocks = set()
        self.readlocks = dict()
        self.gracelocks = dict()
        self.tls = threading.local()
        self.UpdateTimeouts(LOCK_RETRY_BASE, LOCK_RETRY_EXTRA, GRACE_BASE_TIME, GRACE_EXTRA_TIME)

    @property
    def tls_cnt(self):
        try:
            return self.tls.cnt
        except AttributeError:
            self.tls.cnt = 0
            return self.tls.cnt

    @tls_cnt.setter
    def tls_cnt(self, value):
        self.tls.cnt = value

    def UpdateTimeouts(self, lock_retry_base, lock_retry_extra, grace_time_base, grace_time_extra):
        self.oplock.acquire()
        try:
            self.lock_retry_base = lock_retry_base
            self.lock_retry_extra = lock_retry_extra
            self.grace_time_base = grace_time_base
            self.grace_time_extra = grace_time_extra
        finally:
            self.oplock.release()

    def _GenLockRetryTimeout(self):
        return self.lock_retry_base + random.uniform(0, self.lock_retry_extra)

    def _GenGraceTimeout(self):
        return self.grace_time_base + random.uniform(0, self.grace_time_extra)

    def _GracelocksCleanup(self):
        # no need to lock here
        mark = time.monotonic()
        to_remove = set()
        # iterate over gracelocks, and record expired keys
        for key, expiration in self.gracelocks.items():
            if mark > expiration:
                to_remove.add(key)
        for key in to_remove:
            del self.gracelocks[key]
            # log.info("removed key from gracelocks: %s", key)

    def AcquireRead(self, key):
        wait_time = 0
        while True:
            # wait for retention time, calculated at previous step, if any
            if wait_time > 0:
                time.sleep(wait_time)
            self.oplock.acquire()
            mark = time.monotonic()
            try:
                if self.tls_cnt > 0:
                    log.info('recursive use of lock for claiming read operation: %s', key)
                else:
                    # check key is not writelocked, set timer, start over if so
                    if key in self.writelocks:
                        log.warning('trying to read object that is being written: %s', key)
                        wait_time = self._GenLockRetryTimeout()
                        continue
                    # check key is not gracelocked, set timer, start over if so
                    if key in self.gracelocks:
                        mark_end = self.gracelocks[key]
                        wait_time = mark_end - mark
                        if wait_time > 0:
                            log.info(
                                'delaying read to ensure consistency: %0.2fs for %s', wait_time, key
                            )
                            continue
                    # perform housekeeping for gracelocks
                    self._GracelocksCleanup()
                # increase key read counter
                if key in self.readlocks:
                    rcnt = self.readlocks[key]
                    self.readlocks[key] = rcnt + 1
                else:
                    self.readlocks[key] = 1
                # increase thread local counter
                self.tls_cnt += 1
                return
            finally:
                self.oplock.release()

    def ReleaseRead(self, key):
        self.oplock.acquire()
        try:
            # decrease thread local counter
            self.tls_cnt -= 1
            # decrease key read counter
            rcnt = self.readlocks[key] - 1
            if rcnt < 1:
                del self.readlocks[key]
            else:
                self.readlocks[key] = rcnt
        except KeyError:
            log.warning('key error while managing readlocks on read release')
        finally:
            self.oplock.release()

    def AcquireWrite(self, key):
        wait_time = 0
        while True:
            # wait for retention time, calculated at previous step if any
            if wait_time > 0:
                time.sleep(wait_time)
            self.oplock.acquire()
            mark = time.monotonic()
            try:
                if self.tls_cnt > 0:
                    log.info('recursive use of lock for claiming write operation: %s', key)
                else:
                    # check object is not being downloaded or uploaded right now
                    if key in self.readlocks or key in self.writelocks:
                        log.warning('trying to write object that is being accessed: %s', key)
                        wait_time = self._GenLockRetryTimeout()
                        continue
                    # check key is not gracelocked, set timer, start over if so
                    if key in self.gracelocks:
                        mark_end = self.gracelocks[key]
                        wait_time = mark_end - mark
                        if wait_time > 0:
                            log.info(
                                'delaying write to ensure consistency: %0.2fs for %s',
                                wait_time,
                                key,
                            )
                            continue
                    # perform housekeeping for gracelocks
                    self._GracelocksCleanup()
                if self.tls_cnt < 1:
                    # set writelock for this key
                    self.writelocks.add(key)
                # increase thread local counter
                self.tls_cnt += 1
                return
            finally:
                self.oplock.release()

    def ReleaseWrite(self, key):
        self.oplock.acquire()
        try:
            # decrease thread local counter
            self.tls_cnt -= 1
            if self.tls_cnt > 0:
                return
            # remove writelock for this key
            self.writelocks.remove(key)
            # set gracelock for this key
            mark = time.monotonic() + self._GenGraceTimeout()
            self.gracelocks[key] = mark
        except KeyError:
            log.warning('key error while managing writelocks on write release')
        finally:
            self.oplock.release()

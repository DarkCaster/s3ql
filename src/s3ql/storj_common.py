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
        self.id = random.randint(0, 9)

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
            #log.info("removed key from gracelocks: %s", key)

    def AcquireRead(self, key):
        wait_time=0
        while True:
            # wait for retention time, calculated at previous step, if any
            if wait_time > 0:
                time.sleep(wait_time)
            self.oplock.acquire()
            mark = time.monotonic()
            try:
                if self.tls_cnt > 0:
                    log.info('%d, recursive use of lock for claiming read operaion: %s', self.id, key)
                else:
                    # check key is not writelocked, set timer, start over if so
                    if key in self.writelocks:
                        log.warning('%d, trying to read object that is being written: %s', self.id, key)
                        wait_time = self._GenLockRetryTimeout()
                        continue
                    # check key is not gracelocked, set timer, start over if so
                    if key in self.gracelocks:
                        mark_end = self.gracelocks[key]
                        wait_time = mark_end - mark
                        if wait_time > 0:
                            log.info('%d, delaying read to ensure consistency: %0.2fs for %s', self.id, wait_time, key)
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
            log.warning("%d, key error while managing readlocks on read release", self.id)
        finally:
            self.oplock.release()

    def AcquireWrite(self, key):
        wait_time=0
        while True:
            # wait for retention time, calculated at previous step if any
            if wait_time > 0:
                time.sleep(wait_time)
            self.oplock.acquire()
            mark = time.monotonic()
            try:
                if self.tls_cnt > 0:
                    log.info('%d, recursive use of lock for claiming write operaion: %s', self.id, key)
                else:
                    # check object is not being downloaded or uploaded right now
                    if key in self.readlocks or key in self.writelocks:
                        log.warning('%d, trying to write object that is being accessed: %s', self.id, key)
                        wait_time = self._GenLockRetryTimeout()
                        continue
                    # check key is not gracelocked, set timer, start over if so
                    if key in self.gracelocks:
                        mark_end = self.gracelocks[key]
                        wait_time = mark_end - mark
                        if wait_time > 0:
                            log.info('%d, delaying write to ensure consistency: %0.2fs for %s', self.id, wait_time, key)
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
            log.warning("%d, key error while managing writelocks on write release", self.id)
        finally:
            self.oplock.release()


def GetBackendManager():
    if 'BACKEND_MANAGER' not in globals():
        globals()['BACKEND_MANAGER'] = BackendManager()
    return globals()['BACKEND_MANAGER']


BACKEND_MANAGER_TICK = 1
BACKEND_MANAGER_TICK_EXTRA = 2


class BackendManager(threading.Thread):
    def __init__(self):
        super().__init__(target = self.Worker)
        self.daemon = True
        self.oplock = threading.Lock()
        self.UpdateTimeouts(BACKEND_MANAGER_TICK, BACKEND_MANAGER_TICK_EXTRA)

    def UpdateTimeouts(self, tick_interval, tick_interval_extra):
        with self.oplock:
            self.tick_interval = tick_interval
            self.tick_interval_extra = tick_interval_extra

    def _GenTickTime(self):
        return self.tick_interval + random.uniform(0, self.tick_interval_extra)

    def Worker(self):
        log.info("Backend management daemon running, thread id %d", threading.get_native_id())
        while True:
            time.sleep(self._GenTickTime())
            self.Tick()

    def Tick(self):
        return

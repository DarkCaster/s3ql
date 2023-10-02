import logging

from . import storjs3

log = logging.getLogger(__name__)


class Backend(storjs3.Backend):
    # test storjs3 backend with lower grace periods, for test purposes

    def __init__(self, options):
        super().__init__(options)
        self.storjlock.UpdateTimeouts(1, 1, 1, 1)

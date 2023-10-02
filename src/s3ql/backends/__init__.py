'''
backends/__init__.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from . import gs, local, rackspace, s3, s3c, swift, swiftks, storjs3, storjs3t
from .b2.b2_backend import B2Backend

#: Mapping from storage URL prefixes to backend classes
prefix_map = {
    'storjs3': storjs3.Backend,
    'storjs3t': storjs3t.Backend,
    's3': s3.Backend,
    'local': local.Backend,
    'gs': gs.Backend,
    's3c': s3c.Backend,
    'swift': swift.Backend,
    'swiftks': swiftks.Backend,
    'rackspace': rackspace.Backend,
    'b2': B2Backend,
}

__all__ = ['common', 'pool', 'comprenc'] + list(prefix_map.keys())

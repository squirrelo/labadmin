#!/usr/bin/env python

from knimin.lib.configuration import KniminConfig
from knimin.lib.data_access import KniminAccess
from knimin.lib.sql_connection import TRN

config = KniminConfig()
db = KniminAccess(config)

__all__ = ['config', 'db', 'TRN']

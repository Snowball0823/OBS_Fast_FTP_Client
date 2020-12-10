#!/usr/bin/env python
# -*- coding: utf-8 -*-
import weakref, collections
from . import const
import time

class _LocalCacheThread(object):
    class Dict(dict):
        def __del__(self):
            pass

    def __init__(self, maxlen=10):
        self.weak = weakref.WeakValueDictionary()
        self.strong = collections.deque(maxlen=maxlen)

    @staticmethod
    def nowTime():
        return int(time.time())

    def get(self, key):
        value = self.weak.get(key)
        if value is not None and hasattr(value, 'expire') and self.nowTime() > value['expire']:
            value = None
        return value

    def set(self, key, value):
        self.weak[key] = strongRef = self.Dict(value)
        self.strong.append(strongRef)
    
    
class _LocalCacheProcess(object):

    def __init__(self, maxlen=10):
        import multiprocessing
        self.weak = multiprocessing.Manager().dict()

    @staticmethod
    def nowTime():
        return int(time.time())

    def get(self, key):
        value = self.weak.get(key)
        if value is not None and hasattr(value, 'expire') and self.nowTime() > value['expire']:
            del self.weak[key]
            value = None
        return value

    def set(self, key, value):
        self.weak[key] = value
        
LocalCache = _LocalCacheThread if const.IS_WINDOWS else _LocalCacheProcess
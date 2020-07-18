# Licensed to Modin Development Team under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding
# copyright ownership.  The Modin Development Team licenses this file to you under the
# Apache License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

import threading

from rpyc.utils.classic import deliver
import rpyc
from rpyc.lib.compat import pickle

from rpyc.core import brine, consts

from . import get_connection
from .meta_magic import _LOCAL_ATTRS, _WRAP_ATTRS, RemoteMeta, _KNOWN_DUALS

import collections
import time
_msg_to_name = collections.defaultdict(dict)
for name in dir(consts):
    if name.upper() == name:
        category, _ = name.split('_', 1)
        _msg_to_name[category][getattr(consts, name)] = name
_msg_to_name = dict(_msg_to_name)

class WrappingConnection(rpyc.Connection):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._remote_pickle_loads = None
        self._remote_cls_cache = {}

        self.logLock = threading.RLock()
        self.timings = {}
        self.inside = threading.local()
        self.inside.box = False
        self.inside.unbox = False
    def _send(self, msg, seq, args):
        str_args = str(args).replace('\r','').replace('\n', '\tNEWLINE\t')
        if msg == consts.MSG_REQUEST:
            handler, _ = args
            str_handler = f":req={_msg_to_name['HANDLE'][handler]}"
        else:
            str_handler = ''
        with self.logLock:
            with open('rpyc-trace.log', 'a') as out:
                out.write(f"send:msg={_msg_to_name['MSG'][msg]}:seq={seq}{str_handler}:args={str_args}\n")
        self.timings[seq] = time.time()
        return super()._send(msg, seq, args)
    def _dispatch(self, data):
        got1 = time.time()
        try:
            return super()._dispatch(data)
        finally:
            got2 = time.time()
            msg, seq, args = brine.load(data)
            sent = self.timings.pop(seq, 0)
            if msg == consts.MSG_REQUEST:
                handler, args = args
                str_handler = f":req={_msg_to_name['HANDLE'][handler]}"
            else:
                str_handler = ''
            str_args = str(args).replace('\r','').replace('\n', '\tNEWLINE\t')
            with self.logLock:
                with open('rpyc-trace.log', 'a') as out:
                    out.write(f"recv:timing={got1 - sent}+{got2 - got1}:msg={_msg_to_name['MSG'][msg]}:seq={seq}{str_handler}:args={str_args}\n")

    def deliver(self, local_obj):
        """
        More caching version of rpyc.classic.deliver()
        """
        return self._remote_pickle_loads(bytes(pickle.dumps(local_obj)))

    def _netref_factory(self, id_pack):
        id_name, cls_id, inst_id = id_pack
        id_name = str(id_name)
        if id_name.startswith('modin.') and inst_id:
            try:
                cached_cls = self._remote_cls_cache[(id_name, cls_id)]
            except KeyError:
                result = super()._netref_factory(id_pack)
                self._remote_cls_cache[(id_name, cls_id)] = type(result)
            else:
                result = cached_cls(self, id_pack)
        else:
            result = super()._netref_factory(id_pack)
        # try getting __real_cls__ from result.__class__ BUT make sure to
        # NOT get it from some parent class for result.__class__, otherwise
        # multiple wrappings happen

        # we cannot use 'result.__class__' as this could cause a lookup of
        # '__class__' on remote end
        try:
            local_cls = object.__getattribute__(result, "__class__")
        except AttributeError:
            return result

        try:
            # first of all, check if remote object has a known "wrapping" class
            # example: _DataFrame has DataFrame dual-nature wrapper
            local_cls = _KNOWN_DUALS[local_cls]
        except KeyError:
            pass
        try:
            # Try to get local_cls.__real_cls__ but look it up within
            # local_cls.__dict__ to not grab it from any parent class.
            # Also get the __dict__ by using low-level __getattribute__
            # to override any potential __getattr__ callbacks on the class.
            wrapping_cls = object.__getattribute__(local_cls, "__dict__")[
                "__real_cls__"
            ]
        except (AttributeError, KeyError):
            return result
        return wrapping_cls.from_remote_end(result)

    def _box(self, obj):
        start = None if self.inside.box else time.time() 
        self.inside.box = True
        try:
            try:
                remote_obj = object.__getattribute__(obj, "__remote_end__")
            except AttributeError:
                remote_obj = obj
            boxed = super()._box(remote_obj)
        finally:
            if start is not None:
                self.inside.box = False
                str_args = str(boxed).replace('\r','').replace('\n', '\tNEWLINE\t')
                with self.logLock:
                    with open('rpyc-trace.log', 'a') as out:
                        out.write(f"_box:timing={time.time() - start}:args={str_args}\n")
        return boxed

    def _unbox(self, package):
        start = None if self.inside.unbox else time.time() 
        self.inside.unbox = True
        try:
            return super()._unbox(package)
        finally:
            if start is not None:
                self.inside.unbox = False
                str_args = str(package).replace('\r','').replace('\n', '\tNEWLINE\t')
                with self.logLock:
                    with open('rpyc-trace.log', 'a') as out:
                        out.write(f"_unbox:timing={time.time() - start}:args={str_args}\n")

    def _init_deliver(self):
        self._remote_pickle_loads = self.modules["rpyc.lib.compat"].pickle.loads


class WrappingService(rpyc.ClassicService):
    _protocol = WrappingConnection

    def on_connect(self, conn):
        super().on_connect(conn)
        conn._init_deliver()


_PROXY_LOCAL_ATTRS = frozenset(["__name__", "__remote_end__"])
_NO_OVERRIDE = (
    _LOCAL_ATTRS
    | _PROXY_LOCAL_ATTRS
    | frozenset(_WRAP_ATTRS)
    | rpyc.core.netref.LOCAL_ATTRS
)


def make_proxy_cls(remote_cls, origin_cls, override, cls_name=None):
    class ProxyMeta(type):
        def __repr__(self):
            return f"<proxy for {origin_cls.__module__}.{origin_cls.__name__}:{cls_name or origin_cls.__name__}>"

        def __prepare__(self, *args, **kw):
            namespace = type.__prepare__(*args, **kw)
            namespace['__remote_methods__'] = {}

            for base in origin_cls.__mro__:
                if base == object:
                    continue
                for name, entry in base.__dict__.items():
                    if (
                        name not in namespace
                        and name not in override.__dict__
                        and name.startswith("__")
                        and name.endswith("__")
                        and name not in _NO_OVERRIDE
                        and callable(entry)
                    ):

                        def method(_self, *_args, __method_name__=name, **_kw):
                            cache = object.__getattribute__(_self, "__remote_methods__")
                            try:
                                remote = cache[__method_name__]
                            except KeyError:
                                cache[__method_name__] = remote = getattr(remote_cls, __method_name__)
                            return remote(_self.__remote_end__, *_args, **_kw)

                        method.__name__ = name
                        namespace[name] = method
            return namespace

    class Wrapper(override, metaclass=ProxyMeta):
        def __init__(self, *a, __remote_end__=None, **kw):
            if __remote_end__ is None:
                __remote_end__ = remote_cls(*a, **kw)
            object.__setattr__(self, "__remote_end__", __remote_end__)

        @classmethod
        def from_remote_end(cls, remote_inst):
            return cls(__remote_end__=remote_inst)

        def __getattr__(self, name):
            """
            Any attributes not currently known to Wrapper (i.e. not defined here
            or in override class) will be retrieved from the remote end
            """
            return getattr(self.__remote_end__, name)

        if override.__setattr__ == object.__setattr__:
            # no custom attribute setting, define our own relaying to remote end
            def __setattr__(self, name, value):
                if name not in _PROXY_LOCAL_ATTRS:
                    setattr(self.__remote_end__, name, value)
                else:
                    object.__setattr__(self, name, value)

        if override.__delattr__ == object.__delattr__:
            # no custom __delattr__, define our own
            def __delattr__(self, name):
                if name not in _PROXY_LOCAL_ATTRS:
                    delattr(self.__remote_end__, name)

    class Wrapped(origin_cls, metaclass=RemoteMeta):
        __name__ = cls_name or origin_cls.__name__
        __wrapper_remote__ = remote_cls
        __wrapper_local__ = Wrapper
        __class__ = Wrapper

        def __new__(cls, *a, **kw):
            return Wrapper(*a, **kw)

    Wrapper.__name__ = Wrapped.__name__

    return Wrapped


def _deliveringWrapper(origin_cls, methods, mixin=None, target_name=None):
    conn = get_connection()
    remote_cls = getattr(conn.modules[origin_cls.__module__], origin_cls.__name__)

    if mixin is None:

        class DeliveringMixin:
            pass

        mixin = DeliveringMixin

    for method in methods:

        def wrapper(self, *args, __remote_conn__=conn, __method_name__=method, **kw):
            args = tuple(__remote_conn__.deliver(x) for x in args)
            kw = {k: __remote_conn__.deliver(v) for k, v in kw.items()}
            cache = object.__getattribute__(self, '__remote_methods__')
            try:
                remote = cache[__method_name__]
            except KeyError:
                cache[__method_name__] = remote = getattr(remote_cls, __method_name__)
            return remote(self.__remote_end__, *args, **kw)

        wrapper.__name__ = method
        setattr(mixin, method, wrapper)
    return make_proxy_cls(
        remote_cls, origin_cls, mixin, target_name or origin_cls.__name__
    )


def _prepare_loc_mixin():
    from modin.pandas.indexing import _LocIndexer, _iLocIndexer

    DeliveringLocIndexer = _deliveringWrapper(
        _LocIndexer, ["__getitem__", "__setitem__"]
    )
    DeliveringILocIndexer = _deliveringWrapper(
        _iLocIndexer, ["__getitem__", "__setitem__"]
    )

    class DeliveringMixin:
        @property
        def loc(self):
            return DeliveringLocIndexer(self.__remote_end__)

        @property
        def iloc(self):
            return DeliveringILocIndexer(self.__remote_end__)

    return DeliveringMixin


def make_dataframe_wrapper():
    from modin.pandas.dataframe import _DataFrame

    DeliveringDataFrame = _deliveringWrapper(
        _DataFrame,
        ["groupby", "agg", "aggregate", "__getitem__", "astype"],
        _prepare_loc_mixin(),
        "DataFrame",
    )
    return DeliveringDataFrame


def make_base_dataset_wrapper():
    from modin.pandas.base import _BasePandasDataset

    DeliveringBasePandasDataset = _deliveringWrapper(
        _BasePandasDataset,
        ["agg", "aggregate"],
        _prepare_loc_mixin(),
        "BasePandasDataset",
    )
    return DeliveringBasePandasDataset


def make_dataframe_groupby_wrapper():
    from modin.pandas.groupby import _DataFrameGroupBy

    DeliveringDataFrameGroupBy = _deliveringWrapper(
        _DataFrameGroupBy,
        ["agg", "aggregate", "apply"],
        target_name="DataFrameGroupBy",
    )
    return DeliveringDataFrameGroupBy

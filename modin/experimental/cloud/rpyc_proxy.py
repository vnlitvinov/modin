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
import types

from rpyc.utils.classic import deliver
import rpyc
from rpyc.lib.compat import pickle
from rpyc.lib import get_methods

from rpyc.core import brine, consts, netref, AsyncResult

from . import get_connection
from .meta_magic import _LOCAL_ATTRS, RemoteMeta, _KNOWN_DUALS

import collections
import time

_msg_to_name = collections.defaultdict(dict)
for name in dir(consts):
    if name.upper() == name:
        category, _ = name.split("_", 1)
        _msg_to_name[category][getattr(consts, name)] = name
_msg_to_name = dict(_msg_to_name)


def _batch_loads(items):
    return tuple(pickle.loads(item) for item in items)


def _tuplize(arg):
    """turns any sequence or iterator into a flat tuple"""
    return tuple(arg)


class WrappingConnection(rpyc.Connection):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._remote_batch_loads = None
        self._remote_cls_cache = {}
        self._static_cache = collections.defaultdict(dict)
        self._remote_dumps = None
        self._remote_tuplize = None

        self.logLock = threading.RLock()
        self.timings = {}
        self.inside = threading.local()
        self.inside.box = False
        self.inside.unbox = False
        with open("rpyc-trace.log", "a") as out:
            out.write(f"------------[new trace at {time.asctime()}]----------\n")
        self.logfiles = set(["rpyc-trace.log"])

    def _send(self, msg, seq, args):
        """tracing only"""
        str_args = str(args).replace("\r", "").replace("\n", "\tNEWLINE\t")
        if msg == consts.MSG_REQUEST:
            handler, _ = args
            str_handler = f":req={_msg_to_name['HANDLE'][handler]}"
        else:
            str_handler = ""
        with self.logLock:
            for logfile in self.logfiles:
                with open(logfile, "a") as out:
                    out.write(
                        f"send:msg={_msg_to_name['MSG'][msg]}:seq={seq}{str_handler}:args={str_args}\n"
                    )
        self.timings[seq] = time.time()
        return super()._send(msg, seq, args)

    def _dispatch(self, data):
        """tracing only"""
        got1 = time.time()
        try:
            return super()._dispatch(data)
        finally:
            got2 = time.time()
            msg, seq, args = brine.load(data)
            sent = self.timings.pop(seq, got1)
            if msg == consts.MSG_REQUEST:
                handler, args = args
                str_handler = f":req={_msg_to_name['HANDLE'][handler]}"
            else:
                str_handler = ""
            str_args = str(args).replace("\r", "").replace("\n", "\tNEWLINE\t")
            with self.logLock:
                for logfile in self.logfiles:
                    with open(logfile, "a") as out:
                        out.write(
                            f"recv:timing={got1 - sent}+{got2 - got1}:msg={_msg_to_name['MSG'][msg]}:seq={seq}{str_handler}:args={str_args}\n"
                        )

    def __wrap(self, local_obj):
        while True:
            # unwrap magic wrappers first
            try:
                local_obj = object.__getattribute__(local_obj, "__remote_end__")
            except AttributeError:
                break
        # do not pickle netrefs
        if isinstance(local_obj, netref.BaseNetref) and local_obj.____conn__ is self:
            return None
        return bytes(pickle.dumps(local_obj))

    def deliver(self, args, kw):
        """
        More efficient version of rpyc.classic.deliver() - delivers to remote side in batch
        """
        pickled_args = [self.__wrap(arg) for arg in args]
        pickled_kw = [(k, self.__wrap(v)) for (k, v) in kw.items()]

        pickled = [i for i in pickled_args if i is not None] + [
            v for (k, v) in pickled_kw if v is not None
        ]
        remote = iter(self._remote_batch_loads(tuple(pickled)))

        delivered_args = []
        for local_arg, pickled_arg in zip(args, pickled_args):
            delivered_args.append(
                next(remote) if pickled_arg is not None else local_arg
            )
        delivered_kw = {}
        for k, pickled_v in pickled_kw:
            delivered_kw[k] = next(remote) if pickled_v is not None else kw[k]

        return tuple(delivered_args), delivered_kw

    def obtain(self, remote):
        while True:
            try:
                remote = object.__getattribute__(remote, "__remote_end__")
            except AttributeError:
                break
        return pickle.loads(self._remote_dumps(remote))

    def obtain_tuple(self, remote):
        while True:
            try:
                remote = object.__getattribute__(remote, "__remote_end__")
            except AttributeError:
                break
        return self._remote_tuplize(remote)

    def sync_request(self, handler, *args):  # serving
        if handler == consts.HANDLE_INSPECT:
            id_name = str(args[0][0])
            if id_name.split(".", 1)[0] in ("modin", "pandas", "numpy"):
                try:
                    modobj = __import__(id_name)
                    for subname in id_name.split(".")[1:]:
                        modobj = getattr(modobj, subname)
                except (ImportError, AttributeError):
                    pass
                else:
                    return get_methods(netref.LOCAL_ATTRS, modobj)
                modname, clsname = id_name.rsplit(".", 1)
                try:
                    modobj = __import__(modname)
                    for subname in modname.split(".")[1:]:
                        modobj = getattr(modobj, subname)
                    clsobj = getattr(modobj, clsname)
                except (ImportError, AttributeError):
                    pass
                else:
                    # we might have been asked to inspect a class we're preparing a wrapper for
                    # _right now_! try unwrapping it first.
                    def _getfullname(o):
                        return object.__getattribute__(o, '__module__'), object.__getattribute__(o, '__name__')
                    while True:
                        parent = object.__getattribute__(clsobj, '__mro__')[1]
                        if _getfullname(clsobj) == _getfullname(parent):
                            clsobj = parent
                        else:
                            break
                    import inspect
                    def my_get_methods(obj_attrs, obj):
                        """introspects the given (local) object, returning a list of all of its
                        methods (going up the MRO).

                        :param obj: any local (not proxy) python object

                        :returns: a list of ``(method name, docstring)`` tuples of all the methods
                                of the given object
                        """
                        methods = {}
                        attrs = {}
                        if isinstance(obj, type):
                            # don't forget the darn metaclass
                            mros = list(reversed(type(obj).__mro__)) + list(reversed(obj.__mro__))
                        else:
                            mros = reversed(type(obj).__mro__)
                        for basecls in mros:
                            attrs.update(basecls.__dict__)
                        for name, attr in attrs.items():
                            if name not in obj_attrs and hasattr(attr, "__call__"):
                                try:
                                    methods[name] = "<removed>" #inspect.getdoc(attr)
                                except RecursionError:
                                    import traceback
                                    print(name)
                                    traceback.print_exc()
                                    import pdb;pdb.set_trace()
                                    raise
                        return methods.items()

                    return my_get_methods(netref.LOCAL_ATTRS, clsobj)
        elif handler in (consts.HANDLE_GETATTR, consts.HANDLE_STR, consts.HANDLE_HASH):
            if handler == consts.HANDLE_GETATTR:
                obj, attr = args
                key = (attr, handler)
            else:
                obj = args[0]
                key = handler

            if str(obj.____id_pack__[0]) in {"numpy", "numpy.dtype"}:
                cache = self._static_cache[obj.____id_pack__]
                try:
                    result = cache[key]
                except KeyError:
                    result = cache[key] = super().sync_request(handler, *args)
                    if handler == consts.HANDLE_GETATTR:
                        # save an entry in our cache telling that we get this attribute cached
                        self._static_cache[result.____id_pack__]["__getattr__"] = True
                return result

        return super().sync_request(handler, *args)

    def async_request(self, handler, *args, **kw):
        if handler == consts.HANDLE_DEL:
            obj, _ = args
            if str(obj.____id_pack__[0]) in {"numpy", "numpy.dtype"}:
                if obj.____id_pack__ in self._static_cache:
                    # object is cached by us, so ignore the request or remote end dies and cache is suddenly stale;
                    # we shouldn't remove item from cache as it would reduce performance
                    res = AsyncResult(self)
                    res._is_ready = True  # simulate finished async request
                    return res
        return super().async_request(handler, *args, **kw)

    def _netref_factory(self, id_pack):
        id_name, cls_id, inst_id = id_pack
        id_name = str(id_name)
        first = id_name.split(".", 1)[0]
        if first in ("modin", "numpy", "pandas") and inst_id:
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
        while True:
            try:
                obj = object.__getattribute__(obj, "__remote_end__")
            except AttributeError:
                break
        return super()._box(obj)

    class _Logger:
        def __init__(self, conn, logname):
            self.conn = conn
            self.logname = logname

        def __enter__(self):
            with self.conn.logLock:
                self.conn.logfiles.add(self.logname)
                with open(self.logname, "a") as out:
                    out.write(
                        f"------------[new trace at {time.asctime()}]----------\n"
                    )
            return self

        def __exit__(self, *a, **kw):
            with self.conn.logLock:
                self.conn.logfiles.remove(self.logname)

    def _logmore(self, logname):
        return self._Logger(self, logname)

    def _init_deliver(self):
        self._remote_batch_loads = self.modules[
            "modin.experimental.cloud.rpyc_proxy"
        ]._batch_loads
        self._remote_dumps = self.modules["rpyc.lib.compat"].pickle.dumps
        self._remote_tuplize = self.modules[
            "modin.experimental.cloud.rpyc_proxy"
        ]._tuplize


class WrappingService(rpyc.ClassicService):
    _protocol = WrappingConnection

    def on_connect(self, conn):
        super().on_connect(conn)
        conn._init_deliver()


def _in_empty_class():
    class Empty:
        pass

    return frozenset(Empty.__dict__.keys())


_EMPTY_CLASS_ATTRS = _in_empty_class()

_PROXY_LOCAL_ATTRS = frozenset(["__name__", "__remote_end__"])
_NO_OVERRIDE = (
    _PROXY_LOCAL_ATTRS
    | _LOCAL_ATTRS
    | rpyc.core.netref.DELETED_ATTRS
    | frozenset(["__getattribute__"])
    | _EMPTY_CLASS_ATTRS
)


def make_proxy_cls(remote_cls, origin_cls, override, cls_name=None):
    class ProxyMeta(RemoteMeta):
        def __repr__(self):
            return f"<proxy for {origin_cls.__module__}.{origin_cls.__name__}:{cls_name or origin_cls.__name__}>"

        def __prepare__(*args, **kw):
            namespace = type.__prepare__(*args, **kw)
            namespace["__remote_methods__"] = {}

            # try computing overridden differently to allow subclassing one override from another
            no_override = set(_NO_OVERRIDE)
            for base in override.__mro__:
                if base == object:
                    continue
                for attr_name, attr_value in base.__dict__.items():
                    if (
                        attr_name not in namespace
                        and attr_name not in no_override
                        and getattr(object, attr_name, None) != attr_value
                    ):
                        namespace[
                            attr_name
                        ] = attr_value  # force-inherit an attribute manually
                        no_override.add(attr_name)

            for base in origin_cls.__mro__:
                if base == object:
                    continue
                # try unwrapping a dual-nature class first
                while True:
                    try:
                        sub_base = object.__getattribute__(base, "__real_cls__")
                    except AttributeError:
                        break
                    if sub_base is base:
                        break
                    base = sub_base
                for name, entry in base.__dict__.items():
                    if (
                        name not in namespace
                        and name not in no_override
                        and isinstance(entry, types.FunctionType)
                    ):

                        def method(_self, *_args, __method_name__=name, **_kw):
                            try:
                                remote = _self.__remote_methods__[__method_name__]
                            except KeyError:
                                # use remote_cls.__getattr__ to force RPyC return us
                                # a proxy for remote method call instead of its local wrapper
                                _self.__remote_methods__[
                                    __method_name__
                                ] = remote = remote_cls.__getattr__(__method_name__)
                            return remote(_self.__remote_end__, *_args, **_kw)

                        method.__name__ = name
                        namespace[name] = method
            return namespace

    class Wrapper(override, origin_cls, metaclass=ProxyMeta):
        __name__ = cls_name or origin_cls.__name__
        __wrapper_remote__ = remote_cls

        def __new__(cls, *a, **kw):
            return override.__new__(cls)#, *a, **kw)

        def __init__(self, *a, __remote_end__=None, **kw):
            if __remote_end__ is None:
                __remote_end__ = remote_cls(*a, **kw)
            while True:
                # unwrap the object if it's a wrapper
                try:
                    __remote_end__ = object.__getattribute__(
                        __remote_end__, "__remote_end__"
                    )
                except AttributeError:
                    break
            object.__setattr__(self, "__remote_end__", __remote_end__)

        @classmethod
        def from_remote_end(cls, remote_inst):
            return cls(__remote_end__=remote_inst)

        def __getattribute__(self, name):
            """
            Implement "default" resolution order to override whatever __getattribute__
            a parent being wrapped may have defined, but only look up on own __dict__
            without looking into ancestors' ones, because we copy them in __prepare__.

            Effectively, any attributes not currently known to Wrapper (i.e. not defined here
            or in override class) will be retrieved from the remote end.

            Algorithm (mimicking default Python behaviour):
            1) check if type(self).__dict__[name] exists and is a get/set data descriptor
            2) check if self.__dict__[name] exists
            3) check if type(self).__dict__[name] is a non-data descriptor
            4) check if type(self).__dict__[name] exists
            5) pass through to remote end
            """
            dct = object.__getattribute__(self, '__dict__')
            if name == '__dict__':
                return dct
            cls_dct = object.__getattribute__(type(self), '__dict__')
            try:
                cls_attr, has_cls_attr = cls_dct[name], True
            except KeyError:
                has_cls_attr = False
            else:
                oget = None
                try:
                    oget = object.__getattribute__(cls_attr, '__get__')
                    object.__getattribute__(cls_attr, '__set__')
                except AttributeError:
                    pass # not a get/set data descriptor, go next
                else:
                    return oget(self, type(self))
            # type(self).name is not a get/set data descriptor
            try:
                return dct[name]
            except KeyError:
                # instance doesn't have an attribute
                if has_cls_attr:
                    # type(self) has this attribute, but it's not a get/set descriptor
                    if oget:
                        # this attribute is a get data descriptor
                        return oget(self, type(self))
                    return cls_attr # not a data descriptor whatsoever

            # this instance/class does not have this attribute, pass it through to remote end
            return getattr(dct['__remote_end__'], name)

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

    return Wrapper


def _deliveringWrapper(origin_cls, methods=(), mixin=None, target_name=None):
    conn = get_connection()
    remote_cls = getattr(conn.modules[origin_cls.__module__], origin_cls.__name__)

    if mixin is None:

        class DeliveringMixin:
            pass

        mixin = DeliveringMixin

    for method in methods:

        def wrapper(self, *args, __remote_conn__=conn, __method_name__=method, **kw):
            args, kw = __remote_conn__.deliver(args, kw)
            cache = object.__getattribute__(self, "__remote_methods__")
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


def make_dataframe_wrapper(DataFrame):
    from modin.pandas.series import Series

    conn = get_connection()

    class ObtainingItems:
        def items(self):
            return conn.obtain_tuple(self.__remote_end__.items())

        def iteritems(self):
            return conn.obtain_tuple(self.__remote_end__.iteritems())

    ObtainingItems = _deliveringWrapper(Series, mixin=ObtainingItems)

    class DataFrameOverrides(_prepare_loc_mixin()):
        @property
        def dtypes(self):
            remote_dtypes = self.__remote_end__.dtypes
            return ObtainingItems(__remote_end__=remote_dtypes)

    DeliveringDataFrame = _deliveringWrapper(
        DataFrame,
        ["groupby", "agg", "aggregate", "__getitem__", "astype", "drop", "merge"],
        DataFrameOverrides,
        "DataFrame",
    )
    return DeliveringDataFrame


def make_base_dataset_wrapper(BasePandasDataset):
    DeliveringBasePandasDataset = _deliveringWrapper(
        BasePandasDataset,
        ["agg", "aggregate"],
        _prepare_loc_mixin(),
        "BasePandasDataset",
    )
    return DeliveringBasePandasDataset


def make_dataframe_groupby_wrapper(DataFrameGroupBy):
    DeliveringDataFrameGroupBy = _deliveringWrapper(
        DataFrameGroupBy, ["agg", "aggregate", "apply"], target_name="DataFrameGroupBy",
    )
    return DeliveringDataFrameGroupBy


def make_series_wrapper(Series):
    return _deliveringWrapper(Series, target_name="Series")

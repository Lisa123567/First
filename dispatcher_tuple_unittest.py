from tonat.greenlet_management import CompoundException
from tonat.dispatcher_tuple import (dispatcher_tuple, _DispatcherTuple,
                                    _CallableDispatcherTuple)

__author__ = 'rsolash'


def test_get_correct_tuple():
    class O(object):
        pass

    ll = dispatcher_tuple([O, O])
    assert type(ll) == _CallableDispatcherTuple
    ll = dispatcher_tuple([O(), O()])
    assert type(ll) == _DispatcherTuple


def test_multi_level():
    class O(object):
        def __init__(self, i):
            self.p = dispatcher_tuple([P(i), P(i+1)])
            self. ii = 5

        def ooo(self):
            return 'ooo'

    class P(object):
        def __init__(self, i):
            self.i = i

        def ppp(self, a):
            return 'ppp {0}'.format(a)

    ll = dispatcher_tuple([O(0), O(2), O(4)])
    assert ll.ii == (5, 5, 5)
    assert ll.ooo() == ('ooo', 'ooo', 'ooo')
    assert ll.p.i == ((0, 1), (2, 3), (4, 5))
    assert ll.p.ppp(1) == (('ppp 1', 'ppp 1'), ('ppp 1', 'ppp 1'),
                           ('ppp 1', 'ppp 1'))


def test_inner_exception():
    class MyExcp(Exception):
        pass

    class O(object):
        def foo(self):
            raise MyExcp('MyExcp')

    class P(object):
        def foo(self):
            return 'foo'
    ll = dispatcher_tuple([O(), P(), O()])
    try:
        ll.foo()
        assert False
    except CompoundException as e:
        assert e.routines[1].get() == 'foo'
        try:
            e.routines[0].get()
            assert False
        except MyExcp:
            pass


def test_slicing():
    ll = dispatcher_tuple([1, 2])
    assert ll[1:] == (2,)


def test_multi_level_del():
    class O(object):
        def __init__(self, i):
            self.p = dispatcher_tuple([P(i), P(i+1)])
            self. ii = 5

        def ooo(self):
            return 'ooo'

    class P(object):
        def __init__(self, i):
            self.i = i

        def ppp(self, a):
            return 'ppp {0}'.format(a)

    ll = dispatcher_tuple([O(0), O(2), O(4)])
    for lobject in ll:
        for pobject in lobject.p:
            assert hasattr(pobject, 'i') == True
    del ll.p.i
    for lobject in ll:
        for pobject in lobject.p:
            assert hasattr(pobject, 'i') == False


def test_multi_level_set():
    class O(object):
        def __init__(self, i):
            self.p = dispatcher_tuple([P(i), P(i+1)])
            self. ii = 5

        def ooo(self):
            return 'ooo'

    class P(object):
        def __init__(self, i):
            self.i = i

        def ppp(self, a):
            return 'ppp {0}'.format(a)

    ll = dispatcher_tuple([O(0), O(2), O(4)])
    ll.p.yyy = 5
    assert ll.p.yyy == ((5, 5), (5, 5), (5, 5))


if __name__ == '__main__':
    test_get_correct_tuple()
    test_multi_level()
    test_inner_exception()
    test_slicing()
    test_multi_level_del()
    test_multi_level_set()

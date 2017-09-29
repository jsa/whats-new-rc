from collections import namedtuple

store_info = namedtuple('StoreInfo', ('id', 'title', 'url'))


def get_stores():
    from . import hk
    return {hk._store.id: hk._store}

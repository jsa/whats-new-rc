from datetime import datetime
from decimal import Decimal
import logging
import os
import re

from google.appengine.api import taskqueue, urlfetch
from google.appengine.ext import deferred, ndb

from HTMLParser import HTMLParser
import webapp2

from . import store_info
from .models import Category, Item, PAGE_TYPE, Price, ScrapeQueue, Store
from .search import index_items
from .util import cacheize, get, nub, ok_resp


_store = store_info('hk', "HobbyKing")

href = re.compile(r'href="(.+?)"')
itemprop = re.compile(r'itemprop="(.+?)" content="(.+?)"')
ogprop = re.compile(r'property="og:(.+?)" content="(.+?)"')


def trigger(rq):
    deferred.defer(queue_categories,
                   rescan='rescan' in rq.GET,
                   _queue='scrape')
    return webapp2.Response()


def queue_categories(rescan=False):
    assert ScrapeQueue.initialize(_store.id, skip_indexed=not rescan), \
        "Previous crawl still in progress"

    rs = ok_resp(urlfetch.fetch("https://hobbyking.com/en_us",
                                deadline=60))

    m = re.search(r'class="mb_new_pro"><a href="(.+?)"', rs.content)
    assert m, "New items URL not found"
    urls = [m.group(1)]

    nav = rs.content.split('id="nav"', 1)[1] \
                    .split("</nav>", 1)[0]
    urls += nub(href.findall(nav))
    assert len(urls) > 100, "Found only %d category URLs" % len(urls)

    logging.debug("Found %d categories" % len(urls))
    ScrapeQueue.queue(_store.id, categories=urls)
    deferred.defer(process_queue, _queue='scrape', _countdown=2)


def cookie_value(cookies):
    return "; ".join("%s=%s" % (cookie.key, cookie.coded_value)
                     for cookie in cookies.itervalues())


def process_queue():
    queue_url, url_type, cookies = ScrapeQueue.peek(_store.id)
    if not queue_url:
        return

    retries = int(os.getenv('HTTP_X_APPENGINE_TASKRETRYCOUNT', 0))

    @ndb.transactional
    def set_removed(keys):
        now, mod = datetime.utcnow(), []
        for obj in ndb.get_multi(keys):
            if not obj.removed:
                obj.removed = now
                mod.append(obj)
        if mod:
            ndb.put_multi(mod)
            logging.warn("Flagged removed: %r" % [o.key for o in mod])

    url, headers = queue_url, {}
    logging.info("Scraping %r" % url)

    while True:
        if cookies:
            headers['Cookie'] = cookie_value(cookies)

        rs = urlfetch.fetch(url,
                            headers=headers,
                            follow_redirects=False,
                            deadline=20)

        cookie = rs.headers.get('Set-Cookie')
        if cookie:
            cookies.load(cookie)
            ScrapeQueue.save_cookies(_store.id, cookies)

        content = rs.content.decode('utf-8')

        if url_type == PAGE_TYPE.ITEM:
            if rs.status_code == 200:
                scrape_item(url, content)
                break
            elif rs.status_code in (301, 302):
                redir = rs.headers['Location']
                assert redir == url[:-1], \
                    "%d for %s: %s" % (rs.status_code, url, redir)
                url = url[:-1]
            elif rs.status_code == 404 and retries > 3:
                keys = Item.query(Item.url == url) \
                           .fetch(keys_only=True)
                set_removed(keys)
            else:
                raise taskqueue.TransientError(
                          "%d for %s\nBody:\n%s\n\nHeaders:\n%r"
                          % (rs.status_code,
                             url,
                             content.encode('ascii', 'xmlcharrefreplace')[:2000],
                             rs.headers))

        elif url_type == PAGE_TYPE.CATEGORY:
            if rs.status_code == 200:
                scrape_category(url, content)
                break
            elif rs.status_code in (301, 302):
                redir = rs.headers['Location']
                logging.warn("Category redirect %s -> %s" % (url, redir))
                url = redir
            elif rs.status_code == 404 and retries > 3:
                keys = Category.query(Category.url == url) \
                               .fetch(keys_only=True)
                set_removed(keys)
            else:
                raise taskqueue.TransientError(
                          "%d for %s" % (rs.status_code, url))

        else:
            raise ValueError("Unknown URL type %r" % (url_type,))

    ScrapeQueue.pop(_store.id, queue_url)
    deferred.defer(process_queue, _queue='scrape', _countdown=2)


def scrape_category(url, html):
    items = html.split('id="list-item-')[1:]
    item_urls = [href.search(item).group(1) for item in items]
    logging.info("Found %d items" % len(item_urls))

    cat_urls = []

    npage = re.search(r'href="([^"]+)" title="Next"', html)
    if npage:
        cat_urls.append(npage.group(1))
        logging.debug("Queuing next page %s" % (cat_urls,))

    sub_cats = html.split('class="popularBrands', 1)
    if len(sub_cats) > 1:
        sub_cats = sub_cats[1].rsplit('class="brandImage', 1)
        assert len(sub_cats) == 2
        sub_cats = href.findall(sub_cats[0])
        logging.debug("Found %d sub-categories:\n%s"
                      % (len(sub_cats), "\n".join(sub_cats)))
        cat_urls += sub_cats

    ScrapeQueue.queue(_store.id, categories=cat_urls, items=item_urls)


@cacheize(60 * 60)
def children(cat_key):
    # store filter is needed for querying root cats (where parent is None)
    q = Category.query(Category.store == _store.id,
                       Category.parent_cat == cat_key)
    child_cats = q.fetch()
    return {c.title: (c.key, c.url) for c in child_cats}


def save_cats(path):
    ckeys = []
    for url, title in path:
        parent = ckeys[-1] if ckeys else None
        struct = children(parent).get(title)
        if struct:
            cat_key, _url = struct
            if _url != url:
                cat = cat_key.get()
                cat.url = url
                cat.put()
                children(parent, _invalidate=True)
        else:
            cat = Category(store=_store.id,
                           title=title,
                           url=url,
                           parent_cat=parent)
            cat_key = cat.put()
            children(parent, _invalidate=True)
        ckeys.append(cat_key)
    return ckeys


def scrape_item(url, html):
    h = HTMLParser()

    props = dict(itemprop.findall(html))
    og = dict(ogprop.findall(html))

    logging.debug("itemprop: %r\nog:%r" % (props, og))

    sku = props.get('sku')
    assert sku, "Couldn't find SKU"

    cur = props.get('priceCurrency')
    if cur:
        price = cur, int(Decimal(props['price']) * 100)
    else:
        g_params = re.search(r'google_tag_params = *{(.*?)}', html, re.DOTALL)
        assert g_params
        usd = re.search(r"value: '(.+?)'", g_params.group(1)).group(1)
        logging.debug("google_tag_params: %s, usd: %s"
                      % (g_params.group(1), usd))
        price = 'USD', int(Decimal(usd) * 100)

    if not price[1] > 0:
        logging.warn("Failed to find an appropriate price: %r" % (price,))
        price = None

    image, title, typ, _url = map(og.get, ('image', 'title', 'type', 'url'))
    assert typ == "product", "Unexpected type %r" % typ
    assert _url == url, "Item URL mismatch: %s != %s" \
                        % (url, _url)
    assert image

    def img_alt():
        title = re.search(r'itemprop="image"[^>]+alt="(.+?)"', html, re.DOTALL)
        if title:
            return h.unescape(title.group(1)).strip()

    # title isn't encoded properly in og props; priorize alternate source
    title = img_alt() or title
    assert title, "Failed to parse title"

    fields = {'image': image,
              'title': title,
              'url': url,
              'removed': None}

    cat_html = html.split('class="breadcrumbsPos"', 1)[1] \
                   .rsplit('class="breadcrumbsPos"', 1)
    if len(cat_html) == 2:
        cats = re.findall(r'<a href="(.+?)".*?><.+?>(.+?)</', cat_html[0])
        assert cats
        assert len(cats) < 10 \
               and not any("<" in name for url, name in cats), \
            "Category scraping probably failed:\n%s" % (cats,)
        cats = [(url, h.unescape(name).strip()) for url, name in cats]
        logging.debug("Parsed categories:\n%s"
                      % "\n".join("%s (%s)" % (name, url)
                                  for url, name in cats))
        cat_keys = save_cats(cats)
    else:
        assert len(cat_html) == 1
        logging.warn("Couldn't find any categories")
        cat_keys = []

    fields['category'] = cat_keys[-1] if cat_keys else None

    def prod_ids():
        for prod_id in re.findall(r"product_value = (\d+);", html):
            try:
                yield int(prod_id)
            except ValueError as e:
                logging.warn(e, exc_info=True)

    pids = set(prod_ids())
    if len(pids) == 1:
        fields['custom'] = {'hk-id': pids.pop()}
    else:
        logging.warn("Found %d product IDs: %r" % (len(pids), pids))

    logging.debug("Parsed item data:\n%s"
                  % "\n".join("%s: %s" % i
                              for i in sorted(fields.iteritems())))

    key = ndb.Key(Store, _store.id, Item, sku)
    item = key.get()
    if item:
        item.populate(**fields)
        puts = [item]
        if price:
            p = Price.query(ancestor=item.key) \
                     .order(-Price.timestamp) \
                     .get()
            if (p.currency, p.cents) != price:
                puts.append(Price(parent=item.key,
                                  currency=price[0],
                                  cents=price[1]))
            keys = ndb.put_multi(puts)
            logging.debug("Updated %r" % (keys,))
    else:
        item = Item(key=key, **fields)
        puts = [item]
        if price:
            puts.append(Price(parent=item.key,
                              currency=price[0],
                              cents=price[1]))
        keys = ndb.put_multi(puts)
        logging.debug("Added %r" % (keys,))

    deferred.defer(index_items,
                   [item.key],
                   _queue='indexing',
                   _countdown=2)


def proxy(rq):
    queue = ndb.Key(ScrapeQueue, _store.id).get()
    headers = {}
    if queue:
        cookies = queue.get_cookies()
        if cookies:
            headers['Cookie'] = cookie_value(cookies)

    rs = urlfetch.fetch(rq.GET['url'],
                        headers=headers,
                        follow_redirects=False,
                        deadline=60)
    cookie = rs.headers.get('Set-Cookie')
    if cookie:
        cookies.load(cookie)
        ScrapeQueue.save_cookies(_store.id, cookies)

    return webapp2.Response(rs.content, rs.status_code)


routes = [
    get(r"/proxy.html", proxy),
    get(r"/scrape", trigger),
]

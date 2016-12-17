from decimal import Decimal
import logging
import re

from google.appengine.api import taskqueue, urlfetch
from google.appengine.ext import deferred, ndb

from HTMLParser import HTMLParser
import webapp2

from ..models import Category, Item, PAGE_TYPE, Price, ScrapeQueue, Store
from ..search import index_items
from ..util import get, nub, ok_resp


_store = 'hk'

href = re.compile(r'href="(.+?)"')
itemprop = re.compile(r'itemprop="(.+?)" content="(.+?)"')
ogprop = re.compile(r'property="og:(.+?)" content="(.+?)"')


def trigger(rq):
    deferred.defer(queue_categories, _queue='scrape')
    return webapp2.Response()


def queue_categories():
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
    ScrapeQueue.queue(_store, categories=urls)
    deferred.defer(process_queue, _queue='scrape', _countdown=5)


def process_queue():
    url, url_type = ScrapeQueue.peek(_store)
    if not url:
        return

    logging.info("Scraping %r" % url)
    rs = urlfetch.fetch(url, follow_redirects=False, deadline=60)
    if rs.status_code == 200:
        if url_type == PAGE_TYPE.CATEGORY:
            scrape_category(rs.content)
        else:
            assert url_type == PAGE_TYPE.ITEM
            scrape_item(rs.content)
    elif rs.status_code in (301, 302, 404):
        logging.error("%d for %s, skipping" % (rs.status_code, url))
    else:
        raise taskqueue.TransientError("%d for %s" % (rs.status_code, url))

    ScrapeQueue.pop(_store, url)
    deferred.defer(process_queue, _queue='scrape', _countdown=5)


def scrape_category(html):
    items = html.split('id="list-item-')[1:]
    item_urls = [href.search(item).group(1) for item in items]
    logging.info("Found %d items" % len(item_urls))

    cat_urls = []
    npage = re.search(r'href="([^"]+)" title="Next"', html)
    if npage:
        cat_urls.append(npage.group(1))
        logging.debug("Queuing next page %s" % (cat_urls,))

    ScrapeQueue.queue(_store, categories=cat_urls, items=item_urls)


def save_cats(data):
    ckeys = []
    for url, title in data:
        cat = Category.query(Category.store == _store,
                             Category.title == title) \
                      .get()
        parent = ckeys[-1] if ckeys else None
        if cat:
            if not parent:
                # don't remove earlier parent
                parent = cat.parent_cat
            if (cat.url, cat.parent_cat) != (url, parent):
                cat.populate(url=url, parent_cat=parent)
                cat.put()
        else:
            cat = Category(store=_store,
                           title=title,
                           url=url,
                           parent_cat=parent)
            cat.put()
        ckeys.append(cat.key)
    return ckeys


def scrape_item(html):
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

    image, title, typ, url = map(og.get, ('image', 'title', 'type', 'url'))
    assert typ == "product", "Unexpected type %r" % typ
    assert all((image, title, url))

    fields = {'image': image,
              'title': title,
              'url': url,
              'removed': None}

    cat_html = html.split('class="breadcrumbsPos"', 1)[1] \
                   .rsplit('class="breadcrumbsPos"', 1)[0]
    cat_el = re.compile(r'<a href="(.+?)".*?><.+?>(.+?)</')
    cats = [(url, h.unescape(name))
            for url, name in cat_el.findall(cat_html)]
    assert cats
    logging.debug("Parsed categories:\n%s"
                  % "\n".join("%s (%s)" % (name, url)
                              for url, name in cats))
    cat_keys = save_cats(cats)

    fields['category'] = cat_keys[-1]

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

    key = ndb.Key(Store, _store, Item, sku)
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
    rs = urlfetch.fetch(rq.GET['url'], deadline=60)
    return webapp2.Response(rs.content)


routes = [
    get(r"/proxy.html", proxy),
    get(r"/scrape", trigger),
]

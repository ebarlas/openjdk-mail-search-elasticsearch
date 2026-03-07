"""API server for OpenJDK Mail Search, backed by Elasticsearch.

Implements the OpenJDK Mail Search API spec (see openapi.yaml in the
original project), replacing the DynamoDB-backed implementation with
Elasticsearch queries.

Lambda usage:
    Handler: server.lambda_handler
    Environment: ES_URL (required, e.g. https://elastic:pass@host:9200),
                 INDEX_NAME (optional)
"""

import base64
import calendar
import json
import logging
import os
import re
import ssl
import urllib.parse
from typing import NamedTuple
from urllib.parse import urlparse, urlunparse
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

INDEX_NAME = 'openjdk-mail'
CHECKPOINT_INDEX = 'openjdk-mail-checkpoints'

_es_ssl_ctx = ssl.create_default_context()
_es_ssl_ctx.check_hostname = False
_es_ssl_ctx.verify_mode = ssl.CERT_NONE

_es_auth_header = None


def _init_es_auth(es_url):
    """Extract credentials from ES URL, set auth header, return clean URL."""
    global _es_auth_header
    parsed = urlparse(es_url)
    if parsed.username:
        credentials = base64.b64encode(
            f"{parsed.username}:{parsed.password or ''}".encode()
        ).decode()
        _es_auth_header = f"Basic {credentials}"
        netloc = parsed.hostname
        if parsed.port:
            netloc = f"{netloc}:{parsed.port}"
        return urlunparse(parsed._replace(netloc=netloc))
    return es_url


def _es_urlopen(req, **kwargs):
    """urlopen wrapper that adds ES auth and skips TLS verification."""
    if _es_auth_header:
        req.add_header("Authorization", _es_auth_header)
    return urlopen(req, context=_es_ssl_ctx, **kwargs)


class CommonParams(NamedTuple):
    forward: bool
    limit: int
    search_after: object  # list or None
    date_range: object  # tuple(str, str) or None


class ApiRequest(NamedTuple):
    method: str
    uri: str
    query: str
    params: dict[str, list[str]]

    def uri_with_query(self):
        return f'{self.uri}?{self.query}' if self.query else self.uri

    @staticmethod
    def from_event(event):
        request = event['Records'][0]['cf']['request']
        qs = request.get('querystring', '')
        return ApiRequest(
            method=request['method'],
            uri=request['uri'],
            query=qs,
            params=urllib.parse.parse_qs(qs),
        )


# --- Helpers ---

def _b64e(val):
    return base64.urlsafe_b64encode(
        json.dumps(val, separators=(',', ':')).encode()
    ).decode('ascii')


def _b64d(s):
    return json.loads(base64.urlsafe_b64decode(s.encode('ascii')))


def _to_json(val):
    return json.dumps(val, separators=(',', ':'))


def extract_param(params, name, default=None, func=None):
    if name not in params:
        return default
    val = params[name][0]
    if not func:
        return val if val else default
    try:
        return func(val)
    except Exception:
        return default


def common_params(params):
    forward = extract_param(params, 'order', False, lambda p: p == 'asc')
    limit = max(1, min(100, extract_param(params, 'limit', 10, int)))
    search_after = extract_param(params, 'cursor', None, _b64d)
    from_date = extract_param(params, 'from')
    to_date = extract_param(params, 'to')
    date_range = (from_date, to_date) if from_date and to_date else None
    return CommonParams(
        forward=forward, limit=limit,
        search_after=search_after, date_range=date_range,
    )


def json_response(body_str):
    return {
        'status': '200',
        'statusDescription': 'OK',
        'headers': {
            'content-type': [{'key': 'Content-Type', 'value': 'application/json'}],
        },
        'body': body_str,
    }


def not_found():
    return {
        'status': '404',
        'statusDescription': 'Not Found',
        'body': 'Not Found',
    }


def response_body(items, cursor):
    res = {'items': items}
    if cursor is not None:
        res['cursor'] = _b64e(cursor)
    return _to_json(res)


# --- Elasticsearch queries ---

def _es_search(es_url, index_name, body):
    req = Request(
        f'{es_url}/{index_name}/_search',
        data=json.dumps(body).encode(),
        headers={'Content-Type': 'application/json', 'User-Agent': 'openjdk-mail-search'},
    )
    with _es_urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def _build_search(query, cp):
    order = 'asc' if cp.forward else 'desc'
    body = {
        'size': cp.limit,
        'query': query,
        'sort': [{'date': order}, {'message_id': order}],
        '_source': ['list', 'message_id', 'date', 'author', 'email', 'subject'],
    }
    if cp.search_after:
        body['search_after'] = cp.search_after
    return body


def _filters(list_name=None, date_range=None):
    f = []
    if list_name:
        f.append({'term': {'list': list_name}})
    if date_range:
        start, end = date_range
        f.append({'range': {'date': {'gte': start, 'lte': end}}})
    return f


def search_mail(es_url, index_name, query_text, cp, list_name=None):
    filters = _filters(list_name, cp.date_range)
    filters.append({'multi_match': {
        'query': query_text,
        'fields': ['subject', 'body'],
    }})
    q = {'bool': {'filter': filters}}
    return _es_search(es_url, index_name, _build_search(q, cp))


def latest_mail(es_url, index_name, cp, list_name=None):
    filters = _filters(list_name, cp.date_range)
    if filters:
        q = {'bool': {'filter': filters}}
    else:
        q = {'match_all': {}}
    return _es_search(es_url, index_name, _build_search(q, cp))


def mail_by_author(es_url, index_name, author, cp, list_name=None):
    filters = _filters(list_name, cp.date_range)
    filters.append({'match': {'author': {'query': author, 'operator': 'and'}}})
    q = {'bool': {'filter': filters}}
    return _es_search(es_url, index_name, _build_search(q, cp))


def mail_by_email(es_url, index_name, email_addr, cp, list_name=None):
    filters = _filters(list_name, cp.date_range)
    filters.append({'term': {'email': email_addr.lower()}})
    q = {'bool': {'filter': filters}}
    return _es_search(es_url, index_name, _build_search(q, cp))


def relevance_search(es_url, index_name, query_text, limit, page,
                     list_name=None, date_range=None):
    """Scored full-text search with subject boosting and highlighting."""
    filters = _filters(list_name, date_range)
    query = {
        'bool': {
            'must': [{
                'multi_match': {
                    'query': query_text,
                    'fields': ['subject^3', 'body'],
                    'type': 'best_fields',
                }
            }],
        }
    }
    if filters:
        query['bool']['filter'] = filters
    body = {
        'size': limit,
        'from': (page - 1) * limit,
        'track_total_hits': True,
        'query': query,
        'sort': [{'_score': 'desc'}, {'date': 'desc'}],
        'highlight': {
            'fields': {
                'subject': {'number_of_fragments': 0},
                'body': {'fragment_size': 150, 'number_of_fragments': 3},
            },
            'pre_tags': ['<em>'],
            'post_tags': ['</em>'],
        },
        '_source': ['list', 'message_id', 'date', 'author', 'email', 'subject'],
    }
    return _es_search(es_url, index_name, body)


def get_status(es_url, checkpoint_index):
    body = {
        'size': 0,
        'aggs': {
            'last_sync': {'max': {'field': 'synced_at'}},
            'last_update': {'max': {'field': 'updated_at'}},
        },
    }
    req = Request(
        f'{es_url}/{checkpoint_index}/_search',
        data=json.dumps(body).encode(),
        headers={'Content-Type': 'application/json', 'User-Agent': 'openjdk-mail-search'},
    )
    with _es_urlopen(req, timeout=15) as resp:
        result = json.loads(resp.read())
    last_check = result['aggregations']['last_sync'].get('value_as_string')
    last_update = result['aggregations']['last_update'].get('value_as_string') or last_check
    return last_check, last_update


# --- Response conversion ---

def _format_month(date_str):
    """Derive month string like '2025-August' from an ISO date."""
    try:
        return f"{date_str[:4]}-{calendar.month_name[int(date_str[5:7])]}"
    except (ValueError, IndexError, TypeError):
        return ''


def convert_hit(hit):
    src = hit['_source']
    date = src.get('date', '')
    return {
        'list': src.get('list', ''),
        'month': _format_month(date),
        'id': src.get('message_id', hit['_id']),
        'date': date,
        'author': src.get('author', ''),
        'email': src.get('email', ''),
        'subject': src.get('subject', ''),
    }


def convert_hits(result, limit):
    hits = result['hits']['hits']
    items = [convert_hit(h) for h in hits]
    cursor = None
    if len(hits) == limit:
        cursor = hits[-1].get('sort')
    return items, cursor


def convert_relevance_hit(hit):
    item = convert_hit(hit)
    item['score'] = hit.get('_score')
    highlights = hit.get('highlight', {})
    if highlights:
        item['highlights'] = {k: v for k, v in highlights.items()}
    return item


def convert_relevance_results(result, page, page_size):
    hits = result['hits']['hits']
    total_info = result['hits'].get('total', {})
    total = total_info.get('value', 0) if isinstance(total_info, dict) else total_info
    items = [convert_relevance_hit(h) for h in hits]
    return {
        'total': total,
        'page': page,
        'page_size': page_size,
        'items': items,
    }


# --- Lambda handler ---

def lambda_handler(event, context):
    logging.getLogger().setLevel(logging.INFO)
    es_url = _init_es_auth(os.environ['ES_URL'])
    index_name = os.environ.get('INDEX_NAME', INDEX_NAME)
    checkpoint_index = os.environ.get('CHECKPOINT_INDEX', CHECKPOINT_INDEX)

    r = ApiRequest.from_event(event)
    logger.info('method=%s, path=%s', r.method, r.uri_with_query())

    if r.method != 'GET':
        return not_found()

    cp = common_params(r.params)
    page = max(1, extract_param(r.params, 'page', 1, int))

    if (m := re.match(r'.*/lists/([^/]+)/mail/search/relevance$', r.uri)) and 'q' in r.params:
        list_name = m.group(1)
        query_text = extract_param(r.params, 'q')
        result = relevance_search(es_url, index_name, query_text, cp.limit, page,
                                  list_name=list_name, date_range=cp.date_range)
        return json_response(_to_json(convert_relevance_results(result, page, cp.limit)))

    if r.uri.endswith('/mail/search/relevance') and 'q' in r.params:
        query_text = extract_param(r.params, 'q')
        result = relevance_search(es_url, index_name, query_text, cp.limit, page,
                                  date_range=cp.date_range)
        return json_response(_to_json(convert_relevance_results(result, page, cp.limit)))

    if (m := re.match(r'.*/lists/([^/]+)/mail/search$', r.uri)) and 'q' in r.params:
        list_name = m.group(1)
        query_text = extract_param(r.params, 'q')
        result = search_mail(es_url, index_name, query_text, cp, list_name=list_name)
        items, cursor = convert_hits(result, cp.limit)
        return json_response(response_body(items, cursor))

    if r.uri.endswith('/mail/search') and 'q' in r.params:
        query_text = extract_param(r.params, 'q')
        result = search_mail(es_url, index_name, query_text, cp)
        items, cursor = convert_hits(result, cp.limit)
        return json_response(response_body(items, cursor))

    if (m := re.match(r'.*/lists/([^/]+)/mail$', r.uri)):
        list_name = m.group(1)
        result = latest_mail(es_url, index_name, cp, list_name=list_name)
        items, cursor = convert_hits(result, cp.limit)
        return json_response(response_body(items, cursor))

    if (m := re.match(r'.*/lists/([^/]+)/mail/byauthor$', r.uri)) and 'author' in r.params:
        list_name = m.group(1)
        author = extract_param(r.params, 'author')
        result = mail_by_author(es_url, index_name, author, cp, list_name=list_name)
        items, cursor = convert_hits(result, cp.limit)
        return json_response(response_body(items, cursor))

    if (m := re.match(r'.*/lists/([^/]+)/mail/byemail$', r.uri)) and 'email' in r.params:
        list_name = m.group(1)
        email_addr = extract_param(r.params, 'email')
        result = mail_by_email(es_url, index_name, email_addr, cp, list_name=list_name)
        items, cursor = convert_hits(result, cp.limit)
        return json_response(response_body(items, cursor))

    if r.uri.endswith('/mail/byauthor') and 'author' in r.params:
        author = extract_param(r.params, 'author')
        result = mail_by_author(es_url, index_name, author, cp)
        items, cursor = convert_hits(result, cp.limit)
        return json_response(response_body(items, cursor))

    if r.uri.endswith('/mail/byemail') and 'email' in r.params:
        email_addr = extract_param(r.params, 'email')
        result = mail_by_email(es_url, index_name, email_addr, cp)
        items, cursor = convert_hits(result, cp.limit)
        return json_response(response_body(items, cursor))

    if r.uri.endswith('/mail/status'):
        last_check, last_update = get_status(es_url, checkpoint_index)
        return json_response(_to_json({
            'last_check': last_check,
            'last_update': last_update,
        }))

    if r.uri.endswith('/mail'):
        result = latest_mail(es_url, index_name, cp)
        items, cursor = convert_hits(result, cp.limit)
        return json_response(response_body(items, cursor))

    return not_found()

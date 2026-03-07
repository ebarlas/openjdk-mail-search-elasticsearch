import json
import os
import unittest
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

from server import (
    ApiRequest,
    CommonParams,
    _b64d,
    _b64e,
    _filters,
    _format_month,
    common_params,
    convert_hit,
    convert_hits,
    extract_param,
    get_status,
    json_response,
    lambda_handler,
    latest_mail,
    mail_by_author,
    mail_by_email,
    not_found,
    response_body,
    search_mail,
)


def mock_response(body_bytes):
    resp = MagicMock()
    resp.read.return_value = body_bytes
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


def cf_event(uri, qs='', method='GET'):
    """Build a minimal CloudFront Lambda@Edge event."""
    return {
        'Records': [{
            'cf': {
                'request': {
                    'method': method,
                    'uri': uri,
                    'querystring': qs,
                }
            }
        }]
    }


ES_HITS_RESPONSE = {
    'hits': {
        'total': {'value': 2, 'relation': 'eq'},
        'hits': [
            {
                '_id': 'msg-1@example.com',
                '_source': {
                    'list': 'net-dev',
                    'message_id': 'msg-1@example.com',
                    'date': '2025-08-24T20:07:24+0000',
                    'author': 'Brian Goetz',
                    'email': 'brian.goetz@oracle.com',
                    'subject': 'SSL socket behavior',
                },
                'sort': [1724529444000, 'msg-1@example.com'],
            },
            {
                '_id': 'msg-2@example.com',
                '_source': {
                    'list': 'net-dev',
                    'message_id': 'msg-2@example.com',
                    'date': '2025-08-25T10:00:00+0000',
                    'author': 'Alan Bateman',
                    'email': 'alan.bateman@oracle.com',
                    'subject': 'Re: SSL socket behavior',
                },
                'sort': [1724580000000, 'msg-2@example.com'],
            },
        ],
    },
}


# --- Pure function tests ---


class TestFormatMonth(unittest.TestCase):
    def test_standard(self):
        self.assertEqual(_format_month('2025-08-24T20:07:24+0000'), '2025-August')

    def test_january(self):
        self.assertEqual(_format_month('2024-01-15T00:00:00Z'), '2024-January')

    def test_december(self):
        self.assertEqual(_format_month('2023-12-31T23:59:59+0000'), '2023-December')

    def test_empty(self):
        self.assertEqual(_format_month(''), '')

    def test_none(self):
        self.assertEqual(_format_month(None), '')


class TestBase64RoundTrip(unittest.TestCase):
    def test_roundtrip(self):
        val = [1724529444000, 'msg-1@example.com']
        encoded = _b64e(val)
        decoded = _b64d(encoded)
        self.assertEqual(decoded, val)


class TestExtractParam(unittest.TestCase):
    def test_present(self):
        self.assertEqual(extract_param({'q': ['hello']}, 'q'), 'hello')

    def test_missing(self):
        self.assertIsNone(extract_param({}, 'q'))

    def test_default(self):
        self.assertEqual(extract_param({}, 'q', 'default'), 'default')

    def test_with_func(self):
        self.assertEqual(extract_param({'limit': ['25']}, 'limit', 10, int), 25)

    def test_func_error_returns_default(self):
        self.assertEqual(extract_param({'limit': ['abc']}, 'limit', 10, int), 10)


class TestCommonParams(unittest.TestCase):
    def test_defaults(self):
        cp = common_params({})
        self.assertFalse(cp.forward)
        self.assertEqual(cp.limit, 10)
        self.assertIsNone(cp.search_after)
        self.assertIsNone(cp.date_range)

    def test_asc_order(self):
        cp = common_params({'order': ['asc']})
        self.assertTrue(cp.forward)

    def test_desc_order(self):
        cp = common_params({'order': ['desc']})
        self.assertFalse(cp.forward)

    def test_limit_clamped(self):
        cp = common_params({'limit': ['200']})
        self.assertEqual(cp.limit, 100)
        cp = common_params({'limit': ['0']})
        self.assertEqual(cp.limit, 1)

    def test_date_range(self):
        cp = common_params({'from': ['2025-01-01'], 'to': ['2025-12-31']})
        self.assertEqual(cp.date_range, ('2025-01-01', '2025-12-31'))

    def test_date_range_partial(self):
        cp = common_params({'from': ['2025-01-01']})
        self.assertIsNone(cp.date_range)

    def test_cursor(self):
        cursor_val = [1724529444000, 'msg-1@example.com']
        encoded = _b64e(cursor_val)
        cp = common_params({'cursor': [encoded]})
        self.assertEqual(cp.search_after, cursor_val)


class TestFilters(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(_filters(), [])

    def test_list_only(self):
        f = _filters(list_name='net-dev')
        self.assertEqual(f, [{'term': {'list': 'net-dev'}}])

    def test_date_only(self):
        f = _filters(date_range=('2025-01-01', '2025-12-31'))
        self.assertEqual(f, [{'range': {'date': {'gte': '2025-01-01', 'lte': '2025-12-31'}}}])

    def test_both(self):
        f = _filters(list_name='net-dev', date_range=('2025-01-01', '2025-12-31'))
        self.assertEqual(len(f), 2)


class TestConvertHit(unittest.TestCase):
    def test_basic(self):
        hit = ES_HITS_RESPONSE['hits']['hits'][0]
        item = convert_hit(hit)
        self.assertEqual(item['list'], 'net-dev')
        self.assertEqual(item['month'], '2025-August')
        self.assertEqual(item['id'], 'msg-1@example.com')
        self.assertEqual(item['date'], '2025-08-24T20:07:24+0000')
        self.assertEqual(item['author'], 'Brian Goetz')
        self.assertEqual(item['email'], 'brian.goetz@oracle.com')
        self.assertEqual(item['subject'], 'SSL socket behavior')


class TestConvertHits(unittest.TestCase):
    def test_with_cursor(self):
        items, cursor = convert_hits(ES_HITS_RESPONSE, limit=2)
        self.assertEqual(len(items), 2)
        self.assertIsNotNone(cursor)
        self.assertEqual(cursor, [1724580000000, 'msg-2@example.com'])

    def test_without_cursor(self):
        items, cursor = convert_hits(ES_HITS_RESPONSE, limit=10)
        self.assertEqual(len(items), 2)
        self.assertIsNone(cursor)

    def test_empty(self):
        result = {'hits': {'hits': []}}
        items, cursor = convert_hits(result, limit=10)
        self.assertEqual(items, [])
        self.assertIsNone(cursor)


class TestResponseHelpers(unittest.TestCase):
    def test_json_response(self):
        resp = json_response('{"items":[]}')
        self.assertEqual(resp['status'], '200')
        self.assertEqual(resp['body'], '{"items":[]}')

    def test_not_found(self):
        resp = not_found()
        self.assertEqual(resp['status'], '404')

    def test_response_body_with_cursor(self):
        body = response_body([{'list': 'test'}], [123, 'id'])
        parsed = json.loads(body)
        self.assertEqual(parsed['items'], [{'list': 'test'}])
        self.assertIn('cursor', parsed)

    def test_response_body_without_cursor(self):
        body = response_body([{'list': 'test'}], None)
        parsed = json.loads(body)
        self.assertNotIn('cursor', parsed)


# --- Lambda handler routing tests ---


class TestApiRequest(unittest.TestCase):
    def test_from_event(self):
        event = cf_event('/api/lists/net-dev/mail/search', 'q=SSLSocket&limit=5')
        r = ApiRequest.from_event(event)
        self.assertEqual(r.method, 'GET')
        self.assertEqual(r.uri, '/api/lists/net-dev/mail/search')
        self.assertEqual(r.params['q'], ['SSLSocket'])
        self.assertEqual(r.params['limit'], ['5'])

    def test_uri_with_query(self):
        r = ApiRequest('GET', '/api/mail', 'limit=5', {'limit': ['5']})
        self.assertEqual(r.uri_with_query(), '/api/mail?limit=5')

    def test_uri_without_query(self):
        r = ApiRequest('GET', '/api/mail', '', {})
        self.assertEqual(r.uri_with_query(), '/api/mail')


class TestLambdaRouting(unittest.TestCase):
    """Test that the lambda_handler routes to the correct ES query function."""

    @patch('server.search_mail')
    @patch.dict(os.environ, {'ES_URL': 'http://es:9200'})
    def test_list_search(self, mock_search):
        mock_search.return_value = {'hits': {'hits': []}}
        event = cf_event('/api/lists/net-dev/mail/search', 'q=SSLSocket')
        resp = lambda_handler(event, None)
        self.assertEqual(resp['status'], '200')
        mock_search.assert_called_once()
        args = mock_search.call_args
        self.assertEqual(args[1]['list_name'], 'net-dev')
        self.assertEqual(args[0][2], 'SSLSocket')

    @patch('server.search_mail')
    @patch.dict(os.environ, {'ES_URL': 'http://es:9200'})
    def test_global_search(self, mock_search):
        mock_search.return_value = {'hits': {'hits': []}}
        event = cf_event('/api/mail/search', 'q=SSLSocket')
        resp = lambda_handler(event, None)
        self.assertEqual(resp['status'], '200')
        mock_search.assert_called_once()
        args = mock_search.call_args
        self.assertIsNone(args[1].get('list_name'))

    @patch('server.latest_mail')
    @patch.dict(os.environ, {'ES_URL': 'http://es:9200'})
    def test_list_latest(self, mock_latest):
        mock_latest.return_value = {'hits': {'hits': []}}
        event = cf_event('/api/lists/core-libs-dev/mail')
        resp = lambda_handler(event, None)
        self.assertEqual(resp['status'], '200')
        mock_latest.assert_called_once()
        self.assertEqual(mock_latest.call_args[1]['list_name'], 'core-libs-dev')

    @patch('server.latest_mail')
    @patch.dict(os.environ, {'ES_URL': 'http://es:9200'})
    def test_global_latest(self, mock_latest):
        mock_latest.return_value = {'hits': {'hits': []}}
        event = cf_event('/api/mail')
        resp = lambda_handler(event, None)
        self.assertEqual(resp['status'], '200')
        mock_latest.assert_called_once()
        self.assertIsNone(mock_latest.call_args[1].get('list_name'))

    @patch('server.mail_by_author')
    @patch.dict(os.environ, {'ES_URL': 'http://es:9200'})
    def test_list_byauthor(self, mock_author):
        mock_author.return_value = {'hits': {'hits': []}}
        event = cf_event('/api/lists/net-dev/mail/byauthor', 'author=Brian+Goetz')
        resp = lambda_handler(event, None)
        self.assertEqual(resp['status'], '200')
        mock_author.assert_called_once()
        self.assertEqual(mock_author.call_args[1]['list_name'], 'net-dev')
        self.assertEqual(mock_author.call_args[0][2], 'Brian Goetz')

    @patch('server.mail_by_email')
    @patch.dict(os.environ, {'ES_URL': 'http://es:9200'})
    def test_list_byemail(self, mock_email):
        mock_email.return_value = {'hits': {'hits': []}}
        event = cf_event('/api/lists/net-dev/mail/byemail', 'email=brian.goetz%40oracle.com')
        resp = lambda_handler(event, None)
        self.assertEqual(resp['status'], '200')
        mock_email.assert_called_once()
        self.assertEqual(mock_email.call_args[1]['list_name'], 'net-dev')

    @patch('server.mail_by_author')
    @patch.dict(os.environ, {'ES_URL': 'http://es:9200'})
    def test_global_byauthor(self, mock_author):
        mock_author.return_value = {'hits': {'hits': []}}
        event = cf_event('/api/mail/byauthor', 'author=Brian+Goetz')
        resp = lambda_handler(event, None)
        self.assertEqual(resp['status'], '200')
        mock_author.assert_called_once()
        self.assertIsNone(mock_author.call_args[1].get('list_name'))

    @patch('server.mail_by_email')
    @patch.dict(os.environ, {'ES_URL': 'http://es:9200'})
    def test_global_byemail(self, mock_email):
        mock_email.return_value = {'hits': {'hits': []}}
        event = cf_event('/api/mail/byemail', 'email=brian.goetz%40oracle.com')
        resp = lambda_handler(event, None)
        self.assertEqual(resp['status'], '200')
        mock_email.assert_called_once()

    @patch('server.get_status', return_value=('2025-03-07T00:00:00Z', '2025-03-07T00:00:00Z'))
    @patch.dict(os.environ, {'ES_URL': 'http://es:9200'})
    def test_status(self, mock_status):
        event = cf_event('/api/mail/status')
        resp = lambda_handler(event, None)
        self.assertEqual(resp['status'], '200')
        body = json.loads(resp['body'])
        self.assertIn('last_check', body)
        self.assertIn('last_update', body)

    @patch.dict(os.environ, {'ES_URL': 'http://es:9200'})
    def test_not_found(self):
        event = cf_event('/api/nonexistent')
        resp = lambda_handler(event, None)
        self.assertEqual(resp['status'], '404')

    @patch.dict(os.environ, {'ES_URL': 'http://es:9200'})
    def test_post_returns_404(self):
        event = cf_event('/api/mail', method='POST')
        resp = lambda_handler(event, None)
        self.assertEqual(resp['status'], '404')

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_es_url_raises(self):
        event = cf_event('/api/mail')
        with self.assertRaises(KeyError):
            lambda_handler(event, None)

    @patch('server.search_mail')
    @patch.dict(os.environ, {'ES_URL': 'http://es:9200'})
    def test_search_without_q_falls_through(self, mock_search):
        event = cf_event('/api/lists/net-dev/mail/search')
        resp = lambda_handler(event, None)
        self.assertEqual(resp['status'], '404')
        mock_search.assert_not_called()


# --- ES query construction tests ---


class TestSearchMail(unittest.TestCase):
    @patch('server.urlopen')
    def test_list_scoped(self, mock_urlopen):
        mock_urlopen.return_value = mock_response(json.dumps(ES_HITS_RESPONSE).encode())
        cp = CommonParams(forward=False, limit=10, search_after=None, date_range=None)
        result = search_mail('http://es:9200', 'idx', 'SSLSocket', cp, list_name='net-dev')

        req = mock_urlopen.call_args[0][0]
        body = json.loads(req.data)
        self.assertIn('multi_match', body['query']['bool']['must'])
        self.assertEqual(body['query']['bool']['must']['multi_match']['query'], 'SSLSocket')
        filters = body['query']['bool']['filter']
        self.assertTrue(any(f.get('term', {}).get('list') == 'net-dev' for f in filters))

    @patch('server.urlopen')
    def test_global(self, mock_urlopen):
        mock_urlopen.return_value = mock_response(json.dumps(ES_HITS_RESPONSE).encode())
        cp = CommonParams(forward=False, limit=10, search_after=None, date_range=None)
        result = search_mail('http://es:9200', 'idx', 'SSLSocket', cp)

        req = mock_urlopen.call_args[0][0]
        body = json.loads(req.data)
        self.assertNotIn('filter', body['query']['bool'])

    @patch('server.urlopen')
    def test_with_date_range(self, mock_urlopen):
        mock_urlopen.return_value = mock_response(json.dumps(ES_HITS_RESPONSE).encode())
        cp = CommonParams(forward=True, limit=5, search_after=None,
                          date_range=('2025-01-01', '2025-12-31'))
        search_mail('http://es:9200', 'idx', 'test', cp, list_name='net-dev')

        req = mock_urlopen.call_args[0][0]
        body = json.loads(req.data)
        self.assertEqual(body['size'], 5)
        self.assertEqual(body['sort'][0], {'date': 'asc'})
        self.assertEqual(body['sort'][1], {'message_id': 'asc'})
        filters = body['query']['bool']['filter']
        range_filter = [f for f in filters if 'range' in f][0]
        self.assertEqual(range_filter['range']['date']['gte'], '2025-01-01')

    @patch('server.urlopen')
    def test_with_search_after(self, mock_urlopen):
        mock_urlopen.return_value = mock_response(json.dumps(ES_HITS_RESPONSE).encode())
        cp = CommonParams(forward=False, limit=10,
                          search_after=[1724529444000, 'msg-1@example.com'],
                          date_range=None)
        search_mail('http://es:9200', 'idx', 'test', cp)

        req = mock_urlopen.call_args[0][0]
        body = json.loads(req.data)
        self.assertEqual(body['search_after'], [1724529444000, 'msg-1@example.com'])


class TestLatestMail(unittest.TestCase):
    @patch('server.urlopen')
    def test_list_scoped(self, mock_urlopen):
        mock_urlopen.return_value = mock_response(json.dumps(ES_HITS_RESPONSE).encode())
        cp = CommonParams(forward=False, limit=10, search_after=None, date_range=None)
        latest_mail('http://es:9200', 'idx', cp, list_name='net-dev')

        req = mock_urlopen.call_args[0][0]
        body = json.loads(req.data)
        filters = body['query']['bool']['filter']
        self.assertTrue(any(f.get('term', {}).get('list') == 'net-dev' for f in filters))

    @patch('server.urlopen')
    def test_global(self, mock_urlopen):
        mock_urlopen.return_value = mock_response(json.dumps(ES_HITS_RESPONSE).encode())
        cp = CommonParams(forward=False, limit=10, search_after=None, date_range=None)
        latest_mail('http://es:9200', 'idx', cp)

        req = mock_urlopen.call_args[0][0]
        body = json.loads(req.data)
        self.assertIn('match_all', body['query'])


class TestMailByAuthor(unittest.TestCase):
    @patch('server.urlopen')
    def test_query(self, mock_urlopen):
        mock_urlopen.return_value = mock_response(json.dumps(ES_HITS_RESPONSE).encode())
        cp = CommonParams(forward=False, limit=10, search_after=None, date_range=None)
        mail_by_author('http://es:9200', 'idx', 'Brian Goetz', cp, list_name='net-dev')

        req = mock_urlopen.call_args[0][0]
        body = json.loads(req.data)
        match = body['query']['bool']['must']['match']['author']
        self.assertEqual(match['query'], 'Brian Goetz')
        self.assertEqual(match['operator'], 'and')


class TestMailByEmail(unittest.TestCase):
    @patch('server.urlopen')
    def test_query(self, mock_urlopen):
        mock_urlopen.return_value = mock_response(json.dumps(ES_HITS_RESPONSE).encode())
        cp = CommonParams(forward=False, limit=10, search_after=None, date_range=None)
        mail_by_email('http://es:9200', 'idx', 'Brian.Goetz@Oracle.com', cp)

        req = mock_urlopen.call_args[0][0]
        body = json.loads(req.data)
        filters = body['query']['bool']['filter']
        email_filter = [f for f in filters if 'term' in f and 'email' in f['term']][0]
        self.assertEqual(email_filter['term']['email'], 'brian.goetz@oracle.com')


class TestGetStatus(unittest.TestCase):
    @patch('server.urlopen')
    def test_returns_timestamps(self, mock_urlopen):
        body = json.dumps({
            'aggregations': {
                'last_sync': {
                    'value': 1709769600000,
                    'value_as_string': '2025-03-07T00:00:00.000Z',
                }
            }
        }).encode()
        mock_urlopen.return_value = mock_response(body)
        last_check, last_update = get_status('http://es:9200', 'cp-index')
        self.assertEqual(last_check, '2025-03-07T00:00:00.000Z')
        self.assertEqual(last_update, '2025-03-07T00:00:00.000Z')


if __name__ == '__main__':
    unittest.main()

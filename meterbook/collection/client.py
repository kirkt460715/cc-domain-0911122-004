import json
import urllib.error
import urllib.request
from meterbook.values import fields, reject, number, key


class SourceError(Exception):
    """A transport-level failure talking to the delivery queue.

    It is deliberately distinct from MeterError: the collector treats it as
    transient, keeps the local delivery state and never assumes a remote
    write (renew or acknowledge) happened.
    """

    def __init__(self, code, message, status=None):
        super().__init__(message)
        self.code = code
        self.status = status

    def payload(self):
        return {'code': self.code, 'message': str(self), 'status': self.status}


class LeaseLost(SourceError):
    """The queue answered 409: the handle is stale or the lease expired."""

    def __init__(self, code='lease_lost', message='Delivery lease is no longer held'):
        super().__init__(code, message, status=409)


def _decode(raw):
    try:
        return json.loads(raw.decode('utf-8'))
    except (ValueError, UnicodeError) as exc:
        raise SourceError('source_decode', 'Queue returned an undecodable response') from exc


def _message_shape(message):
    # Only the collector's required contract is validated; extra provider-side
    # fields (e.g. an internal ack marker) are ignored rather than rejected.
    if not isinstance(message, dict):
        reject('source_message', 'Queue delivery must be a JSON object')
    missing = {'delivery_id', 'body', 'expires_at', 'receipt_handle'} - set(message)
    if missing:
        reject('source_message', 'Queue delivery is missing required fields', missing=sorted(missing))
    key(message['delivery_id'], 'delivery_id')
    key(message['receipt_handle'], 'receipt_handle')
    number(message['expires_at'], 'expires_at', 0)
    if not isinstance(message['body'], dict):
        reject('source_message', 'Queue delivery body must be a JSON object')
    return {name: message[name] for name in ('delivery_id', 'body', 'expires_at', 'receipt_handle')}


class QueueClient:
    """HTTP adapter for the synthetic usage delivery queue (lab.provider).

    Every call is a single POST returning the parsed JSON envelope. Network
    failures and non-2xx answers become SourceError so the collector can keep
    its durable state instead of guessing the remote outcome.
    """

    def __init__(self, base_url, timeout=5):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout

    def _post(self, path, body):
        request = urllib.request.Request(
            self.base_url + path,
            data=json.dumps(body).encode(),
            headers={'Content-Type': 'application/json'},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return _decode(response.read())
        except urllib.error.HTTPError as exc:
            payload = {}
            try:
                payload = _decode(exc.read())
            except SourceError:
                payload = {}
            if exc.code == 409:
                raise LeaseLost(payload.get('error', 'lease_lost'), 'Queue rejected the receipt handle') from exc
            raise SourceError(payload.get('error', 'source_http'), 'Queue answered with an error', exc.code) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise SourceError('source_unreachable', 'Cannot reach the delivery queue') from exc

    def receive(self, limit):
        number(limit, 'limit', 1, 1000)
        envelope = self._post('/receive', {'limit': limit})
        fields(envelope, {'now', 'messages'})
        number(envelope['now'], 'now', 0)
        if not isinstance(envelope['messages'], list) or len(envelope['messages']) > limit:
            reject('source_envelope', 'Queue receive envelope has an unexpected messages list')
        return {'now': envelope['now'], 'messages': [_message_shape(message) for message in envelope['messages']]}

    def renew(self, receipt_handle):
        envelope = self._post('/renew', {'receipt_handle': receipt_handle})
        fields(envelope, {'expires_at'})
        return number(envelope['expires_at'], 'expires_at', 0)

    def acknowledge(self, receipt_handle):
        # A dropped connection lands here as SourceError; the delivery is not
        # removed locally and the next run reconciles the remote answer.
        return self._post('/ack', {'receipt_handle': receipt_handle})

    def advance_clock(self, seconds):
        return self._post('/clock', {'advance': number(seconds, 'seconds', 0, 1000000000)})

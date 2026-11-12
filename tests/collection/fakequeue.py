import copy
from meterbook.collection.client import SourceError, LeaseLost


def provider_event(sequence=1, event_id=None, usage_id=None, revision=1, action='report', quantity='12.50'):
    return {
        'event_id': event_id or f'ev{sequence}',
        'contract_id': 'contract-a',
        'side': 'provider',
        'sequence': sequence,
        'usage_id': usage_id or f'usage{sequence}',
        'revision': revision,
        'action': action,
        'usage': {'meter': 'records', 'at': '2026-11-01T00:00:00Z',
                  'quantity': quantity, 'purpose': 'research', 'region': 'CN'},
    }


class FakeQueue:
    """In-process double of lab.provider.Provider behind the same surface.

    Logical seconds, ten-second leases, fresh handles on every redelivery,
    409 on stale/expired handles and a switch to drop the ack response after
    the server-side acknowledgement has landed.
    """

    def __init__(self, deliveries=None, lease_seconds=10):
        self.lock_note = None
        self.now = 0
        self.serial = 0
        self.lease_seconds = lease_seconds
        self.drop_ack = False
        self.calls = []
        self.deliveries = []
        for delivery_id, body in (deliveries or [('d1', provider_event()), ('bad', {'unexpected': '合成坏信封'})]):
            self.deliveries.append({'delivery_id': delivery_id, 'body': copy.deepcopy(body),
                                    'expires_at': 0, 'receipt_handle': None, 'ack': False})

    def _record(self, path, body):
        self.calls.append((path, copy.deepcopy(body)))

    def receive(self, limit):
        self._record('/receive', {'limit': limit})
        result = []
        for item in self.deliveries:
            if not item['ack'] and item['expires_at'] <= self.now and len(result) < limit:
                self.serial += 1
                item['receipt_handle'] = f'h{self.serial}'
                item['expires_at'] = self.now + self.lease_seconds
                result.append(copy.deepcopy(item))
        return {'now': self.now, 'messages': result}

    def renew(self, receipt_handle):
        self._record('/renew', {'receipt_handle': receipt_handle})
        item = self._handle(receipt_handle)
        item['expires_at'] = self.now + self.lease_seconds
        return item['expires_at']

    def acknowledge(self, receipt_handle):
        self._record('/ack', {'receipt_handle': receipt_handle})
        item = self._handle(receipt_handle)
        item['ack'] = True
        if self.drop_ack:
            self.drop_ack = False
            raise SourceError('source_unreachable', 'connection reset after server-side ack')
        return {'acknowledged': item['delivery_id']}

    def _handle(self, receipt_handle):
        item = next((row for row in self.deliveries if row['receipt_handle'] == receipt_handle), None)
        if not item or item['ack'] or item['expires_at'] <= self.now:
            raise LeaseLost()
        return item

    def advance_clock(self, seconds):
        self._record('/clock', {'advance': seconds})
        self.now += seconds
        return {'now': self.now}

    def status(self):
        return {'now': self.now, 'deliveries': copy.deepcopy(self.deliveries)}

    def find(self, delivery_id):
        return next(row for row in self.deliveries if row['delivery_id'] == delivery_id)

import unittest
from lab.http import serving
from lab.provider import Provider
from tests.support import Case
from meterbook.collection.client import QueueClient, LeaseLost, SourceError
from meterbook.collection.collector import (
    Collector, ACK_PENDING, ACKNOWLEDGED, LEASE_LOST,
)


class QueueProtocolOverHttp(unittest.TestCase):
    def setUp(self):
        self.provider = Provider()

    def test_client_roundtrip_and_envelope_validation(self):
        with serving(self.provider) as base:
            client = QueueClient(base)
            envelope = client.receive(1)
            self.assertEqual(envelope['now'], 0)
            self.assertEqual(envelope['messages'][0]['delivery_id'], 'd1')
            self.assertTrue(envelope['messages'][0]['receipt_handle'].startswith('h'))

    def test_renew_ack_and_stale_handle_409(self):
        with serving(self.provider) as base:
            client = QueueClient(base)
            first = client.receive(1)['messages'][0]
            expires = client.renew(first['receipt_handle'])
            self.assertEqual(expires, 10)
            client.advance_clock(11)
            with self.assertRaises(LeaseLost):
                client.renew(first['receipt_handle'])
            redelivered = client.receive(1)['messages'][0]
            self.assertEqual(redelivered['delivery_id'], 'd1')
            self.assertNotEqual(redelivered['receipt_handle'], first['receipt_handle'])
            client.acknowledge(redelivered['receipt_handle'])
            with self.assertRaises(LeaseLost):
                client.acknowledge(first['receipt_handle'])

    def test_dropped_ack_surfaces_as_transport_error_not_success(self):
        with serving(self.provider) as base:
            client = QueueClient(base)
            handle = client.receive(1)['messages'][0]['receipt_handle']
            self.provider.drop_ack = True
            with self.assertRaises(SourceError) as caught:
                client.acknowledge(handle)
            self.assertEqual(caught.exception.code, 'source_unreachable')
            # The server did record the ack even though the response was lost.
            self.assertTrue(self.provider.deliveries[0]['ack'])


class CollectorOverHttp(Case):
    def setUp(self):
        super().setUp()
        self.provider = Provider()
        self.provider.deliveries[0]['body'] = {
            'event_id': 'ev1', 'contract_id': 'contract-a', 'side': 'provider',
            'sequence': 1, 'usage_id': 'usage1', 'revision': 1, 'action': 'report',
            'usage': {'meter': 'records', 'at': '2026-11-01T00:00:00Z',
                      'quantity': '12.50', 'purpose': 'research', 'region': 'CN'},
        }
        self.collector = Collector(self.store, renew_threshold=3)

    def test_full_collection_against_lab_server(self):
        with serving(self.provider) as base:
            client = QueueClient(base)
            result = self.collector.run(client, limit=10)
            self.assertEqual(result['stop_reason'], 'drained')
            self.assertEqual(result['stats']['accepted'], 1)
            self.assertEqual(result['stats']['quarantined'], 1)
            self.assertEqual(self.collector.get_delivery('d1')['state'], ACKNOWLEDGED)
            self.assertTrue(self.provider.deliveries[0]['ack'])
            # The malformed synthetic delivery is quarantined and not acked.
            self.assertFalse(self.provider.deliveries[1]['ack'])
            self.assertEqual(len(self.collector.list_quarantine()), 1)

    def test_lease_expiry_and_redelivery_over_http(self):
        with serving(self.provider) as base:
            client = QueueClient(base)
            envelope = client.receive(1)
            self.collector.capture(envelope['messages'][0], envelope['now'])
            client.advance_clock(11)
            self.collector.sweep(client.advance_clock(0)['now'])
            self.assertEqual(self.collector.get_delivery('d1')['state'], LEASE_LOST)
            # Redelivery gives a new handle; processing completes idempotently.
            again = client.receive(1)['messages'][0]
            self.collector.capture(again, again['expires_at'] - 10)
            self.collector.accept('d1')
            self.collector.acknowledge(client, 'd1')
            self.assertEqual(self.collector.get_delivery('d1')['state'], ACKNOWLEDGED)

    def test_ack_disconnect_is_reconciled_on_next_run(self):
        with serving(self.provider) as base:
            client = QueueClient(base)
            # Capture + accept, then drop the ack response on the floor.
            envelope = client.receive(1)
            self.collector.capture(envelope['messages'][0], envelope['now'])
            self.collector.accept('d1')
            self.provider.drop_ack = True
            with self.assertRaises(SourceError):
                self.collector.acknowledge(client, 'd1')
            self.assertEqual(self.collector.get_delivery('d1')['state'], ACK_PENDING)
            self.assertTrue(self.provider.deliveries[0]['ack'])
            # A fresh run reconciles: the provider will 409 the already-acked
            # handle once its lease passes; emulate redelivery with a new handle.
            self.provider.deliveries[0]['ack'] = False
            client.advance_clock(11)
            second = self.collector.run(client, limit=10)
            self.assertEqual(second['stats']['duplicates'], 1)
            self.assertEqual(self.collector.get_delivery('d1')['state'], ACKNOWLEDGED)
            # Exactly one business event despite the disconnect.
            self.assertEqual(self.store.scalar("SELECT COUNT(*) FROM events WHERE event_id='ev1'"), 1)


if __name__ == '__main__':
    unittest.main()

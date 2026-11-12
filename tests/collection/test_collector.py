import threading
import unittest
from tests.support import Case, definition, tariff
from tests.collection.fakequeue import FakeQueue, provider_event
from meterbook.contracts import registry, rates
from meterbook.collection.collector import (
    Collector, HELD, LEASE_LOST, ACK_PENDING, ACKNOWLEDGED, QUARANTINED, CONFLICT,
    A_HELD, A_LOST,
)
from meterbook.collection.client import SourceError, LeaseLost
from meterbook.events import receiver
from meterbook.settlement import invoices


class CollectionCase(Case):
    def setUp(self):
        super().setUp()
        # support.Case already creates contract-a and rate-a matching the
        # synthetic provider event (records / research / CN).
        self.queue = FakeQueue()
        self.collector = Collector(self.store, renew_threshold=3)

    def event_count(self):
        return self.store.scalar('SELECT COUNT(*) FROM events')

    def delivery(self, delivery_id='d1'):
        return self.collector.get_delivery(delivery_id)


class HappyPath(CollectionCase):
    def test_three_stages_are_persisted_separately_and_acks(self):
        result = self.collector.run(self.queue, limit=10)
        self.assertEqual(result['stop_reason'], 'drained')
        self.assertEqual(result['stats']['captured'], 2)
        self.assertEqual(result['stats']['accepted'], 1)
        self.assertEqual(result['stats']['quarantined'], 1)
        self.assertEqual(result['stats']['acknowledged'], 1)
        # Stage 1: delivery capture with its lease.
        delivery = self.delivery()
        self.assertEqual(delivery['state'], ACKNOWLEDGED)
        self.assertEqual(delivery['accepted_event_id'], 'ev1')
        # Stage 2: business receipt traceable from the delivery.
        event = receiver.get(self.store, 'ev1')
        self.assertEqual(event['state'], 'pending')
        # Stage 3: the queue really got the ack, and it used the stored handle.
        self.assertTrue(self.queue.find('d1')['ack'])
        ack_handle = next(body['receipt_handle'] for path, body in self.queue.calls if path == '/ack')
        self.assertEqual(ack_handle, delivery['current_handle'])
        # The bad envelope is quarantined, never dropped, not acked by default.
        self.assertFalse(self.queue.find('bad')['ack'])
        quarantined = self.collector.list_quarantine()
        self.assertEqual(len(quarantined), 1)
        record = self.collector.get_quarantine(quarantined[0]['quarantine_id'])
        self.assertEqual(record['delivery_id'], 'bad')
        self.assertIn('fields', record['reason'])

    def test_business_apply_still_obeys_sequence_contract_without_holding_delivery(self):
        self.collector.run(self.queue)
        # The event was reliably received; draining the stream applies it and
        # the delivery lifecycle is already complete regardless of ordering.
        drained = receiver.drain(self.store, 'contract-a', 'provider')
        self.assertIsNone(drained['blocked'])
        self.assertEqual(receiver.head(self.store, 'contract-a', 'provider', 'usage1')['state'], 'active')
        self.assertEqual(self.delivery()['state'], ACKNOWLEDGED)

    def test_delivery_acked_even_when_stream_waits_for_predecessor(self):
        # Sequence 2 arrives first: it can be reliably received but drain must
        # wait for sequence 1. The delivery must still be confirmed and released
        # instead of holding its lease while the gap is open.
        self.queue.deliveries = [('gap', provider_event(sequence=2, event_id='ev2', usage_id='usage2'))]
        self.queue.__init__(deliveries=self.queue.deliveries)
        result = self.collector.run(self.queue)
        self.assertEqual(result['stats']['accepted'], 1)
        self.assertEqual(self.delivery('gap')['state'], ACKNOWLEDGED)
        self.assertTrue(self.queue.find('gap')['ack'])
        # The business stream honestly reports the gap rather than blocking.
        status = receiver.stream_status(self.store, 'contract-a', 'provider')
        self.assertEqual(status['gaps'], [{'from': 1, 'until_inclusive': 1}])
        self.assertEqual(receiver.get(self.store, 'ev2')['state'], 'pending')


class ExitReport(CollectionCase):
    def test_exit_summary_names_unconfirmed_acknowledgements(self):
        # Accept the event, then make every subsequent ack fail at the transport
        # layer so the run must stop and report the outstanding confirmation.
        class Unreachable:
            def receive(self, limit):
                return self._envelope

            def renew(self, handle):
                raise SourceError('source_unreachable', 'down')

            def acknowledge(self, handle):
                raise SourceError('source_unreachable', 'down')

        envelope = self.queue.receive(10)
        source = Unreachable()
        source._envelope = envelope
        # Capture and accept durably before the source becomes unreachable.
        for message in envelope['messages']:
            self.collector.capture(message, envelope['now'])
        self.collector.accept('d1')
        result = self.collector.run(source)
        self.assertEqual(result['stop_reason'], 'source_unreachable')
        pending = [row['delivery_id'] for row in result['pending_acknowledgements']]
        self.assertIn('d1', pending)
        # The same list is persisted on the run record for later inspection.
        self.assertIn('d1', self.collector.runs()[0]['pending_ack'])
        self.assertEqual(self.collector.status()['pending_acknowledgements'], len(pending))


class LeaseManagement(CollectionCase):
    def test_renew_before_expiry_keeps_the_same_delivery(self):
        envelope = self.queue.receive(1)
        delivery, is_new = self.collector.capture(envelope['messages'][0], envelope['now'])
        self.assertTrue(is_new)
        self.queue.advance_clock(8)
        self.collector.sweep(self.queue.now)
        # Within the 10s lease: renew extends it and the handle stays valid.
        renewed = self.collector.renew_delivery(self.queue, 'd1')
        self.assertEqual(renewed['attempts'][-1]['lease_renewals'], 1)
        self.queue.advance_clock(9)
        self.collector.sweep(self.queue.now)
        self.assertEqual(self.delivery()['state'], HELD)
        self.collector.accept('d1')
        self.collector.acknowledge(self.queue, 'd1')
        self.assertTrue(self.queue.find('d1')['ack'])

    def test_expired_lease_is_marked_lost_and_not_assumed_held(self):
        envelope = self.queue.receive(1)
        self.collector.capture(envelope['messages'][0], envelope['now'])
        old_handle = envelope['messages'][0]['receipt_handle']
        self.queue.advance_clock(11)
        swept = self.collector.sweep(self.queue.now)
        self.assertEqual(swept['expired'], 1)
        self.assertEqual(self.delivery()['state'], LEASE_LOST)
        # The old handle cannot be confirmed remotely.
        with self.assertRaises(LeaseLost):
            self.collector.acknowledge(self.queue, 'd1')
        with self.assertRaises(LeaseLost):
            self.collector.renew_delivery(self.queue, 'd1')
        self.assertEqual(self.delivery()['state'], LEASE_LOST)
        # Nothing was written to the business ledger while the lease was lost.
        self.assertEqual(self.event_count(), 0)

    def test_redelivery_with_new_handle_recovers_and_old_handle_fails(self):
        envelope = self.queue.receive(1)
        self.collector.capture(envelope['messages'][0], envelope['now'])
        old_handle = envelope['messages'][0]['receipt_handle']
        self.queue.advance_clock(11)
        self.collector.sweep(self.queue.now)
        # Provider redelivers with a brand new handle.
        again = self.queue.receive(1)
        self.assertEqual(again['messages'][0]['delivery_id'], 'd1')
        new_handle = again['messages'][0]['receipt_handle']
        self.assertNotEqual(old_handle, new_handle)
        delivery, is_new = self.collector.capture(again['messages'][0], again['now'])
        self.assertTrue(is_new)
        self.assertEqual(delivery['state'], HELD)
        self.assertEqual(delivery['current_handle'], new_handle)
        attempts = delivery['attempts']
        self.assertEqual(attempts[0]['state'], A_LOST)
        self.assertEqual(attempts[1]['state'], A_HELD)
        with self.assertRaises(LeaseLost):
            self.queue.acknowledge(old_handle)
        self.collector.accept('d1')
        self.collector.acknowledge(self.queue, 'd1')
        self.assertTrue(self.queue.find('d1')['ack'])

    def test_auto_renew_inside_run_prevents_expiry_during_slow_processing(self):
        # A short lease with renew_threshold larger than the lease window means
        # every held delivery is renewed on the spot inside the run loop.
        self.queue.lease_seconds = 2
        collector = Collector(self.store)
        collector.configure(renew_threshold=3)
        result = collector.run(self.queue, limit=10)
        self.assertGreaterEqual(result['stats']['renew_calls'], 1)
        self.assertEqual(result['stats']['lease_lost'], 0)
        self.assertEqual(self.delivery()['state'], ACKNOWLEDGED)
        # The provider recorded a renew call against the stored live handle.
        renew_handles = [body['receipt_handle'] for path, body in self.queue.calls if path == '/renew']
        self.assertIn(self.delivery()['current_handle'], renew_handles)


class RedeliveryAndConflicts(CollectionCase):
    def test_same_delivery_with_different_body_is_conflict_and_not_acked(self):
        first = self.queue.receive(1)
        self.collector.capture(first['messages'][0], first['now'])
        self.collector.accept('d1')
        original_handle = self.delivery()['current_handle']
        # Mutate the provider body behind the same delivery_id, then redeliver.
        changed = provider_event(quantity='99.00')
        self.queue.find('d1')['body'] = changed
        self.queue.advance_clock(11)
        redelivery = self.queue.receive(1)['messages'][0]
        delivery, _ = self.collector.capture(redelivery, 11)
        self.assertEqual(delivery['state'], CONFLICT)
        self.assertIsNotNone(delivery['conflict'])
        self.assertIn('delivery_body_conflict', delivery['conflict'])
        # Only the original valid usage exists; no second valid usage recorded.
        self.assertEqual(self.event_count(), 1)
        self.assertEqual(receiver.get(self.store, 'ev1')['payload']['usage']['quantity'], '12.5')
        # Neither handle gets acknowledged automatically.
        self.assertFalse(self.queue.find('d1')['ack'])
        self.assertNotIn(('/ack', {'receipt_handle': original_handle}), self.queue.calls)

    def test_ack_response_lost_then_redelivery_does_not_double_count(self):
        self.queue.drop_ack = True
        result = self.collector.run(self.queue, limit=10)
        self.assertEqual(result['stop_reason'], 'source_unreachable')
        # Server already acked, but the client never learned; local state stays.
        self.assertTrue(self.queue.find('d1')['ack'])
        self.assertEqual(self.delivery('d1')['state'], ACK_PENDING)
        self.assertEqual(self.event_count(), 1)
        # Simulate the provider making it visible again (ack response lost) with
        # a new handle after the lease window passes.
        self.queue.find('d1')['ack'] = False
        self.queue.advance_clock(11)
        second = self.collector.run(self.queue, limit=10)
        self.assertEqual(second['stats']['duplicates'], 1)
        self.assertEqual(second['stats']['accepted'], 1)
        # Still exactly one business event and one effective usage.
        self.assertEqual(self.event_count(), 1)
        self.assertEqual(self.delivery('d1')['attempts'][-1]['duplicate'], 1)
        self.assertEqual(self.delivery('d1')['state'], ACKNOWLEDGED)


class CompetingConsumers(CollectionCase):
    def test_two_collectors_contending_for_one_delivery_record_one_event(self):
        # Two workers sharing the ledger file. The queue guarantees a unique
        # receipt_handle per outstanding delivery, so the UNIQUE(handle) and
        # events primary key serialize the race instead of in-memory guessing.
        other = Collector(self.store)
        envelope = self.queue.receive(1)
        message = envelope['messages'][0]
        # Both collectors durably capture the same delivery and handle; the
        # second capture is recognized as the same outstanding lease.
        first, _ = self.collector.capture(message, envelope['now'])
        second, _ = other.capture(message, envelope['now'])
        self.assertEqual(first['current_handle'], message['receipt_handle'])
        self.assertEqual(second['current_handle'], message['receipt_handle'])
        self.assertEqual(self.store.scalar(
            "SELECT COUNT(*) FROM collection_attempts WHERE receipt_handle=?",
            (message['receipt_handle'],)), 1)
        # Consumer A wins the business insert (B observes the same held lease).
        self.collector.accept('d1')
        self.assertEqual(self.store.scalar("SELECT COUNT(*) FROM events WHERE event_id='ev1'"), 1)
        # The lease window passes before either worker confirms. Both local
        # views must accept that they no longer hold the delivery.
        self.queue.advance_clock(11)
        self.collector.sweep(self.queue.now)
        self.assertEqual(other.get_delivery('d1')['state'], LEASE_LOST)
        with self.assertRaises(LeaseLost):
            other.renew_delivery(self.queue, 'd1')
        with self.assertRaises(LeaseLost):
            self.collector.acknowledge(self.queue, 'd1')
        self.assertFalse(self.queue.find('d1')['ack'])
        # The provider redelivers with a fresh handle; the redriven business
        # receipt is an idempotent duplicate, never a second valid usage.
        redelivery = self.queue.receive(1)['messages'][0]
        self.assertNotEqual(redelivery['receipt_handle'], message['receipt_handle'])
        other.capture(redelivery, redelivery['expires_at'] - 10)
        acceptance = other.accept('d1')
        self.assertTrue(acceptance['duplicate'])
        other.acknowledge(self.queue, 'd1')
        self.assertTrue(self.queue.find('d1')['ack'])
        self.assertEqual(self.store.scalar("SELECT COUNT(*) FROM events WHERE event_id='ev1'"), 1)

    def test_concurrent_accept_threads_produce_a_single_event(self):
        from meterbook.store import Store
        from meterbook.contracts import registry, rates
        # A file-backed store is what separate worker processes/threads share.
        import tempfile
        import os
        path = os.path.join(tempfile.mkdtemp(), 'concurrent.sqlite')
        shared = Store(path)
        registry.create(shared, 'contract-a', 'provider-a', 'consumer-a', 'CNY', definition())
        rates.add(shared, 'rate-a', 'contract-a', 'records',
                  '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z', tariff())
        collector = Collector(shared)
        envelope = self.queue.receive(1)
        collector.capture(envelope['messages'][0], envelope['now'])
        errors = []

        def race():
            # Each thread opens its own connection to the same database file.
            local = Store(path)
            try:
                Collector(local).accept('d1')
            except Exception as exc:  # pragma: no cover - surfaced via assertion
                errors.append(exc)
            finally:
                local.close()

        threads = [threading.Thread(target=race) for _ in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(shared.scalar("SELECT COUNT(*) FROM events WHERE event_id='ev1'"), 1)
        # The single captured delivery was accepted once; losers observed the
        # existing event as a duplicate rather than inserting their own.
        self.assertEqual(shared.scalar(
            "SELECT COUNT(*) FROM collection_attempts WHERE delivery_id='d1'"), 1)
        shared.close()


class QuarantineControl(CollectionCase):
    def test_operator_decides_quarantine_ack_or_retry(self):
        self.collector.run(self.queue)
        quarantine_id = self.collector.list_quarantine()[0]['quarantine_id']
        # Default configuration: bad envelope stays open and unacknowledged.
        self.assertEqual(self.collector.status()['quarantined'], 1)
        self.assertFalse(self.queue.find('bad')['ack'])
        self.assertEqual(self.collector.pending_acknowledgements(), [])
        # Operator chooses to acknowledge it without creating usage.
        self.collector.resolve_quarantine(quarantine_id, 'ack', 'operator reviewed malformed payload')
        # The decision moves it onto the explicit acknowledgement worklist.
        self.assertEqual([r['delivery_id'] for r in self.collector.pending_acknowledgements()], ['bad'])
        self.collector.acknowledge(self.queue, 'bad')
        self.assertTrue(self.queue.find('bad')['ack'])
        # Open quarantine list is empty but the resolved record is retained.
        self.assertEqual(self.collector.list_quarantine(), [])
        self.assertEqual(self.collector.list_quarantine(include_resolved=True)[0]['state'], 'resolved')
        record = self.collector.get_quarantine(quarantine_id)
        self.assertEqual(record['state'], 'resolved')
        self.assertIn('bad', [d['delivery_id'] for d in self.collector.list_deliveries(ACKNOWLEDGED)])

    def test_operator_retry_reruns_business_receipt_after_contract_fix(self):
        # A schema-valid event for a contract that does not exist yet is
        # quarantined at the reliable-receipt stage rather than dropped.
        body = provider_event()
        body['contract_id'] = 'contract-late'
        self.queue.find('d1')['body'] = body
        self.queue.deliveries = [self.queue.find('d1')]
        self.collector.run(self.queue)
        quarantine_id = self.collector.list_quarantine()[0]['quarantine_id']
        self.assertEqual(self.collector.get_delivery('d1')['state'], QUARANTINED)
        self.assertEqual(self.event_count(), 0)
        # Operator registers the contract, then asks for a retry.
        registry.create(self.store, 'contract-late', 'provider-a', 'consumer-a', 'CNY', definition())
        rates.add(self.store, 'rate-late', 'contract-late', 'records',
                  '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z', tariff())
        decision = self.collector.resolve_quarantine(quarantine_id, 'retry', 'contract registered')
        self.assertEqual(decision['action'], 'retry')
        acceptance = self.collector.accept('d1')
        self.assertEqual(acceptance['event_id'], 'ev1')
        self.collector.acknowledge(self.queue, 'd1')
        self.assertTrue(self.queue.find('d1')['ack'])
        self.assertEqual(self.event_count(), 1)

    def test_quarantine_auto_ack_is_configurable_and_never_silent(self):
        auto = Collector(self.store)
        auto.configure(quarantine_ack=True)
        self.assertTrue(auto.status()['quarantine_ack'])
        result = auto.run(self.queue)
        self.assertEqual(result['stats']['quarantined'], 1)
        self.assertTrue(self.queue.find('bad')['ack'])
        # The quarantine record remains queryable even though it was acked.
        self.assertEqual(len(auto.list_quarantine()), 1)
        # The delivery is terminal, not left on the pending-ack worklist.
        self.assertEqual(auto.get_delivery('bad')['state'], ACKNOWLEDGED)
        self.assertEqual(auto.pending_acknowledgements(), [])


class RunControl(CollectionCase):
    def test_pause_resume_and_budgets(self):
        self.collector.pause('freeze for maintenance')
        paused = self.collector.run(self.queue)
        self.assertEqual(paused['stop_reason'], 'paused')
        self.assertEqual(paused['stats']['receive_calls'], 0)
        self.collector.resume()
        # One message budget: the first pass captures only d1 (receive limit 1).
        first = self.collector.run(self.queue, limit=1, max_messages=1)
        self.assertEqual(first['stop_reason'], 'message_budget')
        self.assertEqual(first['stats']['captured'], 1)
        # Attempt budget: allow only one remote call, so no acknowledgement yet.
        collector = Collector(self.store)
        tiny = collector.run(self.queue, limit=1, max_attempts=1)
        self.assertIn(tiny['stop_reason'], {'attempt_budget', 'drained'})
        # Finish the remaining work.
        finished = self.collector.run(self.queue, limit=10)
        self.assertEqual(finished['stop_reason'], 'drained')
        self.assertTrue(self.queue.find('d1')['ack'])
        # Run history is retained and queryable.
        history = self.collector.runs()
        self.assertGreaterEqual(len(history), 2)
        self.assertIn('stats', history[0])

    def test_time_budget_uses_injected_clock(self):
        ticks = iter([0, 0, 6])
        result = self.collector.run(self.queue, deadline=5, monotonic=lambda: next(ticks))
        self.assertEqual(result['stop_reason'], 'time_budget')
        # The pending acknowledgements list names what still must be sent.
        self.assertIsInstance(result['pending_acknowledgements'], list)


class FrozenBilling(CollectionCase):
    def test_collection_retries_do_not_change_a_frozen_invoice(self):
        # Build and freeze a closed invoice through the normal bilateral flow.
        self.pair()
        self.close_invoice('invoice-1', 'run-1')
        closed = invoices.get(self.store, 'invoice-1')
        # Now a queue delivery arrives for the same business event and is
        # redriven repeatedly (ack lost / lease expiry / redelivery).
        duplicate_event = {
            'event_id': 'provider-1', 'contract_id': 'contract-a', 'side': 'provider',
            'sequence': 1, 'usage_id': 'usage-a', 'revision': 1, 'action': 'report',
            'usage': {'meter': 'records', 'at': '2026-11-10T12:00:00Z', 'quantity': '10',
                      'purpose': 'research', 'region': 'CN', 'dimensions': {'source': 'synthetic'}},
        }
        self.queue.deliveries = [{'delivery_id': 'ddup', 'body': duplicate_event,
                                  'expires_at': 0, 'receipt_handle': None, 'ack': False}]
        for _ in range(3):
            self.queue.advance_clock(11)
            redelivery = self.queue.receive(1)['messages'][0]
            self.collector.capture(redelivery, self.queue.now)
            acceptance = self.collector.accept('ddup')
            self.assertTrue(acceptance['duplicate'])
        # Exactly one event, one version and an unchanged frozen invoice digest.
        self.assertEqual(self.event_count(), 2)  # provider-1 + consumer-1 from pair()
        self.assertEqual(self.store.scalar('SELECT COUNT(*) FROM usage_versions'), 2)
        self.assertEqual(invoices.get(self.store, 'invoice-1')['digest'], closed['digest'])
        self.assertEqual(self.store.scalar('SELECT total_cents FROM invoices'), 250)


if __name__ == '__main__':
    unittest.main()

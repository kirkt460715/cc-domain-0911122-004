import json
from meterbook.values import (
    canonical, digest, reject, number, key, prose, MeterError,
)
from meterbook.events import receiver
from meterbook.collection.client import SourceError, LeaseLost

DDL = '''
CREATE TABLE IF NOT EXISTS queue_deliveries(
    delivery_id TEXT PRIMARY KEY,
    body TEXT NOT NULL,
    body_digest TEXT NOT NULL,
    state TEXT NOT NULL,
    current_handle TEXT,
    accepted_event_id TEXT,
    conflict TEXT,
    first_seen_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS collection_attempts(
    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id TEXT NOT NULL,
    receipt_handle TEXT NOT NULL UNIQUE,
    lease_expires_at INTEGER NOT NULL,
    lease_renewals INTEGER NOT NULL,
    state TEXT NOT NULL,
    accepted_event_id TEXT,
    duplicate INTEGER,
    note TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS attempt_delivery ON collection_attempts(delivery_id, attempt_id);
CREATE TABLE IF NOT EXISTS quarantined_envelopes(
    quarantine_id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL,
    body TEXT NOT NULL,
    body_digest TEXT NOT NULL,
    reason TEXT NOT NULL,
    state TEXT NOT NULL,
    resolved_at TEXT,
    resolution TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS collection_runtime(
    id INTEGER PRIMARY KEY CHECK(id=1),
    state TEXT NOT NULL,
    provider_now INTEGER NOT NULL,
    quarantine_ack INTEGER NOT NULL,
    renew_threshold INTEGER NOT NULL,
    renew_limit INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS collection_runs(
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    stop_reason TEXT,
    stats TEXT NOT NULL,
    pending_ack TEXT NOT NULL
);
'''

# Runtime control states.
ACTIVE_STATE = 'active'
PAUSED_STATE = 'paused'

# Delivery lifecycle states.
HELD = 'held'
LEASE_LOST = 'lease_lost'
ACK_PENDING = 'ack_pending'
ACKNOWLEDGED = 'acknowledged'
QUARANTINED = 'quarantined'
CONFLICT = 'conflict'

# Attempt lifecycle states.
A_HELD = 'held'
A_LOST = 'lost'
A_SUPERSEDED = 'superseded'
A_ACK_PENDING = 'ack_pending'
A_ACKNOWLEDGED = 'acknowledged'
A_QUARANTINED = 'quarantined'

TERMINAL_DELIVERY = {ACKNOWLEDGED, CONFLICT}


class Collector:
    """Durable collector for the provider usage delivery queue.

    The three contractual stages are persisted separately:
      1. delivery capture  -> queue_deliveries / collection_attempts
      2. business receipt  -> events table (receiver.receive), id linked back
      3. provider ack      -> attempt/delivery state acknowledged

    No queue message is acknowledged before its body and business result are
    durable, and every remote call is recoverable from local state.
    """

    def __init__(self, store, quarantine_ack=False, renew_threshold=3, renew_limit=5):
        self.store = store
        if store.depth:
            reject('collector_transaction', 'Initialize the collector outside transactions')
        store.db.executescript(DDL)
        if store.scalar('SELECT id FROM collection_runtime') is None:
            store.execute(
                'INSERT INTO collection_runtime VALUES(1,?,?,?,?,?,?)',
                (ACTIVE_STATE, 0, 1 if quarantine_ack else 0,
                 number(renew_threshold, 'renew_threshold', 0, 1000000000),
                 number(renew_limit, 'renew_limit', 0, 1000000000), store.clock()),
            )

    # ------------------------------------------------------------------ config
    def _runtime(self):
        row = self.store.one('SELECT * FROM collection_runtime WHERE id=1')
        if row is None:
            reject('collector_runtime', 'Collector runtime row is missing')
        return row

    def configure(self, quarantine_ack=None, renew_threshold=None, renew_limit=None):
        with self.store.atomic():
            row = self._runtime()
            if quarantine_ack is not None:
                if not isinstance(quarantine_ack, bool):
                    reject('collector_config', 'quarantine_ack must be a boolean')
            values = {
                'quarantine_ack': int(quarantine_ack) if quarantine_ack is not None else row['quarantine_ack'],
                'renew_threshold': number(renew_threshold, 'renew_threshold', 0, 1000000000) if renew_threshold is not None else row['renew_threshold'],
                'renew_limit': number(renew_limit, 'renew_limit', 0, 1000000000) if renew_limit is not None else row['renew_limit'],
            }
            self.store.execute(
                'UPDATE collection_runtime SET quarantine_ack=?,renew_threshold=?,renew_limit=?,updated_at=? WHERE id=1',
                (values['quarantine_ack'], values['renew_threshold'], values['renew_limit'], self.store.clock()),
            )
            self.store.log('collect.configured', values)
            return self.status()

    def pause(self, reason='operator pause'):
        with self.store.atomic():
            self.store.execute(
                'UPDATE collection_runtime SET state=?,updated_at=? WHERE id=1',
                (PAUSED_STATE, self.store.clock()),
            )
            self.store.log('collect.paused', {'reason': prose(reason, 'reason')})
            return self.status()

    def resume(self, reason='operator resume'):
        with self.store.atomic():
            self.store.execute(
                'UPDATE collection_runtime SET state=?,updated_at=? WHERE id=1',
                (ACTIVE_STATE, self.store.clock()),
            )
            self.store.log('collect.resumed', {'reason': prose(reason, 'reason')})
            return self.status()

    # ------------------------------------------------------------------ queries
    def status(self):
        runtime = self._runtime()
        counts = {
            state: self.store.scalar(
                'SELECT COUNT(*) FROM queue_deliveries WHERE state=?', (state,)) or 0
            for state in (HELD, LEASE_LOST, ACK_PENDING, ACKNOWLEDGED, QUARANTINED, CONFLICT)
        }
        return {
            'state': runtime['state'],
            'provider_now': runtime['provider_now'],
            'quarantine_ack': bool(runtime['quarantine_ack']),
            'renew_threshold': runtime['renew_threshold'],
            'renew_limit': runtime['renew_limit'],
            'deliveries': counts,
            'quarantined': self.store.scalar('SELECT COUNT(*) FROM quarantined_envelopes WHERE state=?', ('open',)) or 0,
            'pending_acknowledgements': len(self.pending_acknowledgements()),
        }

    def list_deliveries(self, state=None, limit=100):
        number(limit, 'limit', 1, 1000)
        sql = 'SELECT delivery_id,state,current_handle,accepted_event_id,conflict,updated_at FROM queue_deliveries'
        params = []
        if state is not None:
            if state not in (HELD, LEASE_LOST, ACK_PENDING, ACKNOWLEDGED, QUARANTINED, CONFLICT):
                reject('collector_state', 'Unknown delivery state filter', state=state)
            sql += ' WHERE state=?'
            params.append(state)
        sql += ' ORDER BY first_seen_at,delivery_id LIMIT ?'
        params.append(limit)
        return self.store.all(sql, tuple(params))

    def get_delivery(self, delivery_id):
        row = self.store.one('SELECT * FROM queue_deliveries WHERE delivery_id=?', (delivery_id,))
        if row is None:
            reject('delivery_missing', 'Collected delivery does not exist', delivery_id=delivery_id)
        row['body'] = json.loads(row['body'])
        if digest(row['body']) != row['body_digest']:
            reject('delivery_corrupt', 'Stored delivery body digest differs')
        row['attempts'] = self.store.all(
            'SELECT attempt_id,receipt_handle,lease_expires_at,lease_renewals,state,accepted_event_id,duplicate,note,created_at FROM collection_attempts WHERE delivery_id=? ORDER BY attempt_id',
            (delivery_id,))
        row['quarantine'] = self.store.all(
            'SELECT quarantine_id,reason,state,resolution,created_at,resolved_at FROM quarantined_envelopes WHERE delivery_id=? ORDER BY created_at',
            (delivery_id,))
        return row

    def list_quarantine(self, include_resolved=False, limit=100):
        number(limit, 'limit', 1, 1000)
        sql = 'SELECT quarantine_id,delivery_id,body_digest,reason,state,resolution,created_at,resolved_at FROM quarantined_envelopes'
        if not include_resolved:
            sql += " WHERE state='open'"
        sql += ' ORDER BY created_at,quarantine_id LIMIT ?'
        return self.store.all(sql, (limit,))

    def get_quarantine(self, quarantine_id):
        row = self.store.one('SELECT * FROM quarantined_envelopes WHERE quarantine_id=?', (quarantine_id,))
        if row is None:
            reject('quarantine_missing', 'Quarantine record does not exist')
        row['body'] = json.loads(row['body'])
        return row

    def pending_acknowledgements(self):
        # Only deliveries that the collector itself decided must be confirmed.
        # Quarantined envelopes wait for an explicit operator decision and are
        # never silently (re-)acknowledged by a run.
        return self.store.all(
            "SELECT d.delivery_id,a.receipt_handle,a.lease_expires_at,a.state AS attempt_state,d.state "
            "FROM queue_deliveries d JOIN collection_attempts a ON a.receipt_handle=d.current_handle "
            "WHERE d.state=? ORDER BY d.updated_at,d.delivery_id",
            (ACK_PENDING,),
        )

    def quarantine_worklist(self):
        return self.store.all(
            "SELECT d.delivery_id,a.receipt_handle,a.lease_expires_at,a.state AS attempt_state,d.state "
            "FROM queue_deliveries d JOIN collection_attempts a ON a.receipt_handle=d.current_handle "
            "WHERE d.state=? ORDER BY d.updated_at,d.delivery_id",
            (QUARANTINED,),
        )

    # ------------------------------------------------------------- lease helpers
    def _mark_lost(self, delivery_id, attempt_id, note):
        self.store.execute(
            "UPDATE collection_attempts SET state=?,note=?,updated_at=? WHERE attempt_id=?",
            (A_LOST, note, self.store.clock(), attempt_id),
        )
        # Any non-terminal delivery that loses its lease goes back to waiting
        # for a redelivery: held, ack-pending and quarantined alike. Terminal
        # acknowledged/conflict deliveries are never rewritten.
        self.store.execute(
            "UPDATE queue_deliveries SET state=?,updated_at=? WHERE delivery_id=? AND state NOT IN (?,?)",
            (LEASE_LOST, self.store.clock(), delivery_id, ACKNOWLEDGED, CONFLICT),
        )
        self.store.log('collect.lease_lost', {'delivery_id': delivery_id, 'attempt_id': attempt_id, 'note': note})

    def sweep(self, provider_now=None):
        """Expire local leases using the latest known queue clock.

        Covers both held and accepted-but-unconfirmed attempts: the business
        receipt may already be durable while the acknowledgement is still
        outstanding. After expiry we never assume the lease is still ours; the
        attempt is marked lost and the delivery waits for a redelivery.
        """
        with self.store.atomic():
            if provider_now is None:
                provider_now = self._runtime()['provider_now']
            expired = self.store.all(
                "SELECT attempt_id,delivery_id FROM collection_attempts "
                "WHERE state IN (?,?) AND lease_expires_at<=?",
                (A_HELD, A_ACK_PENDING, provider_now),
            )
            for row in expired:
                self._mark_lost(row['delivery_id'], row['attempt_id'], 'lease_expired_locally')
            self.store.execute('UPDATE collection_runtime SET provider_now=?,updated_at=? WHERE id=1',
                               (number(provider_now, 'provider_now', 0), self.store.clock()))
            return {'expired': len(expired), 'provider_now': provider_now}

    def renew_delivery(self, source, delivery_id):
        """Renew a held lease before it reaches expiry."""
        with self.store.atomic():
            delivery = self.get_delivery(delivery_id)
            attempt = self._current_attempt(delivery)
            if attempt['state'] == A_LOST:
                raise LeaseLost('lease_lost', 'Lease already marked lost')
            if attempt['state'] != A_HELD:
                reject('lease_state', 'Only a held lease can be renewed', state=attempt['state'])
            runtime = self._runtime()
            if attempt['lease_renewals'] >= runtime['renew_limit']:
                reject('lease_renew_limit', 'Delivery already used its renewal budget')
            handle = attempt['receipt_handle']
        try:
            expires_at = source.renew(handle)
        except LeaseLost as exc:
            with self.store.atomic():
                self._mark_lost(delivery_id, attempt['attempt_id'], exc.code)
            raise
        with self.store.atomic():
            self.store.execute(
                'UPDATE collection_attempts SET lease_expires_at=?,lease_renewals=lease_renewals+1,updated_at=? WHERE attempt_id=?',
                (expires_at, self.store.clock(), attempt['attempt_id']),
            )
            self.store.log('collect.lease_renewed',
                           {'delivery_id': delivery_id, 'attempt_id': attempt['attempt_id'], 'expires_at': expires_at})
            return self.get_delivery(delivery_id)

    def _current_attempt(self, delivery):
        attempt_id = self.store.scalar(
            'SELECT MAX(attempt_id) FROM collection_attempts WHERE delivery_id=?', (delivery['delivery_id'],))
        row = self.store.one('SELECT * FROM collection_attempts WHERE attempt_id=?', (attempt_id,))
        if row is None:
            reject('delivery_attempt', 'Delivery has no collection attempt')
        return row

    def _renew_if_near_expiry(self, source, delivery, attempt, stats):
        runtime = self._runtime()
        now_value = runtime['provider_now']
        if attempt['lease_expires_at'] - now_value > runtime['renew_threshold']:
            return attempt
        if attempt['lease_renewals'] >= runtime['renew_limit']:
            return attempt
        stats['renew_calls'] += 1
        try:
            expires_at = source.renew(attempt['receipt_handle'])
        except LeaseLost as exc:
            self._mark_lost(delivery['delivery_id'], attempt['attempt_id'], exc.code)
            return None
        self.store.execute(
            'UPDATE collection_attempts SET lease_expires_at=?,lease_renewals=lease_renewals+1,updated_at=? WHERE attempt_id=?',
            (expires_at, self.store.clock(), attempt['attempt_id']),
        )
        self.store.log('collect.lease_renewed',
                       {'delivery_id': delivery['delivery_id'], 'attempt_id': attempt['attempt_id'], 'expires_at': expires_at})
        attempt = dict(attempt)
        attempt['lease_expires_at'] = expires_at
        attempt['lease_renewals'] += 1
        return attempt

    # ------------------------------------------------------------- capture stage
    def capture(self, message, provider_now):
        """Persist one queue message and its lease. Returns (delivery, new?)."""
        body = message['body']
        body_digest = digest(body)
        delivery_id = key(message['delivery_id'], 'delivery_id')
        handle = key(message['receipt_handle'], 'receipt_handle')
        expires_at = number(message['expires_at'], 'expires_at', 0)
        with self.store.atomic():
            existing = self.store.one('SELECT * FROM queue_deliveries WHERE delivery_id=?', (delivery_id,))
            if existing is None:
                self.store.execute(
                    'INSERT INTO queue_deliveries VALUES(?,?,?,?,?,?,?,?,?)',
                    (delivery_id, canonical(body), body_digest, HELD, handle, None, None,
                     self.store.clock(), self.store.clock()),
                )
                attempt_id = self._insert_attempt(delivery_id, handle, expires_at)
                self.store.log('collect.delivery_received',
                               {'delivery_id': delivery_id, 'attempt_id': attempt_id, 'body_digest': body_digest})
                return self.get_delivery(delivery_id), True
            return self._capture_redelivery(existing, body, body_digest, handle, expires_at)

    def _insert_attempt(self, delivery_id, handle, expires_at, state=A_HELD, note=None):
        cur = self.store.execute(
            'INSERT INTO collection_attempts(delivery_id,receipt_handle,lease_expires_at,lease_renewals,state,note,created_at,updated_at) VALUES(?,?,?,0,?,?,?,?)',
            (delivery_id, handle, expires_at, state, note, self.store.clock(), self.store.clock()),
        )
        return cur.lastrowid

    def _capture_redelivery(self, existing, body, body_digest, handle, expires_at):
        delivery_id = existing['delivery_id']
        if existing['current_handle'] == handle:
            # Same handle observed again: nothing new to capture.
            return self.get_delivery(delivery_id), False
        # A redelivery always invalidates every previous handle: held or
        # ack-pending attempts are superseded, already-lost ones stay recorded
        # as lost so the local expiry history is not rewritten.
        self.store.execute(
            "UPDATE collection_attempts SET state=?,updated_at=? WHERE delivery_id=? AND state IN (?,?)",
            (A_SUPERSEDED, self.store.clock(), delivery_id, A_HELD, A_ACK_PENDING),
        )
        if existing['body_digest'] != body_digest:
            # Same delivery number, different payload: keep the conflict, do
            # not process or acknowledge either version automatically.
            conflict = {
                'code': 'delivery_body_conflict',
                'previous_digest': existing['body_digest'],
                'redelivery_digest': body_digest,
                'receipt_handle': handle,
            }
            attempt_id = self._insert_attempt(delivery_id, handle, expires_at, A_HELD, 'body_conflict')
            self.store.execute(
                'UPDATE queue_deliveries SET state=?,current_handle=?,conflict=?,updated_at=? WHERE delivery_id=?',
                (CONFLICT, handle, canonical(conflict), self.store.clock(), delivery_id),
            )
            self.store.log('collect.delivery_conflict', {'delivery_id': delivery_id, 'attempt_id': attempt_id, **conflict})
            return self.get_delivery(delivery_id), True
        attempt_id = self._insert_attempt(delivery_id, handle, expires_at)
        # A redelivery after an acknowledged attempt means the ack response was
        # lost; the new handle reopens processing, which stays idempotent.
        self.store.execute(
            'UPDATE queue_deliveries SET state=?,current_handle=?,conflict=NULL,updated_at=? WHERE delivery_id=?',
            (HELD, handle, self.store.clock(), delivery_id),
        )
        self.store.log('collect.delivery_redelivered',
                       {'delivery_id': delivery_id, 'attempt_id': attempt_id, 'previous_state': existing['state']})
        return self.get_delivery(delivery_id), True

    # ------------------------------------------------------------ business stage
    def _quarantine(self, delivery, attempt, error):
        raw_body = self.store.scalar('SELECT body FROM queue_deliveries WHERE delivery_id=?',
                                     (delivery['delivery_id'],))
        body = json.loads(raw_body)
        quarantine_id = 'q-' + digest([delivery['delivery_id'], delivery['body_digest'], attempt['attempt_id']])
        reason = canonical(error.payload()) if isinstance(error, MeterError) else canonical({'code': error.code, 'message': str(error)})
        with self.store.atomic():
            if self.store.one('SELECT quarantine_id FROM quarantined_envelopes WHERE quarantine_id=?', (quarantine_id,)) is None:
                self.store.execute(
                    'INSERT INTO quarantined_envelopes VALUES(?,?,?,?,?,?,?,?,?)',
                    (quarantine_id, delivery['delivery_id'], canonical(body), delivery['body_digest'],
                     reason, 'open', None, None, self.store.clock()),
                )
            self.store.execute(
                "UPDATE collection_attempts SET state=?,updated_at=? WHERE attempt_id=?",
                (A_QUARANTINED, self.store.clock(), attempt['attempt_id']),
            )
            self.store.execute(
                "UPDATE queue_deliveries SET state=?,updated_at=? WHERE delivery_id=? AND state NOT IN (?,?)",
                (QUARANTINED, self.store.clock(), delivery['delivery_id'], ACKNOWLEDGED, CONFLICT),
            )
            self.store.log('collect.quarantined',
                           {'delivery_id': delivery['delivery_id'], 'quarantine_id': quarantine_id})
        return quarantine_id

    def accept(self, delivery_id):
        """Run the Meterbook business receipt for one held delivery.

        Idempotent under contention: a delivery another worker already moved to
        ack_pending/acknowledged resolves to its stored event as a duplicate
        instead of raising, so racing collectors never produce a second event.
        """
        with self.store.atomic():
            delivery = self.get_delivery(delivery_id)
            if delivery['state'] in (ACK_PENDING, ACKNOWLEDGED) and delivery['accepted_event_id']:
                self.store.execute(
                    "UPDATE collection_attempts SET duplicate=1 WHERE attempt_id=? AND duplicate IS NULL",
                    (self._current_attempt(delivery)['attempt_id'],))
                return {'delivery_id': delivery_id,
                        'event_id': delivery['accepted_event_id'], 'duplicate': True}
            if delivery['state'] not in (HELD, QUARANTINED):
                reject('accept_state', 'Delivery is not open for business receipt', state=delivery['state'])
            attempt = self._current_attempt(delivery)
            if attempt['state'] not in (A_HELD, A_QUARANTINED):
                reject('accept_state', 'Current attempt cannot be accepted', state=attempt['state'])
            runtime = self._runtime()
            if attempt['lease_expires_at'] <= runtime['provider_now']:
                self._mark_lost(delivery_id, attempt['attempt_id'], 'lease_expired_before_accept')
                reject('lease_lost', 'Lease expired before business receipt')
        raw_body = self.store.scalar('SELECT body FROM queue_deliveries WHERE delivery_id=?', (delivery_id,))
        body = json.loads(raw_body)
        try:
            result = receiver.receive(self.store, body)
        except MeterError as exc:
            with self.store.atomic():
                delivery = self.get_delivery(delivery_id)
                attempt = self._current_attempt(delivery)
                quarantine_id = self._quarantine(delivery, attempt, exc)
            return {'delivery_id': delivery_id, 'quarantined': quarantine_id, 'error': exc.payload()}
        event_id = result['event']['event_id']
        duplicate = 1 if result['duplicate'] else 0
        with self.store.atomic():
            self.store.execute(
                "UPDATE collection_attempts SET state=?,accepted_event_id=?,duplicate=?,updated_at=? WHERE attempt_id=?",
                (A_ACK_PENDING, event_id, duplicate, self.store.clock(), attempt['attempt_id']),
            )
            self.store.execute(
                "UPDATE queue_deliveries SET state=?,accepted_event_id=?,updated_at=? WHERE delivery_id=?",
                (ACK_PENDING, event_id, self.store.clock(), delivery_id),
            )
            self.store.log('collect.event_accepted',
                           {'delivery_id': delivery_id, 'event_id': event_id, 'duplicate': bool(duplicate)})
            return {'delivery_id': delivery_id, 'event_id': event_id, 'duplicate': bool(duplicate)}

    # ------------------------------------------------------------- acknowledge
    def acknowledge(self, source, delivery_id):
        """Acknowledge a durably handled delivery using its current handle."""
        with self.store.atomic():
            delivery = self.get_delivery(delivery_id)
            if delivery['state'] == LEASE_LOST:
                raise LeaseLost('lease_lost', 'Delivery lease was lost and awaits redelivery')
            if delivery['state'] not in (ACK_PENDING, QUARANTINED):
                reject('ack_state', 'Delivery has nothing to acknowledge', state=delivery['state'])
            runtime = self._runtime()
            attempt = self._current_attempt(delivery)
            if attempt['lease_expires_at'] <= runtime['provider_now']:
                self._mark_lost(delivery_id, attempt['attempt_id'], 'lease_expired_before_ack')
                raise LeaseLost('lease_lost', 'Lease expired before acknowledgement')
            handle = attempt['receipt_handle']
        try:
            source.acknowledge(handle)
        except LeaseLost as exc:
            with self.store.atomic():
                self._mark_lost(delivery_id, attempt['attempt_id'], exc.code)
            raise
        except SourceError:
            # The answer never came back. Keep the delivery ack-pending so a
            # later run reconciles; nothing is deleted locally.
            with self.store.atomic():
                self.store.execute(
                    "UPDATE collection_attempts SET note=?,updated_at=? WHERE attempt_id=?",
                    ('ack_response_lost', self.store.clock(), attempt['attempt_id']),
                )
            raise
        with self.store.atomic():
            self.store.execute(
                "UPDATE collection_attempts SET state=?,updated_at=? WHERE attempt_id=?",
                (A_ACKNOWLEDGED, self.store.clock(), attempt['attempt_id']),
            )
            self.store.execute(
                "UPDATE queue_deliveries SET state=?,updated_at=? WHERE delivery_id=?",
                (ACKNOWLEDGED, self.store.clock(), delivery_id),
            )
            self.store.log('collect.acknowledged',
                           {'delivery_id': delivery_id, 'attempt_id': attempt['attempt_id']})
            return self.get_delivery(delivery_id)

    def resolve_quarantine(self, quarantine_id, action, reason):
        """Operator decision for a quarantined envelope.

        action='retry' re-runs the business receipt (e.g. after a contract fix);
        action='ack' acknowledges the delivery with its current handle and
        closes the quarantine without ever producing usage.
        """
        reason = prose(reason, 'reason')
        if action not in {'retry', 'ack'}:
            reject('quarantine_action', 'Action must be retry or ack')
        with self.store.atomic():
            record = self.get_quarantine(quarantine_id)
            if record['state'] != 'open':
                reject('quarantine_state', 'Quarantine record is already resolved')
            delivery = self.get_delivery(record['delivery_id'])
            self.store.execute(
                "UPDATE quarantined_envelopes SET state='resolved',resolved_at=?,resolution=? WHERE quarantine_id=?",
                (self.store.clock(), canonical({'action': action, 'reason': reason}), quarantine_id),
            )
            self.store.log('collect.quarantine_resolved',
                           {'quarantine_id': quarantine_id, 'delivery_id': record['delivery_id'], 'action': action})
            if action == 'retry':
                # Reopen the attempt so accept() will run the business receipt.
                self.store.execute(
                    "UPDATE collection_attempts SET state=? WHERE attempt_id=? AND state=?",
                    (A_HELD, self._current_attempt(delivery)['attempt_id'], A_QUARANTINED),
                )
                self.store.execute(
                    "UPDATE queue_deliveries SET state=? WHERE delivery_id=? AND state=?",
                    (HELD, record['delivery_id'], QUARANTINED),
                )
                return {'quarantine_id': quarantine_id, 'action': action}
        if action == 'ack':
            # Reuse the normal ack path; the body was never accepted as usage.
            with self.store.atomic():
                attempt = self._current_attempt(self.get_delivery(record['delivery_id']))
                self.store.execute(
                    "UPDATE collection_attempts SET state=? WHERE attempt_id=?",
                    (A_ACK_PENDING, attempt['attempt_id']),
                )
                self.store.execute(
                    "UPDATE queue_deliveries SET state=? WHERE delivery_id=? AND state=?",
                    (ACK_PENDING, record['delivery_id'], QUARANTINED),
                )
            return {'quarantine_id': quarantine_id, 'action': action,
                    'delivery_id': record['delivery_id']}

    # -------------------------------------------------------------------- run
    def run(self, source, limit=10, max_attempts=100, max_messages=None, deadline=None, monotonic=None, run_id=None):
        """One bounded collection pass.

        Budgets: max_attempts bounds remote calls (receive/renew/ack each count),
        max_messages bounds newly captured deliveries, deadline (seconds via the
        injectable monotonic clock) bounds elapsed time. The pass always returns
        a summary naming acknowledgements that still need to be sent.
        """
        number(limit, 'limit', 1, 1000)
        number(max_attempts, 'max_attempts', 1, 1000000)
        if max_messages is not None:
            number(max_messages, 'max_messages', 1, 1000000)
        if monotonic is None:
            import time
            monotonic = time.monotonic
        started = monotonic()
        run_id = run_id or ('run-' + digest([started, self.store.clock()]))[:40]
        acknowledged_seen = self.store.scalar(
            "SELECT COUNT(*) FROM queue_deliveries WHERE state=?", (ACKNOWLEDGED,))
        stats = {'receive_calls': 0, 'renew_calls': 0, 'ack_calls': 0,
                 'captured': 0, 'accepted': 0, 'duplicates': 0,
                 'quarantined': 0, 'acknowledged': 0, 'lease_lost': 0,
                 'reconciled_acks': 0, 'conflicts': 0}
        stop_reason = 'drained'
        with self.store.atomic():
            self.store.execute(
                'INSERT INTO collection_runs(run_id,started_at,stats,pending_ack) VALUES(?,?,?,?)',
                (run_id, self.store.clock(), canonical(stats), canonical([])),
            )
        try:
            while True:
                runtime = self._runtime()
                if runtime['state'] == PAUSED_STATE:
                    stop_reason = 'paused'
                    break
                remote_calls = stats['receive_calls'] + stats['renew_calls'] + stats['ack_calls']
                if remote_calls >= max_attempts:
                    stop_reason = 'attempt_budget'
                    break
                if deadline is not None and monotonic() - started >= deadline:
                    stop_reason = 'time_budget'
                    break
                if max_messages is not None and stats['captured'] >= max_messages:
                    stop_reason = 'message_budget'
                    break
                # First reconcile acknowledgements whose response never came back.
                pending = self.pending_acknowledgements()
                reconciled = False
                for row in pending:
                    if stats['receive_calls'] + stats['renew_calls'] + stats['ack_calls'] >= max_attempts:
                        stop_reason = 'attempt_budget'
                        break
                    try:
                        self.acknowledge(source, row['delivery_id'])
                        stats['ack_calls'] += 1
                        stats['acknowledged'] += 1
                        stats['reconciled_acks'] += 1
                        reconciled = True
                    except LeaseLost:
                        stats['lease_lost'] += 1
                    except SourceError:
                        stop_reason = 'source_unreachable'
                        break
                if stop_reason != 'drained':
                    break
                try:
                    envelope = source.receive(limit)
                except SourceError:
                    stop_reason = 'source_unreachable'
                    break
                stats['receive_calls'] += 1
                self.sweep(envelope['now'])
                messages = envelope['messages']
                if not messages:
                    # Nothing held and nothing offered: the queue is drained.
                    break
                for message in messages:
                    delivery, is_new = self.capture(message, envelope['now'])
                    if is_new:
                        stats['captured'] += 1
                    if delivery['state'] == CONFLICT:
                        stats['conflicts'] += 1
                        continue
                    attempt = self._current_attempt(delivery)
                    if attempt['state'] != A_HELD:
                        continue
                    attempt = self._renew_if_near_expiry(source, delivery, attempt, stats)
                    if attempt is None:
                        stats['lease_lost'] += 1
                        continue
                    if self._runtime()['state'] == PAUSED_STATE:
                        stop_reason = 'paused'
                        break
                    acceptance = self.accept(delivery['delivery_id'])
                    if 'error' in acceptance:
                        stats['quarantined'] += 1
                        if self._runtime()['quarantine_ack']:
                            # Operator policy says confirm malformed envelopes:
                            # move onto the standard acknowledgement worklist so
                            # the same durable ack path and reconciliation apply.
                            self.store.execute(
                                "UPDATE queue_deliveries SET state=? WHERE delivery_id=? AND state=?",
                                (ACK_PENDING, delivery['delivery_id'], QUARANTINED))
                            self.store.execute(
                                "UPDATE collection_attempts SET state=? WHERE attempt_id=?",
                                (A_ACK_PENDING, attempt['attempt_id']))
                            try:
                                self.acknowledge(source, delivery['delivery_id'])
                                stats['ack_calls'] += 1
                            except LeaseLost:
                                stats['lease_lost'] += 1
                            except SourceError:
                                stop_reason = 'source_unreachable'
                                break
                        continue
                    stats['accepted'] += 1
                    if acceptance['duplicate']:
                        stats['duplicates'] += 1
                    try:
                        self.acknowledge(source, delivery['delivery_id'])
                        stats['ack_calls'] += 1
                    except LeaseLost:
                        stats['lease_lost'] += 1
                    except SourceError:
                        stop_reason = 'source_unreachable'
                        break
                if stop_reason != 'drained':
                    break
                # A receive that returned messages but none progressed (e.g. all
                # were superseded/non-held) still made a remote call; the attempt
                # budget bounds any further looping.
                if not reconciled and remote_calls + 1 >= max_attempts:
                    stop_reason = 'attempt_budget'
                    break
        finally:
            pending = self.pending_acknowledgements()
            stats['acknowledged'] = self.store.scalar(
                "SELECT COUNT(*) FROM queue_deliveries WHERE state=?", (ACKNOWLEDGED,)) - acknowledged_seen
            with self.store.atomic():
                self.store.execute(
                    'UPDATE collection_runs SET finished_at=?,stop_reason=?,stats=?,pending_ack=? WHERE run_id=?',
                    (self.store.clock(), stop_reason, canonical(stats),
                     canonical([row['delivery_id'] for row in pending]), run_id),
                )
                self.store.log('collect.run_finished',
                               {'run_id': run_id, 'stop_reason': stop_reason, 'stats': stats,
                                'pending_ack': [row['delivery_id'] for row in pending]})
        return {'run_id': run_id, 'stop_reason': stop_reason, 'stats': stats,
                'pending_acknowledgements': pending, 'status': self.status()}

    def runs(self, limit=20):
        number(limit, 'limit', 1, 1000)
        rows = self.store.all(
            'SELECT run_id,started_at,finished_at,stop_reason,stats,pending_ack FROM collection_runs ORDER BY started_at DESC,run_id DESC LIMIT ?',
            (limit,))
        for row in rows:
            row['stats'] = json.loads(row['stats'])
            row['pending_ack'] = json.loads(row['pending_ack'])
        return rows

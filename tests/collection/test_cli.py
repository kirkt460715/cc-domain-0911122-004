import io
import tempfile
import unittest
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
from lab.http import serving
from lab.provider import Provider
from meterbook.cli import main
from meterbook.values import loads
from tests.support import definition


class CollectCli(unittest.TestCase):
    def setUp(self):
        self.provider = Provider()
        self.provider.deliveries[0]['body'] = {
            'event_id': 'ev1', 'contract_id': 'contract-a', 'side': 'provider',
            'sequence': 1, 'usage_id': 'usage1', 'revision': 1, 'action': 'report',
            'usage': {'meter': 'records', 'at': '2026-11-01T00:00:00Z',
                      'quantity': '12.50', 'purpose': 'research', 'region': 'CN'},
        }
        self.temporary = tempfile.TemporaryDirectory()
        self.db = str(Path(self.temporary.name) / 'cli.sqlite')
        self._run('batch', self._contract_request())

    def tearDown(self):
        self.temporary.cleanup()

    def _contract_request(self):
        path = Path(self.temporary.name) / 'contract.json'
        request = [{'operation': 'contract.create', 'arguments': {
            'contract_id': 'contract-a', 'provider': 'p', 'consumer': 'c',
            'currency': 'CNY', 'definition': definition()}}]
        path.write_text(__import__('meterbook.values', fromlist=['canonical']).canonical(request))
        return str(path)

    def _run(self, *args):
        output, errors = io.StringIO(), io.StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            code = main(['--database', self.db, *args])
        return code, output.getvalue(), errors.getvalue()

    def test_pause_blocks_run_and_status_is_queryable(self):
        code, out, _ = self._run('collect', 'pause')
        self.assertEqual(code, 0)
        self.assertEqual(loads(out)['state'], 'paused')
        with serving(self.provider) as base:
            code, out, _ = self._run('collect', 'run', '--source', base)
        self.assertEqual(code, 1)
        self.assertEqual(loads(out)['stop_reason'], 'paused')
        self._run('collect', 'resume')

    def test_run_then_deliveries_and_runs_report(self):
        with serving(self.provider) as base:
            code, out, _ = self._run('collect', 'run', '--source', base,
                                     '--limit', '10', '--max-attempts', '50')
        self.assertEqual(code, 0)
        report = loads(out)
        self.assertEqual(report['stats']['accepted'], 1)
        self.assertEqual(report['stats']['quarantined'], 1)
        code, out, _ = self._run('collect', 'deliveries', '--state', 'acknowledged')
        self.assertEqual([d['delivery_id'] for d in loads(out)['deliveries']], ['d1'])
        code, out, _ = self._run('collect', 'runs')
        self.assertGreaterEqual(len(loads(out)['runs']), 1)

    def test_run_without_source_is_structured_error(self):
        code, _, errors = self._run('collect', 'run')
        self.assertEqual(code, 2)
        self.assertEqual(loads(errors)['error']['code'], 'collector_source')


if __name__ == '__main__':
    unittest.main()

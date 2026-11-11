import unittest
from lab.http import serving
from lab.provider import Provider
from lab.demo import request
class SourceProtocol(unittest.TestCase):
    def test_source_roundtrip(self):
        with serving(Provider()) as base:
            result=request(base,'/status',None)
            self.assertIsNotNone(result)

    def test_expired_handle_cannot_ack_new_delivery(self):
        import urllib.error
        with serving(Provider()) as base:
            old=request(base,'/receive',{'limit':1})['messages'][0]
            request(base,'/clock',{'advance':11})
            new=request(base,'/receive',{'limit':1})['messages'][0]
            self.assertEqual(old['delivery_id'],new['delivery_id'])
            self.assertNotEqual(old['receipt_handle'],new['receipt_handle'])
            with self.assertRaises(urllib.error.HTTPError) as failure:request(base,'/ack',{'receipt_handle':old['receipt_handle']})
            self.assertEqual(failure.exception.code,409)
            request(base,'/renew',{'receipt_handle':new['receipt_handle']})
            request(base,'/ack',{'receipt_handle':new['receipt_handle']})
            self.assertTrue(request(base,'/status')['deliveries'][0]['ack'])

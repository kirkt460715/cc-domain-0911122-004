import threading,copy
from .http import ProtocolError,main
class Provider:
    def __init__(self):
        self.lock=threading.RLock();self.now=0;self.serial=0;self.drop_ack=False
        event={'event_id':'ev1','contract_id':'contract1','side':'provider','sequence':1,'usage_id':'usage1','revision':1,'action':'report','usage':{'meter':'records','at':'2026-11-01T00:00:00Z','quantity':'12.50','purpose':'research','region':'CN'}}
        self.deliveries=[{'delivery_id':'d1','body':event,'expires_at':0,'receipt_handle':None,'ack':False},{'delivery_id':'bad','body':{'unexpected':'合成坏信封'},'expires_at':0,'receipt_handle':None,'ack':False}]
    def handle(self,method,path,args,body):
        with self.lock:
            if path=='/clock':self.now+=int(body.get('advance',0));return {'now':self.now}
            if path=='/control':self.drop_ack=bool(body.get('drop_ack'));return {'ok':True}
            if path=='/receive':
                result=[]
                for x in self.deliveries:
                    if not x['ack'] and x['expires_at']<=self.now and len(result)<int(body.get('limit',1)):
                        self.serial+=1;x['receipt_handle']=f'h{self.serial}';x['expires_at']=self.now+10;result.append(copy.deepcopy(x))
                return {'now':self.now,'messages':result}
            if path in ['/renew','/ack']:
                x=next((x for x in self.deliveries if x['receipt_handle']==body.get('receipt_handle')),None)
                if not x or x['ack'] or x['expires_at']<=self.now:raise ProtocolError(409,'lease_lost')
                if path=='/renew':x['expires_at']=self.now+10;return {'expires_at':x['expires_at']}
                x['ack']=True
                if self.drop_ack:self.drop_ack=False;return None
                return {'acknowledged':x['delivery_id']}
            if path=='/status':return {'now':self.now,'deliveries':copy.deepcopy(self.deliveries)}
            raise ProtocolError(404,'route')
if __name__=='__main__':main(Provider())

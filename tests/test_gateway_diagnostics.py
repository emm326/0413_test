"""Self-test evidence rules; these tests never transmit RF."""
import unittest
from host.diagnostics import downlink_self_test


class FakeServer:
    def __init__(self,acks=True):
        self.run_id=0;self.cancel_run=None;self.gateway_eui='0011223344556677';self.commands={};self.acks=acks;self.requests=[]
    def request(self,action,**kwargs):
        self.requests.append((action,kwargs))
        if action=='self_test':
            self.commands['test']={'gateway_results':{str(n):'NONE' for n in range(3)} if self.acks else {},'state':'gateway_accepted'}
        return 'test'


class FakeManager:
    def __init__(self,sent):self.sent=sent;self.last_tx_test=None
    def read(self):
        return {'tx_allowed':True,'config':{'gateway_conf':{'gateway_ID':'0011223344556677'}},'config_sha256':'hash','device_id':'a'*32}
    def log_cursor(self):return 'cursor'
    def verify_tx(self,cursor):return {'passed':self.sent>=3,'sent':self.sent,'errors':0,'rejected':[]}


class DiagnosticsTests(unittest.TestCase):
    def test_antenna_is_required(self):
        with self.assertRaises(ValueError):downlink_self_test(FakeManager(3),FakeServer(),False)
    def test_counter_and_ack_are_both_required(self):
        self.assertFalse(downlink_self_test(FakeManager(0),FakeServer(),True,timeout=.01)['passed'])
        self.assertFalse(downlink_self_test(FakeManager(3),FakeServer(False),True,timeout=.01)['passed'])
        result=downlink_self_test(FakeManager(3),FakeServer(),True,timeout=1)
        self.assertTrue(result['passed']);self.assertFalse(result['node_confirmed']);self.assertFalse(result['rf_power_measured'])
    def test_wrong_device_is_rejected(self):
        server=FakeServer();server.gateway_eui='ffffffffffffffff'
        with self.assertRaises(ValueError):downlink_self_test(FakeManager(3),server,True)


    def test_exported_result_does_not_form_a_cycle_with_command_evidence(self):
        import json
        manager=FakeManager(3);server=FakeServer()
        result=downlink_self_test(manager,server,True,timeout=1)
        action,options=server.requests[-1]
        self.assertEqual(action,"self_test_result")
        self.assertIsNot(options["evidence"],result)
        result["command"]={"evidence":options["evidence"]}
        exported=json.loads(json.dumps(result))
        self.assertTrue(exported["passed"])
        self.assertNotIn("command",exported["command"]["evidence"])


    def test_live_success_counter_handles_buffered_log_without_weakening_acks(self):
        class Server(FakeServer):
            def __init__(self,sent,acks=True):
                super().__init__(acks);self.sent=sent;self.stats={"tx":0}
            def request(self,action,**kwargs):
                result=super().request(action,**kwargs)
                if action=="self_test":self.stats["tx"]+=3
                return result
            def tx_success_since(self,started):
                return {"sent":self.sent,"samples":[{"txnb":self.sent}]}
        result=downlink_self_test(FakeManager(0),Server(3),True,timeout=1)
        self.assertTrue(result["passed"]);self.assertEqual(result["execution_source"],"udp_stat.txnb")
        self.assertEqual(result["log_sent"],0)
        self.assertFalse(downlink_self_test(FakeManager(0),Server(2),True,timeout=.01)["passed"])
        self.assertFalse(downlink_self_test(FakeManager(0),Server(3,False),True,timeout=.01)["passed"])
        self.assertFalse(downlink_self_test(FakeManager(3),Server(6),True,timeout=.01)["passed"])
        self.assertFalse(downlink_self_test(FakeManager(3),Server(0),True,timeout=.01)["passed"])

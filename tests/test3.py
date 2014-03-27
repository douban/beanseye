#!/usr/bin/env python
# coding:utf-8

import os
import sys
import time
from base import TestBeanseyeBase, BeansdbInstance, random_string, stop_svc
import unittest
from douban.beansdb import BeansDBProxy, MCStore

class Test3(TestBeanseyeBase):

    proxy_addr = 'localhost:7905'
    backend1_addr = 'localhost:57901'
    backend2_addr = 'localhost:57902'
    backend3_addr = 'localhost:57903'
    backend4_addr = 'localhost:57904'

    data_base_path = os.path.join("/tmp", "beanseye_test")
    accesslog = os.path.join(data_base_path, 'beansproxy_test3.log')
    errorlog = os.path.join(data_base_path, 'beansproxy_error_test3.log')

    def setUp(self):
        self._init_dir()
        if os.path.exists(self.accesslog):
            os.remove(self.accesslog)
        if os.path.exists(self.errorlog):
            os.remove(self.errorlog)

        self.backend1 = BeansdbInstance(self.data_base_path, 57901)
        self.backend2 = BeansdbInstance(self.data_base_path, 57902)
        self.backend3 = BeansdbInstance(self.data_base_path, 57903)
        self.backend4 = BeansdbInstance(self.data_base_path, 57904)
        self.backend1.start()
        self.backend2.start()
        self.backend3.start()
        self.backend4.start()
        # start 3 backend, no additional temporary node
        proxy_conf = {
                'servers': [
                    self.backend1_addr + ' F E D C B A 9 8 7 6 5 4 3 2 1 0',
                    self.backend2_addr + ' F E D C B A 9 8 7 6 5 4 3 2 1 0',
                    self.backend3_addr + ' F E D C B A 9 8 7 6 5 4 3 2 1 0',
                    self.backend4_addr + ' -F -E -D -C -B -A -9 -8 -7 -6 -5 -4 -3 -2 -1 -0',
                ],
                'port': 7905,
                'webport': 7908,
                'threads': 8,
                'n': 3,
                'w': 2,
                'r': 1,
                'buckets': 16,
                'slow': 100,
                'listen': '0.0.0.0',
                'proxies': [self.proxy_addr],
            }
        self.proxy_p = self._start_proxy(proxy_conf)


    def test3(self):
        """ test wether will fallback only down 1 primary node """
        proxy = BeansDBProxy([self.proxy_addr])
        self.backend1.stop()
        self.backend2.stop()
        key3 = 'key3'
        i = 0
        store4 = MCStore(self.backend4_addr)
        ts_start = time.time()
        fallbacked = False
        while i < 2000:
            data3 = random_string(10)
            i += 1
            proxy.set(key3, data3)
            self.assertEqual(proxy.get(key3), data3)
            time.sleep(0.1)
            data3_ = store4.get(key3)
            if data3_ is None:
                print "store4 get nothing yet, round=", i
            else:
                print "fallbacked to store4 after %s tries" % (i)
                fallbacked = True
                self.assertEqual(data3_, data3)
                break
        ts_stop = time.time()
        if not fallbacked:
            self.fail("still not fallback to backend 4")
        print "%s seconds passed" % (ts_stop - ts_start)
        self.backend1.start()
        self.assert_(proxy.exists("key3"))
        store1 = MCStore(self.backend1_addr)
        self.assert_(store1.get("key3") is None)
        data3 = random_string(10)
        ts_recover_start = time.time()
        i = 0
        recovered = False
        while i < 2000:
            #data3 = random_string(10)
            i += 1
            time.sleep(0.1)
            proxy.set(key3, data3)
            self.assertEqual(proxy.get(key3), data3)
            data3_ = store1.get(key3)
            if data3_ is None:
                print "store1 get nothing yet, round=", i
            else:
                print "recover to store1 after %s tries, %s sec" % (i, time.time() - ts_recover_start)
                recovered = True
                self.assertEqual(data3_, data3)
                break
        if not recovered:
            self.fail("still not fallback to backend 1")
        


    def tearDown(self):
        stop_svc(self.proxy_p)
        self.backend1.stop()
        self.backend2.stop()
        self.backend3.stop()
        self.backend4.stop()
        self.backend1.clean()
        self.backend2.clean()
        self.backend3.clean()
        self.backend4.clean()
 
if __name__ == '__main__':
    unittest.main()


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 :

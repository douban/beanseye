#!/usr/bin/env python
# coding:utf-8

import os
import sys
import time
from base import TestBeanseyeBase, BeansdbInstance, random_string, stop_svc
import unittest
from douban.beansdb import BeansDBProxy, MCStore


class Test2(TestBeanseyeBase):

    proxy_addr = 'localhost:7905'
    backend1_addr = 'localhost:57901'
    backend2_addr = 'localhost:57902'
    backend3_addr = 'localhost:57903'
    backend4_addr = 'localhost:57904'

    data_base_path = os.path.join("/tmp", "beanseye_test")
    accesslog = os.path.join(data_base_path, 'beansproxy_test2.log')
    errorlog = os.path.join(data_base_path, 'beansproxy_error_test2.log')

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

    def test2(self):
        data1 = random_string(10)
        data2 = random_string(10)
        time.sleep(1)

        print "test normal write"
        proxy = BeansDBProxy([self.proxy_addr])
        proxy.delete('key1')
        proxy.set('key1', data1)
        self._assert_data(self.backend1_addr, 'key1', data1)
        self._assert_data(self.backend2_addr, 'key1', data1)
        self._assert_data(self.backend3_addr, 'key1', data1)
        self._assert_data(self.backend4_addr, 'key1', None, "temporary node should not have the key when all primary nodes is good")

        proxy.delete('key2')
        print "down backend1 and backend2, proxy.get should be ok"
        self.backend1.stop()
        self.backend2.stop()
        proxy.set('key2', data2)
        self.assertEqual(proxy.get('key2'), data2)
        self._assert_data(self.proxy_addr, 'key2', data2)
        with self.assertRaises(Exception) as exc:
            MCStore(self.backend1_addr).get('key2')
            MCStore(self.backend2_addr).get('key2')
        self._assert_data(self.backend3_addr, 'key2', data2)
        #"temporary node should have the key when primary nodes < 2"
        self._assert_data(self.backend4_addr, 'key2', data2)
        print "test delete under bad sistuation, will raise error according to current behavior"
        with self.assertRaises(Exception) as exc:
            proxy.delete('key2')
        self._assert_data(self.backend3_addr, 'key2', None)
        self._assert_data(self.backend4_addr, 'key2', None)

        
        print "start backend2, (backend1 still down), test delete"
        self.backend2.start()
        time.sleep(10)
        proxy.delete('key2')
        self._assert_data(self.proxy_addr, 'key2', None)
        self._assert_data(self.backend2_addr, 'key2', None)
        self._assert_data(self.backend3_addr, 'key2', None)
        self._assert_data(self.backend4_addr, 'key2', None)



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

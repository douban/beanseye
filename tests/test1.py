#!/usr/bin/env python
# coding:utf-8

import os
import sys
import time
from base import TestBeanseyeBase, BeansdbInstance, random_string, stop_svc
import unittest
from douban.beansdb import BeansDBProxy


class Test1(TestBeanseyeBase):

    proxy_addr = 'localhost:7905'
    backend1_addr = 'localhost:57901'
    backend2_addr = 'localhost:57902'
    backend3_addr = 'localhost:57903'

    def setUp(self):
        self._init_dir()
        self.backend1_p = BeansdbInstance(self.data_base_path, 57901)
        self.backend2_p = BeansdbInstance(self.data_base_path, 57902)
        self.backend3_p = BeansdbInstance(self.data_base_path, 57903)
        self.backend1_p.start()
        self.backend2_p.start()
        self.backend3_p.start()
        proxy_conf = {
                'servers': [
                    self.backend1_addr + ' F E D C B A 9 8 7 6 5 4 3 2 1 0',
                    self.backend2_addr + ' F E D C B A 9 8 7 6 5 4 3 2 1 0',
                    self.backend3_addr + ' F E D C B A 9 8 7 6 5 4 3 2 1 0',
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

    def test1(self):
        data1 = random_string(10)
        time.sleep(1)
        # test write
        proxy = BeansDBProxy([self.proxy_addr])
        proxy.delete('key1')
        proxy.set('key1', data1)
        self._assert_data(self.backend1_addr, 'key1', data1)
        self._assert_data(self.backend2_addr, 'key1', data1)
        self._assert_data(self.backend3_addr, 'key1', data1)


    def tearDown(self):
        stop_svc(self.proxy_p)
        self.backend1_p.stop()
        self.backend2_p.stop()
        self.backend3_p.stop()
        self.backend1_p.clean()
        self.backend2_p.clean()
        self.backend3_p.clean()
        
if __name__ == '__main__':
    unittest.main()

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 :

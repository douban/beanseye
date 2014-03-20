#!/usr/bin/env python
# coding:utf-8

import os
import sys
import time
import signal
from base import TestBeanseyeBase, BeansdbInstance, random_string, stop_svc
import unittest
from douban.beansdb import BeansDBProxy, MCStore
import subprocess


class TestLogRotate(TestBeanseyeBase):

    proxy_addr = 'localhost:7905'
    backend1_addr = 'localhost:57901'
    backend2_addr = 'localhost:57902'
    backend3_addr = 'localhost:57903'
    data_base_path = os.path.join("/tmp", "beanseye_test")
    accesslog = os.path.join(data_base_path, 'beansproxy_testlog.log')
    accesslog_bak = os.path.join(data_base_path, 'beansproxy_testlog.log_bak')
    errorlog = os.path.join(data_base_path, 'beansproxy_error_testlog.log')
    errorlog_bak = os.path.join(data_base_path, 'beansproxy_error_testlog.log_bak')


    def setUp(self):
        self._init_dir()
        if os.path.exists(self.accesslog):
            os.remove(self.accesslog)
        if os.path.exists(self.errorlog):
            os.remove(self.errorlog)
        self.backend1 = BeansdbInstance(self.data_base_path, 57901)
        self.backend2 = BeansdbInstance(self.data_base_path, 57902)
        self.backend3 = BeansdbInstance(self.data_base_path, 57903)
        self.backend1.start()
        self.backend2.start()
        self.backend3.start()
        # start 3 backend, no additional temporary node
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

        print "test normal write"
        proxy = BeansDBProxy([self.proxy_addr])
        proxy.delete('key1')
        proxy.set('key1', data1)
        self._assert_data(self.backend1_addr, 'key1', data1)
        self._assert_data(self.backend2_addr, 'key1', data1)
        self._assert_data(self.backend3_addr, 'key1', data1)
        self.assert_(MCStore(self.backend2_addr).exists('key1'))

        cmd = "ls -l /proc/%s/fd" % (self.proxy_p.pid)
        print cmd
        print subprocess.check_output(cmd, shell=True)

        print "move log"
        if os.path.exists(self.accesslog_bak):
            os.remove(self.accesslog_bak)
        if os.path.exists(self.errorlog_bak):
            os.remove(self.errorlog_bak)
        os.rename(self.accesslog, self.accesslog_bak)
        os.rename(self.errorlog, self.errorlog_bak)
        print "write more data to see if new log not exists"
        data1 = random_string(10)
        proxy.set('key1', data1)
        self.assert_(not os.path.exists(self.accesslog))
        self.assert_(not os.path.exists(self.errorlog))

        time.sleep(5)

        print "send SIGINT signal, should re-open log file" 
        os.kill(self.proxy_p.pid, signal.SIGINT)

        cmd = "ls -l /proc/%s/fd" % (self.proxy_p.pid)
        print subprocess.check_output(cmd, shell=True)

        s = os.stat(self.accesslog)
        self.assert_(os.path.exists(self.accesslog))
        self.assert_(os.path.exists(self.errorlog))
        print "see if write to new accesslog"
        proxy.get('key1')
        time.sleep(1)
        s_new = os.stat(self.accesslog)
        print s_new.st_size, s.st_size
        self.assert_(s_new.st_size > s.st_size)


        
        

    def tearDown(self):
        stop_svc(self.proxy_p)
        self.backend1.stop()
        self.backend2.stop()
        self.backend3.stop()
        self.backend1.clean()
        self.backend2.clean()
        self.backend3.clean()
        
if __name__ == '__main__':
    unittest.main()



# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 :

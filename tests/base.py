#!/usr/bin/env python
# coding:utf-8

import unittest
import subprocess
import shlex
import time
import os
import sys
import string
import random
import yaml
import shutil
from douban.beansdb import MCStore

def start_svc(cmd):
    print "start", cmd
    p = subprocess.Popen(isinstance(cmd, (tuple, list,)) and cmd or shlex.split(cmd) , close_fds=True)
    time.sleep(0.2)
    if p.poll() is not None:
        raise Exception("cannot start %s" % (cmd))
    return p

def stop_svc(popen):
    if popen.poll() is not None:
        return
    popen.terminate()
    popen.wait()


class BeansdbInstance:

    def __init__(self, base_path, port):
        self.port = port
        self.popen = None
        self.db_home = os.path.join(base_path, "beansdb_%s" % (self.port))
        if not os.path.exists(self.db_home):
            os.makedirs(self.db_home)
        self.cmd = "/usr/bin/beansdb -p %s -H %s -T 1 -L /dev/null" % (self.port, self.db_home)


    def start(self):
        assert self.popen is None
        self.popen = start_svc(self.cmd)

    def stop(self):
        print "stop", self.cmd
        if self.popen:
            stop_svc(self.popen)
            self.popen = None

    def clean(self):
        if self.popen:
            self.stop()
        if os.path.exists(self.db_home):
            shutil.rmtree(self.db_home)


def random_string(n):
    s = string.ascii_letters + string.digits
    result = ""
    for i in xrange(n):
        result += random.choice(s)
    return result


class TestBeanseyeBase(unittest.TestCase):

    data_base_path = os.path.join("/tmp", "beanseye_test")
    code_base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


    def _init_dir(self):
        if not os.path.exists(self.data_base_path):
            os.makedirs(self.data_base_path)


    def _start_proxy(self, config):
        config_file = self._gen_proxy_config(config)
        proxy_bin = os.path.join(self.code_base_path, "bin", "proxy")
        cmd = "%s -conf %s -basepath %s" % (proxy_bin, config_file, self.code_base_path)
        return start_svc(cmd)

    def _gen_proxy_config(self, config):
        config['basepath'] = self.code_base_path
        config['accesslog'] = os.path.join(self.data_base_path, 'beansproxy.log')
        config['errorlog'] = os.path.join(self.data_base_path, 'beansproxy_error.log')
        config_file = os.path.join(self.data_base_path, 'beanseye_conf.yaml')
        with open(config_file, 'w') as f:
            yaml.safe_dump(config, f, default_flow_style=False, canonical=False)
        return config_file

    def _assert_data(self, addr, key, data):
        store = MCStore(addr) 
        self.assertEqual(store.get(key), data)

        
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4 :

import unittest

from t2db_daemon.process import inputArg
from t2db_daemon.run import 
from t2db_objects.tests.common import randomJob
from t2db_objects.tests.common import randomStringFixed
from t2db_objects.tests.common import randomInteger
from t2db_objects.objects import Configuration

"""
def fakeProcess():
    job = randomJob()
    pythonApp = "ls"
    pythonAppPath = "/usr/bin"
    return Process(pythonApp, pythonAppPath
"""

configurationFields = [
    {"name":"query", "kind":"mandatory", "type":str},
    {"name":"api", "kind":"mandatory", "type":str},
    {"name":"buffer-server", "kind":"mandatory", "type":str},
    {"name":"buffer-server-port", "kind":"mandatory", "type":int},
    {"name":"buffer-maxsize", "kind":"mandatory", "type":int},
    {"name":"buffer-maxtime", "kind":"mandatory", "type":int},
    {"name":"search-id", "kind":"mandatory", "type":int},
    {"name":"con", "kind":"mandatory", "type":str},
    {"name":"con_sec", "kind":"mandatory", "type":str},
    {"name":"acc", "kind":"mandatory", "type":str},
    {"name":"acc_sec", "kind":"mandatory", "type":str},
    ]


def fakeConfig():
    rawConfiguration = {}
    for element in configurationFields:
        if element["type"] == str:
            rawConfiguration[element["name"]] = randomStringFixed(100)
        else:
            rawConfiguration[element["name"]] = randomInteger(100)
    return Configuration(configurationFields, rawConfiguration)

class TestExtraFunctions(unittest.TestCase):
    def setUp(self):
        self.config = fakeConfig()

    def test_inputArg(self):
        inputArg(self.config)

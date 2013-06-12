#socket connection for python3
import socket
import time

## Class to control  socket communication
class control_socket:
    def __init__(self, sock=None):
        if sock is None:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.sock = sock

    def connect(self, host, port):
        self.sock.connect((host, port))

    def send(self, _list):
        #TODO: dynamic message size. Currently using fixed size
        msg = ''
        list_keys = _list.keys()
        for list_key in list_keys:
            msg += str(list_key) + "=" + str(_list[list_key]) + "|"
        # Get length of message
        msg_length = 4096
        # Send length of message
        str_msg_length = str(msg_length)
        sent1 = self.sock.send(bytes(str_msg_length, "utf-8"))
        # Wait confirmation
        recv1 = self.sock.recv(2)
        # Send the message
        sent2 = self.sock.send(bytes(msg, "utf-8"))

    def recv(self):
        # receive length of message
        msg_length_str = self.sock.recv(10)
        msg_length = int(msg_length_str)
        # send confirmation
        confirmation = "ok"
        sent1 = self.sock.send(bytes(confirmation, "utf-8"))
        # receive the message
        msg = self.sock.recv(msg_length)
        msg_str = msg.decode("utf-8")
        _list = msg_str.strip().split("|")
        args = {}
        for i in range(0, len(_list)-1):
            value_list = _list[i].strip().split("=")
            #print(value_list[0])
            args[value_list[0]] = value_list[1]
        return args

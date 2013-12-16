import argparse
import shlex
import subprocess
import sys, time
from daemon import Daemon
import threading
import m_socket
import socket
import signal
from threading import Lock
import pipes
import os

#TODO: Make failure tolerant. Restart process if failed



#Create global variable!
p_list = processes_list()


## Shared variable
next_id_job = 1
id_job_lock = Lock()

## Class to control threads
class client_thread(threading.Thread):
    def __init__(self, thread_id, clientsocket):
        self._thread_id = thread_id
        self._control_socket = m_socket.control_socket(clientsocket)

    def run(self):
        global id_job_lock
        global next_id_job
        # Get data from client
        data = self._control_socket.recv()
        # Obtain new job id
        id_job_lock.acquire()
        try:
            id_job = next_id_job
            next_id_job += 1
        finally:
            id_job_lock.release()
        # Start job
        par_ = parser_data(data)
        job = par_.get_job(id_job)
        args = job.run()
        # Send result to client!
        self._control_socket.send(args)

def worker(socketControl, globalBuffer):
    try:
        job = socketControl.recvObject()
    except Exception as e:
        logger.error(str(e))
        socketControl.close()
        return
    result = run(job)
    try:
        socketControl.sendObject(result)
    except Exception as e:
        logger.error(str(e))
    finally:
        socketControl.close()

# Wrapper thread of worker function
class WorkerThread(Thread):
    def __init__(self, *args):
        self.args = args
        Thread.__init__(self)

    def run(self):
        worker(*self.args)

# This function controls the interacction among the Manager server and the
# petitions
def server(manager):
    logger.info("Server thread started")
    while not manager.stopEvent.isSet():
        try:
            socketControl = manager.socketServer.accept()
            # Start thread to attend new connection
            worker = WorkerThread(socketControl, bufferServer.globalBuffer)
            worker.start()
            # Sequentially, remove for parallel
            worker.join()
        except:
            logger.warn("Timeout incoming connection")
    #TODO:wait for child threads
    #Send signal to reporter. Server ends
    logger.info("StopEvent occurs!")
    logger.debug("Waiting in barrier: " + str(bufferServer.barrier))
    manager.barrier.wait()
    manager.socketServer.close()
    logger.info("Server thread finished")

class ServerThread(Thread):
    def __init__(self, *args):
        self.args = args
        Thread.__init__(self)
    
    def run(self):
        server(*self.args)

class Manager(object):
    def __init__(self, stopEvent, barrier, socketPort, maxConnection, timeout,
            workerApp, pathWorkerApp, workerBufferSize, workerBuferTimer,
            hostBuffer, portBuffer):
        self.stopEvent = stopEvent
        self.barrier = barrier
        self.socketPort = socketPort
        self.maxConnection = maxConnection
        try:
            #create a socket server
            logger.info("Starting Socket Server")
            self.socketServer = psocket.SocketServer(socketPort, maxConnection)
            self.socketServer.setTimeout(timeout)
            self.hostName = self.socketServer.getHostName()
        except Exception as e:
            raise Exception("Could not create socket server:" + str(e))

    def getHostName(self):
        return self.hostName

    def start(self):
        serverThread = ServerThread(self)
        serverThread.start()

#Server
serversocket = None
activeserver = 0

## Class mydaemon. 
class MyDaemon(Daemon):
    def run(self):
        global serversocket
        global activeserver
        try:        
            #create an INET, STREAMing socket
            serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #bind the socket to a public host, and a well-known port
            serversocket.bind((socket.gethostname(), int(config.socket_port)))
            #become a server socket
            serversocket.listen(int(config.max_connection))
        except Exception as e:
            print("Could not established socket connection")
            print(e)
            sys.exit(4)
        activeserver = 1
        #Initialise id_thread
        id_thread = 0
        while True:
            #accept connections from outside
            print("Server online. Waiting connections in: "
                , socket.gethostname(), ":", config.socket_port)
            (clientsocket, address) = serversocket.accept()
            #now do something with the clientsocket
            #in this case, we'll pretend this is a threaded server
            ct = client_thread(id_thread, clientsocket)
            id_thread += 1
            ct.run()

## this function controls SIGINT signal (Ctrl+C).
def signal_handler(signal, frame):
    global activerserver
    global serversocket
    print ("You pressed Ctrl+C!. Stoping server")
    p_list.kill_all()
    if activeserver == 1:
        serversocket.close()
    sys.exit(0)


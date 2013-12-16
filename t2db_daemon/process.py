from t2db_objects import objects

"""
def inputArg(conf, job):
    argStr = (conf.pythonAppPath + "/" + conf.pythonApp 
        + " --db-server " + config.urldatabase
        + " --api " + job.kind
        + " --search-id " + str(job.processId)
        + " --con \"" + job.consumer + "\""
        + " --con_sec \"" + job.consumerSec + "\""
        + " --acc \"" + job.access + "\""
        + " --acc_sec \"" + job.accessSec + "\""
        + " --query \"" + job.query + "\"")
    return argStr
"""

def inputArg(args, fields):
    line = ""
    for field in fields:
        name = field["name"]
        value = args.__dict__[name]
        if field["type"] == str:
            line += "--" + name + " " + value + " "
        elif field["type"] == int:
            line += "--" + name + " " + str(value) + " "
    return line

def confStd(self, config, job, name):
    return (config.outputFolder + "/" + "job_" + job.kind + "_" + 
        str(job.pid) + "_std_" + name)

class process(object):
    def __init__(self, config, job, inputArg):
        self.pythonApp = config.pythonApp
        self.pythonAppPath = config.pythonAppPath
        self.db = None
        self.stdout = self.confStd(conf, job, "out")
        self.stderr = self.confStd(conf, job, "err")
        self.argStr = self.inputArg(conf, job)
        self.pid = job.processId
        self.kind = job.kind

    def run(self):
        # Prepare command arguments
        commandLine = shlex.split(self.inputArg())
        print ("Executing :" + str(commandLine))
        # Prepare output pipes
        self.confPipes() 
        t = pipes.Template()
        stdoutPipe = t.open(self.stdout, "w")
        stderrPipe = t.open(self.stderr, "w")
        # Execute!
        self.proc = subprocess.Popen(commandLine,
                                    stdout = stdoutPipe,
                                    stderr = stderrPipe)
        
    def wait(self):
        print ("Waiting process")
        self.proc.wait()

    def kill(self):
        print ("Killing process")
        self.proc.kill()

    def poll(self):
        ret = self.proc.poll()
        if ret == None:
            return True
        return False

    def getData(self):
        args = {}
        args["kind"] = self.kind
        args["pid"] = self.pid
        args["stdout"] = self.stdout
        args["stderr"] = self.stderr
        args["active"] = self.poll()
        return args
        
## Shared memory. List of process
class processesList(object):
    def __init__(self):
        self.processes = {}
        self.idNextProcess = 0
        self.lock = Lock()

    def add(self, process):
        self.lock.acquire()
        try:
            self.processes[process.pid] = process
        finally:
            self.lock.release()

    def get(self, idProcess):
        try:
            return self.processes[idProcess]
        except:
            raise Exception("Process " + str(idProcess) + " not found")

    def getAll(self):
        val = self.processes
        return val

    def delete(self, idProcess):
        self.lock.acquire()
        try:
            proc = self.processes[idProcess]
            del self.processes[idProcess]
            proc.kill()
        finally:
            self.lock.release()

    def killAll(self):
        idKeys = self.processes.keys()
        for idProc in idKeys:
            proc = self.processes[idProc]
            proc.kill()

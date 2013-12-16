
## Job
def run(job, config):
    global config
    #Run comman
    if job.command == "START":
        # Create new process
        proc = process(python_app = config.collector_app,
                python_app_path = config.path_collector_app,
                consumer = job.consumer,
                consumer_sec = job.consumer_sec,
                access = job.access,
                access_sec = job.access_sec,
                query = job.query,
                process_id = job.process_id,
                kind = job.kind)
        # Add to process list
        p_list.add(proc)
        # Run process
        proc.run()
        # Send return
        args = self.get_base_return()
        args["new_process"] = proc.get_data()
        return args

    elif self._command == "DELETE":
        #get base return
        args = self.get_base_return()
        # Delete process
        print(self._process_id)
        try:
            p_list.delete(int(self._process_id))
        except Exception as e:
            print ("Could not find process")
            print (e)
            args["failed"] = "true"
            args["cause"] = str(e)
            return args

        args["delete"] = "true"
        args["pid"] = self._process_id
        return args

    def get_base_return(self):
        args = {}
        args["id"] = self._id
        args["command"] = self._command
        return args

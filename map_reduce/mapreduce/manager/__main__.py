"""MapReduce framework Manager node."""
import os
import threading
import tempfile
import logging
import json
import time
import click
import socket
from pathlib import Path
import shutil
import mapreduce.utils
import time


# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def check_registration(self, message_dict):
        """Construct a manager instance and start listening for messages."""
        if message_dict["message_type"] == "register":
            LOGGER.info("Received worker registration")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                # connect to the server
                try:
                    client.connect((message_dict["worker_host"],
                                    message_dict["worker_port"]))
                except ConnectionRefusedError:
                    self.connection_refused(message_dict["worker_host"])
                # add to worker array information
                worker = {"worker_host": message_dict["worker_host"],
                          "worker_port": message_dict["worker_port"],
                          "state": "ready", "last_heartbeat": time.time(),
                          "current_task": {}}

                for w in self.workers:
                    if w["worker_host"] == worker["worker_host"] and w[
                            "worker_port"] == worker["worker_port"] and w[
                            "state"] != "dead":
                        w["state"] = "dead"
                        print("worker marked as dead and revived")
                        break

                self.workers.append(worker)
                # send a message
                message = json.dumps({
                        "message_type": "register_ack",
                        "worker_host": message_dict["worker_host"],
                        "worker_port": message_dict["worker_port"],
                        })
                client.sendall(message.encode('utf-8'))

    def connection_refused(self, worker_host):
        """Construct a Manager instance and start listening for messages."""
        for worker in self.workers:
            if (worker["worker_host"] == worker_host and
               (worker["state"] != "dead")):
                if (worker["state"] == "busy" and
                   (worker["current_task"] != {})):
                    self.failed_tasks.append(worker["current_task"])
                print("worker died")
                worker["state"] = "dead"

    def check_shutdown(self, message_dict):
        """Construct a Manager instance and start listening for messages."""
        if message_dict["message_type"] == "shutdown":
            LOGGER.info("Received shutdown message")
            for worker in self.workers:
                if worker["state"] != "dead":
                    with socket.socket(socket.AF_INET,
                         socket.SOCK_STREAM) as client:
                        message = json.dumps({
                                "message_type": "shutdown",
                                })
                        try:
                            client.connect((worker["worker_host"],
                                            worker["worker_port"]))
                        except ConnectionRefusedError:
                            self.connection_refused(worker["worker_host"])

                        client.sendall(message.encode('utf-8'))
            self.signals["shutdown"] = True
            print("updated manager shutdown signal")

    def job_request(self, message_dict):
        """Construct a Manager instance and start listening for messages."""
        if message_dict["message_type"] == "new_manager_job":
            LOGGER.info("Received new job message")
            message_dict["job_id"] = self.num_jobs
            self.num_jobs += 1
            output_dir = Path(message_dict["output_directory"])
            if (output_dir.exists() and output_dir.is_dir()):
                shutil.rmtree(output_dir, ignore_errors=True)
            try:
                os.makedirs(output_dir)
            except FileExistsError:
                print("file exsists error")

            LOGGER.info("Received new job message")
            self.job_queue.append(message_dict)

    def UDP_server(self):
        """Construct a Manager instance and start listening for messages."""
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Bind the UDP socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.settimeout(1)
            # No sock.listen() since UDP doesn't establish connections like TCP
            # Receive incoming UDP messages
            while not self.signals["shutdown"]:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                # message_dict = json.loads(message_str)
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                self.update_last_heartbeat_time(message_dict)

    def update_last_heartbeat_time(self, message_dict):
        """Construct a Manager instance and start listening for messages."""
        if message_dict["message_type"] == "heartbeat":
            for worker in self.workers:
                if message_dict["worker_host"] == worker[
                        "worker_host"] and message_dict[
                        "worker_port"] == worker["worker_port"] and worker[
                        "state"] != "dead":
                    worker["last_heartbeat"] = time.time()
                    break

    def TCP_server(self):
        """Test TCP Socket Server."""
        # Create an INET, STREAMing socket, this is TCP
        # Note: context manager syntax allows for sockets to automatically be
        # closed when an exception is raised or control flow returns.
        LOGGER.debug(
            "Started TCP Server for listening"
            )
        print("starting tcp server on manager")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            # Socket accept() will block for a maximum of 1 second.  If you
            # omit this, it blocks indefinitely, waiting for a connection.
            sock.settimeout(1)
            while not self.signals["shutdown"]:
                # Wait for a connection for 1s.
                # The socket library avoids consuming
                # CPU while waiting for a connection.
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])
                # Socket recv() will block for a maximum of 1 second.
                # If you omit
                # this, it blocks indefinitely,
                # waiting for packets.
                clientsocket.settimeout(1)
                # Receive data, one chunk at a time.
                # If recv() times out before we can read a chunk,
                # then go back to the top of the loop and try again.
                # When the client closes the connection,
                # recv() returns empty data,
                # which breaks out of the loop.
                # We make a simplifying assumption that the client
                # will always cleanly close the connection.
                with clientsocket:
                    message_chunks = []
                    while True and not self.signals["shutdown"]:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)
                # Decode list-of-byte-strings to UTF8 and parse JSON data
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                print(message_dict)
                if message_dict["message_type"] == "finished":
                    print("RECEIVED FINISHED MESSAGE")
                    self.tasks_finished += 1
                    print(self.tasks_finished)
                    for worker in self.workers:
                        if worker["worker_host"] == message_dict[
                                "worker_host"] and worker[
                                "worker_port"] == message_dict["worker_port"]:
                            worker["state"] = "ready"
                            break
                self.check_registration(message_dict)
                self.check_shutdown(message_dict)
                self.job_request(message_dict)
            print("end of TCP server manager")

    def run_job(self):
        """Construct a Manager instance and start listening for messages."""
        while not self.signals["shutdown"]:
            if self.manager_ready and self.job_queue:
                print("running job")
                self.manager_ready = False
                job_id_prefix = self.job_queue[0]['job_id']
                prefix = f"mapreduce-shared-job{job_id_prefix:05d}-"
                with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                    LOGGER.info("Created tmpdir %s", tmpdir)
                    job_request = self.job_queue[0]
                    # get files in input directory
                    files = os.listdir(Path(job_request["input_directory"]))
                    partitions = self.input_partition(files, job_request[
                        "num_mappers"], job_request["input_directory"])
                    LOGGER.info(partitions)
                    print("partitions: ", partitions)
                    # send map jobs to workers
                    for i in range(job_request["num_mappers"]):
                        found_worker = False
                        message = {}
                        while not found_worker and not self.signals[
                                "shutdown"]:
                            # look for worker
                            print("are we stuck here")
                            print(f"we are on task {i}")
                            print(self.workers)
                            for worker in self.workers:
                                LOGGER.info(worker)
                                if worker["state"] == "ready":
                                    worker_host = worker["worker_host"]
                                    worker_port = worker["worker_port"]
                                    message = {
                                        "message_type": "new_map_task",
                                        "task_id": i,
                                        "input_paths": partitions[i],
                                        "executable": job_request[
                                            "mapper_executable"],
                                        "output_directory":
                                            tmpdir,
                                        "num_partitions":
                                            job_request["num_reducers"],
                                        "worker_host": worker_host,
                                        "worker_port": worker_port
                                    }
                                    found_worker = True
                                    LOGGER.info("Found worker")
                                    worker["state"] = "busy"
                                    worker["current_task"] = i
                                    break
                            if not found_worker:
                                time.sleep(1)
                                if self.signals["shutdown"]:
                                    return

                        with socket.socket(socket.AF_INET,
                                           socket.SOCK_STREAM) as sock:
                            try:
                                sock.connect((worker_host, worker_port))
                            except ConnectionRefusedError:
                                self.connection_refused(worker_host)

                            json_message = json.dumps(message)
                            sock.sendall(json_message.encode('utf-8'))
                            LOGGER.info("sent json to worker")
                            LOGGER.info(json_message)

                    # reassign failed tasks
                    while self.failed_tasks or self.tasks_finished != (
                            job_request["num_mappers"]) and not self.signals[
                            "shutdown"]:
                        if self.failed_tasks:
                            while self.failed_tasks and not self.signals[
                                    "shutdown"]:
                                for worker in self.workers:
                                    if worker["state"] == "ready":
                                        worker_host = worker["worker_host"]
                                        worker_port = worker["worker_port"]
                                        message = {
                                            "message_type": "new_map_task",
                                            "task_id": self.failed_tasks[0],
                                            "input_paths": partitions[
                                                self.failed_tasks[0]],
                                            "executable": job_request[
                                                "mapper_executable"],
                                            "output_directory": tmpdir,
                                            "num_partitions":
                                                job_request["num_reducers"],
                                            "worker_host": worker_host,
                                            "worker_port": worker_port
                                        }
                                        found_worker = True
                                        worker["state"] = "busy"
                                        worker["current_task"] = (
                                            self.failed_tasks[0])
                                        self.failed_tasks.pop(0)
                                        break
                                    if not found_worker:
                                        time.sleep(1)
                                        if self.signals["shutdown"]:
                                            return

                            with socket.socket(socket.AF_INET,
                                               socket.SOCK_STREAM) as sock:
                                try:
                                    sock.connect((worker_host, worker_port))
                                except ConnectionRefusedError:
                                    self.connection_refused(worker_host)
                                json_message = json.dumps(message)
                                sock.sendall(json_message.encode('utf-8'))
                                LOGGER.info("sent json to worker")
                                LOGGER.info(json_message)

                    # REDUCE
                    print(self.tasks_finished, "tasks finished")
                    print("BEGINNING REDUCE")
                    self.tasks_finished = 0
                    self.failed_tasks = []
                    files = os.listdir(tmpdir)

                    reduce_partitions = self.input_partition_reduce(
                        files, job_request["num_reducers"], tmpdir)
                    print("THESE ARE THE PARTITIONS,", reduce_partitions)
                    for i in range(job_request["num_reducers"]):
                        found_worker = False
                        message = {}
                        while not found_worker and not self.signals[
                                "shutdown"]:
                            # look for worker
                            for worker in self.workers:
                                if worker["state"] == "ready":
                                    worker_host = worker["worker_host"]
                                    worker_port = worker["worker_port"]
                                    message = {
                                        "message_type": "new_reduce_task",
                                        "task_id": i,
                                        "input_paths":
                                            reduce_partitions[i],
                                        "executable":
                                            job_request["reducer_executable"],
                                        "output_directory":
                                            job_request["output_directory"],
                                        "worker_host": worker_host,
                                        "worker_port": worker_port
                                    }
                                    found_worker = True
                                    worker["state"] = "busy"
                                    worker["current_task"] = i
                                    break
                            if not found_worker:
                                time.sleep(1)
                                if self.signals["shutdown"]:
                                    return

                        with socket.socket(socket.AF_INET,
                                           socket.SOCK_STREAM) as sock:
                            try:
                                sock.connect((worker_host, worker_port))
                            except ConnectionRefusedError:
                                self.connection_refused(worker_host)
                            json_message = json.dumps(message)
                            sock.sendall(json_message.encode('utf-8'))
                            LOGGER.info("sent json to worker")
                            LOGGER.info(json_message)

                    while self.failed_tasks or self.tasks_finished != (
                            job_request["num_reducers"]) and not self.signals[
                            "shutdown"]:
                        if self.failed_tasks:
                            while self.failed_tasks and not self.signals[
                                    "shutdown"]:
                                for worker in self.workers:
                                    if worker["state"] == "ready":
                                        print("worker is ready for reduce")
                                        worker_host = worker["worker_host"]
                                        worker_port = worker["worker_port"]
                                        message = {
                                            "message_type": "new_reduce_task",
                                            "task_id": self.failed_tasks[0],
                                            "input_paths": reduce_partitions[
                                                self.failed_tasks[0]],
                                            "executable": job_request[
                                                "reducer_executable"],
                                            "output_directory": job_request[
                                                "output_directory"],
                                            "worker_host": worker_host,
                                            "worker_port": worker_port
                                        }
                                        found_worker = True
                                        worker["state"] = "busy"
                                        worker["current_task"] = (
                                            self.failed_tasks[0])
                                        self.failed_tasks.pop(0)
                                        break
                                    if not found_worker:
                                        time.sleep(1)
                                        if self.signals["shutdown"]:
                                            return

                            with socket.socket(socket.AF_INET,
                                               socket.SOCK_STREAM) as sock:
                                try:
                                    sock.connect((worker_host, worker_port))
                                except ConnectionRefusedError:
                                    self.connection_refused(worker_host)
                                json_message = json.dumps(message)
                                sock.sendall(json_message.encode('utf-8'))
                                LOGGER.info("sent json to worker")
                                LOGGER.info(json_message)
                    time.sleep(1)
                LOGGER.info("Cleaned up tmpdir %s", tmpdir)
                self.job_queue.pop(0)
            time.sleep(1)

    def fault_tolerance(self):
        """Construct a Manager instance and start listening for messages."""
        while not self.signals["shutdown"]:
            for worker in self.workers:
                if time.time() - worker["last_heartbeat"] > 10:
                    if worker["state"] == "busy" and worker[
                            "current_task"] != {}:
                        self.failed_tasks.append(worker["current_task"])
                        # LOGGER.info("added task ", worker["current_task"])
                        worker["state"] = "dead"
                    if (worker["state"] == "ready"):
                        worker["state"] = "dead"
            time.sleep(2)

    def input_partition(self, files, num_mappers, input_directory):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(f"Num mappers: {num_mappers}")
        results = []
        for i in range(num_mappers):
            results.append([])
        files.sort()
        file_num = 0
        while len(files) != 0 and not self.signals["shutdown"]:
            results[file_num].append(input_directory + "/" + files[0])
            file_num += 1
            if file_num == num_mappers:
                file_num = 0
            files.pop(0)
        results.sort()
        return results

    def input_partition_reduce(self, files, num_partitions, tmpdir):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(f"Num reduce_mappers: {num_partitions}")
        results = []
        for i in range(num_partitions):
            results.append([])
        for file in files:
            part = file.split('-')[-1]
            # part000xx
            num = int(part[4:])
            results[num].append(tmpdir + "/" + file)
        for j in results:
            j.sort()
        return results

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        # This is a fake message to demonstrate pretty printing with logging
        # message_dict = {
        #     "message_type": "register",
        #     "worker_host": "localhost",
        #     "worker_port": 6001,
        # }
        # LOGGER.info("TCP recv\n%s", json.dumps(message_dict, indent=2))
        self.manager_ready = True
        self.job_queue = []
        self.num_jobs = 0
        self.signals = {"shutdown": False}
        self.host = host
        self.port = port
        self.tasks_finished = 0
        self.workers = []
        self.failed_tasks = []
        # create thread with UDP to listen to heart beat messages
        print("starting threads")
        UDP_thread = threading.Thread(target=self.UDP_server)
        TCP_thread = threading.Thread(target=self.TCP_server)
        run_job_thread = threading.Thread(target=self.run_job)
        fault_tolerance_thread = threading.Thread(target=self.fault_tolerance)

        UDP_thread.start()
        TCP_thread.start()
        run_job_thread.start()
        fault_tolerance_thread.start()

        UDP_thread.join()
        TCP_thread.join()
        run_job_thread.join()
        fault_tolerance_thread.join()
        print("ending manager")


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()

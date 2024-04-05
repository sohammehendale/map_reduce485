"""MapReduce framework Worker node."""
import os
import threading
import logging
import json
import time
import click
import socket
import subprocess
import hashlib
import tempfile
import shutil
import mapreduce.utils
import heapq
import contextlib


# Configure logging
LOGGER = logging.getLogger(__name__)


def yield_lines(file_iterator):
    """Construct a worker instance and start listening for messages."""
    for line in file_iterator:
        if line != "":
            yield line


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def receive_ack(self, message_dict):
        """Construct a worker instance and start listening for messages."""
        if message_dict["message_type"] == "register_ack":
            LOGGER.info("Received register_ack")
            heartbeat_thread = threading.Thread(target=self.send_heartbeats)
            self.threads.append(heartbeat_thread)
            heartbeat_thread.start()
            print("started heartbeats")
            # heartbeat_thread.join()

    def check_shutdown(self, message_dict):
        """Construct a worker instance and start listening for messages."""
        if message_dict["message_type"] == "shutdown":
            LOGGER.info("Received worker shutdown message")
            self.signals["shutdown"] = True
            print("updated worker shutdown signal")

    def send_heartbeats(self):
        """Construct a worker instance and start listening for messages."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Connect to the UDP socket on server
            sock.connect((self.manager_host, self.manager_port))
            # Send a message
            message = json.dumps({
                "message_type": "heartbeat",
                "worker_host": self.host,
                "worker_port": self.port
                })
            # sock.settimeout(2)
            while not self.signals["shutdown"]:
                sock.sendall(message.encode('utf-8'))
                time.sleep(2)
                # print(f"sent heartbeat from {self.host}: {self.port}")

    def map_job(self, message_dict):
        """Construct a worker instance and start listening for messages."""
        if message_dict["message_type"] == "new_map_task":
            self.map_job_request = message_dict
            print(f"{self.host}:{self.port} received: {message_dict}")
            LOGGER.info(f"{self.host}:{self.port} received: {message_dict}")

    def reduce_job(self, message_dict):
        """Construct a worker instance and start listening for messages."""
        if message_dict["message_type"] == "new_reduce_task":
            self.reduce_job_request = message_dict

    def run_job(self):
        """Construct a worker instance and start listening for messages."""
        while not self.signals["shutdown"]:
            if self.map_job_request != {}:
                self.run_map_job()
            elif self.reduce_job_request != {}:
                self.run_reduce_job()
            else:
                continue
            time.sleep(1)

    def run_map_job(self):
        """Construct a worker instance and start listening for messages."""
        executable = self.map_job_request["executable"]
        input_paths = self.map_job_request["input_paths"]

        prefix = f"mapreduce-local-task{self.map_job_request['task_id']:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            for input_path in input_paths:
                # create num_partitions files
                all_files = []
                for i in range(self.map_job_request["num_partitions"]):
                    #     prefix=f"maptask{self.map_job_request['task_id']:05d}-part{i:05d}")
                    temp = self.map_job_request['task_id']
                    filename = tmpdir + "/" + f"maptask{temp:05d}-part{i:05d}"
                    all_files.append(filename)
                with open(input_path) as infile:
                    with subprocess.Popen(
                        [executable],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True,
                    ) as map_process:
                        with contextlib.ExitStack() as stack:
                            files = [stack.enter_context(
                                open(fn, 'a+')) for fn in all_files]
                            for line in map_process.stdout:
                                key = line.split("\t")[0]
                                # print(line)
                                hexdigest = hashlib.md5(key.encode(
                                    "utf-8")).hexdigest()
                                keyhash = int(hexdigest, base=16)
                                partition = keyhash % self.map_job_request[
                                    "num_partitions"]
                                # print(all_files[partition])
                                # file = open(all_files[partition], 'a+')
                                # Add line to correct partition output file
                                files[partition].write(line)
                                # file.close()
            with contextlib.ExitStack() as stack:
                files = [stack.enter_context(open(
                    fn, 'r+')) for fn in all_files]
            # here is old code
            # for i in all_files:
                # file = open(i, 'r')
                # data = file.readlines()
                # print(data)
                # data.sort()
                # print("printing data", data)
                # file.close()
                # file = open(i, 'w+')
                # file.close()
                # file = open(i, 'a+')
                # for datum in data:
                #     file.write(datum)
                # file.close()

                # here is other attempt
                # file = stack.enter_context(open(i))
                # merged = []
                # for line in file:
                #     l = [line]
                #     merged = heapq.merge(*[merged, l])
                # for line in merged:
                #     f = open(i, 'w+')
                #     f.close()
                #     f = open(i, 'a+')
                #     f.write(line)

                for file in files:
                    lines = file.readlines()
                    # print("FILENAME", file.name)
                    # print("LINES IN FILE", lines)
                    lines.sort()
                    file.seek(0)
                    file.writelines(lines)
                    shutil.move(file.name, self.map_job_request[
                        "output_directory"] + "/" + file.name.split("/")[-1])
                    LOGGER.info("Moving file " + file.name)

        finished = {
            "message_type": "finished",
            "task_id": self.map_job_request["task_id"],
            "worker_host": self.host,
            "worker_port": self.port
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Connect to the TCP socket on server
            sock.connect((self.manager_host, self.manager_port))
            message = json.dumps(finished)
            sock.sendall(message.encode('utf-8'))

        self.map_job_request = {}

    def run_reduce_job(self):
        """Construct a worker instance and start listening for messages."""
        filenames = self.reduce_job_request["input_paths"]
        with contextlib.ExitStack() as stack:
            files = [stack.enter_context(open(fn)) for fn in filenames]
            executable = self.reduce_job_request["executable"]
            # input_path = heapq.merge(*files)
            temp = self.reduce_job_request['task_id']
            prefix = f"mapreduce-local-task{temp:05d}-"
            with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                temp = self.reduce_job_request['task_id']
                with open(tmpdir + f"/part-{temp:05d}", 'a+') as outfile:
                    with subprocess.Popen(
                        [executable],
                        text=True,
                        stdin=subprocess.PIPE,
                        stdout=outfile,
                    ) as reduce_process:
                        # Pipe input to reduce_process
                        for line in heapq.merge(*files):
                            reduce_process.stdin.write(line)

                shutil.move(tmpdir + (
                            f"/part-{self.reduce_job_request['task_id']:05d}"),
                            self.reduce_job_request["output_directory"] + (
                            f"/part-{self.reduce_job_request['task_id']:05d}")
                            )

        finished = {
            "message_type": "finished",
            "task_id": self.reduce_job_request["task_id"],
            "worker_host": self.host,
            "worker_port": self.port
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            message = json.dumps(finished)
            sock.sendall(message.encode('utf-8'))
        print("sent finished reduce job")
        self.reduce_job_request = {}

    def TCP_server(self):
        """Test TCP Socket Server."""
        # Create an INET, STREAMing socket, this is TCP
        # Note: context manager syntax allows for sockets to automatically be
        # closed when an exception is raised or control flow returns.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            # Socket accept() will block for a maximum of 1 second.  If you
            # omit this, it blocks indefinitely, waiting for a connection.
            sock.settimeout(1)
            # send worker registration
            self.send_registration()

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
                # If you omit this, it blocks indefinitely,
                # waiting for packets.
                clientsocket.settimeout(1)
                # Receive data, one chunk at a time.
                # If recv() times out before we can read a chunk,
                # then go back to the top of the loop and try again.
                # When the client closes the connection, recv() returns
                # empty data, which breaks out of the loop.
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
                self.check_shutdown(message_dict)
                self.map_job(message_dict)
                self.reduce_job(message_dict)
                self.receive_ack(message_dict)

    def send_registration(self):
        """Construct a worker instance and start listening for messages."""
        # connect to the server
        # s.bind((HOST, PORT))
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            while True and not self.signals["shutdown"]:
                try:
                    sock.connect((self.manager_host, self.manager_port))
                    break
                except socket.timeout:
                    continue

            # send a message
            message = json.dumps({
                    "message_type": "register",
                    "worker_host": self.host,
                    "worker_port": self.port,
                    })
            sock.sendall(message.encode('utf-8'))

    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )

        # This is a fake message to demonstrate pretty printing with logging
        # message_dict = {
        #     "message_type": "register_ack",
        #     "worker_host": "localhost",
        #     "worker_port": 6001,
        # }
        # LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))
        self.signals = {"shutdown": False}
        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.map_job_request = {}
        self.reduce_job_request = {}
        self.threads = []
        TCP_thread = threading.Thread(target=self.TCP_server)
        self.threads.append(TCP_thread)
        TCP_thread.start()

        run_job_thread = threading.Thread(target=self.run_job)
        run_job_thread.start()
        self.threads.append(run_job_thread)

        for thread in self.threads:
            thread.join()

        # TODO: you should remove this. This is just so the program doesn't
        # exit immediately!
        print('ending worker')


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()

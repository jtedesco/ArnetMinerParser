from _socket import AF_INET, SOCK_DGRAM
import math
import socket
import subprocess
import select
import sys

__author__ = 'jontedesco'

total_zip_files = 1052
total_docs = 10454961


def output_total_progress(slave_progress):
    """
      Output the total parsing progress, given the progress for each slave
    """

    total_zip_files_processed = 0
    total_docs_processed = 0

    # Estimate the percent complete for each slave
    slave_progress_percent = []
    for i in xrange(0, len(slave_progress)):

        j, zip_files_processed, zip_files_to_process, docs_processed = slave_progress[i]

        # Estimate the percent complete for this slave ...
        zip_files_progress = float(zip_files_processed) / zip_files_to_process * 100
        estimated_slave_docs = int(float(zip_files_to_process) / total_zip_files * total_docs)
        docs_progress = float(docs_processed) / estimated_slave_docs * 100
        slave_progress_percent.append(max(zip_files_progress, docs_progress))

        # Tally progress into total
        total_zip_files_processed += zip_files_processed
        total_docs_processed += docs_processed

    # Output aggregate progress
    total_docs_processed_percent = float(total_docs_processed) / total_docs * 100
    total_zip_files_processed_percent = float(total_zip_files_processed) / total_zip_files * 100
    sys.stdout.write("\rAggregate Progress: %d / %d docs (%2.2f%%), %d / %d files (%2.2f%%);  " %
                     (total_docs_processed, total_docs, total_docs_processed_percent, total_zip_files_processed,
                      total_zip_files, total_zip_files_processed_percent))

    # Output individual slave progress
    slaves_output_data = []
    for i in xrange(0, len(slave_progress_percent)):
        slaves_output_data.append('Slave %d: (%2.2f%%)' % (i + 1, slave_progress_percent[i]))
    sys.stdout.write("Slave Progress: " + ', '.join(slaves_output_data))


if __name__ == '__main__':

    # Calculate number of slaves to spawn
    files_per_slave = 351
    number_of_slaves = int(math.ceil(float(total_zip_files) / files_per_slave))

    # Map of slave ids to progress data
    slave_progress = {}

    # Spawn all child processes
    files_remaining = total_zip_files
    child_pids = []
    for i in range(0, number_of_slaves):
        start_num = str(1 + (files_per_slave * i))
        num_to_process = str(min(files_per_slave, files_remaining))
        child_pid = subprocess.Popen([
            "python",
            "first_pass_slave_parser.py",  # Call the other python file
            start_num,  # Specify the start file
            num_to_process,  # Number of files to process
            'n',  # Output to file, rather than standard output
            'n'  # Do not tally most common titles or perform profiling
        ])
        child_pids.append(child_pid)
        files_remaining -= files_per_slave

        # Initialize child processes progress
        slave_progress[i] = (i, 0, int(num_to_process), 0)

    # Start progress server
    server_address = ('localhost', 6005)
    server_socket = socket.socket(AF_INET, SOCK_DGRAM)
    server_socket.bind(server_address)
    server_socket.setblocking(0)

    # Keep listening until most slaves have neared completion
    not_ready_iterations = 0
    while True:

        # Assume children have finished if this happens ten times
        ready = select.select([server_socket], [], [], 1)
        if not ready[0]:
            not_ready_iterations += 1
            if not_ready_iterations > 10000:
                print "\nNo messages received from children, assuming complete..."
                break
        else:

            # Record the progress update
            received_data, client_address = server_socket.recvfrom(2048)
            start_i, zip_files_processed, zip_files_to_process, docs_processed = received_data.split()
            i = int(start_i) / files_per_slave
            new_data = (i, int(zip_files_processed), int(zip_files_to_process), int(docs_processed))

            # Check to make sure this was a NEW update (not out of order)
            in_order = all([new >= old for new, old in zip(new_data, slave_progress[i])])
            if in_order:
                slave_progress[i] = new_data
                output_total_progress(slave_progress)

    # Wait for all children to complete
    print "\nWaiting for children processes to exit..."
    for pid in child_pids:
        pid.wait()

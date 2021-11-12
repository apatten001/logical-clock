
# to load json file and parse data
import argparse
import json
import multiprocessing
from time import sleep
from concurrent import futures
import grpc
import Branch_pb2_grpc
from Branch import Branch
from Customer import Customer


# start branch server process
def server_for_branch(branch):
    branch.createStubs()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    Branch_pb2_grpc.add_BranchServicer_to_server(branch, server)
    port = str(50000 + branch.id)
    server.add_insecure_port("[::]:" + port)
    server.start()

    # allows for time for writing to the output file to maintain order of pids
    sleep(0.5 * branch.id)
    output_array = json.load(open("output.json"))
    output_array.append({"pid": branch.id, "data": branch.output()})
    output = json.dumps(output_array, indent=4)
    output_file = open("output.json", "w")
    output_file.write(output)
    output_file.close()

    server.wait_for_termination()


# start customer client process
def server_for_customer(customer):
    customer.createStub()
    customer.executeEvents()


# parse the JSON file to create processes
def process_creation(processes):
    # list of customers
    customers = []
    # list of customer processes
    customer_processes = []
    # list of branches
    branches = []
    # list of branch processes
    branch_processes = []
    # list of branch ids
    branch_ids = []

    # create instances of the Branch objects
    for process in processes:
        if process["type"] == "branch":
            branch = Branch(process["id"], process["balance"], branch_ids)
            branches.append(branch)
            branch_ids.append(branch.id)

    # produce the branch processes
    for branch in branches:
        branch_process = multiprocessing.Process(target=server_for_branch, args=(branch,))
        branch_processes.append(branch_process)
        branch_process.start()

    # a small amount of sleep for the branch processes to start execution
    sleep(0.25)

    # create instances of clients
    for process in processes:
        if process["type"] == "client":
            client = Customer(process["id"], process["events"])
            customers.append(client)

    # produce the  customer processes
    for customer in customers:
        customer_process = multiprocessing.Process(target=server_for_customer, args=(customer,))
        customer_processes.append(customer_process)
        customer_process.start()

    # allow customer processes to complete
    for process in customer_processes:
        process.join()

    # allow branches to complete output before terminating
    sleep(1)
    # complete the branch processes
    for process in branch_processes:
        process.terminate()


# Write events to output.json
def outputEvents():
    output = json.load(open("output.json"))
    events_dict = {}

    for pid in output:
        for event in pid["data"]:
            if event["id"] in events_dict.keys():
              events_dict[event["id"]].append(event)
            else:
              events_dict[event["id"]] = [event]

    for event in events_dict:
        data = sorted(events_dict[event], key=lambda event: event["clock"])
        output.append({"eventid": event, "data": data})

    out = open("output.json", "w")
    out.write(json.dumps(output, indent=4))
    out.close()


if __name__ == "__main__":
    # allows for the input for the 'input_file' at the CLI
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file")
    args = parser.parse_args()

    try:
        # load json file from the input argument
        input_data = json.load(open(args.input_file))

        # Initialize output file
        output_file = open("output.json", "w")
        output_file.write("[]")
        output_file.close()

        # create processes from the input data
        process_creation(input_data)

        sleep(1.5)
        outputEvents()
    except FileNotFoundError:
        print("Could not find file")


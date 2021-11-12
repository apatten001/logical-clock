
import grpc
import Branch_pb2_grpc
from Branch_pb2 import MsgRequest, MsgResponse


class Branch(Branch_pb2_grpc.BranchServicer):

    def __init__(self, id, balance, branches):
        # unique ID of the Branch
        self.id = id
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # the list of Client stubs to communicate with the branches
        self.stubList = list()
        # a list of received messages used for debugging purpose
        self.events = list()
        # iterate the processID of the branches
        self.clock = 1

        # setup channel & client stub for each branch using grpc
    def createStubs(self):
        for branch_id in self.branches:
            if branch_id != self.id:
                port = str(50000 + branch_id)
                channel = grpc.insecure_channel("localhost:" + port)
                self.stubList.append(Branch_pb2_grpc.BranchStub(channel))

    # receives incoming message request from the customer transaction and starts message processing
    def MsgDelivery(self, request, context):
        if request.interface != "query":
            self.Event_Request(request)

        return self.ProcessMsg(request, False)

    # allows for the Branch propagation from incoming msg request
    def MsgPropagation(self, request, context):
        if request.interface != "query":
            self.Propagate_Request(request)
        return self.ProcessMsg(request, True)

    # handle received Msg, generate and return a MsgResponse
    def ProcessMsg(self, request, propagate):
        if request.interface != "query":
            if not propagate:
                self.Event_Execute(request)
            else:
                self.Propagate_Execute(request)
        result = "success"

        if request.money < 0:
            result = "fail"
        elif request.interface == "query":
            pass
        elif request.interface == "deposit":
            self.balance += request.money

        elif request.interface == "withdraw":
            if self.balance >= request.money:
                self.balance -= request.money

            else:
                result = "fail"
        else:
            result = "fail"

        # Create msg to be appended to self.recvMsg list
        response = MsgResponse(id=request.id, interface=request.interface, result=result,money=self.balance, clock=self.clock)

        # Add 'money' entry for 'query' events
        if request.interface != "query" and not propagate:
            self.Event_Response(response)
            for stub in self.stubList:
                response = stub.MsgPropagation(
                    MsgRequest(id=request.id, interface=request.interface, money=request.money, clock=self.clock)
                )
                self.Propagate_Response(response)

        return response

    # Receive event from Customer (max + 1)
    def Event_Request(self, request):
        self.clock = max(self.clock, request.clock) + 1
        self.events.append({"id": request.id, "name": request.interface + "_request", "clock": self.clock})

    # Execute event from Customer (+ 1)
    def Event_Execute(self, request):
        self.clock += 1
        self.events.append({"id": request.id, "name": request.interface + "_execute", "clock": self.clock})

    # Receive propagated event from Branch (max + 1)
    def Propagate_Request(self, request):
        self.clock = max(self.clock, request.clock) + 1
        self.events.append({"id": request.id, "name": request.interface + "_propagate_request", "clock": self.clock})

    # Execute propagated event from Branch (+ 1)
    def Propagate_Execute(self, request):
        self.clock += 1
        self.events.append({"id": request.id, "name": request.interface + "_propagate_execute", "clock": self.clock})

    # retrieve the returned prop response from the branch; take max(clock,response.clock) +1
    def Propagate_Response(self, response):
        self.clock = max(self.clock, response.clock) + 1
        self.events.append({"id": response.id, "name": response.interface + "_propagate_response", "clock": self.clock})

    # return the event response to the customer; increase clock 1
    def Event_Response(self, response):
        self.clock += 1
        self.events.append({"id": response.id, "name": response.interface + "_response", "clock": self.clock})

    # create message for output
    def output(self):
        return self.events

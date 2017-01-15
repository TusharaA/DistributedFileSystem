from Logger import Logger
import  pipe_pb2

class ClientServerMessageBuilder:
    def __init__(self):
        self.logger = Logger("MessageBuilder.log")

    def buildGeneralMessage(self, nodeId, time, message):
        newCommandMsg = pipe_pb2.CommandMessage()
        newCommandMsg.header.node_id = nodeId
        newCommandMsg.header.time = time
        newCommandMsg.message = message
        newCommandMsgString = newCommandMsg.SerializeToString()
        self.logger.logInfoMessage("Client: Created a new general build message: " + newCommandMsgString)
        return newCommandMsgString

    def buildReadCommandMessage(self, nodeId, time, myIp, filename):
        newCommandMsg = pipe_pb2.CommandMessage()
        newCommandMsg.header.node_id = nodeId
        newCommandMsg.header.time = time

        newCommandMsg.message = filename
        newCommandMsg.filetask.file_task_type = pipe_pb2.FileTask.FileTaskType.Value("READ")
        newCommandMsg.filetask.sender = myIp
        newCommandMsg.filetask.filename = filename

        newCommandMsgString = newCommandMsg.SerializeToString()
        self.logger.logInfoMessage("Client: Created a new read work message: " + newCommandMsgString)
        return newCommandMsgString

    def buildWriteCommandMessage(self, nodeId, time, myIp, filename, chunkCount, chunkId, fileChunk):
        newCommandMsg = pipe_pb2.CommandMessage()
        newCommandMsg.header.node_id = nodeId
        newCommandMsg.header.time = time

        newCommandMsg.message = filename
        newCommandMsg.filetask.file_task_type = pipe_pb2.FileTask.FileTaskType.Value("WRITE")
        newCommandMsg.filetask.sender = myIp
        newCommandMsg.filetask.filename = filename
        newCommandMsg.filetask.chunk_no = chunkId
        newCommandMsg.filetask.chunk_counts = chunkCount
        newCommandMsg.filetask.chunk = fileChunk

        newCommandMsgString = newCommandMsg.SerializeToString()
        self.logger.logInfoMessage("Client: Created a new read write message: " + newCommandMsgString)
        return newCommandMsgString

    def buildWriteUpdateCommandMessage(self, nodeId, time, myIp, filename, chunkCount, chunkId, fileChunk):
        newCommandMsg = pipe_pb2.CommandMessage()
        newCommandMsg.header.node_id = nodeId
        newCommandMsg.header.time = time

        newCommandMsg.message = filename
        newCommandMsg.filetask.file_task_type = pipe_pb2.FileTask.FileTaskType.Value("UPDATE")
        newCommandMsg.filetask.sender = myIp
        newCommandMsg.filetask.filename = filename
        newCommandMsg.filetask.chunk_no = chunkId
        newCommandMsg.filetask.chunk_counts = chunkCount
        newCommandMsg.filetask.chunk = fileChunk

        newCommandMsgString = newCommandMsg.SerializeToString()
        self.logger.logInfoMessage("Client: Created a new write udpate message: " + newCommandMsgString)
        return newCommandMsgString

    def buildDeleteFileCommandMessage(self, nodeId, time, myIp, filename):
        newCommandMsg = pipe_pb2.CommandMessage()
        newCommandMsg.header.node_id = nodeId
        newCommandMsg.header.time = time

        newCommandMsg.message = filename
        newCommandMsg.filetask.file_task_type = pipe_pb2.FileTask.FileTaskType.Value("DELETE")
        newCommandMsg.filetask.sender = myIp
        newCommandMsg.filetask.filename = filename

        newCommandMsgString = newCommandMsg.SerializeToString()
        self.logger.logInfoMessage("Client: Created a new delete message: " + newCommandMsgString)
        return newCommandMsgString

    def buildPingCommandMessage(self, nodeId, time):
        newCommandMsg = pipe_pb2.CommandMessage()
        newCommandMsg.header.node_id = nodeId
        newCommandMsg.header.time = time
        newCommandMsg.ping = True
        newCommandMsgString = newCommandMsg.SerializeToString()
        self.logger.logInfoMessage("Client: Created a new ping message: " + newCommandMsgString)
        return newCommandMsgString
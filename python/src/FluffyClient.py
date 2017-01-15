import  socket
import  struct
import  time
import common_pb2
import  pipe_pb2
import  pbd
from protobuf_to_dict import protobuf_to_dict
from asyncore import read
from Logger import Logger
from ClientServerMessageBuilder import ClientServerMessageBuilder
from termcolor import colored

class FluffyClient:
    def __init__(self, host, portNumber):
        self.host = host
        self.portNumber = portNumber
        self.nodeId = 999
        self.time = 20000
        self.myIp = "127.0.0.1"
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.logger = Logger("fluffyClient.log")
        self.clientServerMessageBuilder = ClientServerMessageBuilder()
        self.logger.logInfoMessage("Client: Created a Fluffy client object")

    def setName(self, name):
        self.name = name

    def getName(self):
        return self.name

    def print_success(self, msg):
        print (colored(msg, 'green'))

    def print_error(self, msg):
        print (colored(msg, 'red'))

    def print_warning(self, msg):
        print (colored(msg, 'orange'))

    def print_info(self, msg):
        print (colored(msg, 'blue'))

    def send_ping(self):
        commandMsg = self.clientServerMessageBuilder.buildPingCommandMessage(1, 20000)
        self._sendCommandMessage(commandMsg)

    def openConnection(self):
        self.socket.connect((self.host, self.portNumber))
        self.logger.logInfoMessage("Client: Connected to a server socket using address: " + self.host + " and port: " + str(self.portNumber))
        self.print_success("Client: Connected to a server socket using address: " + self.host + " and port: " + str(self.portNumber) + "\n")

    def closeConnection(self):
        connectionStopMsg = self.clientServerMessageBuilder.buildGeneralMessage(self.nodeId, self.time, self.name + " is closing the session!!!")
        self.logger.logInfoMessage("Client: Client closed the connection with the server: " + connectionStopMsg)
        self.print_info("Client: Client closed the connection with the server: " + self.name + " is closing the session!!!")
        self.socket.send(connectionStopMsg)
        self.socket.close()
        self.socket = None

    def sendConnectedInfoToServer(self):
        newConnectionInfoMsg = self.clientServerMessageBuilder.buildGeneralMessage(self.nodeId, self.time, self.name + " is connected to the server")
        self.socket.send(newConnectionInfoMsg)
        self.logger.logInfoMessage("Client: Client is connected with the server: " + newConnectionInfoMsg)
        self.print_info("Client: Client is connected with the server: " + newConnectionInfoMsg)

    def sendMessageToServer(self, message):
        if len(message) > 1024 * 1024:
            print('Client: message exceeds 1MB size')
            return
        msgToServer = self.clientServerMessageBuilder.buildGeneralMessage(self.nodeId, self.time, self.name + " says: " + message)
        self.socket.send(msgToServer)
        self.logger.logInfoMessage("Client: Client is sending a message to server: " + msgToServer)
        self.print_info("Client: Client is sending a message to server: " + msgToServer)

    def chunkFileInto1MB(self, file):
        oneMBfileChunks = []
        fileReadSize = 1024 * 1024
        try:
            with open(file, "rb") as file:
                dataRead = file.read(fileReadSize)
                while dataRead != '' and dataRead != None and len(dataRead) > 0:
                    oneMBfileChunks.append(dataRead)
                    dataRead = file.read(fileReadSize)
                return oneMBfileChunks
        except IOError as e:
            raise ValueError('Unable to read file: ' + file + ' error: ' + e.message)


    def _sendCommandMessage(self, commandMessage):
        messageLength = struct.pack('>L', len(commandMessage))
        self.socket.sendall(messageLength + commandMessage)
        self.logger.logInfoMessage("Client: Client is sending data to server: " + messageLength + commandMessage)

    def _recvCommandMessage(self):
        len_buf = self.receiveMessageFromServer(self.socket, 4)
        msg_in_len = struct.unpack('>L', len_buf)[0]
        msg_in = self.receiveMessageFromServer(self.socket, msg_in_len)
        r = pipe_pb2.CommandMessage()
        r.ParseFromString(msg_in)
        return r

    def sendFileToServer(self, filename, chunkCount, chunkId, fileChunk):
        newCommandMsg = self.clientServerMessageBuilder.buildWriteCommandMessage(self.nodeId, self.time, self.myIp, filename, chunkCount, chunkId, fileChunk)
        self._sendCommandMessage(newCommandMsg)
        return self._recvCommandMessage()

    def getFileFromServer(self, filepath, filename):
        newReadCommandMessage = self.clientServerMessageBuilder.buildReadCommandMessage(self.nodeId, self.time, self.myIp, filename)
        self._sendCommandMessage(newReadCommandMessage)
        self.fileResponseMap = {}
        msg = self._recvCommandMessage()
        self.fileResponseMap[msg.filetask.chunk_no] = msg.filetask.chunk
        chunkCounts = msg.filetask.chunk_counts
        chunkCounts = chunkCounts - 1
        for x in range(chunkCounts):
            msg = self._recvCommandMessage()
            self.fileResponseMap[msg.filetask.chunk_no] = msg.filetask.chunk
            pass

        try:
            with open(filepath + "/" + "new" + msg.filetask.filename, "w") as file:
                for x in range(chunkCounts + 1):
                    file.write(self.fileResponseMap[x + 1])
                file.close()
                pass
            return "created file at: " + filepath + " with name: " + filename
        except IOError as e:
            raise ValueError('Cannot create file: ' + filename + ' at filepath: ' + filepath + ' error: ' + e.message)

    def updateFileInServer(self, filename, chunkCount, chunkId, fileChunk):
        newCommandMsg = self.clientServerMessageBuilder.buildWriteUpdateCommandMessage(self.nodeId, self.time, self.myIp, filename, chunkCount, chunkId, fileChunk)
        self._sendCommandMessage(newCommandMsg)
        return self._recvCommandMessage()
        pass

    def deleteFileFromServer(self, filename):
        newdeleteCommandMessage = self.clientServerMessageBuilder.buildDeleteFileCommandMessage(self.nodeId, self.time, self.myIp, filename)
        self._sendCommandMessage(newdeleteCommandMessage)
        pass

    def receiveMessageFromServer(self, socket, n):
        buf = ''
        while n > 0:
            data = socket.recv(n)
            if data == '':
                raise RuntimeError('data not received!')
            buf += data
            n -= len(data)
        self.logger.logInfoMessage("Client: file contents received: " + str(buf))
        return buf
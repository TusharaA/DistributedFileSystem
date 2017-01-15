import sys
from Logger import Logger
from FluffyClient import FluffyClient
from termcolor import colored
import progressbar
from time import sleep

class FluffyClientApplication:
    def __init__(self):
        self.name = None
        self.host = None
        self.port = None
        self.fluffyClient = None
        pass

    def _printHeaderMessage(self, name):
        print("\n")
        self.print_info("#########################################################################################################")
        self.print_info("###################################Fluffy Client Application#############################################")
        self.print_info("#########################################################################################################")
        print("\n")

    def _printClosingMessage(self, name):
        print("\n")
        self.print_info("#########################################################################################################")
        self.print_info("###################################Thank you for choosing Fluffy#########################################")
        self.print_info("#########################################################################################################")
        print("\n")

    def _printHelpContent(self):
        self.print_menu("Select an option by using the option number tagged along with the option: ")
        self.print_menu("**************************************************************************")
        self.print_menu("(1) upload - uploads a file into fluffy server\n")
        self.print_menu("(2) download - downloads a file from fluffy server\n")
        self.print_menu("(3) re-upload - uploads a file to update in fluffy server\n")
        self.print_menu("(4) delete - deletes the file in fluffy server\n")
        self.print_menu("(5) help - prints all the options present\n")
        self.print_menu("(6) ping - ping the server\n")
        self.print_menu("(7) exit - ends the application\n")
        #print("**************************************************************************")
        self.print_info("Your option: ")

    def print_success(self, msg):
        print (colored(msg, 'green'))

    def print_error(self, msg):
        print (colored(msg, 'red'))

    def print_warning(self, msg):
        print (colored(msg, 'orange'))

    def print_info(self, msg):
        print (colored(msg, 'blue'))

    def print_menu(self, msg):
        print (colored(msg, 'magenta'))

    def start(self):
        self.name = None
        self.host = None
        self.port = None
        self.fluffyClient = None
        while self.name == None or self.host == None or self.port == None:
            if(self.name == None):
                self.print_info("Enter your name to join the connection: ")
                self.name = sys.stdin.readline()
                if(self.name == None):
                    continue
            if(self.host == None):
                self.print_info("Enter the IP of Server to connect: ")
                self.host = raw_input()
                if(self.host == None):
                    continue
            if(self.port == None):
                self.print_info("Enter the port of server: ")
                self.port = int(raw_input())
                if(self.port == None):
                    continue
            if(self.name != None and self.host != None and self.port != None):
                break
        self._printHeaderMessage(self.name)
        self.fluffyClient = FluffyClient(self.host, self.port)
        self.fluffyClient.openConnection()
        self.fluffyClient.setName(self.name)
        self._printHelpContent()
        while(True):
            user_input = raw_input()
            if(user_input == None or len(user_input) == 0 or (not user_input.isdigit())):
                self._printHelpContent()
                continue

            optionSelected = int(user_input)
            ##
            # storing the file in Fluffy
            ##
            if (optionSelected == 1):
                self.print_info("Enter path name of the file (eg: '/home/abcd/folder'): ")
                filepath = raw_input()
                self.print_info("Enter the name of your file (eg: test.txt): ")
                filename = raw_input()
                #bc.sendData(bc.genPing(),"127.0.0.1",4186);
                try:
                    fileChunks = self.fluffyClient.chunkFileInto1MB(filepath + '/' + filename)
                except ValueError as e:
                    self.print_error("Error: Cannot create chunks: " + e.message)
                    continue
                chunkCount = len(fileChunks)
                chunkId = 1
                index = 1
                multiple = chunkCount / 100
                if (multiple > 0):
                    self.print_info("Uploading.......")
                    for fileChunk in fileChunks:
                        serverResponse = self.fluffyClient.sendFileToServer(filename, chunkCount, chunkId, fileChunk)
                        if (chunkId % multiple == 0 and index <= 100):
                            sys.stdout.write('\r')
                            sys.stdout.write("[%-100s] %d%%" % ('=' * index, 1 * index))
                            sys.stdout.flush()
                            index += 1
                        chunkId += 1

                else:
                    self.print_info("Uploading.......")
                    for fileChunk in fileChunks:
                        serverResponse = self.fluffyClient.sendFileToServer(filename, chunkCount, chunkId, fileChunk)
                        chunkId += 1

                self.print_success("Client: Successfully uploaded file")
                self._printHelpContent()
            ##
            # retrieving the file from Fluffy
            ##
            elif (optionSelected == 2):
                self.print_info("Enter the filename you want to download from Fluffy: ")
                filename = raw_input()
                self.print_info("Enter the filepath for file download location: ")
                filepath = raw_input()
                try:
                    fileContents = self.fluffyClient.getFileFromServer(filepath, filename)
                except ValueError as e:
                    self.print_error("Error: cannot create a file at the location: " + e.message)
                    continue
                self.print_success("Client: Successfully downloaded file")
                self._printHelpContent()

            ##
            # re-upload the file i.e update
            ##
            elif (optionSelected == 3):
                self.print_info("Enter path name of the file (eg: '/home/abcd/folder'): ")
                filepath = raw_input()
                self.print_info("Enter the name of your file (eg: test.txt): ")
                filename = raw_input()
                # bc.sendData(bc.genPing(),"127.0.0.1",4186);
                fileChunks = self.fluffyClient.chunkFileInto1MB(filepath + '/' + filename)
                chunkCount = len(fileChunks)
                chunkId = 1
                index = 1
                multiple = chunkCount / 100
                if(multiple > 0):
                    self.print_info("ReUploading.......")
                    for fileChunk in fileChunks:
                        serverResponse = self.fluffyClient.updateFileInServer(filename, chunkCount, chunkId, fileChunk)
                        if (chunkId % multiple == 0 and index <= 100):
                            sys.stdout.write('\r')
                            sys.stdout.write("[%-100s] %d%%" % ('=' * index, 1 * index))
                            sys.stdout.flush()
                            index += 1
                        chunkId += 1

                else:
                    self.print_info("ReUploading.......")
                    for fileChunk in fileChunks:
                        serverResponse = self.fluffyClient.updateFileInServer(filename, chunkCount, chunkId, fileChunk)
                        chunkId += 1


                self.print_success("Client: Successfully re uploaded file")
                self._printHelpContent()

            ##
            #  delete the file
            ##
            elif (optionSelected == 4):
                self.print_info("Enter the filename you want to delete from Fluffy: ")
                filename = raw_input()
                self.fluffyClient.deleteFileFromServer(filename)
                self.print_error("Successfully deleted the file: " + filename)
                self._printHelpContent()
            ##
            # displaying the commands
            ##
            elif (optionSelected == 5):
                self._printHelpContent()
            ##
            # exiting the client application
            ##
            elif (optionSelected == 6):
                self.fluffyClient.send_ping()
                self.print_info("pinged the server")
                self._printHelpContent()

            ##
            # exiting the client application
            ##
            elif (optionSelected == 6):
                self.fluffyClient.closeConnection()
                self._printClosingMessage(self.name)
                break
            else:
                self.print_error("Error: Incorrect option selected: " + str(optionSelected) + " ,option should be in range (1 - 6)")

if __name__ == "__main__":
    fluffyApp = FluffyClientApplication()
    fluffyApp.start()
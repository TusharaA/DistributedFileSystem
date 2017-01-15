import logging


class Logger:
	def __init__(self, logFileName):
		logging.basicConfig(filename = logFileName, level = logging.INFO)

	def logInfoMessage(self, message):
		logging.info(message)

	def logException(self, exception, message):
		logging.error("****Exception occured****")
		logging.error(message)
		logging.error(exception)

	def logWarnMessage(Self, message):
		logging.info("warning: " + message)

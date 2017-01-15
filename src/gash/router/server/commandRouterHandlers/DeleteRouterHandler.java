package gash.router.server.commandRouterHandlers;

import io.netty.channel.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;
import routing.Pipe.FileTask.FileTaskType;
import gash.router.database.DatabaseHandler;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.message.generator.MessageGenerator;
import gash.router.server.queue.management.InternalChannelNode;
import gash.router.server.queue.management.LoadQueueManager;
import gash.router.server.queue.management.NodeLoad;
import gash.router.server.queue.management.QueueManager;
import gash.router.server.replication.DataReplicationManager;

public class DeleteRouterHandler implements ICommandRouterHandlers {
	private  ICommandRouterHandlers nextInChain;
	protected static Logger logger = LoggerFactory.getLogger(DeleteRouterHandler.class);
	
	@Override
	public void setNextChainHandler(ICommandRouterHandlers nextChain) {
		this.nextInChain = nextChain;
		
	}

	@Override
	public void handleFileTask(InternalChannelNode request) throws Exception {
		FileTaskType taskType = request.getCommandMessage().getFiletask().getFileTaskType();
		if(taskType == FileTaskType.DELETE){
			String filename = request.getCommandMessage().getFiletask().getFilename();
			if(DatabaseHandler.isFileAvailable(filename)){
				logger.info("Deleting the file from database : " + filename);
				if(DatabaseHandler.deleteFile(filename)){
					CommandMessage commandMessage = MessageGenerator.getInstance().generateClientResponseMsg("File is deleted successfully");
					QueueManager.getInstance().enqueueOutboundCommmand(commandMessage, request.getChannel());
					DataReplicationManager.getInstance().broadcastDeletion(request.getCommandMessage());
				} else {
					CommandMessage commandMessage = MessageGenerator.getInstance().generateClientResponseMsg("File is not successfully deleted....");
					QueueManager.getInstance().enqueueOutboundCommmand(commandMessage, request.getChannel());
					logger.error("File requested to delete is not deleted from the database");					
				}
			} else {
				CommandMessage commandMessage = MessageGenerator.getInstance().generateClientResponseMsg("File is not available so cannot be deleted....");
				QueueManager.getInstance().enqueueOutboundCommmand(commandMessage, request.getChannel());
				logger.error("File requested to delete is not available in the database");
			}
		} else {
			nextInChain.handleFileTask(request);
		}
		
	}

}

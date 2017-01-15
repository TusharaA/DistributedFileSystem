package gash.router.server.commandRouterHandlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import routing.Pipe.CommandMessage;
import routing.Pipe.FileTask;
import routing.Pipe.FileTask.FileTaskType;
import gash.router.database.DatabaseHandler;
import gash.router.server.message.generator.MessageGenerator;
import gash.router.server.queue.management.InternalChannelNode;
import gash.router.server.queue.management.QueueManager;
import gash.router.server.replication.DataReplicationManager;
import gash.router.server.replication.UpdateFileInfo;

public class UpdateRouterHandler implements ICommandRouterHandlers  {
	private  ICommandRouterHandlers nextInChain;
	protected static Logger logger = LoggerFactory.getLogger(UpdateRouterHandler.class);
	
	@Override
	public void setNextChainHandler(ICommandRouterHandlers nextChain) {
		this.nextInChain = nextChain;
		
	}

	@Override
	public void handleFileTask(InternalChannelNode request) throws Exception {
		// TODO Auto-generated method stub
		FileTask fileTask = request.getCommandMessage().getFiletask();
		FileTaskType taskType = fileTask.getFileTaskType();
		if(taskType == FileTaskType.UPDATE){
			boolean inRiak = DatabaseHandler.isFileAvailableInRiak(fileTask.getFilename());
			boolean inRethink = DatabaseHandler.isFileAvailableInRethink(fileTask.getFilename());
			String filename = fileTask.getFilename();
			if(inRiak || inRethink){
				logger.info("Deleting the file from database to update : " + filename);
				if(!DataReplicationManager.fileUpdateTracker.containsKey(filename)){
					System.out.println("Entered Here::::::::::");
					UpdateFileInfo fileInfo = new UpdateFileInfo(fileTask.getChunkCounts());
					DataReplicationManager.fileUpdateTracker.put(filename, fileInfo);

					if(DatabaseHandler.deleteFile(filename)){
						DataReplicationManager.getInstance().broadcastUpdateDeletion(request.getCommandMessage());						
					} else {
						CommandMessage commandMessage = MessageGenerator.getInstance().generateClientResponseMsg("File is not updated successfully, issues while deleting previous file....");
						QueueManager.getInstance().enqueueOutboundCommmand(commandMessage, request.getChannel());
						logger.error("File requested to update operation failed, in step to delete from the database");					
					} 
				} 
				UpdateFileInfo fileInfo = DataReplicationManager.fileUpdateTracker.get(filename);
				
				if(DatabaseHandler.addFile(fileTask.getFilename(), fileTask.getChunkCounts(), fileTask.getChunk().toByteArray(), fileTask.getChunkNo())){
						fileInfo.decrementChunkProcessed();
						
						CommandMessage commandMessage = MessageGenerator.getInstance().generateClientResponseMsg("File is updated successfully in the database");
						QueueManager.getInstance().enqueueOutboundCommmand(commandMessage, request.getChannel());
						
						DataReplicationManager.getInstance().broadcastUpdateReplication(request.getCommandMessage());
					} else {
						CommandMessage commandMessage = MessageGenerator.getInstance().generateClientResponseMsg("File is not stored in the database, please retry with write ...");
						QueueManager.getInstance().enqueueOutboundCommmand(commandMessage, request.getChannel());
						logger.error("Database write error, couldnot update the file into the database");
					}
				if(fileInfo.getChunksProcessed() > 0) {
					DataReplicationManager.fileUpdateTracker.put(filename, fileInfo);
				} else {
					DataReplicationManager.fileUpdateTracker.remove(filename);
				}
				
				CommandMessage commandMessage = MessageGenerator.getInstance().generateClientResponseMsg("File is deleted successfully");
				QueueManager.getInstance().enqueueOutboundCommmand(commandMessage, request.getChannel());
				
			} else {
				CommandMessage commandMessage = MessageGenerator.getInstance().generateClientResponseMsg("Cannot update as file is not in the database");
				QueueManager.getInstance().enqueueOutboundCommmand(commandMessage, request.getChannel());
				logger.error("File cannot be updated as it is not available in the database");
			}
		} else{
			logger.error("Handles only client read and write requests ");
		}
	}

}

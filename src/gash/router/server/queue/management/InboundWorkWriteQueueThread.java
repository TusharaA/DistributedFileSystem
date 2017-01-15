package gash.router.server.queue.management;

import gash.router.database.DatabaseHandler;
import gash.router.server.message.generator.MessageGenerator;
import gash.router.server.replication.DataReplicationManager;
import gash.router.server.replication.UpdateFileInfo;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.Worktype;
import routing.Pipe.CommandMessage;
import routing.Pipe.FileTask;


public class InboundWorkWriteQueueThread extends Thread{
	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger(InboundWorkWriteQueueThread.class);
	public InboundWorkWriteQueueThread(QueueManager manager) {
		super();
		this.manager = manager;
		if (manager.inboundWorkWriteQueue == null)
			throw new RuntimeException("Manager has no inbound work write queue");
	}
	
	@Override
	public void run() {

		while (true) {
			try {
				InternalChannelNode internalNode = manager.dequeueInboundWorkWrite();
				//To-do comment it after testing
				if(internalNode != null){
					WorkMessage workMessage = internalNode.getWorkMessage();
					Channel channel = internalNode.getChannel();
					FileTask ft= workMessage.getFiletask();
					if(workMessage.getWorktype() == Worktype.REPLICATE_REQUEST){
						if(DatabaseHandler.addFile(ft.getFilename(), ft.getChunkCounts(), ft.getChunk().toByteArray(), ft.getChunkNo())){

							WorkMessage workResponseMessage = MessageGenerator.getInstance().generateReplicationAcknowledgementMessage(workMessage);
							QueueManager.getInstance().enqueueOutboundWorkWrite(workResponseMessage, channel);
						} else {
							logger.info("Data is not replicated in the slave node, enqueuing the message back into the queue");
							QueueManager.getInstance().enqueueInboundWorkWrite(workMessage, channel);
						} 
					} else if (workMessage.getWorktype() == Worktype.DELETE_REQUEST){
						if(DatabaseHandler.deleteFile(ft.getFilename())){
							WorkMessage workResponseMessage = MessageGenerator.getInstance().generateDeletionAcknowledgementMessage(workMessage);
							QueueManager.getInstance().enqueueOutboundWorkWrite(workResponseMessage, internalNode.getChannel());
						} else {
							logger.info("Data is not replicated in the slave node, enqueuing the message back into the queue");
							QueueManager.getInstance().enqueueInboundWorkWrite(workMessage, channel);
						} 					
					} else if (workMessage.getWorktype() == Worktype.UPDATE_DELETE_REQUEST){
						String filename = workMessage.getFiletask().getFilename();
						FileTask fileTask = workMessage.getFiletask();
						if(!DataReplicationManager.fileUpdateTracker.containsKey(filename)){
							UpdateFileInfo fileInfo = new UpdateFileInfo(fileTask.getChunkCounts());
							DataReplicationManager.fileUpdateTracker.put(filename, fileInfo);
							logger.info("Trying to delete the file, in slave");
							
							if(DatabaseHandler.deleteFile(filename)){
								WorkMessage workResponseMessage = MessageGenerator.getInstance().generateUpdateDeletionAcknowledgementMessage(workMessage);
								QueueManager.getInstance().enqueueOutboundWorkWrite(workResponseMessage, channel);
							} else {
								logger.info("Data of the updated file is not deleted in the slave node, enqueuing the message back into the queue");
								QueueManager.getInstance().enqueueInboundWorkWrite(workMessage, channel);				
							} 
						} 
					} else if (workMessage.getWorktype() == Worktype.UPDATE_REPLICATE_REQUEST){
						String filename = workMessage.getFiletask().getFilename();
						FileTask fileTask = workMessage.getFiletask();
						if(!DataReplicationManager.fileUpdateTracker.containsKey(filename)){
							logger.info("Update replication cannot be performed before recieving delete request.... enqueuing the message back into the queue");
							QueueManager.getInstance().enqueueInboundWorkWrite(workMessage, channel);		
						} else {
							UpdateFileInfo fileInfo = DataReplicationManager.fileUpdateTracker.get(filename);
							
							if(DatabaseHandler.addFile(fileTask.getFilename(), fileTask.getChunkCounts(), fileTask.getChunk().toByteArray(), fileTask.getChunkNo())){
									fileInfo.decrementChunkProcessed();
									
									WorkMessage workResponseMessage = MessageGenerator.getInstance().generateUpdateReplicationAcknowledgementMessage(workMessage);
									QueueManager.getInstance().enqueueOutboundWorkWrite(workResponseMessage, channel);
									
								} else {
									logger.info("Update replication is not successful.... enqueuing the message back into the queue");
									QueueManager.getInstance().enqueueInboundWorkWrite(workMessage, channel);	
								}
							
							if(fileInfo.getChunksProcessed() > 0) {
								DataReplicationManager.fileUpdateTracker.put(filename, fileInfo);
							} else {
								DataReplicationManager.fileUpdateTracker.remove(filename);
							}
						}
					}
				}
				//int destinationNode = message.getWorkMessage().getHeader().getNodeId();
		       	//logger.info("Inbound write work message routing to node " + destinationNode);

			} catch (Exception e) {
				logger.error("Unexpected management communcation failure", e);
				break;
			}

		}
	}
}

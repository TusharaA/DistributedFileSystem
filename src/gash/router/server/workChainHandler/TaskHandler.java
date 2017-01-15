package gash.router.server.workChainHandler;

import java.io.IOException;

import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import gash.router.database.DatabaseHandler;
import gash.router.server.ServerState;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.manage.exceptions.EmptyConnectionPoolException;
import gash.router.server.manage.exceptions.FileChunkNotFoundException;
import gash.router.server.manage.exceptions.FileNotFoundException;
import gash.router.server.message.generator.MessageGenerator;
import gash.router.server.queue.management.InternalChannelNode;
import gash.router.server.queue.management.QueueManager;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.Worktype;
import routing.Pipe.CommandMessage;
import routing.Pipe.FileTask;

public class TaskHandler implements IWorkChainHandler{
	private IWorkChainHandler nextChainHandler;
	protected ServerState state;
	protected static Logger logger = LoggerFactory.getLogger(TaskHandler.class);
	@Override
	public void setNextChain(IWorkChainHandler nextChain,ServerState state) {
		this.nextChainHandler = nextChain;
		this.state = state;
	}

	@Override
	public void handle(WorkMessage workMessage, Channel channel) {
		// TODO Auto-generated method stub
		if(workMessage.hasFiletask()) {
			//logger.info("Recieved replicate work message");
			if(workMessage.getWorktype() == Worktype.READ_REQUEST){
				QueueManager.getInstance().enqueueInboundRead(workMessage, channel);
			} else if (workMessage.getWorktype() == Worktype.READ_REQUEST_RESPONSE){
				logger.info("Response from slave node for client read request");	

				InternalChannelNode clientInfo = EdgeMonitor.getClientChannelFromMap(workMessage.getRequestId());
				Channel clientChannel =  clientInfo.getChannel();
				
				clientInfo.decrementChunkCount();
				if(clientInfo.getChunkCount() == 0){
					logger.info("Removing client info from the client channel Map");
					try {
						EdgeMonitor.removeClientChannelInfoFromMap(workMessage.getRequestId());
					} catch (Exception e) {
						logger.info("Client channel is not removed successfully from the client channel Map");
						e.printStackTrace();
					}
				}
				
				CommandMessage outputMsg = MessageGenerator.getInstance().forwardChunkToClient(workMessage);
				QueueManager.getInstance().enqueueOutboundCommand(outputMsg, clientChannel);
				
			} else if(workMessage.getWorktype() == Worktype.REPLICATE_REQUEST){
				logger.info("Recieved replicate work message");
				QueueManager.getInstance().enqueueInboundWorkWrite(workMessage, channel);

			} else if(workMessage.getWorktype() == Worktype.REPLICATE_RESPONSE){
				logger.info("Recieved replication successful message");
				//logger.info("Data Replication successful for filename: " + workMessage.getFiletask().getFilename() + " for chunk id" + 
							//workMessage.getFiletask().getChunkNo());
			} else if(workMessage.getWorktype() == Worktype.DELETE_REQUEST){
				logger.info("Deleting the file from database : " + workMessage.getFiletask().getFilename());
				QueueManager.getInstance().enqueueInboundWorkWrite(workMessage, channel);
			} else if(workMessage.getWorktype() == Worktype.DELETE_RESPONSE){
				logger.info("Recieved deletion successful message");
			} else if(workMessage.getWorktype() == Worktype.UPDATE_DELETE_REQUEST){
				QueueManager.getInstance().enqueueInboundWorkWrite(workMessage, channel);
			} else if(workMessage.getWorktype() == Worktype.UPDATE_DELETE_RESPONSE){
				logger.info("Recieved update deletion successful message");
			} else if(workMessage.getWorktype() == Worktype.UPDATE_REPLICATE_REQUEST){
				QueueManager.getInstance().enqueueInboundWorkWrite(workMessage, channel);
			} else if(workMessage.getWorktype() == Worktype.UPDATE_REPLICATE_RESPONSE){
				logger.info("Recieved update replication successful message");
			}
		} else {
			//this.nextChainHandler.handle(workMessage, channel);
		}
	}
}
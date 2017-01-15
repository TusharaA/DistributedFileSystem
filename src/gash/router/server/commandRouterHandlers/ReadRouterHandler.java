package gash.router.server.commandRouterHandlers;

import io.netty.channel.Channel;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pipe.work.Work.WorkMessage;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.message.generator.MessageGenerator;
import gash.router.server.queue.management.InternalChannelNode;
import gash.router.server.queue.management.LoadQueueManager;
import gash.router.server.queue.management.NodeLoad;
import gash.router.server.queue.management.QueueManager;
import routing.Pipe.CommandMessage;
import routing.Pipe.FileTask.FileTaskType;
import gash.router.database.DatabaseHandler;



public class ReadRouterHandler implements ICommandRouterHandlers{
	private  ICommandRouterHandlers nextInChain;
	protected static Logger logger = LoggerFactory.getLogger(ReadRouterHandler.class);
	
	public void setNextChainHandler(ICommandRouterHandlers nextChain) {
		this.nextInChain = nextChain;
	}

	@Override
	public void handleFileTask(InternalChannelNode request) throws Exception {
		FileTaskType taskType = request.getCommandMessage().getFiletask().getFileTaskType();
		if(taskType == FileTaskType.READ){
			int chunkCount = DatabaseHandler.getFilesChunkCount(request.getCommandMessage().getFiletask().getFilename());
			String clientId = EdgeMonitor.clientInfoMap(request);
			request.setChunkCount(chunkCount);
			NodeLoad node = LoadQueueManager.getInstance().getMininumNodeLoadInfo(chunkCount);
			// convert the read request to work messages of total number of chunks
			for(int index = 0; index < chunkCount; index++) {
				
				WorkMessage worKMessage = 
						MessageGenerator.getInstance().generateReadRequestMessage(request.getCommandMessage(), clientId, node.getNodeId(), index + 1);
				Channel nodeChannel = EdgeMonitor.node2ChannelMap.get(node.getNodeId());
				QueueManager.getInstance().enqueueOutboundRead(worKMessage, nodeChannel); 
			}
		} else {
			nextInChain.handleFileTask(request);
		}
		
	}
}
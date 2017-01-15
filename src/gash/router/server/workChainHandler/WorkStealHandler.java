package gash.router.server.workChainHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.message.generator.MessageGenerator;
import gash.router.server.queue.management.InternalChannelNode;
import gash.router.server.queue.management.QueueManager;
import io.netty.channel.Channel;
import pipe.work.Work.READ_STEAL;
import pipe.work.Work.Steal;
import pipe.work.Work.WRITE_STEAL;
import pipe.work.Work.WorkMessage;

public class WorkStealHandler implements IWorkChainHandler{
	private IWorkChainHandler nextChainHandler;
	protected ServerState state;
	protected static Logger logger = LoggerFactory.getLogger(WorkStealHandler.class);

	@Override
	public void setNextChain(IWorkChainHandler nextChain, ServerState state) {
		this.nextChainHandler = nextChain;
		this.state = state;
	}

	@Override
	public void handle(WorkMessage workMessage, Channel channel) {
		// TODO Auto-generated method stub
		if(workMessage.hasSteal()) {
			Steal steal = workMessage.getSteal();
			if(steal.hasReadSteal()) {
				//logger.info("Got Steal message");
				if(steal.getReadSteal() == READ_STEAL.READ_STEAL_REQUEST) {
					//logger.info("Received Read Steal read request message from node: " + workMessage.getHeader().getNodeId());
					InternalChannelNode internalNode;
					try {
						internalNode = QueueManager.getInstance().dequeueInboundRead();
						if(internalNode != null) {
							WorkMessage workStealResponseMessage = MessageGenerator.getInstance().generateWorkReadStealResponseMsg(internalNode.getWorkMessage());
							QueueManager.getInstance().enqueueOutboundRead(workStealResponseMessage, channel);
						}
						else {
							// need to return for telling no work message
							//logger.info("Got request for read steal, but nothing is available for steal");
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				else if (steal.getReadSteal() == READ_STEAL.READ_STEAL_RESPONSE) {
					//logger.info("Received Read Steal response message from node: " + workMessage.getHeader().getNodeId());
					QueueManager.getInstance().enqueueInboundRead(workMessage, channel);
				}
			}
			else {
				if(steal.getWriteSteal() == WRITE_STEAL.WRITE_STEAL_REQUEST) {
					//To-DO
				}
				else if (steal.getWriteSteal() == WRITE_STEAL.WRITE_STEAL_RESPONSE) {
					//To-DO
				}
			}
		}
		else {
			this.nextChainHandler.handle(workMessage, channel);
		}
	}

	
}
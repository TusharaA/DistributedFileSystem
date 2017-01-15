package gash.router.server.queue.management;

import io.netty.channel.ChannelFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OutboundWorkWriteQueueThread extends Thread{
	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger(OutboundWorkWriteQueueThread.class);
	public OutboundWorkWriteQueueThread(QueueManager manager) {
		super();
		this.manager = manager;
		if (manager.outboundWorkWriteQueue == null)
			throw new RuntimeException("Manager has no outbound write queue");
	}
	
	@Override
	public void run() {

		while (true) {
			try {
				InternalChannelNode message = manager.dequeueOutboundWorkWrite();
				int destinationNode = message.getWorkMessage().getHeader().getNodeId();
		       	logger.info("Outbound write work message routing to node " + destinationNode);

				if (message.getChannel()!= null && message.getChannel().isOpen()) {
					ChannelFuture cf = message.getChannel().write(message.getWorkMessage());
					message.getChannel().flush();
					cf.awaitUninterruptibly();
					if(cf.isSuccess()){
						logger.info("Wrote message to the channel of node : " + destinationNode );
					} else {
						manager.returnOutboundWorkWrite(message);
					}
				} else {
					logger.info("Channel to destination node " + destinationNode + " is not writable");
					logger.info("Checking if channel is null : "+(message.getChannel() == null));
					manager.returnOutboundWorkWrite(message);
				}
			} catch (InterruptedException ie) {
				break;
			} catch (Exception e) {
				logger.error("Unexpected management communcation failure", e);
				break;
			}

		}
	}
}

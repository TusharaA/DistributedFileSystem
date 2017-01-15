package gash.router.server.queue.management;

import io.netty.channel.ChannelFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OutboundCommandQueueThread extends Thread {
	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger(InboundCommandQueueThread.class);

	public OutboundCommandQueueThread(QueueManager manager) {
		super();
		this.manager = manager;
		if (manager.outboundCommandQueue == null)
			throw new RuntimeException("Manager has no outbound command queue");
	}

	@Override
	public void run() {

		while (true) {
			try {
				InternalChannelNode message = manager.dequeueOutboundCommmand();
				logger.info("Routing outbound command message to client ");
				if (message.getChannel()!= null && message.getChannel().isOpen()) {
					if(message.getChannel().isWritable()){
						ChannelFuture cf = message.getChannel().write(message.getCommandMessage());
						message.getChannel().flush();
						cf.awaitUninterruptibly();
						if(!cf.isSuccess()){
							manager.enqueueAtFrontOutboundCommand(message);
						}
						else {
							logger.info("Successfully send the command message to client ");
						}
					}
					else {
						manager.enqueueAtFrontOutboundCommand(message);
					}
				} else {
					manager.enqueueAtFrontOutboundCommand(message);
				}
					
			} catch (Exception e) {
				logger.error("Exception thrown in client communcation", e);
				break;
			}
		}

		}
	}


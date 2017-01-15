package gash.router.server.workChainHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;

/**
 * 
 * @author vaishampayan
 *
 */
public class PingHandler implements IWorkChainHandler{
	private IWorkChainHandler nextChainHandler;
	protected ServerState state;
	protected static Logger logger = LoggerFactory.getLogger(PingHandler.class);
	@Override
	public void setNextChain(IWorkChainHandler nextChain,ServerState state) {
		this.nextChainHandler = nextChain;
		this.state = state;
	}

	@Override
	public void handle(WorkMessage workMessage, Channel channel) {
		// TODO Auto-generated method stub
		if(workMessage.hasPing()) {
			//handling the ping message
			logger.info("ping from " + workMessage.getHeader().getNodeId());
			boolean p = workMessage.getPing();
			WorkMessage.Builder rb = WorkMessage.newBuilder();
			rb.setPing(true);
			channel.write(rb.build());
		}
		else {
			this.nextChainHandler.handle(workMessage, channel);
		}
	}

}

package gash.router.server.workChainHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import io.netty.channel.Channel;
import pipe.common.Common.Failure;
import pipe.work.Work.WorkMessage;

/**
 * 
 * @author vaishampayan
 *
 */
public class FailureHandler implements IWorkChainHandler{
	private IWorkChainHandler nextChainHandler;
	protected ServerState state;
	protected static Logger logger = LoggerFactory.getLogger(FailureHandler.class);

	@Override
	public void setNextChain(IWorkChainHandler nextChain, ServerState state) {
		this.nextChainHandler = nextChain;
		this.state = state;
	}

	@Override
	public void handle(WorkMessage workMessage, Channel channel) {
		// TODO Auto-generated method stub
		if(workMessage.hasErr()) {
			Failure err = workMessage.getErr();
			logger.error("failure from " + workMessage.getHeader().getNodeId());
		}
		else {
			nextChainHandler.handle(workMessage, channel);
		}
	}
}

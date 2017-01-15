package gash.router.server.commandChainHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;


public class PingChainHandler implements ICommandChainHandler{
	private  ICommandChainHandler nextInChain;
	protected static Logger logger = LoggerFactory.getLogger(PingChainHandler.class);
	
	public void setNextChainHandler(ICommandChainHandler nextChain) {
		
		this.nextInChain = nextChain;
		
	}

	@Override
	public void handleMessage(CommandMessage msg, Channel channel) throws Exception {

		if (msg.hasPing()) {
			logger.info("ping from " + msg.getHeader().getNodeId());
		} else {
			logger.error("Handles only ping and message for now");
		}
	}
}
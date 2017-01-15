package gash.router.server.commandChainHandler;

import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;

public interface ICommandChainHandler {

	
	public void setNextChainHandler(ICommandChainHandler nextChain);

	public void handleMessage(CommandMessage msg, Channel channel) throws Exception;
	
}
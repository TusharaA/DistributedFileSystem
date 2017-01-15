package gash.router.server.commandRouterHandlers;

import gash.router.server.queue.management.InternalChannelNode;


public interface ICommandRouterHandlers {

	
	public void setNextChainHandler(ICommandRouterHandlers nextChain);

	public void handleFileTask(InternalChannelNode request) throws Exception;
	
}
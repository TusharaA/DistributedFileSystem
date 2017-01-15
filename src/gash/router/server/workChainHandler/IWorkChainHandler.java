package gash.router.server.workChainHandler;

import gash.router.server.ServerState;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;

public interface IWorkChainHandler {
	public void setNextChain(IWorkChainHandler nextChain, ServerState state);
	public void handle(WorkMessage workMessage, Channel channel);
}

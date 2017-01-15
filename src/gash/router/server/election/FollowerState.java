package gash.router.server.election;

import gash.router.server.edges.EdgeInfo;
import gash.router.util.RaftMessageBuilder;
import pipe.work.Work.WorkMessage;

public class FollowerState implements IRaftNodeState {

	private RaftElectionContext electionCtx;
	private boolean voted = false;
	
	
	@Override
	public synchronized void doAction() {	
		if(electionCtx.getTimeOut() <=0 ) {
			electionCtx.setCurrentState(electionCtx.candidate);		
			return;
		}
		
		if(electionCtx.isAmReady())
			electionCtx.computeTime();
	}	

	@Override
	public synchronized void VoteRequestReceived(WorkMessage msg) {		
		if(msg.getRaftMessage().getTerm() > electionCtx.getTerm()){
			voted =false;
			electionCtx.setTerm(msg.getRaftMessage().getTerm());
			if(!voted){
				electionCtx.generateTimeOut();
				voted = true;
				System.out.println("You voted for "+msg.getHeader().getNodeId()+" in term "+electionCtx.getTerm());
				sendVote(msg,true);
			}else
				sendVote(msg,false);			
		}		
	}
	
	@Override
	public synchronized void sendVote(WorkMessage msg, boolean voteGranted) {
		System.out.println("You are sending vote to Candidate No: " + msg.getHeader().getNodeId());
		EdgeInfo ei = electionCtx.getEmon().getOutBoundEdgesList().getEdgeListMap().get(msg.getHeader().getNodeId());
		WorkMessage voteResponseMessage = RaftMessageBuilder.buildVoteResponseMessage(msg.getHeader().getNodeId(), voteGranted, electionCtx.getTerm());
		if(ei.getChannel()!=null)
			ei.getChannel().writeAndFlush(voteResponseMessage); 	
	}


	@Override
	public synchronized void voteRecieved(WorkMessage msg) {
		return;	
	}

	@Override
	public void setElectionContext(RaftElectionContext ctx) {
		System.out.println(ctx);
		this.electionCtx =ctx;	
	}

	@Override
	public RaftElectionContext getElectionContext() {
		return electionCtx;
	}

	@Override
	public void sendHearbeatAck(WorkMessage msg) {
		// TODO Auto-generated method stub
	}

	@Override
	public void getHearbeatFromLeader(WorkMessage msg) {
		System.out.println("Received HB: " + msg.getHeader().getNodeId());
		if(msg.getRaftMessage().getTerm() >= electionCtx.getTerm()){
			voted = false;
			electionCtx.generateTimeOut();
			electionCtx.generateTimeOut();
			electionCtx.setTerm((int) msg.getRaftMessage().getTerm());

			electionCtx.setLeaderId((int) msg.getRaftMessage().getLeaderId());
			WorkMessage heartbeatAckMessage = RaftMessageBuilder.buildLeaderHbAckMessage(msg.getRaftMessage().getLeaderId(),msg.getRaftMessage().getTerm());
			EdgeInfo ei = electionCtx.getEmon().getOutBoundEdgesList().getEdgeListMap().get(msg.getHeader().getNodeId());
			while(true){
				if(ei.getChannel()!=null){
					ei.getChannel().writeAndFlush(heartbeatAckMessage); 
					break;
				}
				//	electionCtx.setLastKnownBeat(System.currentTimeMillis());
			}
		}
		
	}

	
	
	
}

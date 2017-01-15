package gash.router.server.election;
import pipe.work.Work.WorkMessage;

public interface IRaftNodeState {
		public void setElectionContext(RaftElectionContext ctx);
		public RaftElectionContext getElectionContext();	
		public void doAction();
		public void voteRecieved(WorkMessage msg);
		public void sendVote(WorkMessage msg, boolean voteGranted);
		public void VoteRequestReceived(WorkMessage msg);
		public void getHearbeatFromLeader(WorkMessage msg);
		public void sendHearbeatAck(WorkMessage msg);
		
}



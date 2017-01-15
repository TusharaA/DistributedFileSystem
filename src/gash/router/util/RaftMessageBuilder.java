package gash.router.util;

import java.util.ArrayList;

import gash.router.container.RoutingConf;
import pipe.common.Common.Header;
import pipe.election.Election.AllNodeInfo;
import pipe.election.Election.ElectionMessage;
import pipe.election.Election.LeaderStatus;
import pipe.election.Election.LeaderStatus.Builder;
import pipe.election.Election.LeaderStatus.LeaderQuery;
import pipe.election.Election.LeaderStatus.LeaderState;
import pipe.election.Election.NewNodeMessage;
import pipe.election.Election.RaftElectionMessage;
import pipe.election.Election.RaftElectionMessage.ElectionMessageType;
import pipe.work.Work.WorkMessage;

public class RaftMessageBuilder {
	private static RoutingConf conf;
	
	public static RoutingConf getRoutingConf() {
		System.out.println("entered RaftMsg Bilder constrctiu");
		return conf;
	}

	public static void setRoutingConf(RoutingConf conf) {
		RaftMessageBuilder.conf = conf;
	}
	
	/* Builds the Message in order to request vote to all other nodes during election process */
	public static WorkMessage buildVoteRequestMessage(int currentTerm) {
		
		Header.Builder headerBuilder = Header.newBuilder();
		headerBuilder.setNodeId(conf.getNodeId());
		headerBuilder.setTime(System.currentTimeMillis());
		headerBuilder.setIsElectionProcess(true);
		
		ElectionMessage.Builder voteRequestMsgBuilder = ElectionMessage.newBuilder();
		voteRequestMsgBuilder.setCandidateId(conf.getNodeId());

		RaftElectionMessage.Builder raftMessageBuilder = RaftElectionMessage.newBuilder();
		raftMessageBuilder.setType(ElectionMessageType.VOTE_REQUEST);
		raftMessageBuilder.setTerm(currentTerm);		
		raftMessageBuilder.setElectionMessage(voteRequestMsgBuilder);
		
		
		LeaderStatus.Builder leaderStatusBuilder = LeaderStatus.newBuilder();
		leaderStatusBuilder.setState(LeaderState.LEADERUNKNOWN);
		
		WorkMessage.Builder workMessageBuilder = WorkMessage.newBuilder();
		workMessageBuilder.setRaftMessage(raftMessageBuilder);
		workMessageBuilder.setSecret(2);
		workMessageBuilder.setHeader(headerBuilder);
		raftMessageBuilder.setLeader(leaderStatusBuilder);
		
		return workMessageBuilder.build();
	}
	
	/* Builds the Message in order to cast Vote during election process */
	public static WorkMessage buildVoteResponseMessage(int candidateId, boolean response, int currentTerm) {
		
		Header.Builder headerBuilder = Header.newBuilder();
		headerBuilder.setNodeId(conf.getNodeId());
		headerBuilder.setTime(System.currentTimeMillis());
		headerBuilder.setIsElectionProcess(true);

		ElectionMessage.Builder voteResponseMsgBuilder = ElectionMessage.newBuilder();
		voteResponseMsgBuilder.setCandidateId(candidateId);
		voteResponseMsgBuilder.setVoteGranted(response);

		RaftElectionMessage.Builder raftMessageBuilder = RaftElectionMessage.newBuilder();
		raftMessageBuilder.setType(ElectionMessageType.VOTE_RESPONSE);
		raftMessageBuilder.setElectionMessage(voteResponseMsgBuilder);
		raftMessageBuilder.setTerm(currentTerm);
		
		LeaderStatus.Builder leaderStatusBuilder = LeaderStatus.newBuilder();
		leaderStatusBuilder.setState(LeaderState.LEADERUNKNOWN);
		
		WorkMessage.Builder workMessageBuilder = WorkMessage.newBuilder();
		workMessageBuilder.setRaftMessage(raftMessageBuilder);
		workMessageBuilder.setSecret(2);
		workMessageBuilder.setHeader(headerBuilder);
		raftMessageBuilder.setLeader(leaderStatusBuilder);
		return workMessageBuilder.build();
	}
	
	/* Builds the Message in order to send Leader's heart beat to all other nodes */
	public static WorkMessage buildLeaderResponseMessage(int leaderId, int currentTerm) {
		
		Header.Builder headerBuilder = Header.newBuilder();
		headerBuilder.setNodeId(conf.getNodeId());
		headerBuilder.setTime(System.currentTimeMillis());
		headerBuilder.setIsElectionProcess(true);

		RaftElectionMessage.Builder raftMessageBuilder = RaftElectionMessage.newBuilder();
		raftMessageBuilder.setType(ElectionMessageType.LEADER_HEARTBEAT);
		raftMessageBuilder.setLeaderId(leaderId);
		raftMessageBuilder.setTerm(currentTerm);
		
		LeaderStatus.Builder leaderStatusBuilder = LeaderStatus.newBuilder();
		leaderStatusBuilder.setState(LeaderState.LEADERALIVE);
		
		WorkMessage.Builder workMessageBuilder = WorkMessage.newBuilder();
		workMessageBuilder.setRaftMessage(raftMessageBuilder);
		workMessageBuilder.setHeader(headerBuilder);
		workMessageBuilder.setSecret(2);
		raftMessageBuilder.setLeader(leaderStatusBuilder);

		return workMessageBuilder.build();
	}
	
	/* Builds the Message in order to send acknowledgement from Follower  to Leader */
	public static WorkMessage buildLeaderHbAckMessage(int leaderId, int currentTerm) {
		
		Header.Builder headerBuilder = Header.newBuilder();
		headerBuilder.setNodeId(conf.getNodeId());
		headerBuilder.setTime(System.currentTimeMillis());
		headerBuilder.setIsElectionProcess(true);

		RaftElectionMessage.Builder raftMessageBuilder = RaftElectionMessage.newBuilder();
		raftMessageBuilder.setType(ElectionMessageType.LEADER_HB_ACK);
		raftMessageBuilder.setLeaderId(leaderId);
		raftMessageBuilder.setTerm(currentTerm);
		
		LeaderStatus.Builder leaderStatusBuilder = LeaderStatus.newBuilder();
		leaderStatusBuilder.setState(LeaderState.LEADERALIVE);
		
		WorkMessage.Builder workMessageBuilder = WorkMessage.newBuilder();
		workMessageBuilder.setRaftMessage(raftMessageBuilder);
		workMessageBuilder.setHeader(headerBuilder);
		workMessageBuilder.setSecret(2);
		raftMessageBuilder.setLeader(leaderStatusBuilder);

		return workMessageBuilder.build();
	}
	
	public static WorkMessage buildWhoIsTheLeaderMessage() {
		WorkMessage.Builder workMessageBuilder = WorkMessage.newBuilder();
		Header.Builder headerBuilder = Header.newBuilder();
		LeaderStatus.Builder leaderStatusBuilder = LeaderStatus.newBuilder();

		leaderStatusBuilder.setAction(LeaderQuery.WHOISTHELEADER);
		leaderStatusBuilder.setState(LeaderState.LEADERUNKNOWN);

		headerBuilder.setIsElectionProcess(false);
		headerBuilder.setNodeId(conf.getNodeId());
		headerBuilder.setTime(System.currentTimeMillis());

		
		workMessageBuilder.setLeader(leaderStatusBuilder);
		workMessageBuilder.setHeader(headerBuilder);
		workMessageBuilder.setSecret(2);

		return workMessageBuilder.build();
	}

	public static WorkMessage buildTheLeaderIsMessage(int leaderId,int currentTerm) {
		WorkMessage.Builder workMessageBuilder = WorkMessage.newBuilder();
		Header.Builder headerBuilder = Header.newBuilder();
		LeaderStatus.Builder leaderStatusBuilder = LeaderStatus.newBuilder();

		leaderStatusBuilder.setAction(LeaderQuery.THELEADERIS);
		leaderStatusBuilder.setLeaderId(leaderId);
		leaderStatusBuilder.setTerm(currentTerm);
		leaderStatusBuilder.setState(LeaderState.LEADERALIVE);

		headerBuilder.setIsElectionProcess(false);
		headerBuilder.setNodeId(conf.getNodeId());
		headerBuilder.setTime(System.currentTimeMillis());

		workMessageBuilder.setLeader(leaderStatusBuilder);
		workMessageBuilder.setHeader(headerBuilder);
		workMessageBuilder.setSecret(1234);

		return workMessageBuilder.build();
	}
	
	/*
	//message to add new node details to nodes in cluster
	public static WorkMessage addNewNodeInfo(int flag) {
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setDestination(-1);
		hb.setTime(System.currentTimeMillis());
		
		NewNodeMessage.Builder newNodeMessage = NewNodeMessage.newBuilder();
		newNodeMessage.setNodeId(conf.getNodeId());
		//newNodeMessage.setHostAddr(address);
		newNodeMessage.setPortNo(conf.getWorkPort());
		newNodeMessage.setJoinCluster(flag);

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setSecret(2);
		wb.setNewNodeMessage(newNodeMessage);
		return wb.build();
		
	}
	
	//message to add cluster node details to new node outbound edge
	public static WorkMessage clusterNodeInfo(ArrayList<Integer> nodeIdList, ArrayList<String> hostList, ArrayList<Integer> portList) {
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setDestination(-1);
		hb.setTime(System.currentTimeMillis());
		
		AllNodeInfo.Builder allNodeInfoMessage = AllNodeInfo.newBuilder();
		allNodeInfoMessage.addAllNodeId(nodeIdList);
		allNodeInfoMessage.addAllHostAddr(hostList);
		allNodeInfoMessage.addAllPortNo(portList);

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setSecret(2);
		wb.setAllNodeInfo(allNodeInfoMessage);
		return wb.build();
	}*/
	
}

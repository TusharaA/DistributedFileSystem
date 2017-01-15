package gash.router.server.queue.management;

public class NodeLoad{
	int nodeId;
    int load;
    
    public NodeLoad(int nodeId, int load){
        this.nodeId = nodeId;
        this.load   = load;
    }
    
    /**
	 * @return the nodeId
	 */
	public int getNodeId() {
		return nodeId;
	}

	/**
	 * @param nodeId the nodeId to set
	 */
	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}

	/**
	 * @return the load
	 */
	public int getLoad() {
		return load;
	}

	/**
	 * @param load the load to set
	 */
	public void setLoad(int load) {
		this.load = load;
	}
}
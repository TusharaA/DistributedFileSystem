package gash.router.server.replication;

public class UpdateFileInfo {
	private int totalChunks; 
	private int chunksProcessed;
	
	public UpdateFileInfo(int totalChunks){
		this.totalChunks = totalChunks;
		this.chunksProcessed = totalChunks;
	}
	
	/**
	 * @return the totalChunks
	 */
	public int getTotalChunks() {
		return totalChunks;
	}

	/**
	 * @param totalChunks the totalChunks to set
	 */
	public void setTotalChunks(int totalChunks) {
		this.totalChunks = totalChunks;
	}
	
	
	public void decrementChunkProcessed(){
		if(chunksProcessed <= 0){
			return ;
		} else {
			chunksProcessed --;
		}
	}
	
	/**
	 * @return the chunksProcessed
	 */
	public int getChunksProcessed() {
		return chunksProcessed;
	}

	/**
	 * @param chunksProcessed the chunksProcessed to set
	 */
	public void setChunksProcessed(int chunksProcessed) {
		this.chunksProcessed = chunksProcessed;
	}
}

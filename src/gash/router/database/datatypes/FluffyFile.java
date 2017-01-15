package gash.router.database.datatypes;

import java.util.Arrays;

public class FluffyFile {
	private String filename;
	private int totalChunks;
	private int chunkId;
	private byte[] file;

	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	public int getTotalChunks() {
		return totalChunks;
	}
	public void setTotalChunks(int totalChunks) {
		this.totalChunks = totalChunks;
	}
	public int getChunkId() {
		return chunkId;
	}
	public void setChunkId(int chunkId) {
		this.chunkId = chunkId;
	}
	public byte[] getFile() {
		return file;
	}
	public void setFile(byte[] file) {
		this.file = file;
	}

	@Override
	public String toString() {
		return "FluffyFile [filename=" + filename + ", totalChunks="
				+ totalChunks + ", chunkId=" + chunkId + ", file="
				+ Arrays.toString(file) + "]";
	}
}


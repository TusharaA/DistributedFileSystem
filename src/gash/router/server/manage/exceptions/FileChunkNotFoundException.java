package gash.router.server.manage.exceptions;

public class FileChunkNotFoundException extends Exception{
	private static final long serialVersionUID = 6824642063063153193L;
	private String filename;
	private int chunkId;

	public FileChunkNotFoundException(String filename, int chunkId) {
		this.filename = filename;
		this.chunkId = chunkId;
	}

	@Override
	public String getMessage() {
		return "Error: Cannot find the chunk of file with file name " + this.filename + " and chunk id: " + chunkId + " in the database";
	}
}

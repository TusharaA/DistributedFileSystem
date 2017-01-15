package gash.router.server.manage.exceptions;

public class FileNotFoundException extends Exception {

	private static final long serialVersionUID = 6824642063063153193L;
	private String filename;
	public FileNotFoundException(String filename) {
		this.filename = filename;
	}

	@Override
	public String getMessage() {
		return "Error: Cannot find the file: " + this.filename + " in the database";
	}
}
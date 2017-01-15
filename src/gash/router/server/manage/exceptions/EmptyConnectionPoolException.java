package gash.router.server.manage.exceptions;

public class EmptyConnectionPoolException extends Exception{

	private static final long serialVersionUID = -4089798139497362786L;
	private String message;

	public EmptyConnectionPoolException(String message) {
		this.message = message;
	}
	@Override
	public String getMessage() {
		return "Error: Connection pool size exceeded or not initialized, nested message: " + message;
	}
}

package gash.router.database;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;

import gash.router.database.datatypes.FluffyFile;
import gash.router.server.manage.exceptions.EmptyConnectionPoolException;
import gash.router.server.manage.exceptions.FileChunkNotFoundException;
import gash.router.server.manage.exceptions.FileNotFoundException;
import gash.router.util.Constants;

public class DatabaseHandler {

	private static DatabaseConnectionManager databaseConnectionManager = DatabaseConnectionManager.getInstance();

	public static final RethinkDB rethinkDBInstance = RethinkDB.r;
	public static RiakDatabase riakDatabase = null;
	public static final JSONParser jsonParse = new JSONParser();
	protected static Logger logger = LoggerFactory.getLogger(DatabaseHandler.class);

	/**
	 * adds the give line into the db
	 * 
	 * @param filename
	 * @param line
	 * @param chunkId
	 * @throws EmptyConnectionPoolException 
	 * @throws Exception
	 */
	@Deprecated
	public static boolean addFile(String filename, String line, int chunkId, int totalChunks) throws EmptyConnectionPoolException {
		Connection conn = databaseConnectionManager.getConnection();
		try {
			rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE)
			.insert(rethinkDBInstance.hashMap(Constants.FILE_NAME, filename).with(Constants.FILE_CONTENT, line)
					.with(Constants.CHUNK_COUNT, totalChunks).with(Constants.CHUNK_ID, chunkId))
			.run(conn);
			return true;
		} catch (Exception e) {
			logger.error("ERROR: Unable to store file in the database");
			System.out.println("File is not added");
			return false;
		} finally {
			databaseConnectionManager.releaseConnection(conn);
		}
	}

	/**
	 * adds the give line into the db
	 * 
	 * @param filename
	 * @param line
	 * @param chunkId
	 * @throws EmptyConnectionPoolException 
	 * @throws Exception
	 */
	@Deprecated
	public static boolean addFile(String filename, ByteString line, int chunkId, int totalChunks) throws EmptyConnectionPoolException {
		Connection conn = databaseConnectionManager.getConnection();
		try {
			String contentString = new String(line.toByteArray());
			rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE)
			.insert(rethinkDBInstance.hashMap(Constants.FILE_NAME, filename)
					.with(Constants.CHUNK_COUNT, totalChunks).with(Constants.FILE_CONTENT, contentString)
					.with(Constants.CHUNK_ID, chunkId))
			.run(conn);
			//System.out.println("File saved to DB: " + filename);
			return true;
		} catch (Exception e) {
			logger.error("ERROR: Unable to store file in the database");
			System.out.println("File is not added");
			return false;
		} finally {
			databaseConnectionManager.releaseConnection(conn);
		}
	}

	/**
	 * generic method to store the file as chunks in the form of byte[]
	 * 
	 * @param filename
	 * @param input
	 * @param chunkId
	 * @throws EmptyConnectionPoolException 
	 * @throws Exception
	 */
	public static boolean addFile(String filename, int chunkCount, byte[] input, int chunkId) throws EmptyConnectionPoolException {
		if(chunkCount>3) {
			Connection connection = databaseConnectionManager.getConnection();
			try {
				rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE).insert(rethinkDBInstance
						.hashMap(Constants.FILE_NAME, filename).with(Constants.CHUNK_COUNT, chunkCount)
						.with(Constants.FILE_CONTENT, rethinkDBInstance.binary(input)).with(Constants.CHUNK_ID, chunkId))
				.run(connection);
				return true;
			} catch (Exception e) {
				logger.debug("ERROR: Unable to store file in the database");
				e.printStackTrace();
				return false;
			} finally {
				databaseConnectionManager.releaseConnection(connection);
			}  }else {
				//if total chunk count is less than 3 chunks file will be stored in Riak
				try {
					riakDatabase = RiakDatabase.getRiakInstance();
					riakDatabase.storeFile(filename, chunkCount, input, chunkId);
					return true;
				} catch(Exception e) {
					logger.debug("ERROR: Unable to store file in the Riak database");
					e.printStackTrace();
					return false;
				}
			}
	}

	/**
	 * 
	 * @param filename
	 * @return
	 * @throws FileNotFoundException 
	 * @throws EmptyConnectionPoolException 
	 * @throws Exception
	 */
	public static int getFilesChunkCount(String filename) throws FileNotFoundException, EmptyConnectionPoolException {
		Connection conn = databaseConnectionManager.getConnection();
		Cursor<String> data = rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE)
				.filter(rethinkDBInstance.hashMap("filename", filename)).limit(1).getField(Constants.CHUNK_COUNT)
				.run(conn);
		if (data == null)
			throw new FileNotFoundException(filename);
		else {
			databaseConnectionManager.releaseConnection(conn);
			ArrayList<String> result = data.bufferedItems();
			if (result.size() == 1) {
				return Integer.parseInt(String.valueOf(result.get(0)));
			} else {
				//checking file in riak database
				riakDatabase = RiakDatabase.getRiakInstance();
				int riakResult = riakDatabase.getChunkCount(filename);
				if(riakResult!=0)
					return riakResult;
				return 0;
			}
		}
	}

	/**
	 * 
	 * reads the file with name @param filename from database and returns all
	 * the chunks
	 * 
	 * @param filename
	 * @return
	 * @throws FileNotFoundException
	 * @throws ParseException
	 * @throws IOException
	 * @throws EmptyConnectionPoolException
	 * @throws Exception
	 */
	public static List<FluffyFile> getFileContents(String filename)
			throws FileNotFoundException, IOException, ParseException, EmptyConnectionPoolException {
		Connection connection = databaseConnectionManager.getConnection();
		Cursor<String> dataFromDB = rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE)
				.filter(rethinkDBInstance.hashMap("filename", filename)).run(connection);
		databaseConnectionManager.releaseConnection(connection);

		if (dataFromDB != null) {
			int result = dataFromDB.bufferedSize();
			if(result != 0){
				List<FluffyFile> fileContents = new ArrayList<FluffyFile>();
				for (Object record : dataFromDB) {
					FluffyFile fluffyFile = new FluffyFile();
					HashMap<String, Object> fileContentMap = (HashMap<String, Object>) record;
					fluffyFile.setChunkId(new Integer(fileContentMap.get(Constants.CHUNK_ID).toString()));
					fluffyFile.setFile((byte[]) fileContentMap.get(Constants.FILE_CONTENT));
					fluffyFile.setFilename(fileContentMap.get(Constants.FILE_NAME).toString());
					fluffyFile.setTotalChunks(new Integer(fileContentMap.get(Constants.CHUNK_COUNT).toString()));
					fileContents.add(fluffyFile);
				}
				return fileContents;
			} else {
				//checking in riak database 
				riakDatabase = RiakDatabase.getRiakInstance();
				List<FluffyFile> riakFile = riakDatabase.getFile(filename);
				if(riakFile!=null)
					return riakFile;
			}
		}		
		throw new FileNotFoundException(filename);	
	}

	/**
	 * reads the file with specific fileContent from database
	 * 
	 * @param filename
	 * @param chunkId
	 * @return
	 * @throws FileNotFoundException
	 * @throws ParseException
	 * @throws IOException
	 * @throws EmptyConnectionPoolException
	 * @throws Exception
	 */
	public static List<FluffyFile> getFileContentWithChunkId(String filename, int chunkId)
			throws FileNotFoundException, IOException, ParseException, EmptyConnectionPoolException {
		Connection connection = databaseConnectionManager.getConnection();
		Cursor<String> dataFromDB = rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE)
				.filter(rethinkDBInstance.hashMap("filename", filename).with(Constants.CHUNK_ID, chunkId))
				.run(connection);

		databaseConnectionManager.releaseConnection(connection);
		List<FluffyFile> fileContents = new ArrayList<FluffyFile>();
		if (dataFromDB != null) {
			int result = dataFromDB.bufferedSize();
			if(result != 0){
				for (Object record : dataFromDB) {
					FluffyFile fluffyFile = new FluffyFile();
					HashMap<String, Object> fileContentMap = (HashMap<String, Object>) record;
					fluffyFile.setChunkId(new Integer(fileContentMap.get(Constants.CHUNK_ID).toString()));
					fluffyFile.setFile((byte[]) fileContentMap.get(Constants.FILE_CONTENT));
					fluffyFile.setFilename(fileContentMap.get(Constants.FILE_NAME).toString());
					fluffyFile.setTotalChunks(new Integer(fileContentMap.get(Constants.CHUNK_COUNT).toString()));
					fileContents.add(fluffyFile);
				}
				return fileContents;
			} else {
				riakDatabase = RiakDatabase.getRiakInstance();
				FluffyFile riakFile = riakDatabase.getFileWithChunkID(filename,chunkId);
				if(riakFile!=null){
					fileContents.add(riakFile);
					return fileContents ;
				} else {
					throw new FileNotFoundException(filename);			
				}
			}
		} 
		throw new FileNotFoundException(filename);				
	}

	/**
	 * reads fileContent from given file and chunk id
	 * 
	 * @param filename
	 * @param chunkId
	 * @return
	 * @throws IOException
	 * @throws ParseException
	 * @throws FileNotFoundException
	 * @throws FileChunkNotFoundException
	 * @throws EmptyConnectionPoolException
	 */
	public static ByteString getFileChunkContentWithChunkId(String filename, int chunkId) throws IOException,
	ParseException, FileNotFoundException, FileChunkNotFoundException, EmptyConnectionPoolException {
		Connection connection = databaseConnectionManager.getConnection();

		Cursor<String> dataFromDB = rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE)
				.filter(rethinkDBInstance.hashMap(Constants.FILE_NAME, filename).with(Constants.CHUNK_ID, chunkId))
				// .getField(Constants.FILE_CONTENT)
				.run(connection);

		databaseConnectionManager.releaseConnection(connection);
		if (dataFromDB != null) {
			List<FluffyFile> fileContents = new ArrayList<FluffyFile>();
			int result = dataFromDB.bufferedSize();
			if(result != 0){
				for (Object record : dataFromDB) {
					System.out.println(record);
					HashMap<String, Object> fileContentMap = (HashMap<String, Object>) record;
					System.out.println(fileContentMap.get(Constants.FILE_CONTENT).getClass().getName());
					return ByteString.copyFrom((byte[]) fileContentMap.get(Constants.FILE_CONTENT));
				}
				return ByteString.copyFrom("".getBytes());
			} else {
				//checking in riak database 
				riakDatabase = RiakDatabase.getRiakInstance();
				FluffyFile riakFile = riakDatabase.getFileWithChunkID(filename,chunkId);
				if(riakFile != null)
					return ByteString.copyFrom(riakFile.getFile());
			}
		}
		throw new FileChunkNotFoundException(filename, chunkId);	
	}

	public static boolean isFileAvailable(String filename) throws EmptyConnectionPoolException{
		Connection connection = databaseConnectionManager.getConnection();

		Cursor<String> dataFromDB = rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE)
				.filter(rethinkDBInstance.hashMap(Constants.FILE_NAME, filename)).run(connection);
		databaseConnectionManager.releaseConnection(connection);
		if (dataFromDB == null)
			System.out.println("Database error");
		else{
			int count  = 0;
			for (Object record : dataFromDB) {
				count ++;
			}

			if(count != 0){
				return true;
			} else {
				riakDatabase = RiakDatabase.getRiakInstance();
				int isAvailable = riakDatabase.getChunkCount(filename);
				if(isAvailable>0)
					return true;
			}
		}
		return false;	
	}

	/**
	 * generic method to delete the file from the database
	 * 
	 * @param filename
	 * @param input
	 * @param chunkId
	 * @throws EmptyConnectionPoolException 
	 * @throws Exception
	 */
	public static boolean deleteFile(String filename) throws EmptyConnectionPoolException {
		Connection connection = databaseConnectionManager.getConnection();
		try {
			HashMap<String, Object> dataFromDB = rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE)
					.filter(rethinkDBInstance.hashMap(Constants.FILE_NAME, filename)).delete().run(connection);
			databaseConnectionManager.releaseConnection(connection);
			riakDatabase = RiakDatabase.getRiakInstance();
			riakDatabase.deleteFile(filename);
			return true;
		} catch (Exception e) {
			logger.debug("ERROR: Unable to delete file in the database");
			System.out.println("File in not deleted");
			e.printStackTrace();
			return false;
		} finally {
			databaseConnectionManager.releaseConnection(connection);
		}
	}


	public static List<FluffyFile> getAllFileContentsFromRethink()
			throws FileNotFoundException, IOException, ParseException, EmptyConnectionPoolException {
		System.out.println("Entered DatabaseHandler::getAllFileContents");
		List<FluffyFile> fileContents = new ArrayList<FluffyFile>();
		Connection connection = databaseConnectionManager.getConnection();
		System.out.println("Entered DatabaseHandler::getAllFileContents::querying");
		ArrayList<String> dataFromDB = rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE).getField(Constants.FILE_NAME).distinct().run(connection);
		databaseConnectionManager.releaseConnection(connection);
		System.out.println("Entered DatabaseHandler::getAllFileContents::After querying");
		if (dataFromDB!= null){
			for (Object record : dataFromDB) {
				System.out.println("Record::"+record);
				List<FluffyFile> eachFileContent = new ArrayList<FluffyFile>();
				eachFileContent = getFileContents(record.toString());
				//System.out.println("eachFileContent::"+eachFileContent);
				fileContents.addAll(eachFileContent);
			}
		}
		return fileContents;
	}


	public static List<FluffyFile> getAllFileContentsFromRiak()
			throws FileNotFoundException, IOException, ParseException, EmptyConnectionPoolException {
		List<FluffyFile> fileContents = new ArrayList<FluffyFile>();
		riakDatabase = RiakDatabase.getRiakInstance();
		fileContents = riakDatabase.getAllFiles();
		return fileContents;
	}
	
	public static boolean isFileAvailableInRethink(String filename) throws EmptyConnectionPoolException{
		Connection connection = databaseConnectionManager.getConnection();

		Cursor<String> dataFromDB = rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE)
				.filter(rethinkDBInstance.hashMap(Constants.FILE_NAME, filename)).run(connection);
		databaseConnectionManager.releaseConnection(connection);
		if (dataFromDB == null)
			System.out.println("Database error");
		else{
			int count  = 0;
			for (Object record : dataFromDB) {
				count ++;
			}

			if(count != 0){
				return true;
			}
		}
		return false;	
	}

	public static boolean isFileAvailableInRiak(String filename) throws EmptyConnectionPoolException{
		riakDatabase = RiakDatabase.getRiakInstance();
		int isAvailable = riakDatabase.getChunkCount(filename);
		if(isAvailable>0){
			return true;
		} else {
			return false;
		}
	}
}
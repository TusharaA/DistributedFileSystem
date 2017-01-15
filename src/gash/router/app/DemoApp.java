/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.app;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.protobuf.ByteString;

import gash.router.client.CommConnection;
import gash.router.client.CommListener;
import gash.router.client.MessageClient;
import routing.Pipe.CommandMessage;

public class DemoApp implements CommListener {
	private MessageClient mc;
	private java.util.Map<String, ArrayList<CommandMessage>> byteList = new HashMap<String, ArrayList<CommandMessage>>();

	public DemoApp(MessageClient mc) {
		init(mc);
	}

	private void init(MessageClient mc) {
		this.mc = mc;
		this.mc.addListener(this);
	}

	private void ping(int N) {
		// test round-trip overhead (note overhead for initial connection)
		final int maxN = 10;
		long[] dt = new long[N];
		long st = System.currentTimeMillis(), ft = 0;
		for (int n = 0; n < N; n++) {
			mc.ping();
			ft = System.currentTimeMillis();
			dt[n] = ft - st;
			st = ft;
		}

		System.out.println("Round-trip ping times (msec)");
		for (int n = 0; n < N; n++)
			System.out.print(dt[n] + " ");
		System.out.println("");
	}

	@Override
	public String getListenerID() {
		return "demo";
	}
	@Override
	public void onMessage(CommandMessage msg) {
	//	System.out.println("---> " + msg);	
	//	ByteString content = msg.getFiletask().getChunk();
	//	System.out.println(new String(content.toByteArray()));
		
		if(msg.hasFiletask()){
		/*	System.out.println(msg.getFiletask().getChunk());
			System.out.println(msg.getFiletask().getFilename());
			System.out.println(msg.getFiletask().getChunkNo());
			System.out.println(msg.getFiletask().getChunkCounts());
			
		*/
			if(!byteList.containsKey(msg.getFiletask().getFilename())){
				byteList.put(msg.getFiletask().getFilename(), new ArrayList<CommandMessage>());
				System.out.println("Chunk list created ");
			}
			byteList.get(msg.getFiletask().getFilename()).add(msg);
			System.out.println("Size: " + byteList.get(msg.getFiletask().getFilename()).size() + " Actual size: " + msg.getFiletask().getChunkCounts() );
			if(byteList.get(msg.getFiletask().getFilename()).size() == msg.getFiletask().getChunkCounts()){
				try {
					System.out.println("size is same: ");
					File file = new File("new" + msg.getFiletask().getFilename());
					file.createNewFile();
					//List<ByteString> byteString = new ArrayList<ByteString>();
					FileOutputStream outputStream = new FileOutputStream(file);
					
					List<CommandMessage> abcd = byteList.get(msg.getFiletask().getFilename());
					ByteString[] byteStringArray = new ByteString[abcd.size()];
					for(int index = 0; index < abcd.size(); index++) {
						byteStringArray[abcd.get(index).getFiletask().getChunkNo() - 1] = abcd.get(index).getFiletask().getChunk();
					}
					for(int index = 0; index < byteStringArray.length; index++) {
						outputStream.write(byteStringArray[index].toByteArray());
					}
					/*int i=1;
					while(i <= msg.getFiletask().getChunkCounts()){
						for(int j=0; j < byteList.get(msg.getFiletask().getFilename()).size(); j++){
							System.out.println("Inside the for loop ");
							if(byteList.get(msg.getFiletask().getFilename()).get(j).getFiletask().getChunkNo() == i){
								System.out.println("Added chunk to file "+i);
								byteString.add(byteList.get(msg.getFiletask().getFilename()).get(j).getFiletask().getChunk());
								System.out.println(byteList.get(msg.getFiletask().getFilename()).get(j).getFiletask().getChunk().size());
								System.out.println("ByteArray" + byteList.get(msg.getFiletask().getFilename()).get(j).getFiletask().getChunk().toByteArray());
								outputStream.write(byteList.get(msg.getFiletask().getFilename()).get(j).getFiletask().getChunk().toByteArray());
								
								i++;
								break;
							}
						}
						
					}
					ByteString bs = ByteString.copyFrom(byteString);
					outputStream.write(bs.toByteArray());
					*/
					outputStream.flush();
					outputStream.close();
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
		System.out.flush();
	
		
	}

	/**
	 * sample application (client) use of our messaging service
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		//String host = "127.0.0.1";
		String host = "169.254.203.5";
		int port = 4568;

		try {
			MessageClient mc = new MessageClient(host, port);
			DemoApp da = new DemoApp(mc);

			// do stuff w/ the connection
			// da.ping(2);
			da.sendReadFileTasks(args[0]);
			//da.chunkFile(args[0]);
			//da.sendFileAsChunks(new File(args[0]));
			//da.updateFileAsChunks(args[0], new File(args[1]));
			//da.sendReadFileTasks(args[0]);
			//da.sendDeleteFileTasks(args[0]);
			System.out.println("\n** exiting in 10 seconds. **");
			System.out.flush();
			Thread.sleep(6000 * 1000);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			CommConnection.getInstance().release();
		}
	}

	@Deprecated
	private void chunkFile(String file) throws IOException {
		ArrayList<String> chunkedFile = new ArrayList<String>();
		String line = null;
		try {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(
					file));

			int chunkId = 0;
			while ((line = bufferedReader.readLine()) != null) {
				// databaseHandler.addFile(file, line, chunkId);
				chunkedFile.add(line);
				//mc.sendFileChunks(file, line, chunkId);
				chunkId++;
			}
			
			int size = chunkedFile.size();
			System.out.println("File chunk size: " + size);
			for(int i =0; i < chunkedFile.size(); i++){
				mc.sendFileChunks(file, chunkedFile.get(i), i, size );
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private void sendFileAsChunks(File file) {
		ArrayList<ByteString> chunkedFile = new ArrayList<ByteString>();

		int sizeOfChunk = 1024 * 1024;
		int numOfChunks = 0;
		byte[] buffer = new byte[sizeOfChunk];

		try {
			BufferedInputStream bis = new BufferedInputStream(
					new FileInputStream(file));
			String name = file.getName();

			int tmp = 0;
			while ((tmp = bis.read(buffer)) > 0) {
				try {
					ByteString bs = ByteString.copyFrom(buffer, 0, tmp);
					chunkedFile.add(bs);
					numOfChunks++;
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}

			for (int index = 0; index < chunkedFile.size(); index++) {
				System.out.println(chunkedFile.get(index));
				mc.sendFile(chunkedFile.get(index), name, numOfChunks,
						index + 1); 
				Thread.sleep(100);
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		System.out.println(chunkedFile.size());
	}
	
	private void updateFileAsChunks(String filename, File file) {
		ArrayList<ByteString> chunkedFile = new ArrayList<ByteString>();

		int sizeOfChunk = 1024 * 1024;
		int numOfChunks = 0;
		byte[] buffer = new byte[sizeOfChunk];

		try {
			BufferedInputStream bis = new BufferedInputStream(
					new FileInputStream(file));
			//String name = file.getName();

			int tmp = 0;
			while ((tmp = bis.read(buffer)) > 0) {
				try {
					ByteString bs = ByteString.copyFrom(buffer, 0, tmp);
					chunkedFile.add(bs);
					numOfChunks++;
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}

			for (int index = 0; index < chunkedFile.size(); index++) {
				System.out.println(chunkedFile.get(index));
				mc.updateFile(chunkedFile.get(index), filename, numOfChunks,
						index + 1); 
				Thread.sleep(100);
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		System.out.println(chunkedFile.size());
	}
	
	public void sendReadFileTasks(String filename){
		try {
			mc.sendReadFileRequest(filename);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			System.out.println("Unable to send read file task: " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	
	public void sendDeleteFileTasks(String filename){
		try {
			mc.sendDeleteFileRequest(filename);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			System.out.println("Unable to send delete file task: " + e.getMessage());
			e.printStackTrace();
		}
	}
}

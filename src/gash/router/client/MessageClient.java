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
package gash.router.client;

import java.net.UnknownHostException;

import com.google.protobuf.ByteString;

import pipe.common.Common.Header;
import routing.Pipe.CommandMessage;
import routing.Pipe.FileTask;
import routing.Pipe.FileTask.FileTaskType;

/**
 * front-end (proxy) to our service - functional-based
 * 
 * @author gash
 * 
 */
public class MessageClient {
	// track requests
	private long curID = 0;

	public MessageClient(String host, int port) {
		init(host, port);
	}

	private void init(String host, int port) {
		CommConnection.initConnection(host, port);
	}

	public void addListener(CommListener listener) {
		CommConnection.getInstance().addListener(listener);
	}

	public void ping() {
		// construct the message to send
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(-1);

		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		rb.setPing(true);

		try {
			// direct no queue
			// CommConnection.getInstance().write(rb.build());

			// using queue
			CommConnection.getInstance().enqueue(rb.build());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Deprecated
	public void sendFileChunks(String filename, String line, int chunkId, int chunkCount) {
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(-1);

		FileTask.Builder tb = FileTask.newBuilder();
		tb.setFilename(filename);
		//tb.setChunk(line);
		tb.setChunkNo(chunkId);
		tb.setChunkCounts(chunkCount);
		tb.setFileTaskType(FileTaskType.WRITE);

		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		rb.setFiletask(tb);
		rb.setMessage(filename);

		try {
			// direct no queue
			// CommConnection.getInstance().write(rb.build());

			// using queue
			CommConnection.getInstance().enqueue(rb.build());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void sendFile(ByteString bs, String filename, int numOfChunks, int chunkId) {
		// construct the message to send
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(-1);

		FileTask.Builder tb = FileTask.newBuilder();
		tb.setChunkCounts(numOfChunks); // Num of chunks
		tb.setChunkNo(chunkId); // chunk id
		tb.setFileTaskType(FileTask.FileTaskType.WRITE);
		tb.setSender("127.0.0.1");
		tb.setFilename(filename);
		tb.setChunk(bs);

		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		rb.setFiletask(tb);
		rb.setMessage(filename);

		try {
			// direct no queue
			// CommConnection.getInstance().write(rb.build());

			// using queue
			CommConnection.getInstance().enqueue(rb.build());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void updateFile(ByteString bs, String filename, int numOfChunks, int chunkId) {
		// construct the message to send
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(-1);

		FileTask.Builder tb = FileTask.newBuilder();
		tb.setChunkCounts(numOfChunks); // Num of chunks
		tb.setChunkNo(chunkId); // chunk id
		tb.setFileTaskType(FileTask.FileTaskType.UPDATE);
		tb.setSender("127.0.0.1");
		tb.setFilename(filename);
		tb.setChunk(bs);

		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		rb.setFiletask(tb);
		rb.setMessage(filename);

		try {
			// direct no queue
			// CommConnection.getInstance().write(rb.build());

			// using queue
			CommConnection.getInstance().enqueue(rb.build());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void sendReadFileRequest(String filename) throws UnknownHostException{
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(-1);
		
		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		
		FileTask.Builder task = FileTask.newBuilder();
		task.setFilename(filename);
		task.setFileTaskType(FileTaskType.READ);		
		rb.setMessage(filename);
		task.setSender("127.0.0.1");
		rb.setFiletask(task.build());
		
		try {
			CommConnection.getInstance().enqueue(rb.build());
			System.out.println("Sent Read Message");
		} catch (Exception e) {
			System.out.println("Unable to send read file task: " + e.getMessage());
			e.printStackTrace();
		}
		
	}

	public void sendDeleteFileRequest(String filename) throws UnknownHostException{
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(-1);
		
		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		
		FileTask.Builder task = FileTask.newBuilder();
		task.setFilename(filename);
		task.setFileTaskType(FileTaskType.DELETE);		
		rb.setMessage(filename);
		task.setSender("127.0.0.1");
		
		rb.setFiletask(task.build());
		
		try {
			CommConnection.getInstance().enqueue(rb.build());
			System.out.println("Sent Delete Message");
		} catch (Exception e) {
			System.out.println("Unable to send delete file task: " + e.getMessage());
			e.printStackTrace();
		}
		
	}
	
	public void release() {
		CommConnection.getInstance().release();
	}

	/**
	 * Since the service/server is asychronous we need a unique ID to associate
	 * our requests with the server's reply
	 * 
	 * @return
	 */
	@SuppressWarnings("unused")
	private synchronized long nextId() {
		return ++curID;
	}
}

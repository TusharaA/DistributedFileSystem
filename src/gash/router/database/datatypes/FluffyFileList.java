package gash.router.database.datatypes;

import java.util.ArrayList;

public class FluffyFileList {
	ArrayList<FluffyFile> files;
	
	public FluffyFileList() {
		files = new ArrayList<>();
	}

	public ArrayList<FluffyFile> getFiles() {
		return files;
	}

	public void setFiles(ArrayList<FluffyFile> files) {
		this.files = files;
	}

}

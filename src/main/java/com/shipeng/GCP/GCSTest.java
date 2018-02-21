package com.shipeng.GCP;

import com.google.cloud.storage.Storage;

public class GCSTest {

	private static final String jsonKeyFile = "/Users/sxu/test/GoogleCloud/SamFirstProject-2b6aace7012d.json";
	
	public static void main(String[] args) {
		
		try {
			
			Storage storage = GCPFactory.getGoogleStorageInstance(jsonKeyFile);
			// The name for the new bucket
		    String bucketName = "shipeng-new-bucket";
		    
		    // upload a csv file into newly created bucket
		    GStorageService gStorageService = new GStorageService();
		    gStorageService.uploadFile(storage, bucketName, "salary.csv");
		    
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	} // end main
}

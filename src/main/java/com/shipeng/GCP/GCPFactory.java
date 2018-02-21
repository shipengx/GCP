package com.shipeng.GCP;

import java.io.File;
import java.io.FileInputStream;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class GCPFactory {

	public static Storage getGoogleStorageInstance(String jsonKeyFile) throws Exception {
		Storage storage = null;
		if (jsonKeyFile == null || jsonKeyFile.length() == 0) {
			storage = StorageOptions.getDefaultInstance().getService();
		} else {
			// Load credentials from JSON key file. If you can't set the
			// GOOGLE_APPLICATION_CREDENTIALS
			// environment variable, you can explicitly load the credentials file to
			// construct the credentials.
			GoogleCredentials credentials;
			File credentialsPath = new File(jsonKeyFile);
			try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
				credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
			}
			storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
		}
		return storage;
	}

	public static BigQuery getGoogleBigQueryInstance(String jsonKeyFile) throws Exception {
		BigQuery bigQuery = null;
		if (jsonKeyFile == null || jsonKeyFile.length() == 0) {
			bigQuery = BigQueryOptions.getDefaultInstance().getService();
		} else {
			// Load credentials from JSON key file. If you can't set the
			// GOOGLE_APPLICATION_CREDENTIALS
			// environment variable, you can explicitly load the credentials file to
			// construct the credentials.
			GoogleCredentials credentials;
			File credentialsPath = new File(jsonKeyFile);
			try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
				credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
			}

			bigQuery = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
		}
		return bigQuery;
	}

}

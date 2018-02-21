package com.adara.GCP;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.storage.Storage;

public class GoogleCloudFactory {

	private static Credential getServiceAccountCredential(HttpTransport httpTransport, JsonFactory factory,
			String serviceAccountEmail, String p12File) throws Exception {

		Set<String> scopes = new HashSet<String>();
		scopes.addAll(BigqueryScopes.all());

		// Service account credential.
		GoogleCredential credential = new GoogleCredential.Builder().setTransport(httpTransport).setJsonFactory(factory)
				.setServiceAccountId(serviceAccountEmail).setServiceAccountScopes(scopes)
				.setServiceAccountPrivateKeyFromP12File(new File(p12File))
				// Set the user you are impersonating (this can be yourself).
				// .setServiceAccountUser(ACCOUNT_TO_IMPERSONATE)
				.build();

		credential.refreshToken();
		return credential;
	}

	public static Bigquery getGoogleBigQueryInstance(String serviceAccountEmail, String p12File) throws Exception {
		HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
		JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

		// Build service account credential.
		Credential credential = getServiceAccountCredential(httpTransport, jsonFactory, serviceAccountEmail, p12File);

		// Create DFA Reporting client.
		return new Bigquery.Builder(httpTransport, jsonFactory, credential).setApplicationName("big query test")
				.build();
	}

	public static Storage getGoogleCloudStorageInstance(String serviceAccountEmail, String p12File) throws Exception {

		HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
		JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

		// Build service account credential.
		Credential credential = getServiceAccountCredential(httpTransport, jsonFactory, serviceAccountEmail, p12File);

		// Create DFA Reporting client.
		return new Storage.Builder(httpTransport, jsonFactory, credential).setApplicationName("cloud storage test")
				.build();
	}

}

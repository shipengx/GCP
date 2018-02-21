package com.shipeng.GCP;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Examples for authenticating to Google BigQuery.
 *
 * <p>See: https://cloud.google.com/bigquery/authentication
 */
public class AuthSnippets {

  // [START default_credentials]
  public static void implicit() {
    // Instantiate a client. If you don't specify credentials when constructing a client, the
    // client library will look for credentials in the environment, such as the
    // GOOGLE_APPLICATION_CREDENTIALS environment variable.
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    // Use the client.
    System.out.println("Datasets:");
    for (Dataset dataset : bigquery.listDatasets().iterateAll()) {
      System.out.printf("%s%n", dataset.getDatasetId().getDataset());
    }
  }
  // [END default_credentials]

  // [START explicit_service_account]
  public static void explicit() throws IOException {
    // Load credentials from JSON key file. If you can't set the GOOGLE_APPLICATION_CREDENTIALS
    // environment variable, you can explicitly load the credentials file to construct the
    // credentials.
    GoogleCredentials credentials;
    File credentialsPath = new File("/Users/sxu/test/GoogleCloud/SamFirstProject-2b6aace7012d.json");  // TODO: update to your key path.
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
      credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
    }

    // Instantiate a client.
    BigQuery bigquery =
        BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();

    // Use the client.
    System.out.println("Datasets:");
    for (Dataset dataset : bigquery.listDatasets().iterateAll()) {
      System.out.printf("%s%n", dataset.getDatasetId().getDataset());
    }
  }
  // [END explicit_service_account]

  public static void main(String... args) throws IOException {
	  
	System.out.println("environment variable: " + System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));
    boolean validArgs = args.length == 1;
    String sample = "explicit";
    if (validArgs) {
      sample = args[0];
      if (!sample.equals("explicit") && !sample.equals("implicit")) {
        validArgs = false;
      }
    }

    if (!validArgs) {
      System.err.println("Expected auth type argument: implict|explict");
      System.exit(1);
    }

    if (sample.equals("implicit")) {
      implicit();
    } else {
      explicit();
    }
  }
}

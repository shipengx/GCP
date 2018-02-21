package com.adara.GCP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.Bigquery.Datasets;

public class BQUtil extends AbstractUtil {

	public static Table createTable(String projectId, String datasetId, String tableId, TableSchema schema,
			Bigquery bigquery) throws IOException {
		Table table = new Table();

		TableReference tableRef = new TableReference();
		tableRef.setDatasetId(datasetId);
		tableRef.setProjectId(projectId);
		tableRef.setTableId(tableId);
		table.setTableReference(tableRef);
		table.setFriendlyName(tableId);
		table.setSchema(schema);

		try {
			return bigquery.tables().insert(projectId, datasetId, table).execute();
		} catch (GoogleJsonResponseException e) {
			return null; // table already exists
		}
	}

	public static void listDatasets(final Bigquery bigquery, final String projectId) throws IOException {
		Datasets.List datasetRequest = bigquery.datasets().list(projectId);
		DatasetList datasetList = datasetRequest.execute();
		if (datasetList.getDatasets() != null) {
			List<DatasetList.Datasets> datasets = datasetList.getDatasets();
			System.out.println("Available datasets\n----------------");
			System.out.println(datasets.toString());
			for (DatasetList.Datasets dataset : datasets) {
				System.out.format("%s\n", dataset.getDatasetReference().getDatasetId());
			}
		}
	}

	public static String loadFromGoogleCloudStorage(Bigquery bigQuery, String projectId, String datasetId,
			String tableId, List<String> sourceUris) throws IOException {
		TableReference tableReference = new TableReference().setProjectId(projectId).setDatasetId(datasetId)
				.setTableId(tableId);
		JobConfigurationLoad loadConfig = new JobConfigurationLoad().setDestinationTable(tableReference)
				.setAllowQuotedNewlines(true).setSourceUris(sourceUris).setWriteDisposition("WRITE_APPEND");
		return runJob(bigQuery, projectId, new JobConfiguration().setLoad(loadConfig));
	}

	/**
	 * Submit a job to be run asynchronously and return the id of the job.
	 * 
	 * @param projectId
	 *            the project to run the job in.
	 * @param jobConfiguration
	 *            the job configuration.
	 * @return the id of the running job.
	 * @throws IOException
	 *             if there is an error submitting the job.
	 */
	private static String runJob(Bigquery bigQuery, String projectId, JobConfiguration jobConfiguration)
			throws IOException {
		return runJobWithContent(bigQuery, projectId, jobConfiguration, null);
	}

	/**
	 * Submit a job to be run asynchronously with optional content and return the id
	 * of the job.
	 * 
	 * @param projectId
	 *            the project to run the job in.
	 * @param jobConfiguration
	 *            the job configuration.
	 * @param content
	 *            optional content to run the job with.
	 * @return the id of the running job.
	 * @throws IOException
	 *             if there is an error submitting the job.
	 */
	private static String runJobWithContent(Bigquery bigQuery, String projectId, JobConfiguration jobConfiguration,
			AbstractInputStreamContent content) throws IOException {
		Job job = new Job().setConfiguration(jobConfiguration);
		Job runningJob = content != null ? bigQuery.jobs().insert(projectId, job, content).execute()
				: bigQuery.jobs().insert(projectId, job).execute();
		return runningJob.getJobReference().getJobId();
	}

	public static void main(String[] args) throws Exception {

		// Instantiate a client
		Bigquery bigquery = GoogleCloudFactory.getGoogleBigQueryInstance(serviceAccountEmail, p12File);

		System.out.println(bigquery.getApplicationName());
		System.out.println(bigquery.getBaseUrl());

		listDatasets(bigquery, projectId);

		// Build the table schema for the output table.
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("age").setType("INTEGER").setMode("REQUIRED"));
		fields.add(new TableFieldSchema().setName("salary").setType("FLOAT").setMode("NULLABLE"));
		TableSchema schema = new TableSchema().setFields(fields);

		// create a new table for shipeng
		Table table = createTable(projectId, datasetId, tableId, schema, bigquery);

		List<String> GCSUrls = new ArrayList<>();
		GCSUrls.add("gs://" + bucketName + "/sam_test_folder/sam_test_file.csv");
		// load data into GCS table
		loadFromGoogleCloudStorage(bigquery, projectId, datasetId, tableId, GCSUrls);
	}

}

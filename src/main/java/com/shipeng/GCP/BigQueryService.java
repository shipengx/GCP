package com.shipeng.GCP;

import org.threeten.bp.Duration;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

public class BigQueryService {

	// check table exists
	public boolean existsTable(BigQuery bigQuery, String datasetName, String tableName) {
		TableId tableId = TableId.of(datasetName, tableName);
		Table table = bigQuery.getTable(tableId);
		return table.exists();
	}

	// create a dataset
	public Dataset createDataset(BigQuery bigQuery, String datasetName) {
		Dataset dataset = null;
		DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
		try {
			// the dataset was created
			dataset = bigQuery.create(datasetInfo);
		} catch (BigQueryException e) {
			// the dataset was not created
		}
		return dataset;
	}

	// create a table
	public Table createTable(BigQuery bigQuery, String datasetName, String tableName) {
		TableId tableId = TableId.of(datasetName, tableName);
		// Table field definition
		Field ageField = Field.of("age", LegacySQLTypeName.INTEGER);
		Field salaryField = Field.of("salary", LegacySQLTypeName.FLOAT);
		// Table schema definition
		Schema schema = Schema.of(ageField, salaryField);
		TableDefinition tableDefinition = StandardTableDefinition.of(schema);
		TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
		Table table = bigQuery.create(tableInfo);
		return table;
	}

	// loading data from a single Google Cloud Storage file (append by default)
	// [TARGET load(FormatOptions, String, JobOption...)]
	// [VARIABLE "gs://my_bucket/filename.csv"]
	public Job loadGCSFile(BigQuery bigQuery, Table table, String sourceUri) {
		// [START loadSingle]
		WriteDisposition writeDisposition = JobInfo.WriteDisposition.valueOf("WRITE_TRUNCATE");
		LoadJobConfiguration loadConfig = LoadJobConfiguration.builder(table.getTableId(), sourceUri)
				.setWriteDisposition(writeDisposition).setFormatOptions(FormatOptions.csv()).build();
		JobInfo jobInfo = JobInfo.newBuilder(loadConfig).build();
		Job job = bigQuery.create(jobInfo);
		// Wait for the job to complete
		try {
			Job completedJob = job.waitFor(RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
					RetryOption.totalTimeout(Duration.ofMinutes(3)));
			if (completedJob != null && completedJob.getStatus().getError() == null) {
				// Job completed successfully
			} else {
				// Handle error case
				System.out.println("job failed.");
			}
		} catch (InterruptedException e) {
			// Handle interrupted wait
		}
		// [END loadSingle]
		return job;
	}

}

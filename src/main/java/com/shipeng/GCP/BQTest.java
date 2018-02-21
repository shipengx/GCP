package com.shipeng.GCP;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;

public class BQTest {

	private static final String jsonKeyFile = "/Users/sxu/test/GoogleCloud/SamFirstProject-2b6aace7012d.json";
	private static final String datasetName = "opinmind_qa";
	private static final String tableName = "sam_test_table";

	public static void main(String[] args) {

		try {
			BigQuery bigQuery = GCPFactory.getGoogleBigQueryInstance(jsonKeyFile);
			BigQueryService bigQueryService = new BigQueryService();
			TableId tableId = TableId.of(datasetName, tableName);
			Table table = bigQuery.getTable(tableId);
			if (table == null) {
				table = bigQueryService.createTable(bigQuery, datasetName, tableName);
			}
			
			// load data from gcs into bq table
			bigQueryService.loadGCSFile(bigQuery, table, "gs://ext_qa/sam_test_folder/salary.csv");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}

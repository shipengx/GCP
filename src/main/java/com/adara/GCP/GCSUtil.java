package com.adara.GCP;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;

public class GCSUtil extends AbstractUtil {

	public static void listDatasets(final Storage storage, final String bucketName) throws IOException {

		Storage.Objects.List objectsList = storage.objects().list(bucketName);
		Objects objects;
		do {
			objects = objectsList.execute();
			List<StorageObject> items = objects.getItems();
			if (items != null) {
				for (StorageObject object : items) {
					System.out.println(object.getName());
				}
			}
			objectsList.setPageToken(objects.getNextPageToken());
		} while (objects.getNextPageToken() != null);
	}

	public static void uploadToGCS(Storage storage, String localCSVFile) throws Exception {
		// upload local csv file to GCS

		File file = new File(localCSVFile);
		InputStream inputStream = new FileInputStream(file); // object data, e.g., FileInputStream
		long byteCount = file.length(); // size of input stream

		InputStreamContent mediaContent = new InputStreamContent("application/octet-stream", inputStream);
		// Knowing the stream length allows server-side optimization, and client-side
		// progress
		// reporting with a MediaHttpUploaderProgressListener.
		mediaContent.setLength(byteCount);

		com.google.api.services.storage.Storage.Objects.Insert insertObject = storage.objects().insert(bucketName, null,
				mediaContent);

		insertObject.setName("sam_test_folder/salary.csv");
		insertObject.execute();
	}

	public static StorageObject uploadSimple(Storage storage, String bucketName, String objectName, String data)
			throws UnsupportedEncodingException, IOException {
		return uploadSimple(storage, bucketName, objectName, new ByteArrayInputStream(data.getBytes("UTF-8")),
				"text/plain");
	}

	public static StorageObject uploadSimple(Storage storage, String bucketName, String objectName, File data)
			throws FileNotFoundException, IOException {
		return uploadSimple(storage, bucketName, objectName, new FileInputStream(data), "application/octet-stream");
	}

	public static StorageObject uploadSimple(Storage storage, String bucketName, String objectName, InputStream data,
			String contentType) throws IOException {
		InputStreamContent mediaContent = new InputStreamContent(contentType, data);
		Storage.Objects.Insert insertObject = storage.objects().insert(bucketName, null, mediaContent)
				.setName(objectName);
		// The media uploader gzips content by default, and alters the Content-Encoding
		// accordingly.
		// GCS dutifully stores content as-uploaded. This line disables the media
		// uploader behavior,
		// so the service stores exactly what is in the InputStream, without
		// transformation.
		insertObject.getMediaHttpUploader().setDisableGZipContent(true);
		return insertObject.execute();
	}

	public static StorageObject uploadWithMetadata(Storage storage, StorageObject object, InputStream data)
			throws IOException {
		InputStreamContent mediaContent = new InputStreamContent(object.getContentType(), data);
		Storage.Objects.Insert insertObject = storage.objects().insert(object.getBucket(), object, mediaContent);
		insertObject.getMediaHttpUploader().setDisableGZipContent(true);
		return insertObject.execute();
	}

	public static void main(String[] args) throws Exception {
		Storage storage = GoogleCloudFactory.getGoogleCloudStorageInstance(serviceAccountEmail, p12File);
		listDatasets(storage, bucketName);
		uploadToGCS(storage, fileToUpload);
		uploadSimple(storage, bucketName, "sam_test_folder/sam_test_file.csv", "16,33.52");
	}

}

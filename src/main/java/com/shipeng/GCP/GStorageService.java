package com.shipeng.GCP;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import static java.nio.charset.StandardCharsets.UTF_8;

public class GStorageService {

	// create a bucket
	public Bucket createBucket(Storage storage, String bucketName) {
		Bucket bucket = storage.create(BucketInfo.of(bucketName));
		return bucket;
	}

	// list all buckets
	public void listBuckets(Storage storage, String bucketName) {
		// List all your buckets
		System.out.println("My buckets:");
		for (Bucket currentBucket : storage.list().iterateAll()) {
			System.out.println(currentBucket);
		}
	}
	
	// upload a file
	public Blob uploadFile(Storage storage, String bucketName, String targetFileName) throws Exception {
		BlobId blobId = BlobId.of(bucketName, targetFileName);
		BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
		Blob blob = storage.create(blobInfo, "Hello, Cloud Storage!".getBytes(UTF_8));
		return blob;
	}

}

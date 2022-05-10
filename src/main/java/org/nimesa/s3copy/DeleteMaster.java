package org.nimesa.s3copy;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class DeleteMaster extends KeyMaster {

    public DeleteMaster(AmazonS3Client client, MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executorService) {
        super(client, context, workQueue, executorService);
    }

    protected String getPrefix(MirrorOptions options) {
        return options.hasDestPrefix() ? options.getDestPrefix() : options.getPrefix();
    }

    protected String getBucket(MirrorOptions options) { return options.getDestinationBucket(); }

    @Override
    protected KeyJob getTask(S3VersionSummary summary) {
        return new KeyDeleteJob(client, context, summary, notifyLock);
    }
}

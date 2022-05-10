package org.nimesa.s3copy;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3VersionSummary;
import org.nimesa.s3copy.comparisonstrategies.ComparisonStrategy;
import org.nimesa.s3copy.comparisonstrategies.ComparisonStrategyFactory;
import org.nimesa.s3copy.comparisonstrategies.SizeOnlyComparisonStrategy;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class CopyMaster extends KeyMaster {
    private final ComparisonStrategy comparisonStrategy;

    public CopyMaster(AmazonS3Client client, MirrorContext context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executorService) {
        super(client, context, workQueue, executorService);
        comparisonStrategy = ComparisonStrategyFactory.getStrategy(context.getOptions());
    }

    protected String getPrefix(MirrorOptions options) {
        return options.getPrefix();
    }

    protected String getBucket(MirrorOptions options) {
        return options.getSourceBucket();
    }

    protected KeyCopyJob getTask(S3VersionSummary summary) {
        if (summary.getSize() > MirrorOptions.MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE) {
            return new MultipartKeyCopyJob(client, context, summary, notifyLock, new SizeOnlyComparisonStrategy());
        }
        return new KeyCopyJob(client, context, summary, notifyLock, comparisonStrategy);
    }
}

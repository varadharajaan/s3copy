package org.nimesa.s3copy;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;
import org.nimesa.s3copy.comparisonstrategies.ComparisonStrategy;
import org.slf4j.Logger;

import java.util.Date;

/**
 * Handles a single key. Determines if it should be copied, and if so, performs the copy operation.
 */
@Slf4j
public class KeyCopyJob extends KeyJob {

    protected String keydest;

    protected String sourceVersionId;
    protected ComparisonStrategy comparisonStrategy;

    public KeyCopyJob(AmazonS3Client client, MirrorContext context, S3VersionSummary summary, Object notifyLock, ComparisonStrategy comparisonStrategy) {
        super(client, context, summary, notifyLock);

        keydest = summary.getKey();
        sourceVersionId = summary.getVersionId();
        final MirrorOptions options = context.getOptions();
        if (options.hasDestPrefix()) {
            keydest = keydest.substring(options.getPrefixLength());
            keydest = options.getDestPrefix() + keydest;
        }
        this.comparisonStrategy = comparisonStrategy;
    }

    @Override public Logger getLog() { return log; }

    @Override
    public void run() {
        final MirrorOptions options = context.getOptions();
        final String key = summary.getKey();
        try {
            if (!shouldTransfer()) return;
            final ObjectMetadata sourceMetadata = getObjectMetadata(options.getSourceBucket(), key, options);
            final AccessControlList objectAcl = getAccessControlList(options, key);

            if (options.isDryRun()) {
                log.info("Would have copied " + key + " to destination: " + keydest);
            } else {
                if (keyCopied(sourceMetadata, objectAcl)) {
                    context.getStats().objectsCopied.incrementAndGet();
                } else {
                    context.getStats().copyErrors.incrementAndGet();
                }
            }
        } catch (Exception e) {
            log.error("error copying key: " + key + ": " + e);

        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (options.isVerbose()) log.info("done with " + key);
        }
    }

    boolean keyCopied(ObjectMetadata sourceMetadata, AccessControlList objectAcl) {
        String key = summary.getKey();
        MirrorOptions options = context.getOptions();
        boolean verbose = options.isVerbose();
        int maxRetries= options.getMaxRetries();
        MirrorStats stats = context.getStats();
        for (int tries = 0; tries < maxRetries; tries++) {
            if (verbose) log.info("copying (try #" + tries + "): " + key + " to: " + keydest);
            log.error("sourceVersionId : "+ sourceVersionId);
            final CopyObjectRequest request = new CopyObjectRequest(options.getSourceBucket(), key, sourceVersionId,options.getDestinationBucket(), keydest);
            request.setStorageClass(StorageClass.valueOf(options.getStorageClass()));
            if (options.isEncrypt()) {
				request.putCustomRequestHeader("x-amz-server-side-encryption", "AES256");
			}
            request.setNewObjectMetadata(sourceMetadata);
            if (options.isCrossAccountCopy()) {
                request.setAccessControlList(buildCrossAccountAcl(objectAcl));
            } else {
                request.setAccessControlList(objectAcl);
            }
            try {
                log.error("adding to sourceBucketList list");
                stats.sourceBucketList.add(keydest);
                stats.s3copyCount.incrementAndGet();
                client.copyObject(request);
                final ObjectMetadata destinationMetadata = client.getObjectMetadata(options.getDestinationBucket(),key);
                if(destinationMetadata.getETag().equals(sourceMetadata.getETag())) {
                    log.error("adding to destinationBucketList list");
                    stats.destinationBucketList.add(keydest);
                } else {
                    log.error("error in copying the file, etag mismatch: fileName [{}] versionId[{}]", key,sourceVersionId);
                }
                stats.bytesCopied.addAndGet(sourceMetadata.getContentLength());
                if (verbose) log.info("successfully copied (on try #" + tries + "): " + key + " to: " + keydest);
                return true;
            } catch (AmazonS3Exception s3e) {
                log.error("s3 exception copying (try #" + tries + ") " + key + " to: " + keydest + ": " + s3e);
            } catch (Exception e) {
                log.error("unexpected exception copying (try #" + tries + ") " + key + " to: " + keydest + ": " + e);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                log.error("interrupted while waiting to retry key: " + key);
                return false;
            }
        }
        return false;
    }

    private boolean shouldTransfer() {
        final MirrorOptions options = context.getOptions();
        final String key = summary.getKey();
        final boolean verbose = options.isVerbose();

        if (options.hasCtime()) {
            final Date lastModified = summary.getLastModified();
            if (lastModified == null) {
                if (verbose) log.info("No Last-Modified header for key: " + key);

            } else {
                if (lastModified.getTime() < options.getMaxAge()) {
                    if (verbose) log.info("key "+key+" (lastmod="+lastModified+") is older than "+options.getCtime()+" (cutoff="+options.getMaxAgeDate()+"), not copying");
                    return false;
                }
            }
        }
        final ObjectMetadata metadata;
        try {
            metadata = getObjectMetadata(options.getDestinationBucket(), keydest, options);
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                if (verbose) log.info("Key not found in destination bucket (will copy): "+ keydest);
                return true;
            } else {
                log.warn("Error getting metadata for " + options.getDestinationBucket() + "/" + keydest + " (not copying): " + e);
                return false;
            }
        } catch (Exception e) {
            log.warn("Error getting metadata for " + options.getDestinationBucket() + "/" + keydest + " (not copying): " + e);
            return false;
        }

        if (summary.getSize() > MirrorOptions.MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE) {
            return metadata.getContentLength() != summary.getSize();
        }
        final boolean objectChanged = comparisonStrategy.sourceDifferent(summary, metadata);
        if (verbose && !objectChanged) log.info("Destination file is same as source, not copying: "+ key);

        return objectChanged;
    }
}

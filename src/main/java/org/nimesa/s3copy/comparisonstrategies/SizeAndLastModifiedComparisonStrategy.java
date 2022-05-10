package org.nimesa.s3copy.comparisonstrategies;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;

public class SizeAndLastModifiedComparisonStrategy extends SizeOnlyComparisonStrategy {
    @Override
    public boolean sourceDifferent(S3VersionSummary source, ObjectMetadata destination) {
        return super.sourceDifferent(source, destination) || source.getLastModified().after(destination.getLastModified());
    }
}

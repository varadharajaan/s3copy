package org.nimesa.s3copy.comparisonstrategies;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;

public class EtagComparisonStrategy extends SizeOnlyComparisonStrategy {
    @Override
    public boolean sourceDifferent(S3VersionSummary source, ObjectMetadata destination) {
        return super.sourceDifferent(source, destination) || !source.getETag().equals(destination.getETag());
    }
}

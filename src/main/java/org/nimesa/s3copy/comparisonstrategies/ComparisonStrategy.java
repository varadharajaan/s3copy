package org.nimesa.s3copy.comparisonstrategies;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3VersionSummary;

public interface ComparisonStrategy {
    boolean sourceDifferent(S3VersionSummary source, ObjectMetadata destination);
}

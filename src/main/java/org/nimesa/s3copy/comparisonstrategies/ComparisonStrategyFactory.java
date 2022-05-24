package org.nimesa.s3copy.comparisonstrategies;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.nimesa.s3copy.MirrorOptions;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ComparisonStrategyFactory {
    public static ComparisonStrategy getStrategy(MirrorOptions mirrorOptions) {
        if (mirrorOptions.isSizeOnly()) {
            return new SizeOnlyComparisonStrategy();
        } else if (mirrorOptions.isSizeAndLastModified()) {
            return new SizeAndLastModifiedComparisonStrategy();
        } else {
            System.out.println("found etag comparison strategies");
            return new EtagComparisonStrategy();
        }
    }
}

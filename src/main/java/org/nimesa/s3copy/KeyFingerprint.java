package org.nimesa.s3copy;

import lombok.*;

@EqualsAndHashCode(callSuper=false) @AllArgsConstructor
public class KeyFingerprint {

    @Getter private final long size;
    @Getter private final String etag;
   
    public KeyFingerprint(long size) {
        this(size, null);
    }

}

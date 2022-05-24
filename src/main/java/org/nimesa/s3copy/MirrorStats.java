package org.nimesa.s3copy;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class MirrorStats {

    @Getter private final Thread shutdownHook = new Thread() {
        @Override public void run() { logStats(); }
    };

    private static final String BANNER = "\n--------------------------------------------------------------------\n";
    public void logStats() {
        log.info(BANNER + "STATS BEGIN\n" + toString() + "STATS END " + BANNER);
        log.error("sourceBucketList {}", sourceBucketList);
        log.error("destinationBucketList {}", destinationBucketList);
        final List<String> arrayList = new ArrayList<>(CollectionUtils.disjunction(sourceBucketList, destinationBucketList));
        log.error("error files that not copied [{}]", arrayList.toString());
    }

    private long start = System.currentTimeMillis();

    public final AtomicLong objectsRead = new AtomicLong(0);
    public final AtomicLong objectsCopied = new AtomicLong(0);
    public final AtomicLong copyErrors = new AtomicLong(0);
    public final AtomicLong objectsDeleted = new AtomicLong(0);
    public final AtomicLong deleteErrors = new AtomicLong(0);

    public final AtomicLong s3copyCount = new AtomicLong(0);
    public final AtomicLong s3deleteCount = new AtomicLong(0);
    public final AtomicLong s3getCount = new AtomicLong(0);
    public final AtomicLong bytesCopied = new AtomicLong(0);

    public static final long HOUR = TimeUnit.HOURS.toMillis(1);
    public static final long MINUTE = TimeUnit.MINUTES.toMillis(1);
    public static final long SECOND = TimeUnit.SECONDS.toMillis(1);

    public final Set<String> destinationBucketList = new HashSet<>();

    public final Set<String> sourceBucketList = new HashSet<>();

    public Set<String>  getSourceBucketList() {
        return sourceBucketList;
    }

    public Set<String> getDestinationBucketList() {
        return destinationBucketList;
    }

    public String toString () {
        final long durationMillis = System.currentTimeMillis() - start;
        final double durationMinutes = durationMillis / 60000.0d;
        final String duration = String.format("%d:%02d:%02d", durationMillis / HOUR, (durationMillis % HOUR) / MINUTE, (durationMillis % MINUTE) / SECOND);
        final double readRate = objectsRead.get() / durationMinutes;
        final double copyRate = objectsCopied.get() / durationMinutes;
        final double deleteRate = objectsDeleted.get() / durationMinutes;
        return "read: "+objectsRead+ "\n"
                + "copied: "+objectsCopied+"\n"
                + "copy errors: "+copyErrors+"\n"
                + "deleted: "+objectsDeleted+"\n"
                + "delete errors: "+deleteErrors+"\n"
                + "duration: "+duration+"\n"
                + "read rate: "+readRate+"/minute\n"
                + "copy rate: "+copyRate+"/minute\n"
                + "delete rate: "+deleteRate+"/minute\n"
                + "bytes copied: "+formatBytes(bytesCopied.get())+"\n"
                + "GET operations: "+s3getCount+"\n"
                + "COPY operations: "+ s3copyCount+"\n"
                + "DELETE operations: "+ s3deleteCount+"\n";
    }

    private String formatBytes(long bytesCopied) {
        if (bytesCopied > MirrorConstants.EB) return ((double) bytesCopied) / ((double) MirrorConstants.EB) + " EB ("+bytesCopied+" bytes)";
        if (bytesCopied > MirrorConstants.PB) return ((double) bytesCopied) / ((double) MirrorConstants.PB) + " PB ("+bytesCopied+" bytes)";
        if (bytesCopied > MirrorConstants.TB) return ((double) bytesCopied) / ((double) MirrorConstants.TB) + " TB ("+bytesCopied+" bytes)";
        if (bytesCopied > MirrorConstants.GB) return ((double) bytesCopied) / ((double) MirrorConstants.GB) + " GB ("+bytesCopied+" bytes)";
        if (bytesCopied > MirrorConstants.MB) return ((double) bytesCopied) / ((double) MirrorConstants.MB) + " MB ("+bytesCopied+" bytes)";
        if (bytesCopied > MirrorConstants.KB) return ((double) bytesCopied) / ((double) MirrorConstants.KB) + " KB ("+bytesCopied+" bytes)";
        return bytesCopied + " bytes";
    }

}

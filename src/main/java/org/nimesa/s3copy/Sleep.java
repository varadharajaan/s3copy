package org.nimesa.s3copy;


import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Sleep {

    public static boolean sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            log.error("interrupted!");
            return true;
        }
        return false;
    }

}

package org.csa.truffle.reload;

import org.csa.truffle.SwitchablePythonSource;
import org.csa.truffle.graal.GraalPyInterpreter;
import org.csa.truffle.graal.reload.ScheduledReloader;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class ScheduledReloaderTest {

    private static final Duration INTERVAL = Duration.ofMillis(50);

    private GraalPyInterpreter newInterpreter(String dir) {
        return new GraalPyInterpreter(new SwitchablePythonSource(dir));
    }

    @Test
    void lastCheckedAt_setAfterStart() throws Exception {
        try (GraalPyInterpreter interp = newInterpreter("python_hr_v1");
             ScheduledReloader reloader = new ScheduledReloader(interp, INTERVAL)) {
            reloader.start();
            assertNotNull(reloader.getLastCheckedAt());
        }
    }

    @Test
    void lastChangedAt_setWhenDataChanges() throws Exception {
        // First load always produces changes (new files are loaded)
        try (GraalPyInterpreter interp = newInterpreter("python_hr_v1");
             ScheduledReloader reloader = new ScheduledReloader(interp, INTERVAL)) {
            reloader.start();
            assertNotNull(reloader.getLastChangedAt());
        }
    }

    @Test
    void lastChangedAt_nullWhenNoChanges() throws Exception {
        // Load data once so the interpreter has the files cached
        SwitchablePythonSource src = new SwitchablePythonSource("python_hr_v1");
        try (GraalPyInterpreter interp = new GraalPyInterpreter(src)) {
            interp.reload();   // prime the cache
            // Now create reloader — start() will call reload() again but nothing changed
            try (ScheduledReloader reloader = new ScheduledReloader(interp, Duration.ofSeconds(60))) {
                reloader.start();
                assertNull(reloader.getLastChangedAt(),
                        "lastChangedAt should remain null when reload detects no changes");
            }
        }
    }

    @Test
    void periodicReload_firesWithinInterval() throws Exception {
        try (GraalPyInterpreter interp = newInterpreter("python_hr_v1");
             ScheduledReloader reloader = new ScheduledReloader(interp, INTERVAL)) {
            reloader.start();
            Instant after = reloader.getLastCheckedAt();
            // Wait long enough for at least one background tick
            Thread.sleep(INTERVAL.toMillis() * 3);
            Instant later = reloader.getLastCheckedAt();
            assertNotNull(later);
            assertTrue(later.isAfter(after),
                    "lastCheckedAt should advance after waiting > interval");
        }
    }

    @Test
    void close_stopsScheduler() throws Exception {
        GraalPyInterpreter interp = newInterpreter("python_hr_v1");
        ScheduledReloader reloader = new ScheduledReloader(interp, INTERVAL);
        reloader.start();
        reloader.close();
        interp.close();

        Instant afterClose = reloader.getLastCheckedAt();
        Thread.sleep(INTERVAL.toMillis() * 3);
        Instant stillSame = reloader.getLastCheckedAt();

        assertEquals(afterClose, stillSame,
                "lastCheckedAt should not advance after close()");
    }
}

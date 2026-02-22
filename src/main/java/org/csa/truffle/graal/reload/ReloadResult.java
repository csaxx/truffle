package org.csa.truffle.graal.reload;

import java.time.Instant;
import java.util.Optional;

public record ReloadResult(
        boolean changed,
        Instant reloadedAt,
        Optional<Instant> dataAge
) {}

package org.csa.truffle.source.map;

import org.csa.truffle.source.FileSourceConfig;

/**
 * Config record for {@link MapFileSource}.
 *
 * <p>Since the in-memory map cannot be serialized for Flink distribution,
 * {@code FileSourceFactory} creates an <em>empty</em> {@code MapFileSource} from this config.
 * Callers who need pre-populated state should instantiate {@link MapFileSource} directly.
 */
public record MapFileSourceConfig(String[] filemasks, String[] excludeFilemasks) implements FileSourceConfig {

    public MapFileSourceConfig() {
        this(null, null);
    }

    public MapFileSourceConfig(String[] filemasks) {
        this(filemasks, null);
    }
}

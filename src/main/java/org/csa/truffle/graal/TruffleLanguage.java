package org.csa.truffle.graal;

public enum TruffleLanguage {
    PYTHON("python"),
    JS("js"),
    RUBY("ruby"),
    WASM("wasm"),
    LLVM("llvm");

    private final String id;

    TruffleLanguage(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }
}

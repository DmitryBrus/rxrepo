package com.slimgears.rxrepo.sql;

import com.slimgears.util.stream.Safe;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

public class DigestKeyEncoder implements KeyEncoder {
    private final ThreadLocal<MessageDigest> digestProvider;
    private final Base64.Encoder base64;
    private final int length;

    private DigestKeyEncoder(String algorithm, int length) {
        this.digestProvider = ThreadLocal.withInitial(Safe.ofSupplier(() -> MessageDigest.getInstance(algorithm)));
        this.base64 = Base64.getEncoder();
        this.length = length;
    }

    @Override
    public String encode(Object key) {
        byte[] digest = digestProvider.get().digest(key.toString().getBytes(StandardCharsets.UTF_8));
        String encodedKey = new String(base64.encode(digest));
        return length > 0
                ? encodedKey.substring(0, length)
                : (length < 0)
                ? encodedKey.substring(0, encodedKey.length() + length)
                : encodedKey;
    }

    public static KeyEncoder create(String algorithm, int length) {
        return new DigestKeyEncoder(algorithm, length);
    }

    public static KeyEncoder create() {
        return create("SHA-1", 0);
    }
}

package voldemort.store.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Implementation of CompressionStrategy that returns the original data
 * unchanged. A typical use-case for this is not to compress the keys when using
 * {@link CompressingStore}.
 */
public class NoopCompressionStrategy extends CompressionStrategy {

    @Override
    public byte[] deflate(byte[] data) throws IOException {
        return data;
    }

    @Override
    public byte[] inflate(byte[] data) throws IOException {
        return data;
    }

    @Override
    public String getType() {
        return "noop";
    }

    @Override
    public InputStream wrapInputStream(InputStream underlying) throws IOException {
        return underlying;
    }

    @Override
    public OutputStream wrapOutputStream(OutputStream underlying) throws IOException {
        return underlying;
    }
}

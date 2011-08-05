package voldemort.store.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

/**
 * Implementation of CompressionStrategy for Google's Snappy compression -
 * called through JNI
 */
public class SnappyCompressionStrategy extends CompressionStrategy {

    @Override
    public String getType() {
        return "snappy";
    }

    @Override
    public OutputStream wrapOutputStream(OutputStream underlying) throws IOException {
        return new SnappyOutputStream(underlying);
    }

    @Override
    public InputStream wrapInputStream(InputStream underlying) throws IOException {
        return new SnappyInputStream(underlying);
    }

}

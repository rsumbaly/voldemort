package voldemort.store.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;

/**
 * Implementation of CompressionStrategy for the LZF format. LZF is optimized
 * for speed.
 */
public class LzfCompressionStrategy extends CompressionStrategy {

    @Override
    public String getType() {
        return "lzf";
    }

    @Override
    public OutputStream wrapOutputStream(OutputStream underlying) throws IOException {
        return new LZFOutputStream(underlying);
    }

    @Override
    public InputStream wrapInputStream(InputStream underlying) throws IOException {
        return new LZFInputStream(underlying);
    }
}

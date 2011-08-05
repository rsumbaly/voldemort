package voldemort.store.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;

/**
 * Implementations of this interface provide a strategy for compressing and
 * uncompressing data.
 */
public abstract class CompressionStrategy {

    /**
     * The type of compression performed.
     */
    public abstract String getType();

    /**
     * Uncompresses the data. The array received should not be modified, but it
     * may be returned unchanged.
     * 
     * @param data compressed data.
     * @return uncompressed data.
     * @throws IOException if there is an issue during the operation.
     */
    public byte[] inflate(byte[] data) throws IOException {
        InputStream is = wrapInputStream(new ByteArrayInputStream(data));
        byte[] inflated = IOUtils.toByteArray(is);
        is.close();
        return inflated;
    }

    /**
     * Compresses the data. The array received should not be modified, but it
     * may be returned unchanged.
     * 
     * @param data uncompressed data.
     * @return compressed data.
     * @throws IOException if there is an issue during the operation.
     */
    public byte[] deflate(byte[] data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        OutputStream gos = wrapOutputStream(bos);
        gos.write(data);
        gos.close();
        return bos.toByteArray();
    }

    /**
     * Wrap the output stream
     * 
     * @param underlying The underlying output stream
     * @return The wrapped compressed output stream
     * @throws IOException
     */
    public abstract OutputStream wrapOutputStream(OutputStream underlying) throws IOException;

    /**
     * Wrap the input stream
     * 
     * @param underlying The underlying input stream
     * @return The wrapped compressed input stream
     * @throws IOException
     */
    public abstract InputStream wrapInputStream(InputStream underlying) throws IOException;

}

package voldemort.secondary;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Range query definition for the secondary index feature. Matches all the
 * values that are within the defined range (inclusive).
 * <p>
 * start and end field types should be in-synch with the selected
 * serializer/schema.
 */
public class RangeQuery {

    private final String field;

    private final Object start;
    private final Object end;

    /**
     * New range query on the given field, for the given limits (both
     * inclusive).
     */
    public RangeQuery(String field, Object start, Object end) {
        this.field = field;
        this.start = start;
        this.end = end;
    }

    /**
     * Serializes this query to the given output stream. The query fields are
     * expected to be byte[] at this point.
     */
    public void serialize(DataOutputStream str) throws IOException {
        str.writeUTF(field);
        byte[] start = (byte[]) getStart();
        byte[] end = (byte[]) getEnd();
        str.writeInt(start.length);
        str.write(start);
        str.writeInt(end.length);
        str.write(end);
    }

    /** Deserializes a query from the given input stream */
    public static RangeQuery deserialize(DataInputStream inputStream) throws IOException {
        String fieldName = inputStream.readUTF();

        int startSize = inputStream.readInt();
        byte[] start = new byte[startSize];
        inputStream.readFully(start);

        int endSize = inputStream.readInt();
        byte[] end = new byte[endSize];
        inputStream.readFully(end);

        return new RangeQuery(fieldName, start, end);
    }

    public String getField() {
        return field;
    }

    public Object getStart() {
        return start;
    }

    public Object getEnd() {
        return end;
    }

}

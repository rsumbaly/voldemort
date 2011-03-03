package voldemort.secondary;

import java.util.List;
import java.util.Map;

/**
 * Class responsible for extracting and serializing secondary field values
 */
public interface SecondaryIndexProcessor {

    /** Based on the main value, extract all secondary field values */
    Map<String, byte[]> extractSecondaryValues(byte[] value);

    /** Serialize the given secondary field value */
    byte[] serializeValue(String fieldName, Object value);

    /** @return list of secondary field names */
    List<String> getSecondaryFields();

}
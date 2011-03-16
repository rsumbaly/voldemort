package voldemort.secondary;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import voldemort.serialization.Serializer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Default SecondaryIndexProcessor implementation, that uses
 * {@link SecondaryIndexValueExtractor} objects to retrieve secondary values and
 * Serializer objects to convert them into byte arrays.
 */
public class DefaultSecondaryIndexProcessor implements SecondaryIndexProcessor {

    private final Serializer<?> valueSerializer;
    private final Map<String, Serializer<?>> secSerializersByField;
    private final Map<String, SecondaryIndexValueExtractor<?, ?>> secIdxExtractors;

    /**
     * New secondary index processor.
     * 
     * @param valueSerializer how to transform primary byte arrays from/to an
     *        object
     * @param secIdxSerializers secondary field serializers (by secondary field
     *        name).
     * 
     * @param secIdxExtractors secondary field value extractors (by secondary
     *        field name).
     */
    public DefaultSecondaryIndexProcessor(Serializer<?> valueSerializer,
                                          Map<String, Serializer<?>> secIdxSerializers,
                                          Map<String, SecondaryIndexValueExtractor<?, ?>> secIdxExtractors) {
        this.valueSerializer = valueSerializer;
        this.secSerializersByField = secIdxSerializers;
        this.secIdxExtractors = secIdxExtractors;
    }

    public Map<String, byte[]> extractSecondaryValues(byte[] value) {
        Object obj = valueSerializer.toObject(value);

        Map<String, byte[]> result = Maps.newHashMap();
        for(Entry<String, Serializer<?>> entry: secSerializersByField.entrySet()) {
            String fieldName = entry.getKey();
            Object secObj = getExtractor(fieldName).extractValue(obj);
            result.put(fieldName, serializeValue(fieldName, secObj));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private Serializer<Object> getSerializer(String fieldName) {
        return (Serializer<Object>) secSerializersByField.get(fieldName);
    }

    @SuppressWarnings("unchecked")
    private SecondaryIndexValueExtractor<Object, Object> getExtractor(String fieldName) {
        return (SecondaryIndexValueExtractor<Object, Object>) secIdxExtractors.get(fieldName);
    }

    public byte[] serializeValue(String fieldName, Object value) {
        Serializer<Object> serializer = getSerializer(fieldName);
        if(serializer == null)
            throw new IllegalArgumentException("Secondary index field not found: " + fieldName);
        try {
            return serializer.toBytes(value);
        } catch(Exception ex) {
            throw new IllegalArgumentException("Could not interpret value " + value + " for field "
                                               + fieldName, ex);
        }
    }

    public List<String> getSecondaryFields() {
        return Lists.newArrayList(secSerializersByField.keySet());
    }

}
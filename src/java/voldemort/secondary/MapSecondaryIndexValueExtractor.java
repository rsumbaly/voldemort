package voldemort.secondary;

import java.util.Map;

/**
 * SecondaryIndexValueExtractor implementation that simply retrieves a given
 * field form a Map
 */
public class MapSecondaryIndexValueExtractor implements
        SecondaryIndexValueExtractor<Map<String, Object>, Object> {

    private final String fieldName;

    /** New map extractor for the given field name */
    public MapSecondaryIndexValueExtractor(String fieldName) {
        this.fieldName = fieldName;
    }

    public Object extractValue(Map<String, Object> map) {
        return map.get(fieldName);
    }

}
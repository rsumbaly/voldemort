package voldemort.secondary;

/**
 * Classes that extract secondary values from primary values
 * 
 * @param <V> Original value type
 * @param <E> Extracted value type
 */
public interface SecondaryIndexValueExtractor<V, E> {

    /** Extracts secondary value from given primary value */
    E extractValue(V obj);
}
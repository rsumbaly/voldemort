package voldemort.secondary;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Defines the required information for one secondary index field.
 * <p>
 * Example:
 * 
 * <pre>
 * {@code
 * <secondary-index>
 *     <name>status</name>
 *     <extractor-type>map</extractor-type>
 *     <extractor-info>status</extractor-info>
 *     <serializer-type>json</serializer-type>
 *     <schema-info>"int8"</schema-info>
 * </secondary-index>
 * }
 * </pre>
 */
public class SecondaryIndexDefinition {

    private final String name;
    private final String extractorType;
    private final String extractorInfo;
    private final String serializerType;
    private final String schemaInfo;

    public SecondaryIndexDefinition(String name,
                                    String extractorType,
                                    String extractorInfo,
                                    String serializerType,
                                    String schemaInfo) {
        this.name = name;
        this.extractorType = extractorType;
        this.extractorInfo = extractorInfo;
        this.serializerType = serializerType;
        this.schemaInfo = schemaInfo;
    }

    /**
     * Identifier for this secondary index field (i.e., how it will be
     * referenced by queries)
     */
    public String getName() {
        return name;
    }

    /**
     * Type of method used to extract information from the main value. e.g.
     * "map" for Map objects
     */
    public String getExtractorType() {
        return extractorType;
    }

    /**
     * Additional information about how to extract the secondary index field
     * information from the main value. e.g. for "map" extractor type, this
     * should contain the map key.
     */
    public String getExtractorInfo() {
        return extractorInfo;
    }

    /** How to serialize the extracted value */
    public String getSerializerType() {
        return serializerType;
    }

    /** Complementary information for {@link #getSerializerType()} */
    public String getSchemaInfo() {
        return schemaInfo;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(name = " + this.name + ", extractor-type = "
               + extractorType + ", extractor-info = " + extractorInfo + ", serializer-type = "
               + serializerType + ", schema-info = " + schemaInfo + ")";
    }

    @Override
    public boolean equals(Object o) {
        if(this == o)
            return true;
        else if(o == null)
            return false;
        else if(!(o.getClass() == SecondaryIndexDefinition.class))
            return false;

        SecondaryIndexDefinition def = (SecondaryIndexDefinition) o;
        return new EqualsBuilder().append(getName(), def.getName())
                                  .append(getExtractorType(), def.getExtractorType())
                                  .append(getExtractorInfo(), def.getExtractorInfo())
                                  .append(getSerializerType(), def.getSerializerType())
                                  .append(getSchemaInfo(), def.getSchemaInfo())
                                  .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(11, 41).append(name)
                                          .append(extractorType)
                                          .append(extractorInfo)
                                          .append(serializerType)
                                          .append(schemaInfo)
                                          .toHashCode();
    }

}

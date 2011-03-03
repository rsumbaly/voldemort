package voldemort.secondary;

import java.util.Collection;
import java.util.Map;

import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;

import com.google.common.collect.Maps;

/** Default factory for {@link SecondaryIndexProcessor} objects */
public class SecondaryIndexProcessorFactory {

    /**
     * Creates a new {@link SecondaryIndexProcessor} based on the seconday index
     * definitions and value serializer definition.
     * Actual serializers are created with the given serializerFactory.
     */
    public static SecondaryIndexProcessor getProcessor(SerializerFactory serializerFactory,
                                                       Collection<SecondaryIndexDefinition> secondaryIndexDefs,
                                                       SerializerDefinition valueSerializerDef) {
        Map<String, Serializer<?>> secIdxSerializers = Maps.newHashMap();
        Map<String, SecondaryIndexValueExtractor<?, ?>> secIdxExtractors = Maps.newHashMap();

        for(SecondaryIndexDefinition secDef: secondaryIndexDefs) {
            SerializerDefinition def = new SerializerDefinition(secDef.getSerializerType(),
                                                                secDef.getSchemaInfo());
            secIdxSerializers.put(secDef.getName(), serializerFactory.getSerializer(def));
            secIdxExtractors.put(secDef.getName(), getExtractor(secDef));
        }

        return new DefaultSecondaryIndexProcessor(serializerFactory.getSerializer(valueSerializerDef),
                                                  secIdxSerializers,
                                                  secIdxExtractors);
    }

    private static SecondaryIndexValueExtractor<?, ?> getExtractor(SecondaryIndexDefinition secDef) {
        String type = secDef.getExtractorType();
        if("map".equals(type)) {
            return new MapSecondaryIndexValueExtractor(secDef.getExtractorInfo());
        } else {
            throw new IllegalArgumentException("No known secondary index value extractor type: "
                                               + type);
        }
    }

}

package voldemort.serialization;

import voldemort.store.compress.CompressingStore;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;

/**
 * SerializerFactory implementation that wraps the result of the target factory
 * with a {@link CompressingSerializer}.
 * TODO We should consider using this in conjunction with
 * {@link DefaultSerializerFactory}, and remove the {@link CompressingStore}.
 * In the meantime, we are using it when needed on the server side (Views and
 * SecondaryIndex processing).
 */
public class CompressingSerializerFactory implements SerializerFactory {

    private final SerializerFactory target;

    public CompressingSerializerFactory(SerializerFactory target) {
        this.target = target;
    }

    public Serializer<?> getSerializer(SerializerDefinition serializerDef) {
        Serializer<?> serializer = target.getSerializer(serializerDef);
        if(serializerDef.hasCompression()) {
            CompressionStrategy compressionStrategy = new CompressionStrategyFactory().get(serializerDef.getCompression());
            serializer = new CompressingSerializer(compressionStrategy, serializer);
        }
        return serializer;
    }

}

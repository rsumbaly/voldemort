package voldemort.serialization;

import java.io.IOException;

import voldemort.store.compress.CompressionStrategy;

public class CompressingSerializer<T> implements Serializer<T> {

    private final CompressionStrategy strategy;
    private final Serializer<T> target;

    public CompressingSerializer(CompressionStrategy strategy, Serializer<T> target) {
        this.strategy = strategy;
        this.target = target;
    }

    public byte[] toBytes(T object) {
        try {
            return strategy.deflate(target.toBytes(object));
        } catch(IOException e) {
            throw new SerializationException(e);
        }
    }

    public T toObject(byte[] bytes) {
        try {
            return target.toObject(strategy.inflate(bytes));
        } catch(IOException e) {
            throw new SerializationException(e);
        }
    }

}

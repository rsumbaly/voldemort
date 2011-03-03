package voldemort.secondary;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;

/**
 * When applied to a {@link StorageConfiguration}, the class indicates that it
 * supports secondary index feature, and the constructor should have
 * an extra argument of the type List<StoreDefinition>.
 * <p>
 * This allows the configuration class to have knowledge of the internal of the
 * store (e.g. serializers) and create the actual {@link StorageEngine}
 * accordingly.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface SecondaryIndexSupported {

}

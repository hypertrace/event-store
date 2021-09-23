package org.hypertrace.core.eventstore;

import com.typesafe.config.Config;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.hypertrace.core.eventstore.kafka.KafkaEventStore;

public class EventStoreProvider {

  static Map<String, Class<? extends EventStore>> registry =
      new ConcurrentHashMap<String, Class<? extends EventStore>>();

  static {
    EventStoreProvider.register("Kafka", KafkaEventStore.class);
  }

  /**
   * Creates a DocDatastore, currently it creates a new client/connection on every invocation. We
   * might add pooling later
   *
   * @param type
   * @param params
   * @return
   */
  public static EventStore getEventStore(String type, Config config) {

    Class<? extends EventStore> clazz = registry.get(type.toLowerCase());
    try {
      Constructor<? extends EventStore> constructor = clazz.getConstructor(new Class[] {});
      EventStore instance = constructor.newInstance(new Object[] {});
      instance.init(new EventStoreConfig(config));
      return instance;
    } catch (Exception e) {
      throw new IllegalArgumentException("Exception creating EventStore for type:" + type, e);
    }
  }

  /**
   * Register various possible implementations. Expects a constructor with no-args and an init
   * method that takes in ParamsMap
   *
   * @param type
   * @param clazz
   * @return
   */
  public static boolean register(String type, Class<? extends EventStore> clazz) {
    registry.put(type.toLowerCase(), clazz);
    return true;
  }
}

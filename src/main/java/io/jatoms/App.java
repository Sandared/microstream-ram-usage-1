package io.jatoms;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.eclipse.collections.impl.map.mutable.primitive.AbstractSentinelValues;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import one.microstream.afs.nio.types.NioFileSystem;
import one.microstream.persistence.types.PersistenceEagerStoringFieldEvaluator;
import one.microstream.persistence.types.PersistenceFieldEvaluator;
import one.microstream.reference.Lazy;
import one.microstream.reflect.XReflect;
import one.microstream.storage.embedded.types.EmbeddedStorage;
import one.microstream.storage.embedded.types.EmbeddedStorageManager;
import one.microstream.storage.types.Storage;

/**
 * Minimal example with Lazy References
 * Root -> Map<String, Data>
 * Data -> String
 * String of data will be altered every two seconds
 */
public class App {
    public static void main(String[] args) {
        NioFileSystem fileSystem = NioFileSystem.New();
        final EmbeddedStorageManager storageManager =
        EmbeddedStorage.Foundation(
                Storage.ConfigurationBuilder()
                    // configure the storage Path for Microstream
                    .setStorageFileProvider(
                        Storage.FileProviderBuilder(fileSystem)
                            .setDirectory(
                                fileSystem.ensureDirectory(storagePath("storage")))
                            .createFileProvider())
                    .setChannelCountProvider(Storage.ChannelCountProvider(8))
                    .setEntityCacheEvaluator(Storage.EntityCacheEvaluator(3_600_000, 500_000_000))
                )
            .onConnectionFoundation(
                f -> {
                  f.setFieldEvaluatorPersistable(new CustomPersistenceFieldEvaluator());
                  f.setReferenceFieldEagerEvaluator(new CustomEagerStoringFieldEvaluator());
                })
            .createEmbeddedStorageManager()
            .start();


        NodeList_LOHM root = (NodeList_LOHM)storageManager.root();
        if(root == null) {
            System.out.println("Found no existing root!");
            // Create a root instance
            root = new NodeList_LOHM(100_000);
            storageManager.setRoot(root);
            storageManager.storeRoot();
            System.out.println("Stored root with " + root.nodes.size() + " buckets!");
        } else {
            System.out.println("Found existing root with " + root.nodes.size() + " buckets!");
            System.out.println("Accessing all Lazy References");

            NodeList_LOHM finalRoot = root;
            // try to access all lazy buckets and measure size
            root.nodes.keySet().forEach(key -> {
                long startTime = System.currentTimeMillis();
                LongObjectHashMap<Node> value = Lazy.get(finalRoot.nodes.get(key));
                long endTime = System.currentTimeMillis();
                long lazyGetTime = (endTime - startTime) / 1000;
                System.out.println("Lazy Loading took " + lazyGetTime + " s");
            });

        }
        
        System.out.println("Successfully shut down microstream: " + storageManager.shutdown());
        System.exit(0);
    }

    private static final Random rand = new Random();

    public static class NodeList_LOHM {
        LongObjectHashMap<Lazy<LongObjectHashMap<Node>>> nodes = new LongObjectHashMap<>();
        public NodeList_LOHM(int size) {
            for (int i = 0; i < size; i++) {
                Node node = new Node(12);
                Lazy<LongObjectHashMap<Node>> bucket = nodes.getIfAbsentPut(node.uid % 50, Lazy.Reference(new LongObjectHashMap<Node>()));
                bucket.get().put(node.uid, node);
            }
        }
    }

    public static class NodeList_HM {
        Map<Long, Lazy<Map<Long, Node>>> nodes = new HashMap<>();
        public NodeList_HM(int size) {
            for (int i = 0; i < size; i++) {
                Node node = new Node(12);
                Lazy<Map<Long, Node>> bucket = nodes.computeIfAbsent(node.uid % 50, uid -> Lazy.Reference(new HashMap<>()));
                bucket.get().put(node.uid, node);
            }
        }
    }

    public static class Node {
        long uid;
        Map<String, Property> properties = new HashMap<>();
        public Node(int nrProps) {
            uid = rand.nextLong();
            for (int i = 0; i < nrProps; i++) {
                String rand = UUID.randomUUID().toString();
                properties.put(rand, new Property(rand, rand + rand + rand));
            }
        }
    }

    public static class Property {
        String name;
        Object value;

        public Property(String name, Object value) {
            this.name = name;
            this.value = value;
        }
    }

    private static class CustomEagerStoringFieldEvaluator
      implements PersistenceEagerStoringFieldEvaluator {
    @Override
    public boolean isEagerStoring(Class<?> t, Field u) {
      // return true if field should be persisted at every store
      if (t == LongObjectHashMap.class) return true;
      if (AbstractSentinelValues.class.isAssignableFrom(t)) return true;
      // this one is for the items field of longarraylists
      if (t == org.eclipse.collections.impl.list.mutable.primitive.LongArrayList.class
          && u.getName().equals("items")) return true;
      // The time part of LocalDateTime seems to make problems, however we just store
      // both always
      return t == LocalDateTime.class && (u.getName().equals("date") || u.getName().equals("time"));
    }
  }

  public static class CustomPersistenceFieldEvaluator implements PersistenceFieldEvaluator {
    @Override
    public boolean applies(final Class<?> entityType, final Field field) {
      // return true if field should be persisted, false if not
      if (entityType == org.eclipse.collections.impl.list.mutable.primitive.LongArrayList.class
          && field.getName().equals("items")) {
        return true;
      }
      // default: return false if field has the transient modifier
      return !XReflect.isTransient(field);
    }
  }

  public static Path storagePath(String subFolder) {
    if (subFolder == null || subFolder.isBlank()) {
      throw new RuntimeException("subFolder for storage  may not be null or blank!");
    }
    String rootDir = System.getenv("QB_STORAGE_ROOT");
    if (rootDir == null || rootDir.isBlank()) {
      rootDir = subFolder;
    } else {
      if (rootDir.endsWith("/")) {
        rootDir = rootDir + subFolder;
      } else {
        rootDir = rootDir + "/" + subFolder;
      }
    }
    return Paths.get(rootDir).toAbsolutePath();
  }


}

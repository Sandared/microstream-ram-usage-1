package io.jatoms;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import one.microstream.reference.Lazy;
import one.microstream.storage.embedded.types.EmbeddedStorage;
import one.microstream.storage.embedded.types.EmbeddedStorageManager;
import one.microstream.storage.types.Storage;
import one.microstream.storage.types.StorageEntityCache;

/**
 * Minimal example with Lazy References
 * Root -> Map<String, Data>
 * Data -> String
 * String of data will be altered every two seconds
 */
public class App {
    public static void main(String[] args) {
        final EmbeddedStorageManager storageManager = EmbeddedStorage.Foundation(
            Storage.ConfigurationBuilder()
                .setChannelCountProvider(Storage.ChannelCountProvider(8))
                .setEntityCacheEvaluator(Storage.EntityCacheEvaluator(1000, 10))
                .setHousekeepingController(Storage.HousekeepingController(100, 1000_000_000))
                .createConfiguration()
            )
            .start();
        StorageEntityCache.Default.setGarbageCollectionEnabled(true);

        NodeListElement root = (NodeListElement)storageManager.root();
        if(root == null) {
            System.out.println("Found no existing root!");
            // Create a root instance
            root = createInitial();
            storageManager.setRoot(root);
            storageManager.storeRoot();
        } else {
            System.out.println("Found existing root with " + root.state().nodes().count() + " nodes!");
        }

        NodeListElement finalRoot = root;
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(2);

        // Create two threads 
        // One that updates x percent of the nodes from the NodeList by switching its state every 2 seconds
        exec.scheduleAtFixedRate(() -> {
            System.out.println("Start update");
            // update data every 2 seconds
            List<NodeElement> updatedElements = finalRoot.update(0.3);
            // and store it
            storageManager.store(updatedElements);
            System.out.println("Stored " + updatedElements.size() + " updates");
        }, 0, 2, TimeUnit.SECONDS);

        // One that randomly reads x percent of the nodes once every 1 second
        exec.scheduleAtFixedRate(() -> {
            List<NodeState> readNodes = finalRoot.read(0.3);
            System.out.println("Read " + readNodes.size() + " states");
            finalRoot.create(5_000);
            storageManager.store(finalRoot);
            finalRoot.remove(2_000);
            storageManager.store(finalRoot);
        }, 0, 1, TimeUnit.SECONDS);
        
        // wait for the user to abort this process
        try(Scanner scanner = new Scanner(System.in)) {
            System.out.println("Hit Enter to Exit");
            scanner.nextLine(); 
            System.out.println("Exiting"); 
            storageManager.shutdown();
            System.exit(0);
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }

    public static NodeListElement createInitial() {
        NodeListElement nlelem = new NodeListElement();
        NodeListState nlstate = new NodeListState(createInitialNodes(100_000));
        nlelem.state(nlstate);
        return nlelem;
    }

    public static  Map<String, Lazy<NodeElement>> createInitialNodes(int nrOfNodes) {
        Map<String, Lazy<NodeElement>> nodes = new HashMap<>();
        for (int i = 0; i < nrOfNodes; i++) {
            String key = UUID.randomUUID().toString();
            NodeElement nelem = createNodeElement(key);
            Lazy<NodeElement> lazy = Lazy.Reference(nelem);
            nodes.put(key, lazy);
        }
        return nodes;
    }

    private static NodeElement createNodeElement(String key) {
        NodeElement nelem = new NodeElement();
        NodeState nstate = new NodeState(key, nelem);
        nelem.state(nstate);
        return nelem;
    }

    private static class NodeListElement {
        private volatile NodeListState state;
        public NodeListState state() {
            return state;
        }

        public void state(NodeListState state) {
            this.state = state;
        }

        // Helper methods for this example
        private Random rand = new Random();
        public List<NodeElement> update(double percent) {
            List<NodeElement> elementsToUpdate = this.state.nodes()
            .filter(node -> {
                double random = rand.nextDouble();
                return percent < random;
            })
            // actually load them if necessary
            .map(elem -> Lazy.get(elem).state())
            .map(node -> node.element()).collect(Collectors.toList());

            elementsToUpdate.forEach(elem -> {
                elem.update();
            });

            return elementsToUpdate;
        }

        public List<NodeState> read(double percent) {
            return this.state.nodes()
                .filter(node -> {
                    double random = rand.nextDouble();
                    return percent < random;
                })
                // actually load them if necessary
                .map(elem -> Lazy.get(elem).state())
                .collect(Collectors.toList());
        }

        public void create(int nrOfElements) {
            NodeListState newState = new NodeListState(new HashMap<>(this.state.nodes));
            for (int i = 0; i < nrOfElements; i++) {
                String key = UUID.randomUUID().toString();
                NodeElement nelem = createNodeElement(key);
                Lazy<NodeElement> lazy = Lazy.Reference(nelem);
                newState.nodes.put(key, lazy);
            }
            this.state = newState;
        }

        public void remove(int nrOfElements) {
            NodeListState newState = new NodeListState(new HashMap<>(this.state.nodes));
            for (int i = 0; i < nrOfElements; i++) {
                newState.nodes.remove(newState.nodes.keySet().iterator().next());
            }
            this.state = newState;
        }
    }

    private static class NodeListState {
        private final Map<String, Lazy<NodeElement>> nodes;

        public NodeListState(Map<String, Lazy<NodeElement>> nodes) {
            this.nodes = nodes;
        }

        public Stream<Lazy<NodeElement>> nodes() {
            return nodes.values().stream();
        }
    }

    private static class NodeElement {
        private volatile NodeState state;

        public NodeState state() {
            return state;
        }

        public void state(NodeState state) {
            this.state = state;
        }

        public void update() {
            NodeState newState = new NodeState(UUID.randomUUID().toString(), this);
            this.state = newState;
        }
    }

    private static class NodeState {
        private final String name;
        private final NodeElement element;

        public NodeState(String name, NodeElement element) {
            this.name = name;
            this.element = element;
        }

        public String name() {
            return name;
        }

        public NodeElement element() {
            return element;
        }
    }
}

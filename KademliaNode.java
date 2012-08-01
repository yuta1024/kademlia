import java.rmi.*;
import java.rmi.server.*;
import java.util.*;

public class KademliaNode extends UnicastRemoteObject implements KademliaNodeInterface {
    private final boolean DEBUG = true;

    private final int BIT = 60;
    private final long MASK = (1 << (BIT+1))-1;
    private final int BUCKET_SIZE = 5;
    //private final int ALPHA = 3;

    private long id;
    private List<KademliaNode>[] buckets;
    private Map<Long, KeyValue> data;

    public KademliaNode() throws RemoteException {
        id = new Random().nextLong() & MASK;

        if (DEBUG)
            System.out.println("id: " + id);

        buckets = new LinkedList[BIT];
        for (int i = 0; i < BIT; ++i)
            buckets[i] = new LinkedList<KademliaNode>();
        data = new HashMap<Long, KeyValue>();
    }

    public void join(KademliaNode introducer) throws RemoteException {
        if (DEBUG)
            System.out.println(id + ".join(" + introducer.getId() + ")");

        buckets[getBucketIndex(introducer.getId())].add(introducer);
        lookup(id);
    }

    public LinkedList<KademliaNode> lookup(long key) throws RemoteException {
        TreeSet<KademliaNode> closest = new TreeSet<KademliaNode>(new ClosestNode(key));
        for (int i = 0; i < BIT; ++i)
            closest.addAll(buckets[i]);

        boolean update = true;
        HashSet<KademliaNode> visited = new HashSet<KademliaNode>();
        while (update) {
            update = false;
            Iterator<KademliaNode> it = closest.iterator();
            for (int i = 0; i < BUCKET_SIZE && it.hasNext(); ++i) {
                KademliaNode node = it.next();
                if (visited.contains(node) || node.equals(this))
                    continue;
                closest.addAll(node.findNode(this, key));
                visited.add(node);
                update = true;
                break;
            }
        }

        synchronized (buckets) {
            for (Iterator<KademliaNode> it = closest.iterator(); it.hasNext(); ) {
                KademliaNode node = it.next();
                if (node.equals(this))
                    continue;
                
                int index = getBucketIndex(node.getId());
                if (buckets[index].size() < BUCKET_SIZE && !buckets[index].contains(node))
                    buckets[index].add(node);
            }
        }

        LinkedList<KademliaNode> ret = new LinkedList<KademliaNode>();
        Iterator<KademliaNode> it = closest.iterator();
        while (it.hasNext() && ret.size() < BUCKET_SIZE) {
            KademliaNode node = it.next();
            ret.add(node);
        }
        return ret;
    }

    public void put(Object data) throws RemoteException {
        long key = new Random().nextLong() & MASK;
        KeyValue kv = new KeyValue(key, data);
        if (DEBUG)
            System.out.println("put: " + kv.id + ", " + kv.data);
        LinkedList<KademliaNode> target = lookup(key);
        for (Iterator<KademliaNode> it = target.iterator(); it.hasNext(); ) {
            KademliaNode node = it.next();
            if (DEBUG)
                System.out.println(node.getId() + ".put(" + kv.id + ", " + kv.data +")");
            node.store(this, kv);
        }
    }

    public void get(long key) throws RemoteException {
        LinkedList<KademliaNode> target = lookup(key);
        for (Iterator<KademliaNode> it = target.iterator(); it.hasNext(); ) {
            KademliaNode node = it.next();
            KeyValue kv = node.findValue(this, key);
            if (DEBUG) {
                if (kv != null)
                    System.out.println(node.getId() + ".get(" + kv.id + ") -> " + kv.data);
                else
                  System.out.println(node.getId() + ".get(" + key + ") -> null");  
            }
        }
    }

    public LinkedList<KademliaNode> findNode(KademliaNode sender, long key) throws RemoteException {
        if (DEBUG)
            System.out.println(id + ".findNode("+sender.getId() + "," + key +")");
        updateBucket(sender);

        TreeSet<KademliaNode> closest = new TreeSet<KademliaNode>(new ClosestNode(key));
        for (int i = 0; i < BIT; ++i)
            closest.addAll(buckets[i]);

        LinkedList<KademliaNode> ret = new LinkedList<KademliaNode>();
        Iterator<KademliaNode> it = closest.iterator();
        while (it.hasNext() && ret.size() < BUCKET_SIZE) {
            KademliaNode node = it.next();
            if (node.equals(sender))
                continue;
            ret.add(node);
        }
        return ret;
    }

    public void store(KademliaNode sender, KeyValue kv) throws RemoteException {
        Long key = kv.id;
        data.put(key, kv);
    }

    public KeyValue findValue(KademliaNode sender, long key) throws RemoteException {
        Long id = key;
        return data.get(id);
    }

    private void updateBucket(KademliaNode sender) {
        int index = getBucketIndex(sender.getId());
        synchronized (buckets) {
            if (buckets[index].contains(sender)) {
                // move last
                buckets[index].remove(sender);
                buckets[index].add(sender);
            } else {
                if (buckets[index].size() < BUCKET_SIZE) {
                    buckets[index].add(sender);
                } else {
                    // ping
                    if (buckets[index].get(0) == null) {
                        buckets[index].remove(0);
                        buckets[index].add(sender);
                    } else {
                        // move last
                        KademliaNode tmp = buckets[index].get(0);
                        buckets[index].remove(0);
                        buckets[index].add(tmp);
                    }
                }
            }
        }
    }

    public long getId() {
        return id;
    }

    private long getDistance(long key) {
        return id ^ key;
    }

    private int getBucketIndex(long key) {
        return (int)(Math.log(getDistance(key)) / Math.log(2.0));
    }

    public class ClosestNode implements Comparator {
        private long baseId;
        ClosestNode(long id) {
            baseId = id;
        }
        
        public int compare(Object o1, Object o2) {
            long id1 = ((KademliaNode)o1).getDistance(baseId);
            long id2 = ((KademliaNode)o2).getDistance(baseId);
            if (id1 < id2)
                return -1;
            else if (id1 == id2)
                return 0;
            else
                return 1;
        }
    }

    public void printInfo() {
        System.out.println("id: " + id);
        for (int i = 0; i < BIT; ++i) {
            if (buckets[i].size() > 0) {
                System.out.printf("%2d [%d", i, buckets[i].get(0).getId());
                for (int j = 1; j < buckets[i].size(); ++j)
                    System.out.printf(", %d", buckets[i].get(j).getId());
                System.out.println("]");
            }
        }
        System.out.println("");
    }
}



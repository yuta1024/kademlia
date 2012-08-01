import java.rmi.*;
import java.util.*;

public interface KademliaNodeInterface extends Remote
{
    public List<KademliaNode> findNode(KademliaNode sender, long key) throws RemoteException;
    public void store(KademliaNode sender, KeyValue kv) throws RemoteException;
    public KeyValue findValue(KademliaNode sender, long key) throws RemoteException;
}

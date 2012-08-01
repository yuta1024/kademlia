import java.rmi.*;
import java.rmi.server.*;
import java.util.ArrayList;
import java.util.Scanner;

public class Kademlia
{
    public static void main(String[] args)
    {
        final Scanner sc = new Scanner(System.in);
        ArrayList<KademliaNode> nodes = new ArrayList<KademliaNode>();

        // first node
        try {
            nodes.add(new KademliaNode());
        } catch (RemoteException e) {
            System.err.println(e);
            System.exit(1);
        }

        for ( ; ; ) {
            String cmd = sc.next();
            if (cmd.equals("join")) {
                int num = sc.nextInt();
                try {
                    for (int i = 0; i < num; ++i) {
                        KademliaNode node = new KademliaNode();
                        node.join(nodes.get(((int)(Math.random() * nodes.size()))));
                        nodes.add(node);
                    }
                } catch (RemoteException e) {
                    System.err.println(e);
                    System.exit(1);
                }
            } else if (cmd.equals("put")) {
                String data = sc.next();
                try {
                    nodes.get((int)(Math.random() * nodes.size())).put(data);
                } catch (RemoteException e) {
                    System.err.println(e);
                    System.exit(1);
                }
            } else if (cmd.equals("get")) {
                long key = sc.nextLong();
                try {
                    nodes.get((int)(Math.random() * nodes.size())).get(key);
                } catch (RemoteException e) {
                    System.err.println(e);
                    System.exit(1);
                }
            } else if (cmd.equals("info")) {
                for (int i = 0; i < nodes.size(); ++i)
                    nodes.get(i).printInfo();
            }
        }
    }
}

1. javac *.java
2. rmic KademliaNode
3. rmiregistry &
4. java -Djava.security.policy=java.policy Kademlia

command
join [number of node]
put [data]
get [key]
info

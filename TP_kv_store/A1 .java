
import oracle.kv.*;
import oracle.kv.stats.*;

/**
 * TME avec KVStore : A1 
 */
public class A1{

    private final KVStore store;

    /**
     * Runs A1
     */
    public static void main(String args[]) {
        try {
            A1 a = new A1(args);
            a.go();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Parses command line args and opens the KVStore.
     */
    private A1(String[] argv) {

        String storeName = "kvstore";
        String hostName = "localhost";
        String hostPort = "5000";

        final int nArgs = argv.length;
        int argc = 0;

        while (argc < nArgs) {
            final String thisArg = argv[argc++];

            if (thisArg.equals("-store")) {
                if (argc < nArgs) {
                    storeName = argv[argc++];
                } else {
                    usage("-store requires an argument");
                }
            } else if (thisArg.equals("-host")) {
                if (argc < nArgs) {
                    hostName = argv[argc++];
                } else {
                    usage("-host requires an argument");
                }
            } else if (thisArg.equals("-port")) {
                if (argc < nArgs) {
                    hostPort = argv[argc++];
                } else {
                    usage("-port requires an argument");
                }
            } else {
                usage("Unknown argument: " + thisArg);
            }
        }

        store = KVStoreFactory.getStore
            (new KVStoreConfig(storeName, hostName + ":" + hostPort));
    }

    private void usage(String message) {
        System.out.println("\n" + message + "\n");
        System.out.println("usage: " + getClass().getName());
        System.out.println("\t-store <instance name> (default: kvstore) " +
                           "-host <host name> (default: localhost) " +
                           "-port <port number> (default: 5000)");
        System.exit(1);
    }

    /**
     * Incrémentation
     */
    void go() throws Exception {
        System.out.println("Incrémentation P1...");

	String key = "produit1";
	
	Key k1 = Key.createKey(key);

	for(int j=0;j<4000;j++){
		Value v1 = store.get(k1).getValue();
		int i = Integer.parseInt(new String(v1.getValue()));
		System.out.println(i);
		i ++;
		String v2 = String.valueOf(i);
	
		store.put(k1, Value.createValue(v2.getBytes()));
	}
	

        System.out.println("Fin d'incrémentation");


        store.close();
    }
}

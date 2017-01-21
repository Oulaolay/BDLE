
import oracle.kv.*;
import oracle.kv.stats.*;

/**
 * TME avec KVStore : A2 
 */
public class A2{

    private final KVStore store;

    /**
     * Runs A2
     */
    public static void main(String args[]) {
        try {
            A2 a = new A2(args);
            a.go();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Parses command line args and opens the KVStore.
     */
    private A2(String[] argv) {

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
	
	int j = 0;
	while(j<4000){
		String cat = "C1";
		String prod = "P" + String.valueOf((i - 1)*20 + j);
		String key = cat + "_" + prod;
		ValueVersion v0 = store.get(k1);
		Value v1 = v0.getValue();
		int i = Integer.parseInt(new String(v1.getValue()));
		i ++;

		createPutIfVersion(Key key, Value value, Version version)

		String v2 = String.valueOf(i);
		Version v3 = store.putIfVersion(k1, Value.createValue(v2.getBytes()), v0.getVersion());
		if (v3 != null){
			System.out.println(i);
			j++;
		}
	}
	

        System.out.println("Fin d'incrémentation");


        store.close();
    }
}

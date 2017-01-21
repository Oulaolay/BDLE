
import oracle.kv.*;
import oracle.kv.stats.*;

/**
 * TME avec KVStore : InitExo2
 */
public class InitExo2{

    private final KVStore store;

    /**
     * Runs InitExo2
     */
    public static void main(String args[]) {
        try {
            InitExo2 a = new InitExo2(args);
            a.go();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Parses command line args and opens the KVStore.
     */
    private InitExo2(String[] argv) {

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
     * InitExo2ialisation
     */
    void go() throws Exception {
        System.out.println("InitExo2ialisation...");
	String value = "1";
	for(int i=1;i<=10;i++){
		for(int j=1;j<=20;j++){
			String cat = "C" + String.valueOf(i);
			String prod = "P" + String.valueOf((i - 1)*20 + j);
			String key = cat + "_" + prod;
			Key k1 = Key.createKey(key);
			    
			store.put(k1, Value.createValue(value.getBytes()));
		}	
	}
	System.out.println("Fin d'InitExo2ialisation");
        store.close();
    }
}

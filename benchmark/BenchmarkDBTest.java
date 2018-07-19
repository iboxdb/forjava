package benchmark;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import iBoxDB.LocalServer.*;
import iBoxDB.LocalServer.IO.*;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

//  iBoxDB.java v2.8.3
//  mongodb-linux-x86_64-ubuntu1604-4.0.0,  mongodb-driver-3.8.0.jar
public class BenchmarkDBTest {

    static int threadCount = 100000;
    static int batchCount = 10;

    public static boolean UseMMAP = false;
    public static boolean UseMem = false;

    public static void main(String[] args) {
        try {

            System.out.println("threadCount=" + threadCount + " , batchCount="
                    + batchCount);

            // -Xmx1024m , following needs more memory
            iBoxDB.LocalServer.DB.root("/tmp");
            UseMMAP = UseMem = false;
            System.out.println("iBoxDB(File Mode)");
            TestiBoxDB();
            System.out.println();

            System.gc();
            System.runFinalization();
            UseMMAP = UseMem = false;
            UseMMAP = true;
            System.out.println("iBoxDB(MemoryMappedFile Mode)");
            TestiBoxDB();
            System.out.println();

            System.gc();
            System.runFinalization();
            Thread.sleep(1000 * 15);

            UseMMAP = UseMem = false;
            UseMem = true;
            System.out.println("iBoxDB(InMemory Mode)");
            TestiBoxDB();

            System.out.println("MongoDB(Default)");
            TestMongoDB();

            System.out.println("Test End.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void TestiBoxDB() {
        BoxSystem.DBDebug.DeleteDBFiles(1);

        try (iBoxDBServer server = new iBoxDBServer()) {
            final Database db = server.getInstance();
            System.out.print("Database Transaction Test: ");
            Box box1 = db.cube();
            box1.d("T1").insert(T1.M(-1, Integer.toString(-1)));

            Box box2 = db.cube();
            box2.d("T1").insert(T1.M(-2, Integer.toString(-2)));

            List<Local> transaction1 = T1
                    .toArray(box1.select("from T1"));
            List<Local> transaction2 = T1
                    .toArray(box2.select("from T1"));
            if (transaction1.size() == 1
                    && transaction1.get(0).get("_id").equals(-1)
                    && transaction2.size() == 1
                    && transaction2.get(0).get("_id").equals(-2)) {
                System.out.println("Succeeded");
            } else {
                System.out.println("Failed");
            }
            box1.commit().Assert();
            box2.commit().Assert();

            long watch = System.currentTimeMillis();
            final AtomicInteger count = new AtomicInteger(0);
            ExecutorService pool = CreatePool();
            for (int i = 0; i < threadCount; i++) {
                final int p = i;
                pool.execute(new Runnable() {
                    @Override
                    public void run() {
                        {
                            Box box = db.cube();
                            for (int i = 0; i < batchCount; i++) {
                                int id = (p * batchCount) + i;
                                box.d("T1").insert(T1.M(id, Integer.toString(id)));
                            }
                            box.commit().Assert();
                        }
                        {
                            AutoBox box = db.get();
                            int minId = p * batchCount + 0;
                            int maxId = p * batchCount + batchCount;
                            Iterator<Local> reader = box
                                    .select("from T1 where _id>=? & _id<? order by _id",
                                            minId, maxId).iterator();
                            int ti = minId;
                            while (reader.hasNext()) {
                                Integer iv = (Integer) reader.next().get("_id");
                                if (ti != iv.intValue()) {
                                    System.out.println("e");
                                    throw new RuntimeException(ti + "  " + iv);
                                }
                                ti++;
                                count.incrementAndGet();
                            }
                            if (ti != maxId) {
                                System.out.println("e");
                                throw new RuntimeException();
                            }
                        }
                    }
                });
            }
            pool.shutdown();
            pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
            watch = System.currentTimeMillis() - watch;
            if (count.get() != (batchCount * threadCount)) {
                throw new RuntimeException(count + "  "
                        + (batchCount * threadCount));
            }
            int avg = (int) (count.get() / (watch / 1000.0));
            System.out.println("iBoxDB Insert&Counting:" + count.get()
                    + "  AVG:" + avg + " objects/s");

            // ----------------------Update------------------
            watch = System.currentTimeMillis();
            count.set(0);
            pool = CreatePool();
            for (int i = 0; i < threadCount; i++) {
                final int p = i;
                pool.execute(new Runnable() {
                    @Override
                    public void run() {
                        try (Box box = db.cube()) {
                            for (int i = 0; i < batchCount; i++) {
                                int id = (p * batchCount) + i;
                                Binder bd = box.bind("T1", id);
                                HashMap hm = bd.select(HashMap.class);
                                hm.put("s", hm.get("s") + new Date().toString());
                                bd.update(hm);
                                count.incrementAndGet();
                            }
                            box.commit().Assert();
                        }
                    }
                });
            }
            pool.shutdown();
            pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
            watch = System.currentTimeMillis() - watch;
            if (count.get() != (batchCount * threadCount)) {
                throw new RuntimeException(count + "  "
                        + (batchCount * threadCount));
            }
            avg = (int) (count.get() / (watch / 1000.0));
            System.out.println("iBoxDB Update:" + count.get() + "  AVG:" + avg
                    + " objects/s");

            // ------------------------Delete------------------
            watch = System.currentTimeMillis();
            count.set(0);
            pool = CreatePool();
            for (int i = 0; i < threadCount; i++) {
                final int p = i;
                pool.execute(new Runnable() {
                    @Override
                    public void run() {
                        try (Box box = db.cube()) {
                            for (int i = 0; i < batchCount; i++) {
                                int id = (p * batchCount) + i;
                                Binder bd = box.bind("T1", id);
                                bd.delete();
                                count.incrementAndGet();
                            }
                            box.commit().Assert();
                        }
                    }
                });
            }
            pool.shutdown();
            pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
            watch = System.currentTimeMillis() - watch;
            if (count.get() != (batchCount * threadCount)) {
                throw new RuntimeException(count + "  "
                        + (batchCount * threadCount));
            }
            avg = (int) (count.get() / (watch / 1000.0));
            System.out.println("iBoxDB Delete:" + count.get() + "  AVG:" + avg
                    + " objects/s");

            try (Box box = db.cube()) {
                // id = -1 ,-2 from transaction testing
                if (box.selectCount("from T1") != 2) {
                    throw new RuntimeException("SC");
                }
                for (Map<String, Object> m : box.select("from T1")) {
                    //System.out.println(m.get("_id"));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void TestMongoDB() throws Exception {
        MongoClient mongo = new MongoClient();

        mongo.dropDatabase("test");
        MongoDatabase db = mongo.getDatabase("test");

        //System.out.print("Database Transaction Test: ");
        final MongoCollection<Document> coll = db.getCollection("T1");

        long watch = System.currentTimeMillis();
        final AtomicInteger count = new AtomicInteger(0);
        ExecutorService pool = CreatePool();
        for (int i = 0; i < threadCount; i++) {
            final int p = i;
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    List<Document> list = new ArrayList<>(batchCount);
                    for (int i = 0; i < batchCount; i++) {
                        int id = (p * batchCount) + i;
                        list.add(T1.O(id, Integer.toString(id)));
                    }
                    coll.insertMany(list);

                    int minId = p * batchCount + 0;
                    int maxId = p * batchCount + batchCount;
                    Document bd = new Document();
                    bd.put("$gte", minId);
                    bd.put("$lt", maxId);
                    Document q = new Document();
                    q.put("_id", bd);

                    try (MongoCursor<Document> reader = coll.find(q).iterator()) {
                        int ti = minId;
                        while (reader.hasNext()) {
                            Integer iv = (Integer) reader.next().get("_id");
                            if (ti != iv.intValue()) {
                                System.out.println("e");
                                throw new RuntimeException(ti + "  " + iv);
                            }
                            ti++;
                            count.incrementAndGet();
                        }
                        if (ti != maxId) {
                            System.out.println("e");
                            throw new RuntimeException();
                        }
                    }

                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        watch = System.currentTimeMillis() - watch;
        if (count.get() != (batchCount * threadCount)) {
            throw new Exception(count + "  " + (batchCount * threadCount));
        }
        int avg = (int) (count.get() / (watch / 1000.0));
        System.out.println("MongoDB Insert&Counting:"
                + Integer.toString(count.get()) + "  AVG:"
                + Integer.toString(avg) + " objects/s");

        // ---------------Update-----------------------------
        watch = System.currentTimeMillis();
        count.set(0);
        pool = CreatePool();
        for (int i = 0; i < threadCount; i++) {
            final int p = i;
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < batchCount; i++) {
                        int id = (p * batchCount) + i;
                        count.incrementAndGet();
                        Document q = new Document();
                        q.put("_id", id);
                        coll.findOneAndUpdate(q, new Document());
                    }
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        watch = System.currentTimeMillis() - watch;
        if (count.get() != (batchCount * threadCount)) {
            throw new Exception(count + "  " + (batchCount * threadCount));
        }
        avg = (int) (count.get() / (watch / 1000.0));
        System.out.println("MongoDB findOneAndUpdate:" + Integer.toString(count.get())
                + "  AVG:" + Integer.toString(avg) + " objects/s");
        mongo.close();
    }

    private static ExecutorService CreatePool() {
        return Executors.newFixedThreadPool(8);
    }

    public static class T1 {

        //public int _id;
        public static org.bson.Document O(int id, String s) {
            Document o = new Document();
            o.append("_id", id);
            o.append("s", s);
            return o;
        }

        public static HashMap<String, Object> M(int id, String s) {
            HashMap<String, Object> o = new HashMap<String, Object>();
            o.put("_id", id);
            o.put("s", s);
            return o;
        }

        public static <T> List<T> toArray(Iterable<T> it) {
            ArrayList<T> list = new ArrayList<>();
            for (T t : it) {
                list.add(t);
            }
            return list;
        }
    }

    public static class iBoxDBServer extends LocalDatabaseServer {

        @Override
        protected DatabaseConfig BuildDatabaseConfig(long address) {
            if (UseMem) {
                return new InMemoryConfig();
            } else if (UseMMAP) {
                return new MMPConfig();
            } else {
                return new FileConfig();
            }
        }

        public static class FileConfig extends BoxFileStreamConfig {

            public FileConfig() {
                W.ensureTable("T1", T1.M(0, ""), "_id");
            }
        }

        public static class InMemoryConfig extends BoxMemoryStreamConfig {

            public InMemoryConfig() {
                W.ensureTable("T1", T1.M(0, ""), "_id");
                this.FileIncSize = 1024 * 1024 * 512;
                this.CacheLength = 1024 * 1024 * 512;
            }
        }

        public static class MMPConfig extends MMapConfig {

            public MMPConfig() {
                W.ensureTable("T1", T1.M(0, ""), "_id");
            }
        }
    }

}

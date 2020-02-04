package benchmark;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import iBoxDB.LocalServer.*;
import iBoxDB.LocalServer.IO.*;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
iBoxDB.java v2.15.0
 */
 /*
MySQL, mysql-8.0.19-linux-x86_64-minimal , mysql-connector-java-8.0.19.jar

sudo mv /etc/my.cnf /etc/my.cnf.bak
rm -rf /tmp/mysqldata

./mysqld --initialize --datadir=/tmp/mysqldata 
./mysqld --datadir=/tmp/mysqldata

./mysql -u root -p

ALTER USER 'root'@'localhost' IDENTIFIED BY '123456';
create database test;
 */
public class BenchmarkDBTestMySQL {

    static int threadCount = 100_000;
    static int batchCount = 10;
    static int reinterationSelect = 3;

    public static void main(String[] args) {
        try {

            System.out.println(System.getProperty("java.version"));
            System.out.format("threadCount= %,d batchCount= %,d reinterationSelect= %,d %n",
                    threadCount, batchCount, reinterationSelect);

            //never set root = "" or "./" when inside IDE
            //the IDE would block writing.
            String root = "../"; //"/tmp"
            iBoxDB.LocalServer.DB.root(root);
            System.out.println("iBoxDB");
            TestiBoxDB();
            System.out.println();

            System.gc();
            System.runFinalization();

            System.out.println("MySQL");
            TestMySQL();

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
            box1.d("T1").insert(new T1(-1, Integer.toString(-1)));

            Box box2 = db.cube();
            box2.d("T1").insert(new T1(-2, Integer.toString(-2)));

            List<T1> transaction1 = T1
                    .toArray(box1.select(T1.class, "from T1"));
            List<T1> transaction2 = T1
                    .toArray(box2.select(T1.class, "from T1"));
            if (transaction1.size() == 1
                    && transaction1.get(0).getId() == -1
                    && transaction2.size() == 1
                    && transaction2.get(0).getId() == -2) {
                System.out.println("Succeeded");
            } else {
                System.out.println("Failed");
            }
            box1.commit();
            box2.commit();

            long watch = System.currentTimeMillis();
            final AtomicInteger count = new AtomicInteger(0);
            ExecutorService pool = CreatePool();
            for (int i = 0; i < threadCount; i++) {
                final int p = i;
                pool.execute(new Runnable() {
                    @Override
                    public void run() {

                        try (Box box = db.cube()) {
                            for (int i = 0; i < batchCount; i++) {
                                int id = (p * batchCount) + i;
                                box.d("T1").insert(new T1(id, Integer.toString(id)));
                                count.incrementAndGet();
                            }
                            var cr = box.commit();
                        }

                        for (int r = 0; r < reinterationSelect; r++) {
                            AutoBox auto = db.get();
                            int minId = p * batchCount + 0;
                            int maxId = p * batchCount + batchCount;
                            var reader = auto
                                    .select(T1.class, "from T1 where Id>=? & Id<? order by Id",
                                            minId, maxId).iterator();
                            int ti = minId;
                            while (reader.hasNext()) {
                                var iv = reader.next().getId();
                                if (ti != iv) {
                                    System.out.println("e");
                                    throw new RuntimeException(ti + "  " + iv);
                                }
                                ti++;
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
            System.out.format("iBoxDB Insert: %,d AVG: %,d objects/s %n", count.get(), avg);

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
                                Binder bd = box.d("T1", id);
                                var hm = bd.select(T1.class);
                                hm.setValue("U" + hm.getValue());
                                bd.update(hm);
                                count.incrementAndGet();
                            }
                            var cr = box.commit();
                        }

                        {
                            AutoBox auto = db.get();
                            int minId = p * batchCount + 0;
                            int maxId = p * batchCount + batchCount;
                            var reader = auto
                                    .select(T1.class, "from T1 where Id>=? & Id<? order by Id",
                                            minId, maxId).iterator();
                            int ti = minId;
                            while (reader.hasNext()) {
                                T1 t1 = reader.next();
                                var iv = t1.getId();
                                if (ti != iv) {
                                    System.out.println("e");
                                    throw new RuntimeException(ti + "  " + iv);
                                }
                                if (!("U" + ti).equals(t1.getValue())) {
                                    System.out.println("eu");
                                    throw new RuntimeException(ti + "  " + t1.getValue());
                                }
                                ti++;
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
            avg = (int) (count.get() / (watch / 1000.0));
            System.out.format("iBoxDB Update: %,d AVG: %,d objects/s %n", count.get(), avg);

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
                                box.d("T1", id).delete();
                                count.incrementAndGet();
                            }
                            var cr = box.commit();
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
            System.out.format("iBoxDB Delete: %,d AVG: %,d objects/s %n", count.get(), avg);

            try (Box box = db.cube()) {
                // id = -1 ,-2 from transaction test
                if (box.selectCount("from T1") != 2) {
                    throw new RuntimeException("SC");
                }
                for (Map<String, Object> m : box.select("from T1")) {
                    //System.out.println(m.get("Id"));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void TestMySQL() {

        var connPool = CreateConnectionPool();
        try {
            {
                var conn = connPool.take();
                conn.setAutoCommit(true);
                var stmt = conn.createStatement();
                try {
                    stmt.execute("drop table T1");
                } catch (Exception ex) {
                    //Logger.getLogger(BenchmarkDBTestMySQL.class.getName()).log(Level.INFO, null, ex);
                }
                stmt.execute("CREATE TABLE T1 ( Id INT NOT NULL PRIMARY KEY, Value VARCHAR(100) )");
                connPool.put(conn);
            }

            {

                System.out.print("Database Transaction Test: ");

                var conn1 = connPool.take();
                conn1.setAutoCommit(false);
                var stmt1 = conn1.prepareStatement("insert into T1 values (? , ?)");
                stmt1.setInt(1, -1);
                stmt1.setString(2, Integer.toString(-1));
                stmt1.execute();

                var conn2 = connPool.take();
                conn2.setAutoCommit(false);
                var stmt2 = conn2.prepareStatement("insert into T1 values (? , ?)");
                stmt2.setInt(1, -2);
                stmt2.setString(2, Integer.toString(-2));
                stmt2.execute();

                stmt1 = conn1.prepareStatement("select * from T1");
                List< T1> transaction1 = T1
                        .toArray(stmt1.executeQuery());

                stmt2 = conn2.prepareStatement("select * from T1");
                List<T1> transaction2 = T1
                        .toArray(stmt2.executeQuery());
                if (transaction1.size() == 1
                        && transaction1.get(0).getId() == -1
                        && transaction2.size() == 1
                        && transaction2.get(0).getId() == -2) {
                    System.out.println("Succeeded");
                } else {
                    System.out.println("Failed");
                }

                stmt1.close();
                stmt2.close();
                conn1.commit();
                conn2.commit();
                connPool.put(conn1);
                connPool.put(conn2);

            }

            long watch = System.currentTimeMillis();
            final AtomicInteger count = new AtomicInteger(0);
            ExecutorService pool = CreatePool();
            for (int i = 0; i < threadCount; i++) {
                final int p = i;
                pool.execute(new Runnable() {
                    @Override
                    public void run() {

                        try {
                            var conn = connPool.take();
                            conn.setAutoCommit(false);
                            var stmt = conn.prepareStatement("insert into T1(Id,Value) values( ?, ? )");

                            for (int i = 0; i < batchCount; i++) {
                                int id = (p * batchCount) + i;

                                stmt.setInt(1, id);
                                stmt.setString(2, Integer.toString(id));
                                stmt.addBatch();
                                count.incrementAndGet();
                            }
                            stmt.executeBatch();
                            stmt.close();

                            conn.commit();
                            connPool.put(conn);

                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }

                        for (int r = 0; r < reinterationSelect; r++) {
                            try {
                                var conn = connPool.take();
                                conn.setAutoCommit(true);
                                var stmt = conn.prepareStatement("select Id,Value from T1 where Id>=? and Id<? order by Id");
                                int minId = p * batchCount + 0;
                                int maxId = p * batchCount + batchCount;
                                stmt.setInt(1, minId);
                                stmt.setInt(2, maxId);

                                var reader = T1.toArray(stmt.executeQuery()).iterator();
                                stmt.close();

                                connPool.put(conn);

                                int ti = minId;
                                while (reader.hasNext()) {
                                    var iv = reader.next().getId();
                                    if (ti != iv) {
                                        System.out.println("e");
                                        throw new RuntimeException(ti + "  " + iv);
                                    }
                                    ti++;

                                }
                                if (ti != maxId) {
                                    System.out.println("e2");
                                    throw new RuntimeException(ti + "  " + maxId);
                                }

                            } catch (Exception ex) {
                                ex.printStackTrace();
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
            System.out.format("MySQL  Insert: %,d AVG: %,d objects/s %n", count.get(), avg);

            // ----------------------Update------------------
            watch = System.currentTimeMillis();
            count.set(0);
            pool = CreatePool();
            for (int i = 0; i < threadCount; i++) {
                final int p = i;
                pool.execute(new Runnable() {
                    @Override
                    public void run() {

                        try {
                            var conn = connPool.take();
                            conn.setAutoCommit(false);
                            var stmt = conn.prepareStatement("update T1 set Value=? where Id=?");

                            for (int i = 0; i < batchCount; i++) {
                                int id = (p * batchCount) + i;

                                stmt.setInt(2, id);
                                stmt.setString(1, "U" + id);
                                stmt.addBatch();

                                count.incrementAndGet();
                            }
                            stmt.executeBatch();
                            stmt.close();

                            conn.commit();
                            connPool.put(conn);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }

                        try {
                            var conn = connPool.take();
                            conn.setAutoCommit(true);
                            var stmt = conn.prepareStatement("select Id,Value from T1 where Id>=? and Id<? order by Id");
                            int minId = p * batchCount + 0;
                            int maxId = p * batchCount + batchCount;
                            stmt.setInt(1, minId);
                            stmt.setInt(2, maxId);

                            var reader = T1.toArray(stmt.executeQuery()).iterator();
                            stmt.close();

                            connPool.put(conn);

                            int ti = minId;
                            while (reader.hasNext()) {
                                var t1 = reader.next();
                                var iv = t1.getId();
                                if (ti != iv) {
                                    System.out.println("e");
                                    throw new RuntimeException(ti + "  " + iv);
                                }
                                if (!("U" + ti).equals(t1.getValue())) {
                                    System.out.println("eu");
                                    throw new RuntimeException(ti + "  " + t1.getValue());
                                }
                                ti++;

                            }
                            if (ti != maxId) {
                                System.out.println("e2");
                                throw new RuntimeException(ti + "  " + maxId);
                            }

                        } catch (Exception ex) {
                            ex.printStackTrace();
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
            System.out.format("MySQL  Update: %,d AVG: %,d objects/s %n", count.get(), avg);

            // ------------------------Delete------------------
            watch = System.currentTimeMillis();
            count.set(0);
            pool = CreatePool();
            for (int i = 0; i < threadCount; i++) {
                final int p = i;
                pool.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            var conn = connPool.take();
                            conn.setAutoCommit(false);
                            var stmt = conn.prepareStatement("delete from T1 where Id=?");

                            for (int i = 0; i < batchCount; i++) {
                                int id = (p * batchCount) + i;

                                stmt.setInt(1, id);
                                stmt.addBatch();

                                count.incrementAndGet();
                            }
                            stmt.executeBatch();
                            stmt.close();

                            conn.commit();
                            connPool.put(conn);
                        } catch (Exception ex) {
                            ex.printStackTrace();
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
            System.out.format("MySQL  Delete: %,d AVG: %,d objects/s %n", count.get(), avg);

            //-------------------------END-------------------------
            try {
                var conn = connPool.take();
                conn.setAutoCommit(false);
                var stmt = conn.prepareStatement("select Id,Value from T1");

                var list = T1.toArray(stmt.executeQuery());
                if (list.size() != 2) {
                    throw new Exception("Delete Error");
                }
                for (var t : list) {
                    //System.out.println(t.getId() + "," + t.getValue());
                }
                stmt.close();
                connPool.put(conn);
            } catch (Exception ex) {
                ex.printStackTrace();
            }

            if (connPool.size() != (poolSize * 2)) {
                throw new Exception("PoolSize");
            }
            for (var conn : connPool) {
                conn.close();
            }
        } catch (Exception ex) {
            Logger.getLogger(BenchmarkDBTestMySQL.class.getName()).log(Level.SEVERE, null, ex);

        }
    }

    private static int poolSize = 8;

    private static LinkedBlockingQueue<java.sql.Connection> CreateConnectionPool() {

        //put/take
        LinkedBlockingQueue<java.sql.Connection> queue = new LinkedBlockingQueue(poolSize * 2);
        for (int i = 0; i < poolSize * 2; i++) {
            try {
                var conn
                        = DriverManager.getConnection("jdbc:mysql://localhost/test?"
                                + "user=root&password=123456");

                queue.put(conn);
            } catch (Exception ex) {
                Logger.getLogger(BenchmarkDBTestMySQL.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return queue;
    }

    private static ExecutorService CreatePool() {
        return Executors.newFixedThreadPool(poolSize);
    }

    public static class T1 {

        public T1() {
        }

        public T1(int id, String val) {
            setId(id);
            setValue(val);
        }

        private int _id;

        public final int getId() {
            return _id;
        }

        public final void setId(int id) {
            _id = id;
        }
        private String _value;

        public final String getValue() {
            return _value;
        }

        public final void setValue(String val) {
            _value = val;
        }

        public static <T> List<T> toArray(Iterable<T> it) {
            ArrayList<T> list = new ArrayList<>();
            for (T t : it) {
                list.add(t);
            }
            return list;
        }

        public static List<T1> toArray(java.sql.ResultSet it) throws SQLException {
            ArrayList<T1> list = new ArrayList<>();
            while (it.next()) {
                T1 t1 = new T1();
                t1.setId(it.getInt(1));
                t1.setValue(it.getString(2));
                list.add(t1);
            }
            return list;
        }
    }

    public static class iBoxDBServer extends LocalDatabaseServer {

        @Override
        protected DatabaseConfig BuildDatabaseConfig(long address) {
            return new FileConfig();

        }

        public static class FileConfig extends BoxFileStreamConfig {

            public FileConfig() {
                //this.FileIncSize = (int) mb(4);
                //this.CacheLength = mb(512);
                //this.ReadStreamCount = 8;

                //getId -> Id
                EnsureTable(T1.class, "T1", "Id");

            }
        }

    }

}

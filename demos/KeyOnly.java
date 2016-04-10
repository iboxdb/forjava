package iBoxDB;

import iBoxDB.LocalServer.*;
import iBoxDB.LocalServer.Replication.IBStreamReader;
import java.text.NumberFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class KeyOnly {

    public static void main() throws InterruptedException {
        System.out.println(java.lang.Runtime.getRuntime().maxMemory());
        DB.root("/tmp/");
        helper.deleteDB();

        DB db = new DB(1);
        // JVM=1G, DB=0.5G
        db.getConfig().DBConfig.CacheLength = 512 * 1024 * 1024;
        db.getConfig().ensureTable(ShortMSG.class, "/shortmsg", "productId", "time", "userId", "msg(30)");
        final AutoBox auto = db.open();

        final long count = 100;

        //Insert
        long begin = System.currentTimeMillis();

        //long total = SingleInsert(auto, count);
        long total = BatchInsert(auto, count);

        double sec = (System.currentTimeMillis() - begin) / 1000.0;
        long avg = (long) (total / sec);
        IBStreamReader reader = auto.getDatabase().createFileReader();
        long length = reader.length();
        System.out.println("Insert TotalObjects: " + JDB.helper.format(total)
                + " FileSize: " + JDB.helper.format(length / 1024.0 / 1024.0) + "MB");
        System.out.println("Elapsed " + JDB.helper.getDou(sec) + "s, AVG "
                + JDB.helper.format(avg) + " o/sec");
        reader.close();

        System.out.println();
        //Select
        begin = System.currentTimeMillis();

        total = SelectObjects(auto, count);
        total += SelectObjects(auto, count);

        sec = (System.currentTimeMillis() - begin) / 1000.0;
        avg = (long) (total / sec);
        System.out.println("Select TotalObjects: " + JDB.helper.format(total));
        System.out.println("Elapsed " + JDB.helper.getDou(sec) + "s, AVG "
                + JDB.helper.format(avg) + " o/sec");

        //Reopen Select
        db.close();

        System.gc();
        System.out.println();

        AutoBox auto2 = db.open();

        begin = System.currentTimeMillis();
        total = SelectObjects(auto2, count);
        total += SelectObjects(auto2, count);

        sec = (System.currentTimeMillis() - begin) / 1000.0;
        avg = (long) (total / sec);
        System.out.println("Select TotalObjects: " + JDB.helper.format(total) + " -Reopen");
        System.out.println("Elapsed " + JDB.helper.getDou(sec) + "s, AVG "
                + JDB.helper.format(avg) + " o/sec");

        System.out.println();
        begin = System.currentTimeMillis();
        total = SelectObjects(auto2, count);
        total += SelectObjects(auto2, count);

        sec = (System.currentTimeMillis() - begin) / 1000.0;
        avg = (long) (total / sec);
        System.out.println("Select TotalObjects: " + JDB.helper.format(total) + " -Reopen2");
        System.out.println("Elapsed " + JDB.helper.getDou(sec) + "s, AVG "
                + JDB.helper.format(avg) + " o/sec");

    }

    private static long SelectObjects(final AutoBox auto, final long count) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(
                java.lang.Runtime.getRuntime().availableProcessors() * 2);

        final AtomicLong total = new AtomicLong(0);
        for (long proId = 1; proId <= count; proId++) {
            final long fproId = proId;
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    try (Box box = auto.cube()) {
                        for (ShortMSG smsg : box.select(ShortMSG.class, "from /shortmsg where productId==?", fproId)) {
                            if (smsg.productId != fproId) {
                                System.out.println("Unreachable");
                            }
                            String cs = smsg.productId + "-" + (smsg.time.getTime()) + "-" + smsg.userId;
                            if (!smsg.msg.equals(cs)) {
                                System.out.println("Unreachable");
                            }
                            total.incrementAndGet();
                        }
                    }
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        return total.get();
    }

    private static long BatchInsert(final AutoBox auto, final long count) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(
                java.lang.Runtime.getRuntime().availableProcessors() * 2);
        final long fakeTime = System.currentTimeMillis();
        final AtomicLong total = new AtomicLong(0);
        for (long proId = 1; proId <= count; proId++) {
            for (long ft = 1; ft <= count; ft++) {
                final long fproId = proId;
                final long fft = ft;
                pool.execute(new Runnable() {
                    @Override
                    public void run() {
                        try (Box box = auto.cube()) {
                            for (long userid = 1; userid <= count; userid++) {
                                ShortMSG smsg = new ShortMSG();
                                smsg.productId = fproId;
                                smsg.time = new Date(fakeTime + fft);
                                smsg.userId = userid;
                                smsg.msg = smsg.productId + "-" + (fakeTime + fft) + "-" + smsg.userId;
                                box.d("/shortmsg").insert(smsg);
                            }
                            if (box.commit().equals(CommitResult.OK)) {
                                total.addAndGet(count);
                            }
                        }
                    }
                });
            }
        }
        pool.shutdown();
        pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        return total.get();
    }

    private static long SingleInsert(final AutoBox auto, long count) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(
                java.lang.Runtime.getRuntime().availableProcessors() * 2);
        final long fakeTime = System.currentTimeMillis();
        final AtomicLong total = new AtomicLong(0);
        for (long proId = 1; proId <= count; proId++) {
            for (long ft = 1; ft <= count; ft++) {
                for (long userid = 1; userid <= count; userid++) {
                    final long fproId = proId;
                    final long fft = ft;
                    final long fuserid = userid;
                    pool.execute(new Runnable() {
                        @Override
                        public void run() {
                            ShortMSG smsg = new ShortMSG();
                            smsg.productId = fproId;
                            smsg.time = new Date(fakeTime + fft);
                            smsg.userId = fuserid;
                            smsg.msg = smsg.productId + "-" + (fakeTime + fft) + "-" + smsg.userId;
                            if (auto.insert("/shortmsg", smsg)) {
                                total.incrementAndGet();
                            }
                        }
                    });
                }
            }
        }
        pool.shutdown();
        pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        return total.get();
    }

    public static class ShortMSG {

        public long productId;
        public java.util.Date time;
        public long userId;
        public String msg;
    }

    private static class helper {

        public static void deleteDB() {
            BoxSystem.DBDebug.DeleteDBFiles(1);
        }

        public static String getDou(double d) {
            long l = (long) (d * 1000);
            return Double.toString(l / 1000.0);
        }

        public static String format(double d) {
            return NumberFormat.getInstance().format((int) d);
        }
    }
}

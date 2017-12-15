package com.sequoiadb;

import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.Sequoiadb;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.springframework.data.sequoiadb.SequoiadbFactory;
import org.springframework.data.sequoiadb.assist.DBCollection;
import org.springframework.data.sequoiadb.assist.Sdb;
import org.springframework.data.sequoiadb.core.SequoiadbTemplate;
import org.springframework.data.sequoiadb.core.SimpleSequoiadbFactory;


import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.System.exit;


abstract class LocalTask implements Runnable {
    SequoiadbTemplate template ;
    int runTimes;
    String clName;
    Random random;
    int sequence;
    AtomicLong atomicLong;

    public LocalTask(SequoiadbTemplate template, int runTimes, String clName, int sequence, AtomicLong atomicLong) {
        this.template = template;
        this.runTimes = runTimes;
        this.clName = clName;
        this.sequence = sequence;
        this.random = new Random();
        this.atomicLong = atomicLong;
    }
}

class InsertTask extends LocalTask {
    public InsertTask(SequoiadbTemplate template, int runTimes, String clName, int sequence, AtomicLong atomicLong) {
        super(template, runTimes, clName, sequence, atomicLong);
    }

    public void run() {
        DBCollection cl = template.getCollection(clName);
        long beginTime = System.currentTimeMillis();
        int counter = 0;
        while (runTimes-- != 0) {
            counter++;
            int times = 500;
            List<BSONObject> list = new ArrayList<BSONObject>(times);
            while (times-- >= 0) {
                BSONObject obj = new BasicBSONObject();
                obj.put("a", random.nextInt(1000));
                obj.put("b", random.nextInt(1000));
                obj.put("c", random.nextInt(1000));
                list.add(obj);
            }
            cl.insert(list);
//                checkConnectionStatus(template, "Insert task");
//                System.out.println(String.format("In insert task, thread[%d] finish running the [%d] time",
//                        sequence, counter));
        }
        long endTime = System.currentTimeMillis();
        atomicLong.addAndGet(endTime - beginTime);
    }
}

public class TemplateSample
{

    static void initForTest(SequoiadbTemplate template, String clName) {
        if (template.collectionExists(clName)) {
            template.dropCollection(clName);
        }
        template.createCollection(clName);
    }

    static void checkConnectionStatus(SequoiadbTemplate template, String msg) {
        Sdb sdb = template.getDb().getSdb();
        int usedConnCount = sdb.getUsedConnCount();
        int idleConnCount = sdb.getIdleConnCount();
        System.out.println(String.format("%s, used: %d, idle: %d", msg, usedConnCount, idleConnCount));
    }

    public static void main(String[] args) throws UnknownHostException {

        if (args.length < 2) {
            System.out.println("<hostname> <port>");
            exit(-1);
        }
        func2(args);
    }

    public static void func2(String[] args) {
        final String hostName = args[0];
        final int port = Integer.parseInt(args[1]);
        final String databaseName = "database";
        final String clName = "test2";

        Sequoiadb db = new Sequoiadb(hostName, port, "", "");
        if (db.isCollectionSpaceExist(databaseName)) {
            db.dropCollectionSpace(databaseName);
        }
        CollectionSpace cs = db.createCollectionSpace(databaseName);
        com.sequoiadb.base.DBCollection cl = cs.createCollection(clName);
        Random random = new Random();
        long beginTime = System.currentTimeMillis();

        int times = 500;
        List<BSONObject> list = new ArrayList<BSONObject>(times);
        while (times-- >= 0) {
            BSONObject obj = new BasicBSONObject();
            obj.put("a", random.nextInt(1000));
            obj.put("b", random.nextInt(1000));
            obj.put("c", random.nextInt(1000));
            list.add(obj);
        }
        cl.bulkInsert(list, 0);
        long endTime = System.currentTimeMillis();
        System.out.println("Takes: " + (endTime - beginTime) + "ms");
    }

    public static void func1(String[] args) throws UnknownHostException {
        final String hostName = args[0];
        final int port = Integer.parseInt(args[1]);
        final String databaseName = "database";
        final String clName = "test2";
        AtomicLong atomicLong = new AtomicLong(0);
        Sdb sdb = new Sdb(hostName, port);
        SequoiadbFactory factory = new SimpleSequoiadbFactory(sdb, databaseName);
        SequoiadbTemplate template = new SequoiadbTemplate(factory);

        int insertRumTimes = 1;
        int insertThreadCount = 1;
        Thread[] insertTaskTheads = new Thread[insertThreadCount];

        // init
        initForTest(template, clName);

        // create insert threads
        for(int i = 0; i < insertThreadCount; i++) {
            insertTaskTheads[i] = new Thread(new InsertTask(template, insertRumTimes, clName, i, atomicLong));
        }

        // start insert threads
        for(int i = 0; i < insertThreadCount; i++) {
            insertTaskTheads[i].start();
        }

        // join theads
        for(int i = 0; i < insertThreadCount; i++) {
            try {
                insertTaskTheads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // finish
        System.out.println(String.format("Takes %dms", (int)atomicLong.get() / insertThreadCount));
        System.out.println("Finish!");

        // clean up
        if (sdb != null) {
            sdb.close();
        }
    }

}

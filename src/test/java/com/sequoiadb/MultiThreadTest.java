package com.sequoiadb;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.*;
import org.springframework.data.sequoiadb.SequoiadbFactory;
import org.springframework.data.sequoiadb.assist.DB;
import org.springframework.data.sequoiadb.assist.DBCollection;
import org.springframework.data.sequoiadb.assist.DBCursor;
import org.springframework.data.sequoiadb.assist.Sdb;
import org.springframework.data.sequoiadb.core.SequoiadbTemplate;
import org.springframework.data.sequoiadb.core.SimpleSequoiadbFactory;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit test for simple App.
 */
public class MultiThreadTest
{
    static final String hostName = "192.168.3.211";
    static final int port = 11810;
    static final String databaseName = "database";
    static Sdb sdb;
    static SequoiadbFactory factory;
    static SequoiadbTemplate template;
    AtomicLong atomicLong = new AtomicLong(0);

    @BeforeClass
    public static void beforeClass() throws UnknownHostException {
        sdb = new Sdb(hostName, port);
        factory = new SimpleSequoiadbFactory(sdb, databaseName);
        template = new SequoiadbTemplate(factory);
    }

    @AfterClass
    public static void afterClass() {
        if (sdb != null) {
            sdb.close();
        }
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    abstract class LocalTask implements Runnable {
        SequoiadbTemplate template ;
        int runTimes;
        String clName;
        Random random;
        int sequence;

        public LocalTask(SequoiadbTemplate template, int runTimes, String clName, int sequence) {
            this.template = template;
            this.runTimes = runTimes;
            this.clName = clName;
            this.sequence = sequence;
            this.random = new Random();
        }
    }

    class QueryTask extends LocalTask {
        public QueryTask(SequoiadbTemplate template, int runTimes, String clName, int sequence) {
            super(template, runTimes, clName, sequence);
        }

        public void run() {
            int counter = 0;
            while (runTimes-- != 0) {
                counter++;
                int limit = random.nextInt(1000);
                DB db = template.getDb();
                DBCollection cl = template.getCollection(clName);
                DBCursor cursor = cl.find(null, null, null, null, 0, limit, 0);
                int recordCounter = 0;
                try {
                    while (cursor.hasNext()) {
                        BSONObject obj = cursor.next();
//                        System.out.println(obj);
                        recordCounter++;
                    }
                } finally {
                    cursor.close();
                }
                checkConnectionStatus(template, "Query task");
                try {
//                    Thread.sleep(random.nextInt(3000) + 1000);
                    Thread.sleep(random.nextInt(1000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(String.format("In query task, thread[%d] finish running the [%d] time, return [%d] records",
                        sequence, counter, recordCounter));
            }
        }
    }

    class InsertTask extends LocalTask {
        public InsertTask(SequoiadbTemplate template, int runTimes, String clName, int sequence) {
            super(template, runTimes, clName, sequence);
        }

        public void run() {
            long beginTime = System.currentTimeMillis();
            int counter = 0;
            while (runTimes-- != 0) {
                counter++;
//                int times = random.nextInt(300);
                int times = 500;
                List<BSONObject> list = new ArrayList<BSONObject>(times);
//                List<DBObject> list = new ArrayList<DBObject>(times);
                while (times-- >= 0) {
                    BSONObject obj = new BasicBSONObject();
//                    DBObject obj = new BasicDBObject();
                    obj.put("a", random.nextInt(1000));
                    obj.put("b", random.nextInt(1000));
                    obj.put("c", random.nextInt(1000));
                    list.add(obj);
                }
                DBCollection cl = template.getCollection(clName);
                cl.insert(list);
//                checkConnectionStatus(template, "Insert task");
//                System.out.println(String.format("In insert task, thread[%d] finish running the [%d] time",
//                        sequence, counter));
            }
            long endTime = System.currentTimeMillis();
            atomicLong.addAndGet(endTime - beginTime);
        }
    }

    void initForTest(SequoiadbTemplate template, String clName) {
        if (template.collectionExists(clName)) {
            template.dropCollection(clName);
        }
        template.createCollection(clName);
    }

    void checkConnectionStatus(SequoiadbTemplate template, String msg) {
        Sdb sdb = template.getDb().getSdb();
        int usedConnCount = sdb.getUsedConnCount();
        int idleConnCount = sdb.getIdleConnCount();
        System.out.println(String.format("%s, used: %d, idle: %d", msg, usedConnCount, idleConnCount));
    }

    @Test
    public void runTasksTest() {
        String clName = "test2";
        int insertRumTimes = 1;
        int queryRumTimes = 500;
        int insertThreadCount = 1;
        int queryThreadCount = 0;
        Thread[] queryTaskTheads = new Thread[queryThreadCount];
        Thread[] insertTaskTheads = new Thread[insertThreadCount];

        // init
        initForTest(template, clName);

        // create insert threads
        for(int i = 0; i < insertThreadCount; i++) {
            insertTaskTheads[i] = new Thread(new InsertTask(template, insertRumTimes, clName, i));
        }

        // start insert threads
        for(int i = 0; i < insertThreadCount; i++) {
            insertTaskTheads[i].start();
        }

        // create query threads
        for(int i = 0; i < queryThreadCount; i++) {
            queryTaskTheads[i] = new Thread(new QueryTask(template, queryRumTimes, clName, i));
        }

        // start query threads
        for(int i = 0; i < queryThreadCount; i++) {
            queryTaskTheads[i].start();
        }

        // join theads
        for(int i = 0; i < queryThreadCount; i++) {

            try {
                queryTaskTheads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
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
    }

}


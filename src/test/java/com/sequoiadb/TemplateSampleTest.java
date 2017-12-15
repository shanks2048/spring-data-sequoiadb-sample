package com.sequoiadb;

import org.bson.BasicBSONObject;
import org.junit.*;
import org.springframework.data.sequoiadb.SequoiadbFactory;
import org.springframework.data.sequoiadb.assist.DBCollection;
import org.springframework.data.sequoiadb.assist.DBCursor;
import org.springframework.data.sequoiadb.assist.Sdb;
import org.springframework.data.sequoiadb.core.SequoiadbTemplate;
import org.springframework.data.sequoiadb.core.SimpleSequoiadbFactory;


import java.net.UnknownHostException;

import static org.junit.Assert.*;

/**
 * Unit test for simple App.
 */
public class TemplateSampleTest
{
    static final String hostName = "192.168.3.211";
    static final int port = 11810;
    static final String databaseName = "database";
    static final String clName = "sample";
    static Sdb sdb;
    static SequoiadbFactory factory;
    static SequoiadbTemplate template;

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
        if (template.collectionExists(clName)) {
            template.dropCollection(clName);
        }
        template.createCollection(clName);
    }

    @After
    public void tearDown() {
        template.dropCollection(clName);
    }

   @Test
   public void queryTest() {
        DBCollection cl = template.getCollection(clName);
        DBCursor cursor = cl.find(null, null, null, null,0, 10, 0);
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }
        assertTrue(true);
    }

    @Test
    public void CRUDTest() {
        DBCollection cl = template.getCollection(clName);
        // insert
        cl.insert(new BasicBSONObject().append("a", 1));
        // query
        DBCursor cursor = cl.find(new BasicBSONObject().append("a", new BasicBSONObject("$gt", 0)),
                            new BasicBSONObject().append("_id", new BasicBSONObject("$include", 0)),
                null, null,
                    0, -1, 0);
        System.out.println(String.format("idle: %d, used: %d", template.getDb().getSdb().getIdleConnCount(),
                template.getDb().getSdb().getUsedConnCount()));
        while(cursor.hasNext()) {
            System.out.println("after insert, record is: " + cursor.next().toString());
        }
        System.out.println(String.format("idle: %d, used: %d", template.getDb().getSdb().getIdleConnCount(),
                template.getDb().getSdb().getUsedConnCount()));
        // update
        cl.update(null, new BasicBSONObject().append("$set", new BasicBSONObject("a", 2)), null, false );
        cursor = cl.find();
        System.out.println(String.format("idle: %d, used: %d", template.getDb().getSdb().getIdleConnCount(),
                template.getDb().getSdb().getUsedConnCount()));
        try {
            while (cursor.hasNext()) {
                System.out.println("after update, record is: " + cursor.next().toString());
            }
            System.out.println(String.format("idle: %d, used: %d", template.getDb().getSdb().getIdleConnCount(),
                    template.getDb().getSdb().getUsedConnCount()));
        } finally {
            cursor.close();
        }
        System.out.println(String.format("idle: %d, used: %d", template.getDb().getSdb().getIdleConnCount(),
                template.getDb().getSdb().getUsedConnCount()));
        // delete
        cl.remove(new BasicBSONObject().append("a", new BasicBSONObject("$gt", 1)));
        cursor = cl.find();
        Assert.assertFalse(cursor.hasNext());
    }

}

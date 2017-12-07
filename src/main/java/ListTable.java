import org.apache.hadoop.hbase.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class ListTable {

    private static String tableName = "test2";
    private static String exampleProduct = "p4";
    private static String exampleAttribute = "red";
    private static String[] tableFamilies = new String[]{"core","attributes"};
    private static String[][] dummyData = new String [][] {
            { "hash1", "p1", "red", "S"},
            { "hash2", "p1", "red", "M"},
            { "hash3", "p1", "blue", "S"},
            { "hash4", "p1", "blue", "M"},
            { "hash5", "p1", "blue", "L"},
            { "hash6", "p2", "white", "S"},
            { "hash7", "p2", "black", "S"},
            { "hash8", "p2", "purple", "S"},
            { "hash9", "p3", "", "M"},
            { "hash10", "p4", "black", "L"}};

    public static void main(String... args) throws Exception {
        Connection connection = ConnectToHBase("myhbase");

        //ListTables(connection);
        //InsertData(connection, tableName);
        //ScanTable(connection, tableName);
        //DeleteData(connection, tableName);
        GetAttributesOfProducts(connection, tableName);
        //GetProductsForOneAttribute(connection, tableName);
        connection.close();
        System.out.println("Done!");
    }

    public static void GetAttributesOfProducts(Connection connection, String tableName) throws IOException {
        TableName t = TableName.valueOf(tableName);
        Table table = connection.getTable(t);

        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("core"),
                Bytes.toBytes("title"),
                CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(exampleProduct))
        );

        filter.setFilterIfMissing(true);

        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanresult = table.getScanner(scan);

        printResults(scanresult);

        table.close();
    }

    public static void GetProductsForOneAttribute(Connection connection, String tableName) throws IOException {
        TableName t = TableName.valueOf(tableName);
        Table table = connection.getTable(t);

        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("attributes"),
                Bytes.toBytes("color"),
                CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(exampleAttribute))
        );

        filter.setFilterIfMissing(true);

        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanresult = table.getScanner(scan);

        printResults(scanresult);

        table.close();
    }

    private static void printResults(ResultScanner scanResult) {
        System.out.println();
        System.out.println("Results: ");

        for(Result res : scanResult) {
            for(Cell cell : res.listCells()) {
                String row = new String (CellUtil.cloneRow(cell));
                String family = new String (CellUtil.cloneFamily(cell));
                String column = new String (CellUtil.cloneQualifier(cell));
                String value = new String (CellUtil.cloneValue(cell));


                if(family.equals("attributes")) {
                    System.out.println(row + " " + column + " " + value);
                }
            }
        }
    }

    public static void InsertData(Connection connection, String tableName) throws IOException {
        TableName t = TableName.valueOf(tableName);
        Table table = connection.getTable(t);

        for(int i=0; i < dummyData.length; i++) {
            Put p = new Put(Bytes.toBytes(dummyData[i][0]));

            p.addColumn(Bytes.toBytes("core"), Bytes.toBytes("title"), Bytes.toBytes(dummyData[i][1]));
            p.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("color"), Bytes.toBytes(dummyData[i][2]));
            p.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("size"), Bytes.toBytes(dummyData[i][3]));

            table.put(p);
        }

        table.close();
    }

    public static void DeleteData(Connection connection, String tableName) throws IOException {
        TableName t = TableName.valueOf(tableName);
        Table table = connection.getTable(t);

        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);

        for(Result r : scanner) {
            String key = Bytes.toString(r.getRow());
            Delete delete = new Delete(Bytes.toBytes(key));
            table.delete(delete);
        }

        table.close();
    }

//    public static void ScanTable(Connection connection, String tableName) throws IOException {
//        TableName t = TableName.valueOf(tableName);
//        Table table = connection.getTable(t);
//        Scan scan = new Scan();
//        scan.addColumn(Bytes.toBytes("attributes"), Bytes.toBytes("color"));
//
//        ResultScanner scanner = table.getScanner(scan);
//
//        for (Result result = scanner.next(); result != null; result = scanner.next())
//            System.out.println("Found row : " + result);
//    }

    public static void CreateTable(Connection connection, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));

        if (!admin.tableExists(table.getTableName())) {
            HTableDescriptor htable = new HTableDescriptor(table);

            for (String family : tableFamilies) {
                htable.addFamily(new HColumnDescriptor(family));
            }

            admin.createTable(htable);
        } else {
            System.out.println(tableName + " table already exists!");
        }
        admin.close();
        System.out.println(tableName + " created successfully");
    }

    public static void ListTables(Connection connection) throws Exception  {
        Admin admin = connection.getAdmin();
        HTableDescriptor[] tableDescriptors = admin.listTables();
        for (HTableDescriptor tableDescriptor : tableDescriptors) {
            System.out.println("Table Name: "+ tableDescriptor.getNameAsString());
        }
        admin.close();
    }

    public static Connection ConnectToHBase(String zookeeperQuorum) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        configuration.set("zookeeper.session.timeout", "5000");
        configuration.set("zookeeper.znode.parent", "/hbase");
        configuration.set("hbase.client.retries.number", "5");

        return ConnectionFactory.createConnection(configuration);
    }
}
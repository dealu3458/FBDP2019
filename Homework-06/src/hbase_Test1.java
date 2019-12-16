import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * Created by hadoopuser on 8/14/19.
 */

public class hbase_Test1 {


    private static final String TABLE_NAME="students";

    public static final String FAMILY_NAME_1 = "Description";
    public static final String FAMILY_NAME_2 = "Courses";
    public static final String FAMILY_NAME_3 = "Home";

    //conf
    private static Configuration getHBaseConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("zookeeper.znode.parent", "/hbase");
        return conf;
    }

    //createTable
    private static void createTable(Configuration conf) throws IOException {
        Connection connection = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();

            if (!admin.tableExists(TableName.valueOf(TABLE_NAME))) {
                //create table  ,create family
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
                HColumnDescriptor columnDescriptor_1 = new HColumnDescriptor(Bytes.toBytes(FAMILY_NAME_1));
                HColumnDescriptor columnDescriptor_2 = new HColumnDescriptor(Bytes.toBytes(FAMILY_NAME_2));
                HColumnDescriptor columnDescriptor_3 = new HColumnDescriptor(Bytes.toBytes(FAMILY_NAME_3));
                tableDescriptor.addFamily(columnDescriptor_1);
                tableDescriptor.addFamily(columnDescriptor_2);
                tableDescriptor.addFamily(columnDescriptor_3);
                admin.createTable(tableDescriptor);


            } else {
                System.err.println("table is exists!!!!!");
            }

            //put data
            table = connection.getTable(TableName.valueOf(TABLE_NAME));

            Put put=new Put(Bytes.toBytes("001")); //rowkey  1
            put.addColumn(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes("Name"), Bytes.toBytes("Li Lei"));
            put.addColumn(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes("Height"), Bytes.toBytes("176"));

            put.addColumn(Bytes.toBytes(FAMILY_NAME_2), Bytes.toBytes("Chinese"), Bytes.toBytes("80"));
            put.addColumn(Bytes.toBytes(FAMILY_NAME_2), Bytes.toBytes("Math"), Bytes.toBytes("90"));
            put.addColumn(Bytes.toBytes(FAMILY_NAME_2), Bytes.toBytes("Physics"), Bytes.toBytes("95"));

            put.addColumn(Bytes.toBytes(FAMILY_NAME_3), Bytes.toBytes("Province"), Bytes.toBytes("Zhejiang"));



            Put put2=new Put(Bytes.toBytes("002")); //rowkey  1
            put2.addColumn(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes("Name"), Bytes.toBytes("Han Meimei"));
            put2.addColumn(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes("Height"), Bytes.toBytes("183"));

            put2.addColumn(Bytes.toBytes(FAMILY_NAME_2), Bytes.toBytes("Chinese"), Bytes.toBytes("88"));
            put2.addColumn(Bytes.toBytes(FAMILY_NAME_2), Bytes.toBytes("Math"), Bytes.toBytes("77"));
            put2.addColumn(Bytes.toBytes(FAMILY_NAME_2), Bytes.toBytes("Physics"), Bytes.toBytes("66"));

            put2.addColumn(Bytes.toBytes(FAMILY_NAME_3), Bytes.toBytes("Province"), Bytes.toBytes("Beijing"));


            Put put3=new Put(Bytes.toBytes("003")); //rowkey  1
            put3.addColumn(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes("Name"), Bytes.toBytes("Xiao Ming"));
            put3.addColumn(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes("Height"), Bytes.toBytes("162"));

            put3.addColumn(Bytes.toBytes(FAMILY_NAME_2), Bytes.toBytes("Chinese"), Bytes.toBytes("90"));
            put3.addColumn(Bytes.toBytes(FAMILY_NAME_2), Bytes.toBytes("Math"), Bytes.toBytes("90"));
            put3.addColumn(Bytes.toBytes(FAMILY_NAME_2), Bytes.toBytes("Physics"), Bytes.toBytes("90"));

            put3.addColumn(Bytes.toBytes(FAMILY_NAME_3), Bytes.toBytes("Province"), Bytes.toBytes("Shanghai"));

            table.put(put);
            table.put(put2);
            table.put(put3);

            Get getE001=new Get(Bytes.toBytes("001"));
            //String result=getE001.getFamilyMap().get("salary").toString();
            //System.out.println(result);

            byte [] ss=table.get(getE001).getValue(Bytes.toBytes(FAMILY_NAME_3),Bytes.toBytes("Province"));
            System.out.println("读出rowkey为“001”的Home:Province: "+new String(ss));


            Scan scan=new Scan();
            scan.setStartRow(Bytes.toBytes("001"));
            scan.setStopRow(Bytes.toBytes("004"));   //到 004 要不然003 的值不会输出
            scan.addColumn(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes("Name"));
            scan.setCaching(100);

            ResultScanner results=table.getScanner(scan);

            /*
            for (Result result : results) {
                while (result.advance()) {
                    System.out.println("name :"+new String(result.current().getValue()));

                }
            }
             */

        }catch (IOException e){
            e.printStackTrace();
        }finally {
            //close
            if (table != null) table.close();
            if (connection != null) connection.close();
        }
    }


    public static void  main(String [] args){
        Configuration conf=getHBaseConfiguration();
        try {
            createTable(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {

        }
    }


}
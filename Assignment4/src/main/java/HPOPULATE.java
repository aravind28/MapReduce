import java.io.IOException;

import com.opencsv.CSVParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.*;

public class HPOPULATE extends Configured implements Tool {

    /*
     Inner-class for map
     This Mapper splits data using RecordReader and inputs into a HBase Table
     Keys selected: Year, AirlineID, Month, Date, DepartureTime

      */
    static class HPOPULATEMapper<K, V> extends MapReduceBase implements
            Mapper<LongWritable, Text, K, V> {

        private HTable table;

        public void map(LongWritable key, Text value,
                        OutputCollector<K, V> output, Reporter reporter)
                throws IOException {

            CSVParser csvParser = new CSVParser(',', '"');
            String[] flightRecord = csvParser.parseLine(value.toString());
            if(flightRecord[0].equalsIgnoreCase("2008") ) {

                String abc = flightRecord[24].length() == 3 ? "0" + flightRecord[24] : flightRecord[24];
                String month = flightRecord[2].length() == 2 ? flightRecord[2] : "0" + flightRecord[2];
                String keyToReducer = flightRecord[0] + "-" + flightRecord[6] + "-" + month + "-" + flightRecord[3] + "-" + abc;

                // Adds the data from the Mapper to the Table
                // Selected records : AirlineID, ArrivalDelayMinutes
                Put put = new Put(Bytes.toBytes(keyToReducer));
                put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("UniqueCarrier"),
                        Bytes.toBytes(flightRecord[6]));
                put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("ArrDelayMinutes"),
                        Bytes.toBytes(flightRecord[37]));
                table.put(put);
            }
        }

        public void configure(JobConf jc) {
            super.configure(jc);
            try {
                this.table = new HTable(new HBaseConfiguration(jc), "FlightDelay");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close() throws IOException {
            super.close();
            table.close();
        }
    }

    public int run(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: HBaseTemperatureImporter <input>");
            return -1;
        }
        JobConf jc = new JobConf(getConf(), getClass());
        FileInputFormat.addInputPath(jc, new Path(args[0]));
        jc.setMapperClass(HPOPULATEMapper.class);
        jc.setNumReduceTasks(0);
        jc.setOutputFormat(NullOutputFormat.class);
        try {
            HBaseAdmin admin = new HBaseAdmin(jc);
            HTableDescriptor tableDescriptor = new HTableDescriptor("FlightDelay");
            tableDescriptor.addFamily(new HColumnDescriptor("colfam1"));
            if(admin.isTableAvailable("FlightDelay")){
                admin.disableTable("FlightDelay");
                admin.deleteTable("FlightDelay");
            }


            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            throw new RuntimeException("Failed HTable construction", e);
        }

        JobClient.runJob(jc);

        return 0;
    }
    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new HBaseConfiguration(), new HPOPULATE(), args);
        System.exit(exitCode);
    }
}


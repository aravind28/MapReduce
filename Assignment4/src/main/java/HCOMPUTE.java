import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HCOMPUTE extends Configured implements Tool {

    /*
    Scan object reads data from HBase table
     */

    public Map<String, String> getStationInfo(HTable table)
            throws IOException {
        Scan scan = new Scan(Bytes.toBytes("2008"));
        scan.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("UniqueCarrier"));
        scan.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("ArrDelayMinutes"));
        ResultScanner scanner = table.getScanner(scan);

        Result res = null;

        try {
            String airlineKey = "";
            String airlineIdInKey = null;
            int totalFlightDelay=0, totalFlights=0, mon=0;

            // HashMap stores data of each airline and then by average of arrival delay minutes
            Map<String, ArrayList<Integer>> sols = new HashMap<String, ArrayList<Integer>>();

            while ((res = scanner.next()) != null ) {
                byte[] row = res.getRow();
                byte[] value = res.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("UniqueCarrier"));
                byte[] delayMin = res.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("ArrDelayMinutes"));
                String[] flightRecord = Bytes.toString(row).split("-");
                int monthInKey= Integer.parseInt(flightRecord[2]);
                airlineIdInKey = Bytes.toString(value);

                int delayMin2 = Bytes.toString(delayMin) == ""? 0: (int)Double.parseDouble(Bytes.toString(delayMin));

                if(airlineKey.equalsIgnoreCase(airlineIdInKey) && (mon==monthInKey)){
                    totalFlightDelay = totalFlightDelay + delayMin2;
                    totalFlights++;
                }
                else{
                    if (!sols.containsKey(airlineIdInKey)){
                        if(sols.containsKey(airlineKey)){
                            sols.get(airlineKey).add((int) Math.ceil((double)(totalFlightDelay/totalFlights)));
                        }
                        sols.put(airlineIdInKey, new ArrayList<Integer>());
                        airlineKey = airlineIdInKey;
                    }
                    else{
                        sols.get(airlineIdInKey).add((int) Math.ceil((double)(totalFlightDelay/totalFlights)));
                    }
                    mon=monthInKey;
                    totalFlightDelay = delayMin2;
                    totalFlights=1;
                }
            }
            sols.get(airlineIdInKey).add((int) Math.ceil((double)(totalFlightDelay/totalFlights)));
            for(String s : sols.keySet()){
                for(int i : sols.get(s)){
                    System.out.println(s+" : "+i);
                }
            }
        } finally {
            scanner.close();
        }
        return null;
    }

    public int run(String[] args) throws IOException {

        HTable table = new HTable(new HBaseConfiguration(getConf()), "FlightDelay");
        getStationInfo(table);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HBaseConfiguration(), new HCOMPUTE(), args);
        System.exit(exitCode);
    }
}



import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.CSVParser;

public class AverageFlightDelayPerMonth {

    public static class AverageFlightDelayPerMonthMapper extends Mapper<Object, Text, TextPair, Text> {

        public void map(Object offset, Text value, Context context) throws IOException, InterruptedException {

            CSVParser csvParser = new CSVParser(',', '"');
            String[] flightRecord = csvParser.parseLine(value.toString());
            TextPair key = new TextPair();

            if (flightDateIn2008(flightRecord[0])) {
                String airlineID = flightRecord[6];
                String month = flightRecord[2].length() == 2 ? flightRecord[2] : "0" + flightRecord[2];
                String delay = flightRecord[37];

                if (flightRecordHasNoEmptyValues(airlineID, month, delay)) {
                    key.set(new Text(airlineID), new Text(month));

                    context.write(new TextPair(airlineID, month), new Text(delay));
                }
            }
        }

        private Boolean flightDateIn2008(String flightDate) {

            return flightDate.equals("2008");
        }

        private Boolean flightRecordHasNoEmptyValues(String airlineID2, String month2, String delay2) {

            if ((airlineID2.length() == 0) || (month2.length() == 0) || (delay2.length() == 0)) {
                return false;
            }
            return true;
        }
    }

    public static class AverageDelayPerMonthPartitioner
            extends Partitioner<TextPair, Text> {

        @Override
        public int getPartition(TextPair key, Text value, int numPartitions) {
            // multiply by 127 to perform some mixing to improve load balancing
            return Math.abs(key.hashCode() * 127) % numPartitions;
        }
    }

    // Order by airlineID and then order by increasing order of month
    public static class AverageDelayPerMonthKeyComparator extends WritableComparator {

        protected AverageDelayPerMonthKeyComparator() {
            super(TextPair.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            TextPair ip1 = (TextPair) w1;
            TextPair ip2 = (TextPair) w2;

            return ip1.compareTo(ip2);
        }
    }

    public static class AverageFlightDelayPerMonthReducer extends Reducer<TextPair, Text, Text, Text> {

        int totalFlights = 0;
        double totalDelay = 0;
        String flightName = "";
        int[] averageDelaysPerMonth = new int[12];
        int i = 0;

        public void reduce(TextPair airlineNameAndMonthOfFlight, Iterable<Text> delayMinutes, Context context)
                throws IOException, InterruptedException {
            String flightNameInReduceCall = airlineNameAndMonthOfFlight.getFirst().toString();
            totalDelay = 0;
            totalFlights = 0;

            if (flightName == "") {
                flightName = flightNameInReduceCall;
            }

            if (flightName != flightNameInReduceCall) {
                StringBuilder sb = new StringBuilder();
                for (int a = 0; a <= 11; a++) {
                    sb.append("(");
                    sb.append(a + 1);
                    sb.append(", ");
                    sb.append(averageDelaysPerMonth[a]);
                    sb.append(")");
                }
                context.write(new Text(flightName + ", "), new Text(sb.toString()));
                i = 0;
                flightName = flightNameInReduceCall;
                averageDelaysPerMonth = new int[12];
            }
            if (i < 12) {
                Iterator<Text> delayMinutesIterator = delayMinutes.iterator();
                while (delayMinutesIterator.hasNext()) {
                    totalFlights++;
                    totalDelay += Double.parseDouble(delayMinutesIterator.next().toString());
                }
                int average = (int) Math.ceil(totalDelay / totalFlights);
                if (average > 0) {
                    averageDelaysPerMonth[i] = average;
                }
                i++;
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: flightdelay <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Average Flight Delay per month for 2008");
        job.setJarByClass(AverageFlightDelayPerMonth.class);
        job.setMapperClass(AverageFlightDelayPerMonthMapper.class);

        job.setPartitionerClass(AverageDelayPerMonthPartitioner.class);
        job.setSortComparatorClass(AverageDelayPerMonthKeyComparator.class);

        job.setReducerClass(AverageFlightDelayPerMonthReducer.class);
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

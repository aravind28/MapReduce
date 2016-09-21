import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.CSVParser;

public class FlightDelay{

    // Global counter
    public enum AVGFLIGHTDELAY{
        TOTAL_FLIGHTS, AVERAGE_DELAY
    }

    public static class FlightDelayMapper extends Mapper<Object, Text, Text, Text>{

        public void map(Object offset, Text value, Context context) throws IOException, InterruptedException{

            CSVParser parser = new CSVParser(',','"');
            String[] flightRecord = parser.parseLine(value.toString());
            Text keyToReducer = new Text();
            Text valueToReducer = new Text();

            //Check the Origin and Destination of the Flight
            if(flightFromORDOrflightToJFK(flightRecord)){
                String keyStringToReducer;
                String valueStringToReducer;
                keyStringToReducer = flightRecord[5];

                //Check if any field in the flight record is empty or not
                if(valueinFlightRecordNotEmpty(flightRecord)){
                    String flightDate = flightRecord[5];
                    String originCity = flightRecord[11];
                    String destinationCity = flightRecord[17];
                    String departureTime = flightRecord[24];
                    String arrivalTime = flightRecord[35];
                    String arrivalDelayMinutes = flightRecord[37];

                    /*
                    Value to reducer has the the following data :
                    FlightDate, Origin City, Destination City, Departure Time,
                    Arrival Time and Arrival Delay Minutes
                    */
                    valueStringToReducer = flightDate + "," +  originCity + "," + destinationCity + "," +
                            departureTime + "," + arrivalTime + "," + arrivalDelayMinutes;

                    keyToReducer.set(keyStringToReducer);
                    valueToReducer.set(valueStringToReducer);

                    context.write(keyToReducer, valueToReducer);
                }
            }
        }

        // Check all conditions for a valid flight originating from ORD Or Landing in JFK
        private boolean flightFromORDOrflightToJFK(String[] flightRecord){

            if(flightRecord[11].equalsIgnoreCase("ord") && flightRecord[17].equalsIgnoreCase("jfk")){
                return false;
            }

            if(!(flightRecord[11].equalsIgnoreCase("ord") || flightRecord[17].equalsIgnoreCase("jfk"))){
                return false;
            }

            try{
                SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd");
                Date date = sd.parse(flightRecord[5]);
                Date startDate = sd.parse("2007-05-31");
                Date endDate = sd.parse("2008-06-01");
                Boolean start = date.after(startDate);
                Boolean end = date.before(endDate);
                if(!(start && end)){
                    return false;
                }
            }
            catch(ParseException e){
                e.printStackTrace();
            }

            if(flightRecord[41].equals("1") || flightRecord[43].equals("1")){
                return false;
            }
            return true;
        }

        // Method to check the completeness of a flight record
        public boolean valueinFlightRecordNotEmpty(String[] flightRecord){
            String flightDate = flightRecord[5];
            String originCity = flightRecord[11];
            String destinationCity = flightRecord[17];
            String departureTime = flightRecord[24];
            String arrivalTime = flightRecord[35];
            String arrivalDelayMinutes = flightRecord[37];
            String isCancelled = flightRecord[41];
            String isDiverted = flightRecord[43];
            if(flightDate.isEmpty() || originCity.isEmpty() || destinationCity.isEmpty()
                    || departureTime.isEmpty() || arrivalTime.isEmpty()
                    || arrivalDelayMinutes.isEmpty() || isCancelled.isEmpty() || isDiverted.isEmpty()){
                return false;
            }
            return true;
        }
    }

    public static class FlightDelayReducer extends Reducer<Text, Text, LongWritable, LongWritable>{

        IntWritable key = new IntWritable();
        DoubleWritable value = new DoubleWritable();

        int totalFlights;
        double totalDelay;
        double avgDelay;

        @Override
        protected void setup(Context context){
            totalFlights = 0;
            totalDelay = 0.0;
            avgDelay = 0;
        }

        // Reduce call is grouped by date field emitted from Mapper
        public void reduce(Text date, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{

            ArrayList<String> originFromORD= new ArrayList<String>();
            ArrayList<String> destinationToJFK = new ArrayList<String>();

            for(Text value : values){
                if(value.toString().contains("ORD") || value.toString().contains("ord")){
                    originFromORD.add(value.toString());
                }
                else{
                    destinationToJFK.add(value.toString());
                }
            }

            /*
            For every flight originating from ORD check for possible next flight to JFK from the
            such that destination of ORD flight is same as departure city of JFK flight
             */
            for(String everyFlightFromORD : originFromORD){
                for(String everyFlightToJFK : destinationToJFK){
                    String[] ORDFlightRecord = everyFlightFromORD.split(",");
                    String[] JFKFlightRecord = everyFlightToJFK.split(",");
                    String ORDFlightArrivalCity = ORDFlightRecord[2];
                    String ORDFlightArrivalTime = ORDFlightRecord[4];
                    String JFKFlightDepartureCity = JFKFlightRecord[1];
                    String JFKFlightDepartureTime = JFKFlightRecord[3];

                    // if the flights are two legged, add to task level counter
                    if(ORDFlightArrivalCity.equals(JFKFlightDepartureCity)){
                        if(ifFlightTwoLegged(ORDFlightArrivalTime, JFKFlightDepartureTime)){
                            Double ORDFlightDelay = Double.parseDouble(JFKFlightRecord[5]);
                            Double JFKFlightDelay = Double.parseDouble(ORDFlightRecord[5]);
                            totalFlights = totalFlights + 1;
                            totalDelay = totalDelay + ORDFlightDelay + JFKFlightDelay;
                        }
                    }
                }
            }
        }

        // Check if the flights have two legged path from ORD to JFK
        public boolean ifFlightTwoLegged(String arrivalTime, String departureTime){
            Double arrival = Double.parseDouble(arrivalTime);
            Double departure = Double.parseDouble(departureTime);

            if(departure > arrival) {
                return true;
            }
            return false;
        }

        // Add data to global counter after every reduce task
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            key.set(totalFlights);
            value.set(totalDelay);
            avgDelay = totalDelay/totalFlights;
            context.getCounter(AVGFLIGHTDELAY.TOTAL_FLIGHTS).increment(totalFlights);
            context.getCounter(AVGFLIGHTDELAY.AVERAGE_DELAY).increment((long)avgDelay);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: flightdelay <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Flight Delay");
        job.setJarByClass(FlightDelay.class);
        job.setMapperClass(FlightDelayMapper.class);
        job.setNumReduceTasks(10);
        job.setReducerClass(FlightDelayReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // Get the value of the Global counter after all reduce tasks finish executing and print them
        job.waitForCompletion(true);
        long totalFlights = job.getCounters().findCounter(AVGFLIGHTDELAY.TOTAL_FLIGHTS).getValue();
        long averageDelay = job.getCounters().findCounter(AVGFLIGHTDELAY.AVERAGE_DELAY).getValue();

        System.out.println(totalFlights + "  " + averageDelay);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

import com.opencsv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BooksTable {
    public static class BooksTableMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object offset, Text value, Context context) throws IOException, InterruptedException {
            CSVParser parser = new CSVParser(',');
            String[] record = parser.parseLine(value.toString());

            // record[] = {User_Id, Book_Id, Book_Rating, User_Id, Electronics_Id, Electronics_Rating}
            context.write(new Text(record[1]), value);
        }
    }

    public static class BooksTableReducer extends Reducer<Text, Text, Text, Text> {

        Map<String,String> usersIdAndAvgBookRating;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            usersIdAndAvgBookRating = new HashMap<String, String>();
            super.setup(context);
            String csvFile = "/Users/aravindheswaran/Documents/NEU_Coursework/Summer_2016/MapReduce/Project/Output/output_BooksAndElectronics/Output_BooksAndElectroncs.csv";
            BufferedReader br = null;
            String line = "";
            String cvsSplitBy = ",";

            try {

                br = new BufferedReader(new FileReader(csvFile));
                while ((line = br.readLine()) != null) {
                    String[] record = line.split(cvsSplitBy);
                    //record[] = {user_Id, no_Of_Books_Read_By_User, avg_rating_By_User}
                    usersIdAndAvgBookRating.put(record[0],record[1]+","+record[2]);
                }

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        public void reduce(Text bookId, Iterable<Text> records, Context context)
                throws IOException, InterruptedException{
            int noOfUser=0;
            double totalRating = 0.0d;
            Map<String,String> usersIdAndBookRatings = new HashMap<String, String>();

            for(Text record : records){
                noOfUser++;
                String[] temp =  record.toString().split(",");
                // temp[] = {User_Id, Book_Id, Book_Rating, User_Id, Electronics_Id, Electronics_Rating}
                usersIdAndBookRatings.put(temp[0],temp[2]);
                totalRating = totalRating + Double.parseDouble(temp[2]);
            }

            double avg = totalRating/noOfUser;

            for(String user_Id : usersIdAndBookRatings.keySet()){
                String str = usersIdAndAvgBookRating.get(user_Id);
                String[] data = str.split(",");
                StringBuffer result = new StringBuffer();
                result.append(user_Id).append(",");
                result.append(String.valueOf(avg)).append(",");
                result.append(data[1]).append(",");
                result.append(data[0]).append(",");
                result.append(String.valueOf(noOfUser)).append(",");
                result.append(usersIdAndBookRatings.get(user_Id));

                /*
                /result = "user_Id, average_Rating_For_This_Book_By_All_Users, avg_rating_By_This_User_For_All_Books,
                           no_Of_Books_Read_By_User, no_Of_Users_Reading_This_Book,
                           rating_Given_For_This_Book_By_This_User}
                 */
                context.write(new Text(bookId.toString()+", "), new Text(result.toString()));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            usersIdAndAvgBookRating =null;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Book Table<in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Average Books");
        job.setJarByClass(Books.class);
        job.setMapperClass(BooksTableMapper.class);
        job.setReducerClass(BooksTableReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

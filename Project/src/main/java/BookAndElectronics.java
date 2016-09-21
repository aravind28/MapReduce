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
import java.io.IOException;
import java.util.HashSet;

public class BookAndElectronics {

    public static class BookAndElectronicsMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object offset, Text value, Context context) throws IOException, InterruptedException {

            CSVParser parser = new CSVParser(',');
            String[] record = parser.parseLine(value.toString());

            StringBuffer valueToreducer = new StringBuffer();

            // record[] = {User_Id, Book_Id, Book_Rating, User_Id, Electronics_Id, Electronics_Rating}
            valueToreducer.append(record[1]).append(",").append(record[2]);
            valueToreducer.append(",");
            valueToreducer.append(record[4]).append(",").append(record[5]);

            context.write(new Text(record[0]), new Text(valueToreducer.toString()));

        }
    }

    public static class BookAndElectronicsReducer extends Reducer<Text, Text, Text, Text>{
        Double ratingForBooks = 0.0;
        Double ratingForElectronics = 0.0;

        public void reduce(Text userId, Iterable<Text> records, Context context)
                throws IOException, InterruptedException{
            HashSet<String> booksId = new HashSet<String>();
            HashSet<String> electronicsId = new HashSet<String>();

            for(Text record : records){
                String[] temp = record.toString().split(",");

                // temp[] = {User_Id, Book_Id, Book_Rating, User_Id, Electronics_Id, Electronics_Rating}
                if(booksId.add(temp[0])){
                    ratingForBooks += Double.parseDouble(temp[1]);
                }
                if(temp.length > 2){
                    if(electronicsId.add(temp[2])){
                        ratingForElectronics += Double.parseDouble(temp[3]);
                    }
                }
            }
            Double avgOfBooks = ratingForBooks/booksId.size();
            Double avgOfElectronics = 0.0;
            if(electronicsId.size() > 0){
                avgOfElectronics = ratingForElectronics/electronicsId.size();
            }
            StringBuilder result = new StringBuilder();
            result.append(String.valueOf(booksId.size()));
            result.append(", ");
            result.append(String.valueOf(avgOfBooks));
            result.append(", ");
            result.append(String.valueOf(electronicsId.size()));
            result.append(", ");
            result.append(String.valueOf(avgOfElectronics));

            context.write(new Text(userId.toString() + ", "), new Text(result.toString()));
            ratingForBooks = 0.0;
            ratingForElectronics = 0.0;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: BookAndElectronics <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Average BookAndElectronics");
        job.setJarByClass(BookAndElectronics.class);
        job.setMapperClass(BookAndElectronicsMapper.class);
        job.setReducerClass(BookAndElectronicsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

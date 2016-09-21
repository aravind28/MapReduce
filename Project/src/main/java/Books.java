import com.opencsv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;


public class Books {
    public static class BooksMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object offset, Text value, Context context) throws IOException, InterruptedException {
            CSVParser parser = new CSVParser(',');
            String[] record = parser.parseLine(value.toString());

            // record[] = {User_Id, Book_Id, Book_Rating, User_Id, Movie_Id, Movie_Rating}
            context.write(new Text(record[1]), new Text((record[2])));
        }
    }

    public static class BooksReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text bookId, Iterable<Text> ratings, Context context)
                throws IOException, InterruptedException{
            int noOfBooks = 0;
            Double bookRatings = 0.0;
            for(Text rating : ratings){
                noOfBooks++;
                bookRatings += Double.parseDouble(rating.toString());
            }
            context.write(new Text(bookId.toString()+", "), new DoubleWritable(bookRatings/noOfBooks));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Book <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Average of rating per book");
        job.setJarByClass(Books.class);
        job.setMapperClass(BooksMapper.class);
        job.setReducerClass(BooksReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

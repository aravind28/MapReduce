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

public class Movies {
    public static class MoviesMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object offset, Text value, Context context) throws IOException, InterruptedException {
            CSVParser parser = new CSVParser(',');
            String[] record = parser.parseLine(value.toString());

            // record[] = {User_Id, Book_Id, Book_Rating, User_Id, Movie_Id, Movie_Rating}
            context.write(new Text(record[4]), new Text((record[5])));
        }
    }

    public static class MoviesReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text movieId, Iterable<Text> ratings, Context context)
                throws IOException, InterruptedException{
            int noOfBooks = 0;
            Double movieRatings = 0.0;
            for(Text rating : ratings){
                if(rating.toString().length() > 0){
                    noOfBooks++;
                    movieRatings += Double.parseDouble(rating.toString());
                }
            }
            context.write(new Text(movieId.toString()+", "), new DoubleWritable(movieRatings/noOfBooks));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Movies <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Average Movies");
        job.setJarByClass(Movies.class);
        job.setMapperClass(MoviesMapper.class);
        job.setReducerClass(MoviesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

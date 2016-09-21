import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

public class WordCountPerMapTask {

    private static boolean isWordReal(String word){
        return word.toUpperCase().matches("^[M-Q].*$");
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        HashMap<Text, IntWritable> hashMapPerMapTask;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            hashMapPerMapTask = new HashMap<Text, IntWritable>();
        }

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String writableWord = itr.nextToken();
                if(isWordReal(writableWord)){
                    word = new Text();
                    word.set(writableWord);
                    if(hashMapPerMapTask.get(word) != null) {
                        IntWritable wordCount = hashMapPerMapTask.get(word);
                        IntWritable newWordCount = new IntWritable(wordCount.get() + 1);
                        hashMapPerMapTask.put(word, newWordCount);
                    }
                    else {
                        hashMapPerMapTask.put(word, one);
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            Iterator<Text> keySetItr = hashMapPerMapTask.keySet().iterator();

            while(keySetItr.hasNext()){
                Text keyToWrite = keySetItr.next();
                IntWritable valueToWrite = hashMapPerMapTask.get(keyToWrite);
                context.write(keyToWrite, valueToWrite);
            }
        }
    }

    public static class CustomPartitioner extends Partitioner<Text, IntWritable> {

        public int getPartition(Text key, IntWritable value, int numPartitions){
            String word = key.toString();

            if(word.toUpperCase().charAt(0) == 'M'){
                return (0 % numPartitions);
            }
            else if(word.toUpperCase().charAt(0) == 'N'){
                return (1 % numPartitions);
            }
            else if(word.toUpperCase().charAt(0) == 'O'){
                return (2 % numPartitions);
            }
            else if(word.toUpperCase().charAt(0) == 'P'){
                return (3 % numPartitions);
            }
            else {
                return (4 % numPartitions);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCountPerMapTask.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setPartitionerClass(CustomPartitioner.class);
        job.setNumReduceTasks(5);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
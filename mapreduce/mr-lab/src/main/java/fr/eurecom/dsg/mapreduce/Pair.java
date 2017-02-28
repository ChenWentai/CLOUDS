package fr.eurecom.dsg.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;


public class Pair extends Configured implements Tool {

    public static class PairMapper
            extends Mapper<LongWritable, Text, TextPair, IntWritable> {

        private static IntWritable ONE = new IntWritable(1);
        private static TextPair textPair = new TextPair();
        private int window = 2;
        private String pattern = "[^a-zA-Z0-9]";

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            window = context.getConfiguration().getInt("window", 2);
            System.out.println("Map here\n");
            String line = value.toString();
            line = line.replaceAll(pattern, " ");
            String[] words = line.split("\\s+"); //split string to tokens
            for(int i = 0; i < words.length; i++) {
                for(int j = 0; j < words.length; j++) {
                    if(i == j)
                        continue;
                    else if (words[j].length() == 0)
                        continue;
                    else {
                        textPair.set(new Text(words[i]), new Text(words[j]));
                        context.write(textPair, ONE);
                    }
                }

  /*            if (words.length == 0)
                  continue;
              for(int j = i - window; j < i + window + 1; j++) {
                  if(i == j || j < 0)
                      continue;
                  else if(j >= words.length)
                      break;
                  else if (words[j].length() == 0) //skip empty tokens
                      break;
                  else{
                      textPair.set(new Text(words[i]), new Text(words[j]));
                      context.write(textPair, ONE);
                  }
              }*/
            }
        }
    }

    public static class PairReducer
            extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {
        private
        static IntWritable SumValue = new IntWritable();
        @Override
        public void reduce(TextPair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SumValue.set(sum);
            context.write(key, SumValue);
        }
        // TODO: implement reducer
    }

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    public Pair(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: Pair <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }


    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        Job job = new Job(conf, "Pair");

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(PairMapper.class);
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(PairReducer.class);
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(IntWritable.class); //how to build the matrix ???

        job.setOutputFormatClass(TextOutputFormat.class);

        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[1])); //Why?? from different lib
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setNumReduceTasks(Integer.parseInt(args[0]));

        job.setJarByClass(Pair.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Pair(args), args);
        System.exit(res);
    }
}
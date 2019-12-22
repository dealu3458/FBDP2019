import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Hot {
    public static String province = "";

    public static class SalesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private IntWritable zero = new IntWritable(0);

        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            province = conf.get("province", "四川");
        }

        protected void map(LongWritable offset, Text line, Context context)
                throws IOException, InterruptedException {
            fileReader log = new fileReader(line.toString());

            if (log.getProvince().equals(province))
            {
                context.write(new Text(log.getItemID()), one);
            }
        }
    }

    public static class SalesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        protected void reduce(Text item_id, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value:values) {
                sum = sum + value.get();
            }
            context.write(item_id, new IntWritable(sum));
        }
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {
        String province = args[2];
        Configuration conf = new Configuration();
        conf.setStrings("province", province);

        Job hotStatJob = new Job(conf, "hotStatJob");
        hotStatJob.setJarByClass(Hot.class);

        hotStatJob.setMapperClass(Hot.SalesMapper.class);
        hotStatJob.setMapOutputKeyClass(Text.class);
        hotStatJob.setMapOutputValueClass(IntWritable.class);

        hotStatJob.setReducerClass(SalesReducer.class);
        hotStatJob.setOutputKeyClass(Text.class);
        hotStatJob.setOutputValueClass(IntWritable.class);

        hotStatJob.setInputFormatClass(TextInputFormat.class);
        hotStatJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(hotStatJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(hotStatJob, new Path(args[1]));

        hotStatJob.waitForCompletion(true);
        System.out.println("finished!");
    }
}

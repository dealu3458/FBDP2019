import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NewDifference {
    public static class DifferenceMapper extends Mapper<LongWritable, Text, RelationA, Text> {
        private Text relationName = new Text();

        protected void setup(Context context)
            throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            relationName.set(fileSplit.getPath().getName());
        }

        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            RelationA record = new RelationA(value.toString());
            context.write(record, relationName);
        }
    }

    public static class DifferenceReducer extends Reducer<RelationA, Text, RelationA, NullWritable> {
        String setR;

        protected void setup(Context context)
            throws IOException, InterruptedException {
            setR = context.getConfiguration().get("setR");
        }

        protected void reduce(RelationA key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            for (Text val : values) {
                if (!val.toString().equals(setR)) {
                    return ;
                }
            }
            context.write(key, NullWritable.get());
        }
    }

    public void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {
        Job differenceJob = new Job();
        differenceJob.setJobName("differenceJob");
        differenceJob.setJarByClass(NewDifference.class);
        differenceJob.getConfiguration().set("setR", args[2]);

        differenceJob.setMapperClass(DifferenceMapper.class);
        differenceJob.setMapOutputKeyClass(RelationA.class);
        differenceJob.setMapOutputValueClass(Text.class);

        differenceJob.setReducerClass(DifferenceReducer.class);
        differenceJob.setOutputKeyClass(RelationA.class);
        differenceJob.setOutputValueClass(NullWritable.class);

        differenceJob.setInputFormatClass(TextInputFormat.class);
        differenceJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(differenceJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(differenceJob, new Path(args[1]));

        differenceJob.waitForCompletion(true);
        System.out.println("finished!");
    }
}

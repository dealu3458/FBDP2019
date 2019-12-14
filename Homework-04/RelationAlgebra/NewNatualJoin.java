import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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


public class NewNatualJoin {
    public static class NaturalJoinMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private String fileName = "";
        private Text val = new Text();
        private IntWritable stuID = new IntWritable();

        protected void setup(Context context)
            throws IOException, InterruptedException {

            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            fileName = fileSplit.getPath().getName();
        }

        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            String[] arr = value.toString().split(",");
            stuID.set(Integer.parseInt(arr[0]));
            val.set(fileName + "," + value.toString());
            context.write(stuID, val);
        }
    }

    public static class NaturalJoinReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

        private Text student = new Text();
        private Text value = new Text();

        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

            List<String> grades = new ArrayList<String>();
            for (Text val : values) {
                if (val.toString().contains("Ra.txt")) {
                    student.set(studentStr(val.toString()));
                } else {
                    grades.add(gradeStr(val.toString()));
                }
            }
            for (String grade : grades) {
                value.set(convert(student.toString(), grade));
                context.write(value, NullWritable.get());
            }
        }

        private String studentStr(String line) {
            String[] arr = line.split(",");
            StringBuilder str= new StringBuilder();
            for (int i = 1; i < arr.length; i++) {
                str.append(arr[i] + ",");
            }
            return str.toString();
        }

        private String gradeStr(String line) {
            String[] arr = line.split(",");
            StringBuilder str = new StringBuilder();
            for (int i = 2; i < arr.length; i++) {
                str.append(arr[i] + ",");
            }
            return str.toString();
        }

        private String convert(String str1, String str2) {
            //str1: id, name, age, weight
            //str2: gender, height
            //required: id, name, age, gender, weight, height
            String[] arr1 = str1.split(",");
            String[] arr2 = str2.split(",");
            return arr1[0] + "," + arr1[1] + "," + arr1[2] + "," + arr2[0] + "," + arr1[3] + "," + arr2[1];
        }
    }

    public static void main(String[] args)
        throws IOException, InterruptedException, ClassNotFoundException {

        Job naturalJoinJob = new Job();
        naturalJoinJob.setJobName("naturalJoinJob");
        naturalJoinJob.setJarByClass(NewNatualJoin.class);

        naturalJoinJob.setMapperClass(NewNatualJoin.NaturalJoinMapper.class);
        naturalJoinJob.setMapOutputKeyClass(IntWritable.class);
        naturalJoinJob.setMapOutputValueClass(Text.class);

        naturalJoinJob.setReducerClass(NewNatualJoin.NaturalJoinReducer.class);
        naturalJoinJob.setOutputKeyClass(Text.class);
        naturalJoinJob.setOutputValueClass(NullWritable.class);

        naturalJoinJob.setInputFormatClass(TextInputFormat.class);
        naturalJoinJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(naturalJoinJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(naturalJoinJob, new Path(args[1]));

        naturalJoinJob.waitForCompletion(true);
        System.out.println("finished!");
    }
}

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Selection {
	public static class SelectionMap extends Mapper<LongWritable, Text, RelationA, NullWritable> {
		private int id;
		private String value;
		//private Text val = new Text();
		private int act;

		protected void setup(Context context)
				throws IOException, InterruptedException
		{
			id = context.getConfiguration().getInt("col", 0);
			value = context.getConfiguration().get("value");
			act = context.getConfiguration().getInt("act", 0);
		}

		protected void map(LongWritable offset, Text line, Context context)
				throws IOException, InterruptedException
		{
			/*
			switch(act)
			{
				case 0:
					RelationA record_0 = new RelationA(line.toString());
					if(record_0.isCondition(id, value))
						context.write(record_0, NullWritable.get());
				case 1:
					RelationA record_1 = new RelationA(line.toString());
					if(record_1.largerCondition(id, value))
						context.write(record_1, NullWritable.get());
				case 2:
					RelationA record_2 = new RelationA(line.toString());
					if(record_2.smallerCondition(id, value))
						context.write(record_2, NullWritable.get());
			}

			 */
			RelationA record = new RelationA(line.toString());
			if (act == 0){
				if(record.isCondition(id, value)) {
					context.write(record, NullWritable.get());
				}
			}
			else if (act == 1) {
				if(record.largerCondition(id, value)) {
					context.write(record, NullWritable.get());
				}
			}
			else if (act == 2) {
				if(record.smallerCondition(id, value)) {
					context.write(record, NullWritable.get());
				}
			}
		}
	}
	public void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Job selectionJob = new Job();
		selectionJob.setJobName("selectionJob");
		selectionJob.setJarByClass(Selection.class);
		selectionJob.getConfiguration().setInt("col", Integer.parseInt(args[2]));
		selectionJob.getConfiguration().set("value", args[3]);
		selectionJob.getConfiguration().setInt("act", Integer.parseInt(args[4]));
		
		selectionJob.setMapperClass(SelectionMap.class);
		selectionJob.setMapOutputKeyClass(Text.class);
		selectionJob.setMapOutputValueClass(NullWritable.class);

		selectionJob.setNumReduceTasks(0);

		/*
		selectionJob.setInputFormatClass(WholeFileInputFormat.class);
		selectionJob.setOutputFormatClass(TextOutputFormat.class);
		 */

		FileInputFormat.addInputPath(selectionJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(selectionJob, new Path(args[1]));
		
		selectionJob.waitForCompletion(true);
		System.out.println("finished!");
	}
}

package mapreduce.paths.extraction.step2;
import java.io.IOException;

import mapreduce.paths.extraction.Groups;
import mapreduce.paths.extraction.writables.keys.PathTriple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class MinNumFeatureFilter{

	public static class MapperClass extends Mapper<PathTriple, VLongWritable, PathTriple, VLongWritable> {
		private VLongWritable one=new VLongWritable(1);
		private Text orderInverter=new Text(" ");
		private PathTriple xSlotFeatures=new PathTriple(Groups.SLOT_X_FEATURE_COUNTER,orderInverter);
		private PathTriple ySlotFeatures=new PathTriple(Groups.SLOT_Y_FEATURE_COUNTER,orderInverter);

		public void map(PathTriple key, VLongWritable value, Context context)throws IOException, InterruptedException {
			if(key.getType().get()==Groups.PATH_SLOT_X_WORD){
				xSlotFeatures.setPath(key.getPath());
				context.write(xSlotFeatures, one);
			}
			else{
				ySlotFeatures.setPath(key.getPath());
				context.write(ySlotFeatures, one);
			}
			context.write(key, value);
		}
	}

	public static class Combiner extends Reducer<PathTriple,VLongWritable,PathTriple,VLongWritable> {
		private VLongWritable occurence=new VLongWritable();
		public void reduce(PathTriple key,Iterable<VLongWritable> values,Context context) throws IOException, InterruptedException{
			long sum=0;
			for(VLongWritable val:values){
				sum+=val.get();
			}
			occurence.set(sum);
			context.write(key, occurence);
		}
	}
	public static class PathTypeWordComparator extends WritableComparator{
		public PathTypeWordComparator(){
			super(PathTriple.class,true);
		}
		public int compare(WritableComparable wc1,WritableComparable wc2){
			PathTriple pt1=(PathTriple)wc1;
			PathTriple pt2=(PathTriple)wc2;
			int compare=pt1.getPath().compareTo(pt2.getPath());
			if(compare!=0){
				return compare;
			}
			else{
				compare=pt1.getType().compareTo(pt2.getType());
				if(compare!=0){
					return compare;
				}
				else{
					return pt1.getWord().compareTo(pt2.getWord());
				}
			}
		}
	}
	public static class CustomPartitioner extends Partitioner<PathTriple, VLongWritable>{
		@Override
		public int getPartition(PathTriple key, VLongWritable value,int numReduceTasks) {
			int hash=17;
			hash=hash*31+key.getPath().hashCode();
			return (hash&Integer.MAX_VALUE)%numReduceTasks;
		}
	}
	public static class Reduce extends Reducer<PathTriple,VLongWritable,PathTriple,VLongWritable> {
		private long minFeatureNum =0;
		private boolean xPassedMinFeatureNum=false;
		private boolean yPassedMinFeatureNum=false;
		private VLongWritable occurence=new VLongWritable();

		public void setup(Context context){
			Configuration conf=context.getConfiguration();
			minFeatureNum=Long.parseLong(conf.get("MinFeatureNum"));
		}

		public void reduce(PathTriple key,Iterable<VLongWritable> values,Context context) throws IOException, InterruptedException{
			long sum=0;
			for(VLongWritable val:values){
				sum+=val.get();
			}
			if(key.getType().get()==Groups.SLOT_X_FEATURE_COUNTER){
				if(sum>=minFeatureNum){
					xPassedMinFeatureNum=true;
				}
				else{
					xPassedMinFeatureNum=false;
				}
			}
			else if(xPassedMinFeatureNum==true&&key.getType().get()==Groups.SLOT_Y_FEATURE_COUNTER){
				if(sum>=minFeatureNum){
					yPassedMinFeatureNum=true;
				}
				else{
					yPassedMinFeatureNum=false;
				}
			}
			else if(xPassedMinFeatureNum==true&&yPassedMinFeatureNum==true) {
				occurence.set(sum);
				context.write(key, occurence);
			}
		}
	}

	@SuppressWarnings({ "deprecation" })
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		if(args.length==3){
			Configuration conf=new Configuration();
			conf.set("MinFeatureNum",args[2]);

			Job job=new Job(conf,"MinNumFeature Filter");
			job.setMapperClass(mapreduce.paths.extraction.step2.MinNumFeatureFilter.MapperClass.class);
			job.setJarByClass(mapreduce.paths.extraction.step2.MinNumFeatureFilter.class);

			job.setReducerClass(mapreduce.paths.extraction.step2.MinNumFeatureFilter.Reduce.class);
			job.setCombinerClass(mapreduce.paths.extraction.step2.MinNumFeatureFilter.Combiner.class);

			//job.setSortComparatorClass(PathTriple.RawComparatorPathTypeWord.class);
			
			job.setSortComparatorClass(mapreduce.paths.extraction.step2.MinNumFeatureFilter.PathTypeWordComparator.class);
			job.setPartitionerClass(mapreduce.paths.extraction.step2.MinNumFeatureFilter.CustomPartitioner.class);
			

			job.setMapOutputKeyClass(PathTriple.class);
			job.setMapOutputValueClass(VLongWritable.class);

			job.setOutputKeyClass(PathTriple.class);
			job.setOutputValueClass(VLongWritable.class);

			job.setInputFormatClass(SequenceFileInputFormat.class);
			
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setCompressOutput(job, true);
			SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1) ;
		}
		else{
			System.out.println("Not enough or too much arguments provided");
			System.exit(1);
		}
	}
}
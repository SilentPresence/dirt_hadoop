package mapreduce.paths.extraction.step3;
import java.io.IOException;

import mapreduce.paths.extraction.Groups;
import mapreduce.paths.extraction.writables.keys.PathTriple;
import mapreduce.paths.extraction.writables.values.FeatureVector;
import mapreduce.paths.extraction.writables.values.PartialTripleData;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SlotOccurenceInPathCounter{

	public static class MapperClass extends Mapper<PathTriple, VLongWritable, PathTriple, VLongWritable> {
		private Text emptyText=new Text();
		private PathTriple xSlotCounterOrderInverter=new PathTriple(emptyText,Groups.PATH_SLOT_X_WORD,emptyText);
		private PathTriple ySlotCounterOrderInverter=new PathTriple(emptyText,Groups.PATH_SLOT_Y_WORD,emptyText);

		public void map(PathTriple key, VLongWritable value, Context context)throws IOException, InterruptedException {
			if(key.getType().get()==Groups.PATH_SLOT_X_WORD){
				xSlotCounterOrderInverter.setPath(key.getPath());
				context.write(xSlotCounterOrderInverter, value);
			}
			else{
				ySlotCounterOrderInverter.setPath(key.getPath());
				context.write(ySlotCounterOrderInverter, value);
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
	public static class Reduce extends Reducer<PathTriple,VLongWritable,PathTriple,PartialTripleData> {
		private long featureCount=0;
		private PartialTripleData data=new PartialTripleData();
		private Text emptyText=new Text();

		public void reduce(PathTriple key,Iterable<VLongWritable> values,Context context) throws IOException, InterruptedException{
			long sum=0;
			for(VLongWritable val:values){
				sum+=val.get();
			}
			if(key.getWord().equals(emptyText)){
				featureCount=sum;
			}
			else{
				data.set(sum, featureCount);
				context.write(key, data);
			}
		}
	}

	@SuppressWarnings({ "deprecation" })
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		if(args.length==2){
			Configuration conf=new Configuration();

			Job job=new Job(conf,"Slot Occurence In Path Counter");
			job.setMapperClass(mapreduce.paths.extraction.step3.SlotOccurenceInPathCounter.MapperClass.class);
			job.setJarByClass(mapreduce.paths.extraction.step3.SlotOccurenceInPathCounter.class);

			job.setReducerClass(mapreduce.paths.extraction.step3.SlotOccurenceInPathCounter.Reduce.class);
			job.setCombinerClass(mapreduce.paths.extraction.step3.SlotOccurenceInPathCounter.Combiner.class);

			//job.setSortComparatorClass(PathTriple.RawComparatorPathTypeWord.class);
			job.setSortComparatorClass(mapreduce.paths.extraction.step3.SlotOccurenceInPathCounter.PathTypeWordComparator.class);
			
			job.setPartitionerClass(mapreduce.paths.extraction.step3.SlotOccurenceInPathCounter.CustomPartitioner.class);

			job.setMapOutputKeyClass(PathTriple.class);
			job.setMapOutputValueClass(VLongWritable.class);

			job.setOutputKeyClass(PathTriple.class);
			job.setOutputValueClass(PartialTripleData.class);

			job.setInputFormatClass(SequenceFileInputFormat.class);
			
			//job.setOutputFormatClass(TextOutputFormat.class);
			//TextOutputFormat.setCompressOutput(job, true);
			//TextOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);
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
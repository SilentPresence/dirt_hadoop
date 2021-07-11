package mapreduce.paths.extraction.step4;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordSlotCounter{

	public static class MapperClass extends Mapper<PathTriple, PartialTripleData, PathTriple, PartialTripleData> {
		private Text orderInverter=new Text(" ");
		//(*,Slot X,*)
		private PathTriple xSlotCounter=new PathTriple(orderInverter,Groups.PATH_SLOT_X_WORD,orderInverter);
		//(*,Slot Y,*)
		private PathTriple ySlotCounter=new PathTriple(orderInverter,Groups.PATH_SLOT_Y_WORD,orderInverter);
		//(*,Slot X,W)
		private PathTriple xSlotWordCounter=new PathTriple(orderInverter,Groups.PATH_SLOT_X_WORD);
		//(*,Slot Y,W)
		private PathTriple ySlotWordCounter=new PathTriple(orderInverter,Groups.PATH_SLOT_Y_WORD);

		public void map(PathTriple key, PartialTripleData value, Context context)throws IOException, InterruptedException {
			if(key.getType().get()==Groups.PATH_SLOT_X_WORD){
				xSlotWordCounter.setWord(key.getWord());
				context.write(xSlotCounter, value);
				context.write(xSlotWordCounter, value);
			}
			else{
				ySlotWordCounter.setWord(key.getWord());
				context.write(ySlotCounter, value);
				context.write(ySlotWordCounter, value);
			}
			context.write(key, value);
		}
	}

	public static class Combiner extends Reducer<PathTriple,PartialTripleData,PathTriple,PartialTripleData> {
		private VLongWritable occurence=new VLongWritable();
		private PartialTripleData data=new PartialTripleData();
		private Text emptyText=new Text();
		public void reduce(PathTriple key,Iterable<PartialTripleData> values,Context context) throws IOException, InterruptedException{
			if(!key.getPath().equals(emptyText)){
				context.write(key, values.iterator().next());
			}
			else{
				long sum=0;
				for(PartialTripleData val:values){
					sum+=val.getSlotWordOccurence().get();
				}
				occurence.set(sum);
				data.setSlotWordOccurence(occurence);
				context.write(key, data);
			}
		}
	}
	public static class TypeWordPathComparator extends WritableComparator{
		public TypeWordPathComparator(){
			super(PathTriple.class,true);
		}
		public int compare(WritableComparable wc1,WritableComparable wc2){
			PathTriple pt1=(PathTriple)wc1;
			PathTriple pt2=(PathTriple)wc2;
			int compare=pt1.getType().compareTo(pt2.getType());
			if(compare!=0){
				return compare;
			}
			else{
				compare=pt1.getWord().compareTo(pt2.getWord());
				if(compare!=0){
					return compare;
				}
				else{
					return pt1.getPath().compareTo(pt2.getPath());
				}
			}
		}
	}
	public static class CustomPartitioner extends Partitioner<PathTriple, PartialTripleData>{
		@Override
		public int getPartition(PathTriple key, PartialTripleData value,int numReduceTasks) {
			return (key.getType().hashCode()&Integer.MAX_VALUE)%numReduceTasks;
		}

	}
	public static class Reduce extends Reducer<PathTriple,PartialTripleData,PathTriple,FeatureVector> {
		private long fillers=0;
		private long words=0;
		private Text orderInverter=new Text(" ");
		private FeatureVector vector=new FeatureVector();

		public void reduce(PathTriple key,Iterable<PartialTripleData> values,Context context) throws IOException, InterruptedException{
			long sum=0;
			if(!key.getPath().equals(orderInverter)){
				vector.calculateVector(values.iterator().next(),words,fillers);
				context.write(key, vector);
			}
			else{
				for(PartialTripleData val:values){
					sum+=val.getSlotWordOccurence().get();
				}
				if(key.getWord().equals(orderInverter)){
					fillers=sum;
				}
				else{
					words=sum;
				}
			}
		}
	}
	@SuppressWarnings({ "deprecation" })
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		if(args.length==2){
			Configuration conf=new Configuration();

			Job job=new Job(conf,"Word Slot Counter");
			job.setMapperClass(mapreduce.paths.extraction.step4.WordSlotCounter.MapperClass.class);
			job.setJarByClass(mapreduce.paths.extraction.step4.WordSlotCounter.class);

			job.setReducerClass(mapreduce.paths.extraction.step4.WordSlotCounter.Reduce.class);

			//job.setSortComparatorClass(PathTriple.RawComparatorTypeWordPath.class);
			job.setSortComparatorClass(mapreduce.paths.extraction.step4.WordSlotCounter.TypeWordPathComparator.class);

			job.setPartitionerClass(mapreduce.paths.extraction.step4.WordSlotCounter.CustomPartitioner.class);

			job.setMapOutputKeyClass(PathTriple.class);
			job.setMapOutputValueClass(PartialTripleData.class);

			job.setOutputKeyClass(PathTriple.class);
			job.setOutputValueClass(FeatureVector.class);

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
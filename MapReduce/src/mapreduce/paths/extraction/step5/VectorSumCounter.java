package mapreduce.paths.extraction.step5;			

import java.io.IOException;

import mapreduce.paths.extraction.Groups;
import mapreduce.paths.extraction.writables.keys.PathTriple;
import mapreduce.paths.extraction.writables.values.FeatureVector;
import mapreduce.paths.extraction.writables.values.FeatureVectorSums;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class VectorSumCounter{

	public static class MapperClass extends Mapper<PathTriple, FeatureVector, PathTriple, FeatureVector> {
		private Text orderInverter=new Text(" ");
		private PathTriple xSlotWordCounter=new PathTriple(Groups.PATH_SLOT_X_WORD,orderInverter);
		private PathTriple ySlotWordCounter=new PathTriple(Groups.PATH_SLOT_Y_WORD,orderInverter);

		public void map(PathTriple key, FeatureVector value, Context context)throws IOException, InterruptedException {
			if(key.getType().get()==Groups.PATH_SLOT_X_WORD){
				xSlotWordCounter.setPath(key.getPath());
				context.write(xSlotWordCounter, value);
			}
			else{
				ySlotWordCounter.setPath(key.getPath());
				context.write(ySlotWordCounter, value);
			}
			context.write(key, value);
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
	public static class CustomPartitioner extends Partitioner<PathTriple, FeatureVector>{
		@Override
		public int getPartition(PathTriple key, FeatureVector value,int numReduceTasks) {
			int hash=17;
			hash=hash*31+key.getPath().hashCode();
			return (hash&Integer.MAX_VALUE)%numReduceTasks;
		}
	}
	
	public static class Reduce extends Reducer<PathTriple,FeatureVector,PathTriple,FeatureVectorSums> {
		private FeatureVectorSums vector=new FeatureVectorSums();	
		private DoubleWritable tfidfSum=new DoubleWritable();
		private DoubleWritable diceSum=new DoubleWritable();
		private DoubleWritable pmiSum=new DoubleWritable();
		private Text orderInverter=new Text(" ");

		public void reduce(PathTriple key,Iterable<FeatureVector> values,Context context) throws IOException, InterruptedException{
			if(key.getWord().equals(orderInverter)){
				double tfidf=0;
				double dice=0;
				double pmi=0;
				for(FeatureVector val:values){
					tfidf+=Math.pow(val.getTfidf().get(),2);
					dice+=val.getDice().get();
					pmi+=val.getPmi().get();
				}
				tfidfSum.set(Math.sqrt(tfidf));
				diceSum.set(dice);
				pmiSum.set(pmi);				
			}
			else{
				vector.set(values.iterator().next(), tfidfSum, diceSum, pmiSum);
				context.write(key,vector);
			}

		}
	}
	@SuppressWarnings({ "deprecation" })
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		if(args.length==2){
			Configuration conf=new Configuration();

			Job job=new Job(conf,"Vector Sum Counter");
			job.setMapperClass(mapreduce.paths.extraction.step5.VectorSumCounter.MapperClass.class);
			job.setJarByClass(mapreduce.paths.extraction.step5.VectorSumCounter.class);

			job.setReducerClass(mapreduce.paths.extraction.step5.VectorSumCounter.Reduce.class);

			//job.setSortComparatorClass(PathTriple.RawComparatorPathTypeWord.class);
			job.setSortComparatorClass(mapreduce.paths.extraction.step5.VectorSumCounter.PathTypeWordComparator.class);

			job.setPartitionerClass(mapreduce.paths.extraction.step5.VectorSumCounter.CustomPartitioner.class);

			job.setMapOutputKeyClass(PathTriple.class);
			job.setMapOutputValueClass(FeatureVector.class);

			job.setOutputKeyClass(PathTriple.class);
			job.setOutputValueClass(FeatureVectorSums.class);

			job.setInputFormatClass(SequenceFileInputFormat.class);
			
			job.setOutputFormatClass(TextOutputFormat.class);
			TextOutputFormat.setCompressOutput(job, true);
			TextOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);

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
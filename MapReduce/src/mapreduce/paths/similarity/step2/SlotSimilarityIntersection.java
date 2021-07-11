package mapreduce.paths.similarity.step2;
import java.io.IOException;
import mapreduce.paths.similarity.PathNumber;
import mapreduce.paths.similarity.writables.key.TemplateKey;
import mapreduce.paths.similarity.writables.key.TemplateTriple;
import mapreduce.paths.similarity.writables.key.TemplateTripleWithoutWord;
import mapreduce.paths.similarity.writables.values.CompositeValue;
import mapreduce.paths.similarity.writables.values.FeatureIntersection;
import mapreduce.paths.similarity.writables.values.FeatureVectorWithMarker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class SlotSimilarityIntersection{
	public static class MapperClass extends Mapper<TemplateKey, CompositeValue, TemplateTriple, FeatureVectorWithMarker> {
		TemplateTriple newKey=new TemplateTriple();
		FeatureVectorWithMarker data=new FeatureVectorWithMarker();
		public void map(TemplateKey key, CompositeValue value, Context context)throws IOException, InterruptedException {
			newKey.setTemplate(key);
			newKey.set(value.getType(),value.getWord());
			data.set(value);
			context.write(newKey, data);
		}
	}


	public static class Reduce extends Reducer<TemplateTriple,FeatureVectorWithMarker,TemplateTripleWithoutWord,FeatureIntersection> {
		private TemplateTripleWithoutWord newKey=new TemplateTripleWithoutWord();
		private FeatureIntersection data=new FeatureIntersection();
		private FeatureVectorWithMarker firstPath=new FeatureVectorWithMarker();
		private FeatureVectorWithMarker secondPath=new FeatureVectorWithMarker();
		public void reduce(TemplateTriple key,Iterable<FeatureVectorWithMarker> values,Context context) throws IOException, InterruptedException{
			int wordCount=0;
			for(FeatureVectorWithMarker value:values){
				wordCount++;
				if(value.getPathNumber().get()==PathNumber.leftPath){
					firstPath.set(value);
				}
				if(value.getPathNumber().get()==PathNumber.rightPath){
					secondPath.set(value);
				}
			}
			if(wordCount==2){
				data.set(firstPath, secondPath,key.getWord());
				newKey.set(key);
				context.write(newKey, data);
			}
		}
	}

	@SuppressWarnings({ "deprecation" })
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		if(args.length==2){
			Configuration conf=new Configuration();

			Job job=new Job(conf,"Path Creator");
			job.setMapperClass(mapreduce.paths.similarity.step2.SlotSimilarityIntersection.MapperClass.class);
			job.setJarByClass(mapreduce.paths.similarity.step2.SlotSimilarityIntersection.class);

			job.setReducerClass(mapreduce.paths.similarity.step2.SlotSimilarityIntersection.Reduce.class);

			job.setSortComparatorClass(TemplateTriple.RawComparatorPathTypeWord.class);

			job.setMapOutputKeyClass(TemplateTriple.class);
			job.setMapOutputValueClass(FeatureVectorWithMarker.class);

			job.setOutputKeyClass(TemplateTripleWithoutWord.class);
			job.setOutputValueClass(FeatureIntersection.class);

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
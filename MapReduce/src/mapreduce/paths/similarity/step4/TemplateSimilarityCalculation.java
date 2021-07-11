package mapreduce.paths.similarity.step4;
import java.io.IOException;
import mapreduce.paths.similarity.writables.key.TemplateTripleWithoutWord;
import mapreduce.paths.similarity.writables.values.SimilarityVector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TemplateSimilarityCalculation{
	public static class MapperClass extends Mapper<TemplateTripleWithoutWord, SimilarityVector, Text, SimilarityVector> {
		public void map(TemplateTripleWithoutWord key, SimilarityVector value, Context context)throws IOException, InterruptedException {
			context.write(key.getTemplate(), value);
		}
	}
	public static class Reduce extends Reducer<Text,SimilarityVector,Text,SimilarityVector> {
		private SimilarityVector data=new SimilarityVector();
		public void reduce(Text key,Iterable<SimilarityVector> values,Context context) throws IOException, InterruptedException{
			double cosineProduct=1;
			double coverProduct=1;
			double simProduct=1;
			for(SimilarityVector value:values){
				cosineProduct*=value.getCosine().get();
				coverProduct*=value.getCover().get();
				simProduct*=value.getSim().get();
			}
			data.set(Math.sqrt(cosineProduct), Math.sqrt(coverProduct), Math.sqrt(simProduct));
			context.write(key, data);
		}
	}
	@SuppressWarnings({ "deprecation" })
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		if(args.length==2){
			Configuration conf=new Configuration();
			
			Job job=new Job(conf,"Path Creator");
			job.setMapperClass(mapreduce.paths.similarity.step4.TemplateSimilarityCalculation.MapperClass.class);
			job.setJarByClass(mapreduce.paths.similarity.step4.TemplateSimilarityCalculation.class);
			
			

			job.setReducerClass(mapreduce.paths.similarity.step4.TemplateSimilarityCalculation.Reduce.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(SimilarityVector.class);


			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(SimilarityVector.class);


			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

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
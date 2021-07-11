package mapreduce.paths.similarity.step3;
import java.io.IOException;
import mapreduce.paths.similarity.writables.key.TemplateTripleWithoutWord;
import mapreduce.paths.similarity.writables.values.FeatureIntersection;
import mapreduce.paths.similarity.writables.values.SimilarityVector;

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

public class SlotSimilarityCalculation{
	public static class MapperClass extends Mapper<TemplateTripleWithoutWord, FeatureIntersection, TemplateTripleWithoutWord, FeatureIntersection> {
		public void map(TemplateTripleWithoutWord key, FeatureIntersection value, Context context)throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	public static class Reduce extends Reducer<TemplateTripleWithoutWord,FeatureIntersection,TemplateTripleWithoutWord,SimilarityVector> {
		private FeatureIntersection temp=null;
		private SimilarityVector data=new SimilarityVector();
		public void reduce(TemplateTripleWithoutWord key,Iterable<FeatureIntersection> values,Context context) throws IOException, InterruptedException{
			double tfidfProductSum=0;
			double diceIntersectionSum=0;
			double pmiIntersectionSum=0;
			double tfidfSumProduct=0;
			double diceFirstSum=0;
			double pmiSum=0;
			boolean first=true;
			for(FeatureIntersection value:values){
				if(first==true){
					tfidfSumProduct=value.getTfidfSumProduct().get();
					diceFirstSum=value.getDiceFirstSum().get();
					pmiSum=value.getPmiSum().get();
					first=false;
				}
				tfidfProductSum+=value.getTfidfIntersection().get();
				diceIntersectionSum+=value.getDiceIntersection().get();
				pmiIntersectionSum+=value.getPmiIntersection().get();
			}
			data.set(tfidfSumProduct,diceFirstSum,pmiSum, tfidfProductSum, diceIntersectionSum, pmiIntersectionSum);
			context.write(key, data);
		}
	}
	@SuppressWarnings({ "deprecation" })
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		if(args.length==2){
			Configuration conf=new Configuration();

			Job job=new Job(conf,"Path Creator");
			job.setMapperClass(mapreduce.paths.similarity.step3.SlotSimilarityCalculation.MapperClass.class);
			job.setJarByClass(mapreduce.paths.similarity.step3.SlotSimilarityCalculation.class);

			job.setReducerClass(mapreduce.paths.similarity.step3.SlotSimilarityCalculation.Reduce.class);

			job.setSortComparatorClass(TemplateTripleWithoutWord.RawComparatorTemplateType.class);

			job.setMapOutputKeyClass(TemplateTripleWithoutWord.class);
			job.setMapOutputValueClass(FeatureIntersection.class);

			job.setOutputKeyClass(TemplateTripleWithoutWord.class);
			job.setOutputValueClass(SimilarityVector.class);


			job.setInputFormatClass(SequenceFileInputFormat.class);
			//job.setOutputFormatClass(TextOutputFormat.class);

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
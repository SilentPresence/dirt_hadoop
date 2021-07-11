package mapreduce.paths.extraction.step1;
import java.util.HashSet;
import java.util.Set;

public class FunctionalMarkers {
	private static String[]  functionalMarkers={
		"aux","auxpass","complm","det","mark","neg","poss","ps","prt"
	};

	public static Set<String> getFunctionalMarkers(){
		Set<String> functionalMarkersSet=new HashSet<String>();
		for(String word:functionalMarkers){
			functionalMarkersSet.add(word);
		}
		return functionalMarkersSet;
	}
}

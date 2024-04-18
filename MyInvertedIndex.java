import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

// bible inverted index로 변환
public class MyInvertedIndex{
    
    public static class MyMapper extends Mapper<Object, Text, Text, Text>{
        //입력의 key, value, 출력의 key, value
        private Text word = new Text();
        // 단어를 담는 용도
        private Text docInfo = new Text();
        // 제목이랑 챕터:벌스 담을 용도
        public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException{
            //text value : 한줄단위로 들어오는 map함수의 입력
            // Context : 결과를 Hadoop으로 전달하기 위한 연결고리
            String line = value.toString();
            // Hadoop에서 사용하는 Text 자료구조를 String으로 변환
            String DocId_doc_title = line.substring(0, value.toString().indexOf("\r"));
            //처음부터 \n 나올때까지 -> 제목
            String value_raw = line.substring(value.toString().indexOf("\r")+1);
            // \n 나온 이후 => 본문
            String chapter_verse = value_raw.substring(0, value_raw.indexOf(' '));
            // DocId_doc_title과 chapter_verse 결합
            String combinedInfo = DocId_doc_title + "\t" + chapter_verse;
            docInfo.set(combinedInfo);

            String match = "[^\uAC00-\uD7A3xfea-zA-Z\\s]";  // 특수문자 제거
            StringTokenizer st = new StringTokenizer(value_raw, " '-");
            while(st.hasMoreTokens()){
                word.set(st.nextToken().replaceAll(match, "").toLowerCase());
                if(word.toString()!=""&&!word.toString().isEmpty()){
                    context.write(word, new Text(combinedInfo));
                }
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException{
            // HashMap<String, Integer> map = new HashMap<String, Integer>();
            // for(Text val: values){  
            //     if(map.containsKey(val.toString())){
            //         map.put(val.toString(), map.get(val.toString())+1); //빈도수 +1
            //     }else{
            //         map.put(val.toString(), 1);
            //     }
            // }
            StringBuilder docValueList = new StringBuilder();
            // for(String docID:map.keySet()){
            //     docValueList.append(docID+":"+map.get(docID)+" ");
            // }
            for(Text val:values){
                String[] parts = val.toString().split("\t");
                docValueList.append(parts[0]+":"+parts[1]+" ");
            }
            context.write(key, new Text(docValueList.toString()));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted index");
        job.setJarByClass(MyInvertedIndex.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    };

}


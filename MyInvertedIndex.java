import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        private String filename;

        protected void setup(Context context) throws IOException, InterruptedException{
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filename = fileSplit.getPath().getName();
            Pattern pat = Pattern.compile("^\\d+-(.+)\\.txt$");
            Matcher mat = pat.matcher(filename);//파일이름에서 제목만 따기
            if(mat.find()){
                filename = mat.group(1);
            }else{
                // 그냥 bible을 신뢰한다.
            }

        }

        public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException{
            //text value : 한줄단위로 들어오는 map함수의 입력
            // Context : 결과를 Hadoop으로 전달하기 위한 연결고리
            String line = value.toString();
            // Hadoop에서 사용하는 Text 자료구조를 String으로 변환
            
            
            Pattern pat = Pattern.compile("^(\\d+:\\d+)\\s(.*)");
            Matcher mat = pat.matcher(line);//line을 위 정규형식으로 매칭
            if(mat.find()){
                String chapter_verse = mat.group(1);
                String restOfLine = mat.group(2);

                String match = "[^\uAC00-\uD7A3xfea-zA-Z\\s]";  // 특수문자 제거
                StringTokenizer st = new StringTokenizer(restOfLine, " '-");
                // 라인 본문 부분 토큰화(단어별로 쪼개기)
                while(st.hasMoreTokens()){
                    word.set(st.nextToken().replaceAll(match, "").toLowerCase());
                    // 단어 -> 특수문자 제거
                    if(word.toString()!=""&&!word.toString().isEmpty()){
                        context.write(word, new Text(filename+":"+chapter_verse));
                        //file이름:chapter:verse 형식으로 매핑
                    }
                }
            }else{
                //그냥 넘어가기
            }

            
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException{
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            for(Text val: values){  
                if(map.containsKey(val.toString())){
                    map.put(val.toString(), map.get(val.toString())+1); //빈도수 +1
                }else{
                    map.put(val.toString(), 1);
                }
            }
            StringBuilder docValueList = new StringBuilder();
            for(String docID:map.keySet()){
                docValueList.append(docID+":"+map.get(docID)+" ");
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
        //TextInputFormat으로 설정하면 파일에서 Mapper로 한줄씩 들어온다
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));   //  Main함수에 전달되는 첫번째 인자 : 입력파일경로
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Main함수에 전달되는 두번째 인자 : 출력파일경로
        job.waitForCompletion(true);
    };

}


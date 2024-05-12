import java.io.*;
import java.util.*;

import javax.security.auth.callback.TextOutputCallback;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.w3c.dom.Text;

public class MyWordCount{
    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{
        //Mapper를 상속받을 <입력의 Key, Value, 출력의 Key, Value>
        //왜 <Object, Text>받아서 <Text, IntWritable>을 출력할까?
        // 파일은 HDFS에 업로드 되고 블록으로 저장되는데, Mapper에 블록단위로 입력되기 때문에 블록을 처리하기 위해 Object 타입을 입력으로  받는 것일까?
        
        private final static IntWritable one = new IntWritable(1);
        // static final 상수로 정의하여 공유, 1을 값으로 설정
        // 어떤 단어가 1번 나올 때 (단어, 1)로 설정하기 위함
        private Text word = new Text();
        // 단어를 담는 용도, Map에 생성되면 Map이 호출될때마다 매번 객체 생성
        // 미리 생성하여 성능 향상
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            // Text value : 한줄 단위로 들어오는 map 함수의 입력
            // Context : 결과를 Hadoop으로 전달하기 위한 연결고리
            String line = value.toString();
            // Hadoop에서 사용하는 Text자료구조를 String으로 변환
            String match = "[^\uAC00-\uD7A3xfea-zA-Z\\s]";
            line = line.replaceAll(match, "");
            // 특수기호 제거
            StringTokenizer st = new StringTokenizer(line);
            // StringTokenizer(String, 구분자)로 한 줄을 분리함
            // 구분자 없으니까 공백으로 분리
            // ex) st = [hello, hadoop, word]
            // st.nextToken() -> 하나씩 꺼냄
            while(st.hasMoreTokens()){
                word.set(st.nextToken().toLowerCase());
                context.write(word, one);
                //word.set -> 하나씩 담음
                //context.write(word, one) => ('hello' , 1)
            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        //  Reducer를 상속받음 <입력의 key, value, 출력의 Key, value>
        
        private IntWritable sumWritable = new IntWritable();
        //  key로 들어온 입력들의 개수를 합하기 위한 변수

        protected void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException{
            // key : 단어
            // values : key의 단어에 대한 value들
            int sum = 0;
            for(IntWritable val : values){
                sum += val.get();
            }
            sumWritable.set(sum);
            context.write(key, sumWritable);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCount");

        job.setJarByClass(MyWordCount.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(tre);
    }

}

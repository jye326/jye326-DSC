import java.io.IOException;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import org.w3c.dom.Text;

public class MyInvertedIndex{
    public static class MyMapper extends Mapper<Object, Text, Text, Text>{
        private Text word = new Text();
        public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException{
            String line = value.toString();
            String DocId = line.substring(0, value.toString().indexOf("\t"));
            String value_raw = line.substring(value.toString().indexOf("\t")+1);
            String match = "[^\uAC00-\uD7A3xfea-zA-Z\\s]";
            StringTokenizer st = new StringTokenizer(value_raw, " '-");
            while(st.hasMoreTokens()){
                word.set(st.nextToken().replaceAll(match, "").toLowerCase());
                if(word.toString()!=""&&!word.toString().isEmpty()){
                    context.write(word, new Text(DocId));
                }
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text>{
        protected voic reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException{
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            for(Text val: values){
                if(map.containsKey(val.toString())){
                    map.put(val.toString(), map.get(val.toString())+1);
                }else{
                    map.put(val.toString(), 1);
                }
            }
            StringBuilder docValueList = new StringBuilder();
            for(String docID:map.keySet()){
                docValueList.append(docID+":"+map.get(doxID)+" ");
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
        FileOutputFormat.addOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    };

}


package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by jizhe.pan on 2019-03-20
 */
public class WordCount {

    private static Logger LOGGER = LoggerFactory.getLogger(WordCount.class);

    // 自定义的 TokenizerMapper 类将继承自 Mapper 类，以实现相关的接口和方法
    // 在 Map 阶段将会执行其中的作业逻辑
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        // 在 MapReduce 框架中，基本数据类型都封装成了 Writable 类型
        // 因此 int 类型对应于 IntWritable 类型，在初始化时将其声明为静态常量是为了方便地使用 1 的值
        private final static IntWritable one = new IntWritable(1);

        // 声明一个 Text 类型的私有成员变量 word
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            LOGGER.info("map task begined");
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String s = itr.nextToken();
                LOGGER.info("Cur String is: {}", s);
                word.set(s);
                context.write(word, one);
            }
        }
    }

    // 自定义的 IntSumReducer 类将继承自 Reducer 类，以实现相关的接口和方法
    // 在 Reduce 阶段将会执行其中的作业逻辑
    public static class IntSumReducer
            extends Reducer<Text,IntWritable, Text, IntWritable> {

        // 声明一个 IntWritable 类型值用于存放累加结果
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            LOGGER.info("reduce task begined");
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            // 将 int 基本类型通过 set 方法赋予到结果中
            result.set(sum);
            // 写入上下文中进行保存
            context.write(key, result);
            LOGGER.info("key is {}", key);
        }
    }

    // main 方法是整个程序的入口，在这里涉及到作业（Job）的各项设置
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 注意jar包不要引错
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // job.waitForCompletion(true) 相当于开启执行任务的开关, 执行到此处时一个 MapReduce 应用才会真正地开始计算
        // 使用 System.exit 方法来告知程序运行的状态
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

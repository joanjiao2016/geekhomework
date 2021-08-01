package mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import java.io.IOException;

public class FlowBeanDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.设置jar
        job.setJarByClass(FlowBeanDriver.class);
        //3.关联mapper Reducer
        job.setMapperClass(FlowBeanMapper.class);
        job.setReducerClass(FlowBeanReducer.class);
        //4.设置 mapper 输出key 和 value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //5.设置最终数据输出key 和value 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //6.设置数据的输入和输出路径
//
//        FileInputFormat.setInputPaths(job, new Path("D:\\geekhadoop\\HTTP_20130313143750.dat"));
//        FileOutputFormat.setOutputPath(job, new Path("D:\\geekhadoop\\output"));
        FileInputFormat.setInputPaths(job, new Path("hdfs://47.101.206.249:8020/user/student/joan/HTTP_20130313143750.dat"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://47.101.206.249:8020/user/student/joan/output"));
        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
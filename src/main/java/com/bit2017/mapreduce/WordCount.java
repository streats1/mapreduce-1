package com.bit2017.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		private Text word = new Text();
		private static LongWritable one = new LongWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			StringTokenizer tokenizer = 
					 new StringTokenizer( line, "\r\n\t,|()<> ''.:" );
			while( tokenizer.hasMoreTokens() ) {
				word.set( tokenizer.nextToken().toLowerCase() );
				context.write( word, one );
			}
		}
	}

	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		private LongWritable sumWritable = new LongWritable();
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			long sum = 0;
			for( LongWritable value : values ) {
				sum += value.get();
			}
			
			sumWritable.set( sum );
			context.write( key, sumWritable );
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job( conf, "WordCount" );
		
		//1. Job Instance 초기화 작업
		job.setJarByClass( WordCount.class );
		
		//2. 맵퍼 클래스 지정
		job.setMapperClass( MyMapper.class );
		//3. 리듀서 클래스 지정
		job.setReducerClass( MyReducer.class);
		
		//4. 출력 키 타입
		job.setMapOutputKeyClass( Text.class );
		//5. 출력 밸류 타입
		job.setMapOutputValueClass( LongWritable.class );
		
		//6. 입력 파일 포맷 지정(생략 가능)
		job.setInputFormatClass( TextInputFormat.class );
		//7. 출력 파일 포맷 지정(생략 가능)
		job.setOutputFormatClass( TextOutputFormat.class );
		
		//8.입력 파일 이름 지정
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		//9. 출력 디렉토리 지정
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//10. 실행
		job.waitForCompletion( true );
	}
}

package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.lang.Integer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MatrixMultiplication {

    public static class IntTupleWritable implements Writable {
        public ArrayList<Integer> tuple;

        public IntTupleWritable() {
            tuple = new ArrayList<Integer>();
        }
        
        public IntTupleWritable(ArrayList<Integer> tuple) {
            this.tuple=tuple;            
        }

        @Override 
        public void write(DataOutput out) throws IOException {
            out.writeInt(tuple.size());
            for(int data:tuple){
               out.writeInt(data);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            int size = in.readInt();
            tuple.clear();
            for(int i=0;i<size;i++){
                tuple.add(in.readInt());
            }
        } 
	}


    public static class MultiplicationMapper 
    	extends Mapper<Object, Text, IntWritable, IntTupleWritable>{

	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {

	        StringTokenizer itr = new StringTokenizer(value.toString());
	        while (itr.hasMoreTokens()) {
	            String s = itr.nextToken();
	            String[] data = s.split(","); // M,i,j,mij
	            ArrayList<Integer> values = new ArrayList<Integer>();
                int flag;
	            values.add((int)data[0].charAt(0)); // M or N
                if(data[0].compareTo("M")==0)
	               flag = 1; // i
                else
                   flag = 2; // k
                values.add(Integer.valueOf(data[flag])); // i/k
	            values.add(Integer.valueOf(data[3])); // mij
	            context.write(new IntWritable(Integer.valueOf(data[3-flag])), new IntTupleWritable(values)); // (j,(M,i,mij))
	        }
	    }
	}

	public static class MultiplicationReducer
	        extends Reducer<IntWritable,IntTupleWritable,Text,IntWritable> {
	    private IntWritable result = new IntWritable();

	    public void reduce(IntWritable key, Iterable<IntTupleWritable> values,
	                        Context context
	                        ) throws IOException, InterruptedException {           
	       ArrayList<IntTupleWritable> matrixM = new ArrayList<IntTupleWritable>();
	       ArrayList<IntTupleWritable> matrixN = new ArrayList<IntTupleWritable>();
	       for(IntTupleWritable value: values){
	       		if(value.tuple.get(0) == (int)'M')
	       			matrixM.add((IntTupleWritable)WritableUtils.clone(value, context.getConfiguration()));
	       		else
	       			matrixN.add((IntTupleWritable)WritableUtils.clone(value, context.getConfiguration()));	
	       }
           
	       for(IntTupleWritable m: matrixM){
	       		for(IntTupleWritable n: matrixN){
	       			String s = m.tuple.get(1)+","+n.tuple.get(1);
	       			context.write(new Text(s),new IntWritable(m.tuple.get(2)*n.tuple.get(2)));
	       		}
	       }
	    }
	}

    public static class SumMapper 
        extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String s = itr.nextToken();
                context.write(new Text(s), new IntWritable(Integer.valueOf(itr.nextToken())));
            }
        }
    }
    public static class SumComparator
    	extends WritableComparator {
	
		public SumComparator() {
			super(Text.class, true);
		}
		public int compare(WritableComparable o1, WritableComparable o2) {
			Text key1 = (Text) o1;
			Text key2 = (Text) o2;

			
			String[] sk1 = key1.toString().split(",");
			String[] sk2 = key2.toString().split(",");
            IntWritable i1,i2;
			if(sk1[0].compareTo(sk2[0]) != 0 ){
				i1 = new IntWritable(Integer.valueOf(sk1[0]));
                i2 = new IntWritable(Integer.valueOf(sk2[0]));
            }
			else{
				i1 = new IntWritable(Integer.valueOf(sk1[1]));
                i2 = new IntWritable(Integer.valueOf(sk2[1]));
            }
            return i1.compareTo(i2);    

		}
	
	}
    public static class SumCombiner
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                            Context context
                            ) throws IOException, InterruptedException {           
           int sum = 0;
           for(IntWritable value: values){
                sum += value.get();
           }
           context.write(key,new IntWritable(sum));
        }
    }
    public static class SumReducer
            extends Reducer<Text,IntWritable,NullWritable,Text> {

        public void reduce(Text key, Iterable<IntWritable> values,
                            Context context
                            ) throws IOException, InterruptedException {           
           long sum = 0;
           for(IntWritable value: values){
                sum += value.get();
           }
           context.write(NullWritable.get(),new Text(key.toString()+","+sum));
        }
    }
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 3) {
        System.err.println("Usage: Matrix Multiplication <in> <out>");
        System.exit(2);
    }
    Job job = new Job(conf, "Matrix Multiplication");
    job.setJarByClass(MatrixMultiplication.class);
    job.setMapperClass(MultiplicationMapper.class);
    //job.setCombinerClass(MultiplicationReducer.class);
    job.setReducerClass(MultiplicationReducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(IntTupleWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    if(job.waitForCompletion(true)){
        Job job2 = new Job(conf, "Matrix Multiplication");
        job2.setJarByClass(MatrixMultiplication.class);
        job2.setMapperClass(SumMapper.class);
        job2.setCombinerClass(SumCombiner.class);
        job2.setSortComparatorClass(SumComparator.class);
        job2.setReducerClass(SumReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }

    }
}


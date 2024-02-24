import java.io.*;
 import java.util.*;
 import org.apache.hadoop.conf.*;
 import org.apache.hadoop.util.*;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.*;
 import org.apache.hadoop.mapreduce.*;
 import org.apache.hadoop.mapreduce.lib.input.*;
 import org.apache.hadoop.mapreduce.lib.output.*;
 class MatrixElement implements Writable {
    int tagType;
    int rowIndex;
    double elementValue;
    MatrixElement() {
        tagType = 0;
        rowIndex = 0;
        elementValue = 0.0;
    }
    MatrixElement(int tagType, int rowIndex, double elementValue) {
        this.tagType = tagType;
        this.rowIndex = rowIndex;
        this.elementValue = elementValue;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        tagType = in.readInt();
        rowIndex = in.readInt();
        elementValue = in.readDouble();
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(tagType);
        out.writeInt(rowIndex);
        out.writeDouble(elementValue);
    }
 }
 class MatrixIndex implements WritableComparable<MatrixIndex> {
    int firstIndex;
    int secondIndex;
    MatrixIndex() {
        firstIndex = 0;
        secondIndex = 0;
    }
    MatrixIndex(int firstIndex, int secondIndex) {
        this.firstIndex = firstIndex;
        this.secondIndex = secondIndex;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        firstIndex = in.readInt();
    }
        secondIndex = in.readInt();
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(firstIndex);
        out.writeInt(secondIndex);
    }
    @Override
    public int compareTo(MatrixIndex p) {
        int compareValue = Integer.compare(firstIndex, p.firstIndex);
        if (compareValue != 0) {
            return compareValue;
        } else {
            return Integer.compare(secondIndex, p.secondIndex);
        }
    }
    public String toString() {
        return (firstIndex + "," + secondIndex);
    }
 }
 public class Multiply {
    public static class MatrixMMapper extends Mapper<Object, Text, IntWritable, 
MatrixElement> {
        @Override
        public void map(Object key, Text value, Context context) throws 
IOException, InterruptedException {
            Scanner scanner = new Scanner(value.toString()).useDelimiter(",");
            int rowIndex = scanner.nextInt();
            IntWritable columnIndex = new IntWritable(scanner.nextInt());
            double elementValue = scanner.nextDouble();
            context.write(columnIndex, new MatrixElement(0, rowIndex, 
elementValue));
        }
    }
    public static class MatrixNMapper extends 
Mapper<Object,Text,IntWritable,MatrixElement> {
        @Override
        public void map(Object key, Text value, Context context) throws 
IOException, InterruptedException {
            Scanner scanner = new Scanner(value.toString()).useDelimiter(",");
            IntWritable rowIndex = new IntWritable(scanner.nextInt());
            int columnIndex = scanner.nextInt();
            double elementValue = scanner.nextDouble();
            context.write(rowIndex, new MatrixElement(1, columnIndex, 
elementValue));
        }
    }
    public static class MatrixReducer extends Reducer<IntWritable,MatrixElement, 
MatrixIndex, DoubleWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<MatrixElement> values, Context
 context) throws IOException, InterruptedException {
            Vector<MatrixElement> matrixM = new Vector<>();
            Vector<MatrixElement> matrixN = new Vector<>();
            Configuration conf = context.getConfiguration();
            for(MatrixElement element : values) {
                MatrixElement temp = 
ReflectionUtils.newInstance(MatrixElement.class, conf);
                ReflectionUtils.copy(conf, element, temp);
                if (temp.tagType == 0) {
                    matrixM.add(temp);
                } else if(temp.tagType == 1) {
                    matrixN.add(temp);
                }
            }
            for(int i=0;i<matrixM.size();i++) {
                for(int j=0;j<matrixN.size();j++) {
                    MatrixIndex indexPair = new 
MatrixIndex(matrixM.get(i).rowIndex, matrixN.get(j).rowIndex);
                    context.write(indexPair, new 
DoubleWritable(matrixM.get(i).elementValue * matrixN.get(j).elementValue));
                }
            }
        }
    }
    public static class MatrixAddMapper extends Mapper<Object, Text, MatrixIndex, 
DoubleWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws 
IOException, InterruptedException {
            Scanner scanner = new Scanner(value.toString()).useDelimiter(",");
            MatrixIndex indexPair = new MatrixIndex(scanner.nextInt(), 
scanner.nextInt());
            context.write(indexPair, new DoubleWritable(scanner.nextDouble()));
        }
    }
    public static class MatrixAddReducer extends Reducer<MatrixIndex, 
DoubleWritable, MatrixIndex, DoubleWritable> {
        @Override
        public void reduce(MatrixIndex key, Iterable<DoubleWritable> values, 
Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            for(DoubleWritable value : values) {
                sum += value.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }
    public static void main(String[] args) throws Exception {
        Job job1 = Job.getInstance();
        final Configuration conf1 = job1.getConfiguration();
        conf1.set("mapreduce.textoutputformat.separator",",");
        conf1.set("mapreduce.output.textoutputformat.separator",",");
        conf1.set("mapreduce.output.key.field.separator",",");
        conf1.set("mapred.textoutputformat.separatorText",",");
        job1.setJobName("MatrixMultiplication");
        job1.setJarByClass(Multiply.class);
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class,
 MatrixMMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class,
 MatrixNMapper.class);
        job1.setReducerClass(MatrixReducer.class);
        job1.setOutputKeyClass(MatrixIndex.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(MatrixElement.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);
        Job job2 = Job.getInstance();
        final Configuration conf2 = job2.getConfiguration();
        conf2.set("mapreduce.textoutputformat.separator",",");
        conf2.set("mapreduce.output.textoutputformat.separator",",");
        conf2.set("mapreduce.output.key.field.separator",",");
        conf2.set("mapred.textoutputformat.separatorText",",");
        job2.setJobName("MatrixAddition");
        job2.setJarByClass(Multiply.class);
        job2.setMapperClass(MatrixAddMapper.class);
        job2.setReducerClass(MatrixAddReducer.class);
        job2.setOutputKeyClass(MatrixIndex.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setMapOutputKeyClass(MatrixIndex.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        job2.waitForCompletion(true);
    }
 }
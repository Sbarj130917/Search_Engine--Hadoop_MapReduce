   //Shaily Barjatya
   //sbarjaty@uncc.edu
   package org.myorg;

   import java.io.IOException;
   import java.util.regex.Pattern;
   import org.apache.hadoop.conf.Configured;
   import org.apache.hadoop.util.Tool;
   import org.apache.hadoop.util.ToolRunner;
   import org.apache.log4j.Logger;
   import org.apache.hadoop.mapreduce.Job;
   import org.apache.hadoop.mapreduce.Mapper;
   import org.apache.hadoop.mapreduce.Reducer;
   import org.apache.hadoop.fs.Path;
   import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
   import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
   import org.apache.hadoop.io.IntWritable;
   import org.apache.hadoop.io.DoubleWritable;
   import org.apache.hadoop.io.LongWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.lib.input.FileSplit;

   public class TermFrequency extends Configured implements Tool {

      private static final Logger LOG = Logger .getLogger( TermFrequency.class);

      public static void main( String[] args) throws  Exception {

         //This will invoke the tool runner which will create and run new
         // instance of TermFrequency.
         int res  = ToolRunner .run( new TermFrequency(), args);
         System .exit(res);
      }

      public int run( String[] args) throws  Exception {

         // Creating new instance of Job object
         Job job  = Job .getInstance(getConf(), " wordcount ");

         //Setting the JAR 
         job.setJarByClass( this .getClass());

         //Setting the input and output paths for this application.
         FileInputFormat.addInputPaths(job,  args[0]);
         FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));

         //Setting the map class and reduce class for this job. Using Map and Reduce inner classes here.
         job.setMapperClass( Map .class);
         job.setReducerClass( Reduce .class);

         //Using a Text object to output the key and IntWritable object to output the value
         job.setOutputKeyClass( Text .class);
         job.setOutputValueClass( IntWritable .class);

         //When the job completes successfully, the method returns 0. When it fails, it returns 1.
         return job.waitForCompletion( true)  ? 0 : 1;
      }
      

         // Mapper
         // This Map class (an extension of Mapper) will transform the key/value input into intermediate 
         // key/value pairs to be sent to the Reducer. 
         // Text object is used to store each word as it is parsed from the input string.
      public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
         private final static IntWritable one  = new IntWritable( 1);
         private Text word  = new Text();


         //Splitting the line into individual words based on word boundaries.
         private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

         public void map( LongWritable offset,  Text lineText,  Context context)
           throws  IOException,  InterruptedException {
   	 FileSplit fileSplit = (FileSplit)context.getInputSplit();
   	 String filename= fileSplit.getPath().getName();

            //Converting the Text object to a string.
            String line  = lineText.toString().toLowerCase();
            Text currentWord  = new Text();

            //Splitting the line into individual words based on word boundaries. 
            for ( String word  : WORD_BOUNDARY .split(line)) {
               if (word.isEmpty()) {
                  continue;
               }

               //This creates a key/value pair for each word, where:
               // key = word<delimiter>filename and value= one
               currentWord  = new Text(word+"#####"+filename);

               //writing a key/value pair to the context object for this job
               context.write(currentWord,one);
            }
         }
      }

      // Reducer
      // The reducer processes each key/value pair, taking teh output of Mapper as its input.
      public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
         @Override 
         public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
            throws IOException,  InterruptedException {
            double log_tf;	
            int sum  = 0;

            //calculating term frequency of a word
            for ( IntWritable count  : counts) {
               sum  += count.get();
            }

            //calculating logarithmic term frequency
   	      log_tf=1+Math.log10(sum);

            //The result of this job will be key/value pairs where,
            // key = word<delimiter>file and value = logarithmic term frequency
            //Writing this result for a word to the reducer context object.
            context.write(word,  new DoubleWritable(log_tf));
         }
      }
   }

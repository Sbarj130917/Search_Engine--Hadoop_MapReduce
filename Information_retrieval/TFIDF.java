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
   import org.apache.hadoop.fs.FileSystem;
   import org.apache.hadoop.fs.ContentSummary;
   import java.util.ArrayList;
   import org.apache.hadoop.conf.Configuration;

   public class TFIDF extends Configured implements Tool {

      private static final Logger LOG = Logger .getLogger( TFIDF.class);

      public static void main( String[] args) throws  Exception {

          //This will invoke the tool runner which will create and run new
         // instance of TFIDF.
         int res  = ToolRunner .run( new TFIDF(), args);
         System .exit(res);
      }

      public int run( String[] args) throws  Exception {

   // <************************** First MapReduce Job Instance creation**************************>
         
         // Creating new instance of Job object for MapReduce Job1
         Job job  = Job .getInstance(getConf(), " wordcount ");
         job.setJarByClass( this .getClass());
         FileInputFormat.addInputPaths(job,  args[0]);
         FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));

         //Setting the map class and reduce class for MapReduce Job1. Using Map and Reduce inner classes here.     
         job.setMapperClass( Map .class);
         job.setReducerClass( Reduce .class);

         //Using a Text object to output the key and IntWritable object to output the value
         job.setOutputKeyClass( Text .class);
         job.setOutputValueClass( IntWritable .class);
         job.waitForCompletion( true);

   	Configuration conf = getConf();
   	FileSystem fs = FileSystem.get(conf);

      //getting the path of input folder from terminal command window
   	Path pt = new Path(args[0]);

      // calculating total number of files in a input folder
   	ContentSummary cs = fs.getContentSummary(pt);
   	long fileCount = cs.getFileCount();

      //setting the file count value in configuration file
   	conf.set("FileCount",String.valueOf(fileCount)); 
        
   // <************************** Second MapReduce Job Instance creation **************************>
   	
      // Creating new instance of Job object for Mapreduce Job2
      Job job2  = Job .getInstance(getConf(), " TFIDF ");
         job2.setJarByClass( this .getClass());
         FileInputFormat.addInputPaths(job2,  args[1]);
         FileOutputFormat.setOutputPath(job2,  new Path(args[ 2]));

         //Setting the map class and reduce class for MapReduce Job2. Using Map and Reduce inner classes here.     
         job2.setMapperClass( Map2 .class);
         job2.setReducerClass( Reduce2 .class);
         
         //Using a Text object to output both the key and the value
         job2.setOutputKeyClass( Text .class);
         job2.setOutputValueClass( Text .class);

         //When both jobs 'job' and 'job2' completes successfully, the method returns 0. When it fails, it returns 1. 
         return job2.waitForCompletion(true) ? 0 : 1;
      }

      // <************************** First Mapper **************************>
      
      //First Map Reducer will generate the term frequency
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

      // <************************** First Reducer **************************>
      
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
            context.write(new Text(word),  new DoubleWritable(log_tf));
         }
      }

       // <************************** Second Mapper **************************>
      public static class Map2 extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
         private Text word2  = new Text();
         public void map( LongWritable offset,  Text lineText,  Context context)
           throws  IOException,  InterruptedException {

   	//Splitting the line into individual words, in order to get key and value pair
       String line = lineText.toString();
   	 String arrOfStr[] = line.split("#####");
   	 String key = arrOfStr[0];
   	 String value = arrOfStr[1].replaceAll("\\s+","=");
            //writing a key/value pair to the context object for this job 
               context.write(new Text(key),new Text(value));
            }
         }
         //Will get the output fromt this mapper, key as words and for each word the 
         // value will be a posting list in form of (file name = logarithmic term frequency) pairs
      
       // <************************** Second Reducer **************************>
      public static class Reduce2 extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
          @Override
         public void reduce( Text word,  Iterable<Text> counts,  Context context)
            throws IOException,  InterruptedException {

      // Output from Mapper 2 will be taken as input in Reducer2
   	 int i=0;
   	 double df=0.0;
   	
   	 String totalDoc = context.getConfiguration().get("FileCount");
   	 double totalNoDocs = Double.valueOf(totalDoc);
   	
   	//calculating document frequency, df
   	 ArrayList<String> valueArr = new ArrayList<String>();
   	 for (Text count: counts){
   		String value= count.toString();
   		valueArr.add(value);
   		df++;
   	}
   	
   	//calculating idf, by looping in the posting list of a particular word
   	double idf = Math.log10(1.0+ (totalNoDocs / df));
   	
   	for(i=0;i<valueArr.size();i++){
   		//tokenizing the input value array to get filename and tf
   		String valueA = valueArr.get(i);
   		String fileName = valueA.substring(0, valueA.lastIndexOf("="));
   		double tf = Double.valueOf(valueA.substring(valueA.indexOf("=")+1));

   		//calculating tfidf
   		double tfidf = tf * idf;

         //The result of this job will be key/value pairs where,
            // key = word/file pair and value = td-idf score
            //Writing this result for a word to the reducer context object.
   		context.write(new Text(word+"#####"+fileName),new DoubleWritable(tfidf));
         }
      }
   }
   }

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
      import java.util.Scanner;

      public class Search extends Configured implements Tool {

         private static final Logger LOG = Logger .getLogger( Search.class);
         private static String quer = null;
         public static void main( String[] args) throws  Exception {

      	// Fetching the input user query from the terminal window
            //System.out.println("Enter your query:");
            //Scanner inputLn = new Scanner(System.in);
            //quer = inputLn.nextLine();
      	
      	//This will invoke the tool runner which will create and run new
      	// instance of Search.
            int res  = ToolRunner .run( new Search(), args);	
            System .exit(res);
         }

         public int run( String[] args) throws  Exception {


      	// Creating new instance of Job object
            Job job  = Job .getInstance(getConf(), " search ");

      	//Setting the JAR 
            job.setJarByClass( this .getClass());
	//saving input query in configurations
      	    Configuration conf = job.getConfiguration();
	//Argument 2 will be a search query
      	conf.set("query",args[2]);
	
      	//Setting the input and output paths for this application.
            FileInputFormat.addInputPaths(job,  args[0]);
            FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));

      	//Setting the map class and reduce class for this job. Using Map and Reduce inner classes here.
            job.setMapperClass( Map .class);
            job.setReducerClass( Reduce .class);

         //Using a Text object to output the key (in this case, the filename) and DoubleWritable object to 
         //output the value (in this case, the sum of td-idf score of all query term in a document)
            job.setOutputKeyClass( Text .class);
            job.setOutputValueClass( DoubleWritable .class);

         //When the job completes successfully, the method returns 0. When it fails, it returns 1.
            return job.waitForCompletion(true) ? 0 : 1;
         }

         // Mapper
         // This Map class (an extension of Mapper) will transform the key/value input into intermediate 
         // key/value pairs to be sent to the Reducer. 
         // Text object is used to store each word as it is parsed from the input string.
          public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
            public void map( LongWritable offset,  Text lineText,  Context context)
              throws  IOException,  InterruptedException {

      	//splitting query into words
      	 String queryRaw = context.getConfiguration().get("query");
	 String query = queryRaw.toLowerCase();
      	 String queryWord[] = query.split("\\s+");

         //Converting the Text object to a string.
      	 String line = lineText.toString().toLowerCase();

          //Splitting the line to get word, filename and word's tf-idf score 
      	 String arrOfStr[] = line.split("#####");
      	 String word = arrOfStr[0];
      	 String value = arrOfStr[1];
      	 String fileValue[] = value.split("\\s+");
      	 String file = fileValue[0];
      	 String tfidf = fileValue[1];

          //This creates a key/value pair for each word, where:
          // key = filename and value= tf-idf score
      	 for (String q :queryWord){
      		if(word.equalsIgnoreCase(q)){
      		   context.write(new Text(file),new DoubleWritable(Double.valueOf(tfidf)));
               	}
            	}
            }
         }

         // Reducer
         // The reducer processes each key/value pair. 
         public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
            @Override 
            public void reduce( Text fileName,  Iterable<DoubleWritable> counts,  Context context)
               throws IOException,  InterruptedException {
      	 double sum =0.0;

               //doing sum of all the tfidfs score for a query terms in a document
               for ( DoubleWritable count  : counts) {		
                    sum  += count.get();
               }
               //The result will be key/value pairs where,
               // key = filename and value = sum of tf-idf score for query term in a dcoument
               //Writing this result for a word to the reducer context object.
               context.write(fileName,  new DoubleWritable(sum));
            }
         }

      }

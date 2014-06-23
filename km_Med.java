import java.io.*;
import java.util.*;
import java.lang.Math.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.*;

//needs iterating method and stopping criteria.

public class km_Med extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
    {

	
    	//Cluster centroids stored in cached file that is read in as an ArrayList of array lists.
		//Format is 1 centroid per line, tab-delimited values --> delimiter can be changed.
		private	ArrayList<ArrayList<Double>> clusters = new ArrayList<ArrayList<Double>>();		

		@Override
		public void configure(JobConf job)
		{
			
			try
			{
				//Locate cache file and initialize reader
				Path[] localFiles = new Path[0];
				localFiles = DistributedCache.getLocalCacheFiles(job);
				BufferedReader fileIn = new BufferedReader (new FileReader(localFiles[0].toString()));

				//line will hold centroid information in String format as it is being read in
				String line;
				//cluster will hold centroid information after it has been converted into Double format

				try
				{
					while((line = fileIn.readLine()) != null)
					{
						ArrayList<Double> cluster = new ArrayList<Double>();
						//Breaks up line into individual centroid parameters
						StringTokenizer sepLine = new StringTokenizer(line);
						//Converts each parameter to double and adds it to cluster ArrayList
						while(sepLine.hasMoreTokens())
						{
							cluster.add(Double.parseDouble(sepLine.nextToken()));
						}
						
						//cluster ArrayList is added to clusters list of lists	
						clusters.add(cluster);
					}
				}

				finally
				{
					fileIn.close();
				}
			}

			catch(IOException e)
			{
				System.out.println("Input file not found");
			}
		}

		
		//Individual observations are read in and assigned to cluster with lowest SSE
		//Key is cluster number, value is DoubleWritable ArrayList
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{	    
			//convert line to string and then split into tokens 
	    	StringTokenizer line = new StringTokenizer(value.toString());
	    	//convert tokens to double and store in array -->this will be converted to Text for emission
	    	ArrayList<Double> values = new ArrayList<Double>();
			

	    	while (line.hasMoreTokens())
	    	{
	    		values.add(Double.parseDouble(line.nextToken().trim()));
	    	}


	    	//keeps track of best Euclidean Distance as algorithm cycles thru centroids. Will be updated with actual value on calculation of centroid 1.
	    	double bestD=0;
	    	//keeps track of cluster with best Euclidean Distance
	    	int bestCluster=0;

	    	
	    	//cycles thru individual cluster centroids
	    	try
			{
				for(int i=0; i<clusters.size(); i++)
	    		{
	    			//this will be used to sum SSE for cluster
	    			double dist=0;
					//pulls centroid data
	    			ArrayList<Double> centroid = new ArrayList<Double>(clusters.get(i));
	    			//calculates error and squared error on each dimension and adds it to Euclidean distance for this cluster
	    		
				
					for(int j=0; j<values.size(); j++)
	    			{
	    				try
						{
							double error = centroid.get(j)-values.get(j);
	    					double d = error*error;
	    					dist = dist + d;
	    				}
					
						catch(IndexOutOfBoundsException e)
						{
							System.out.println("I'm in the error calc loop");
						}
					}
	    		
	    			dist = Math.sqrt(dist);

	    			//if cycle has just started, then best SSE and assigned cluster are current SSE and cluster
	    			if(i == 0)
	   				{
	   					bestD = dist;
	   					bestCluster = 1;
	   				}

    				//else compare best SSE to current SSE
	   				//update SSE and assign to new cluster if current SSE is better than recorded best SSE
	   				else
	   				{
	   					if(dist < bestD)
	    				{
	    					bestD = dist;
	    					bestCluster = i+1;
	    				}
	    			}
				}
			}
			
			catch (IndexOutOfBoundsException e)
			{
				System.out.println("I'm in the cluster assignment loop");
			}
			

	    	//cluster number is key
	    	String cluster = Integer.toString(bestCluster);
	    	//values array is convert to String and then text for emission

	    	String stringValue = values.get(0).toString();
		
			try
			{
				for (int i=1; i<values.size(); i++)
	    		{
					try
					{
	   					stringValue = stringValue+","+values.get(i).toString().trim();
	   				}
					
					catch (IndexOutOfBoundsException e)
					{
						System.out.println("I'm in the concatenation loop");
					}
				}
			}

			catch (IndexOutOfBoundsException e)
			{
				System.out.println("I'm in concatenation loop");
			}
			
	    	output.collect(new Text(cluster), new Text(stringValue)); 
		}
	}
	

	//new cluster centroids are calculated by 
   public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
   {
	
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
	    
			//For each key, the reduce portion will sum up the individual variable values, count the number or observation assigned to that key and then take the average of the
			//individual variable values. These averages will make up the new cluster centroid.

	    	//This ArrayList will hold each iterator value
	    	ArrayList<Double> observation = new ArrayList<Double>();
	    	//This ArrayList will hold a running total of the individual variable values
	    	ArrayList<Double> sum = new ArrayList<Double>();
	    	//This value keeps track of the number of observations assigned to each key
	    	double count = 0;


	    	//cycle thru the values
	    	while (values.hasNext()) 
	    	{
	    		//assign individual observation
				String line = values.next().toString();
	    		String [] token = line.split("\\,|\\s+|\\t+");
				try
				{
	    			//convert tokens to double and store in array -->this will be converted to Text for emission
	    			for (int i=0; i<token.length; i++)
	    			{
	    				observation.add(Double.parseDouble(token[i].trim()));
	    			}
	    		}
			
				catch(IndexOutOfBoundsException e)
				{
					System.out.println("observation.add - index");
				}	    		
				catch(NumberFormatException e)
				{
					System.out.println("observation.add - number format");
					System.out.println(line);
				}

	    		//if first observation in values list
	    		if (count == 0)
	    		{	
	    			try
					{
						//cycle thru observation variables
	    				for(int i=0; i<observation.size(); i++)
	    				{
	    					//add individual variable value to running total and establishes size of sum array
							sum.add(observation.get(i));
						}
					}
				
					catch(IndexOutOfBoundsException e)
					{
						System.out.println("sum.add - index");
					}
					catch(NumberFormatException e)
					{
						System.out.println("sum.add - number format");
					}
	    		}

	    		//else
	    		else
	    		{
	    			try
					{
						for(int i=0; i<observation.size(); i++)
	    				{
	    					//updates running total for each value of sum	    				
	    					double newSum = sum.get(i)+observation.get(i);
	    					sum.set(i, newSum);
	    				}
					}
				
					catch(IndexOutOfBoundsException e)
					{	
						System.out.println("newSum - index");
					}
					catch(NumberFormatException e)
					{
						System.out.println("newSum - number format");
					}	
				}
	    		//increment observation count
	    		count++;
			}

	    	//This ArraList will hold new centroid information until it is ready to be converted to string and emitted
	    	ArrayList<Double> newCentroidValues = new ArrayList<Double>();
		
			try
			{
	    		//cycle thru sums
	    		for(int i=0; i<sum.size(); i++)
	    		{
	    			//calculate individual variable averages
	    			Double newValue = (sum.get(i))/count;
	    			//assign value to centroid ArrayList
	    			newCentroidValues.add(newValue);
	    		}
			}

			catch(IndexOutOfBoundsException e)
			{
				System.out.println("I'm in the cycle loop");
			}
			catch(NumberFormatException e)
			{
				System.out.println("I'm in the cycle loop");
			}

	    	//and convert to string for emission

	    	String newCentroid = newCentroidValues.get(0).toString();

			try
			{
	    		for(int i=1; i<newCentroidValues.size(); i++)
	    		{
	    			newCentroid= newCentroid+"	"+newCentroidValues.get(i).toString();
	    		}
			}
		
			catch(IndexOutOfBoundsException e)
			{
				System.out.println("I'm in the centroid loop");
			}
			catch(NumberFormatException e)
			{
				System.out.print("I'm in the centroid loop");
			}

	    	
			String k = key.toString();

	    	//output new cluster information --> needs to overwrite cached cluster data.
	    	output.collect(new Text(k), new Text(newCentroid));
		}
 	}


    public int run(String[] args) throws Exception 
    {
		JobConf conf = new JobConf(getConf(), km_Med.class); ///
		conf.setJobName("km_Med");

		DistributedCache.addCacheFile(new Path("/user/huser27/med/").toUri(), conf);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class); ///

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
    }

    public static void main(String[] args) throws Exception 
    {
		int res = ToolRunner.run(new Configuration(), new km_Med(), args);///
		System.exit(res);
    }
}

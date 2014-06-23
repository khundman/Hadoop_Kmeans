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

public class k_means_errorChecking extends Configured implements Tool 
{

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
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException 
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

	    	if(values.size()==60)
	    	{
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

	    		output.collect(new Text(cluster), new DoubleWritable(bestD)); 
			}

			else
			{
			}
		}
	}
	

	//new cluster centroids are calculated by 
	public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> 
	{
	
		public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException 
		{    
		//This value will record the current cluster's Sum Squared Error
	    	Double sSE = 0.0;
	    	//cycle thru the values
	    	while (values.hasNext()) 
	    	{
	    		sSE = sSE + values.next().get();
	    	}
	    	
			String k = key.toString();

    		//output new cluster information --> needs to overwrite cached cluster data.
    		output.collect(new Text(k), new DoubleWritable(sSE));
		}
	}


    public int run(String[] args) throws Exception 
    {
		JobConf conf = new JobConf(getConf(), k_means_errorChecking.class); ///
		conf.setJobName("k_means_errorChecking");

		DistributedCache.addCacheFile(new Path("/user/huser27/k_means/").toUri(), conf);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class); ///

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
		int res = ToolRunner.run(new Configuration(), new k_means_errorChecking(), args);///
		System.exit(res);
    }
}

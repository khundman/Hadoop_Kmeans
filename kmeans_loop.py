import os

results = []
sse =[]

def cleanup(iteration):

    os.system("hdfs dfs -getmerge Centroids Centroids")
    os.system("hdfs dfs -getmerge sse sse")
    centroidsFile = open('/home/huser27/Centroids','r')
    results.append('iteration: '+ str(iteration+1))
    for line in centroidsFile:
    	results.append(line)
    SSE = open('/home/huser27/sse','r')	
    for line in SSE:
    	sse.append('Iteration: ' + str(iteration+1) + line)
    os.system("hdfs dfs -rm -r centroids")
    os.system("hdfs dfs -copyFromLocal Centroids centroids")
    os.system("hdfs dfs -rm -r Centroids")
    os.system("hdfs dfs -rm -r sse")
    

def mapReduceKmeans():
    os.system("hadoop jar /home/huser27/k_means/k_means.jar clustering.txt Centroids")

def mapReduceError():
    os.system("hadoop jar /home/huser27/k_means/k_means_errorChecking.jar clustering.txt sse")

def main():
    for i in range(5):
        mapReduceKmeans()
        mapReduceError()
        cleanup(i)
        finalResults = open('results.txt','w')
        finalSSE = open('results_sse.txt','w')
        finalResults.write(str(results))
        finalSSE.write(str(sse))
        finalResults.close
        finalSSE.close
main()

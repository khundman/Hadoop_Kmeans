import os

results = []

def cleanup(iteration):

    os.system("hdfs dfs -getmerge med_centroids med_centroids")
    os.system("hdfs dfs -getmerge med_sse med_sse")
    centroidsFile = open('/home/huser27/med_centroids','r')
    results.append('iteration: '+ str(iteration+1))
    for line in centroidsFile:
    	results.append(line)
    SSE = open('/home/huser27/med_sse','r')	
    for line in SSE:
    	results.append(line)
    os.system("hdfs dfs -rm -r med")
    os.system("hdfs dfs -copyFromLocal med_centroids med")
    os.system("hdfs dfs -rm -r med_sse med_centroids")
    

def mapReduceKmeans():
    os.system("hadoop jar med/km_Med.jar Med.txt med_centroids") #execute

def mapReduceError():
    os.system("hadoop jar med/med_EC.jar Med.txt med_sse") #execute

def main():
    for i in range(0,5):
        mapReduceKmeans()
        mapReduceError()
        cleanup(i)
        finalResults = open('med_results.txt','w')
        finalResults.write(str(results))
        finalResults.close
main()

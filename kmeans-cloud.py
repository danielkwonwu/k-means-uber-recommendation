import sys
import re
from pyspark import SparkContext
from pyspark import SparkConf
import math
import sys

def parse(line):
    res = line.split(",")
    #return (str(res[0][1:9]), float(res[1]), float(res[2]))
    return (str(res[0]),int(res[1]),int(res[2]),float(res[3]),float(res[4]))


def distanceSquared(p1, p2):
    return math.pow(p1[0]-p2[0], 2) + math.pow(p1[1] - p2[1], 2)


def addPoints(p1, p2):
    return (p1[0] + p2[0], p1[1] + p2[1])

def closestPoint(p, points):
    index = 0
    bestIndex = 0
    closest = sys.float_info.max
    for i in range(len(points)):
      dist = distanceSquared(p, points[i])
      if dist < closest:
        closest = dist
        bestIndex = i  
    return bestIndex

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print sys.stderr, "Usage: WordCound <file>"
        exit(-1)
    else:
        sconf = SparkConf().setAppName("kmeans").set("spark.ui.port", "4141")
        sc = SparkContext(conf = sconf)

        K = int(sys.argv[1])
        inputPath = sys.argv[2]
        outputPath = sys.argv[3]

        convergeDist = .00001

        fileRdd = sc.textFile(inputPath).filter(lambda line:not line.startswith("D")).map(parse)

        date = [[1,2,3,4,5],[6,7]]
        for n in range(2):
            parsedRdd_weekDay = fileRdd.filter(lambda x:x[2] in date[n])

            for j in range(8):
                tempTime = [3*j, 3*j + 1, 3*j + 2]
                parsedRdd_time = parsedRdd_weekDay.filter(lambda x:x[1] in tempTime)
                pairLatLongRdd = parsedRdd_time.map(lambda x:(x[-2],x[-1]))

                kPoints = pairLatLongRdd.takeSample(False, K, 42)
                
                tempDist = sys.float_info.max
                while tempDist > convergeDist:
                    closestToKpointRdd = pairLatLongRdd.map(lambda point:(closestPoint(point, kPoints), (point, 1)))
                    pointCalculatedRdd = closestToKpointRdd.reduceByKey(lambda (point1, n1), (point2, n2): (addPoints(point1, point2), n1 + n2))
                    newPoints = pointCalculatedRdd.map(lambda (i, (point, n)): (i, (point[0] / n, point[1] / n)) ).collectAsMap()
                    tempDist = 0.0
                    print("########$$$$$$$")
                    for i in range(K): 
                        tempDist += distanceSquared(kPoints[i], newPoints[i])
                    print("Distance between iterations: " + str(tempDist));
                    for i in range(K):
                        kPoints[i] = newPoints[i]
                    print("########$$$$$$$$")
                print("Final center points :")
                for point in kPoints:
                    print(point)

                results = pairLatLongRdd.map(lambda x:(x[0],x[1],closestPoint(point, kPoints)))
                fileName = ""
                if i == 0:
                    fileName += "weekDay"
                else:
                    fileName += "weekends"
                fileName += str(tempTime[0])+":00~"+str(tempTime[-1])
                results.saveAsTextFile(outputPath + fileName) 
        

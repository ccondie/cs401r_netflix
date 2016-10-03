import sys
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: netflix <netflixData.txt> <userID>")
        exit(-1)
    sc = SparkContext(appName="Netflix")

    lines = sc.textFile(sys.argv[1], 1)
    print(lines)
    # counts = lines.flatMap(wordsFromLine) \
    #     .map(lambda x: (x, 1)) \
    #     .reduceByKey(add)

    # counts_sorted = counts.sortBy(lambda a: a[1])
    # counts_sorted.collect()
    # counts_sorted.saveAsTextFile(sys.argv[2])
    sc.stop()

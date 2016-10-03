import sys
from operator import add
from pyspark import SparkContext

def wordsFromLine(line):
    words = line.split(' ')
    clean_words = []
    for word in words:
        word_list = list(word)

        # clean left side
        alphaFound = False
        # while not alphaFound or len(word_list) == 0:
        while not alphaFound and len(word_list) != 0:
            if word_list[0].isalpha():
                alphaFound = True
            else:
                del word_list[0]

        # clean right side
        alphaFound = False
        # while not alphaFound or len(word_list) == 0:
        while not alphaFound and len(word_list) != 0:
            if word_list[len(word_list) - 1].isalpha():
                alphaFound = True
            else:
                del word_list[len(word_list) - 1]

        clean_words.append(''.join(word_list).lower())
    return clean_words

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount_v2 <input> <output>")
        exit(-1)
    sc = SparkContext(appName="WordCountAgain")
    lines = sc.textFile(sys.argv[1], 1)
    counts = lines.flatMap(wordsFromLine) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)
    counts_sorted = counts.sortBy(lambda a: a[1])
    counts_sorted.collect()
    counts_sorted.saveAsTextFile(sys.argv[2])
    sc.stop()
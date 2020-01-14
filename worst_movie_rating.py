# What are the worst movies in the movielens dataset?
# (worst === Lowest rated)

# frequency / sum
import pyspark
from pyspark import SparkConf, SparkContext

# creates a python dictionary; to be later used
# use to convert movie ID's to movie names while printing out
# the final result
def loadMovieNames():
    movieNames = {}
    with open("./ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2])), 1.0)

if __name__ == "__main__":
    conf = (SparkConf().setMaster("local").setAppName("Worst Movies").set("spark.executor.memory", "1g"))
    sc = SparkContext(conf = conf)

    movieNames = loadMovieNames()

    # RDD object
    # lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    lines = sc.textFile(".ml-100k/u.data")


    # Convert to a turple: (movieID, averageRating)
    movieRatings = lines.map(parseInput)

    # Reduce to (movieID, (sumOfRatings, totalRatings))
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[1]))

    # Map to (MovieId, averageRating)
    averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount: totalAndCount[0] / totalAndCount[1])

    # Sort by average rating
    sortedMovies = averageRatings.sortBy(lambda x: x[1])

    # Take the top 10 results
    results = sortedMovies.take((10))


    # Print them out:
    for result in results:
        print(movieNames[result[0]], result[1])
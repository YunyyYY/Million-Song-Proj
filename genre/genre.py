import pandas as pd
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint


def parsePoint(line):
    values = [float(x) for x in line.split(',')]
    label = 0
    if values[-1] > 0.85:
        label = 1
    return LabeledPoint(label, values)


def main():
	a = sc.textFile("ge.csv")
	data = a.map(parsePoint)

	(trainingData, testData) = data.randomSplit([0.7, 0.3])
	model = RandomForest.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={},
                                     numTrees=5, featureSubsetStrategy="auto",
                                     impurity='gini', maxDepth=4, maxBins=32)

	predictions = model.predict(testData.map(lambda x: x.features))
	labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
	testErr = labelsAndPredictions.map(lambda vp: (vp[0] - vp[1])**2).reduce(lambda x, y: x + y) /labelsAndPredictions.count()

	print("Test error is {}".format(testErr))


if __name__ == '__main__':
	main()


# Reference
# https://spark.apache.org/docs/2.1.0/mllib-ensembles.html#random-forests
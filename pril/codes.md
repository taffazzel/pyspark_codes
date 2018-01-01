>>> num = [1,2,3,4,5,6]
>>> sc.parallelize(num).reduce(lambda x,y: x+y)
lines = sc.textFile("file:///home/hdfs/test2.txt",2)
>>> lines.flatMap(lambda x:x.split(",")).collect()
[u'crazy crazy fox jumped', u'crazy fox jumped', u'fox is fast', u'fox is smart', u'dog is smart']
>>> lines.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).collect()
[(u'crazy', 3), (u'fast', 1), (u'fox', 4), (u'is', 3), (u'smart', 2), (u'jumped', 2), (u'dog', 1)]
>>> lines.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).collect()
[(u'crazy', 1), (u'crazy', 1), (u'fox', 1), (u'jumped', 1), (u'crazy', 1), (u'fox', 1), (u'jumped', 1), (u'fox', 1), (u'is', 1), (u'fast', 1), (u'fox', 1), (u'is', 1), (u'smart', 1), (u'dog', 1), (u'is', 1), (u'smart', 1)]
>>> lines.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda s:s[1],1).collect()
[(u'fast', 1), (u'dog', 1), (u'smart', 2), (u'jumped', 2), (u'crazy', 3), (u'is', 3), (u'fox', 4)]
>>> lines.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda s:s[1],0).collect()
[(u'fox', 4), (u'crazy', 3), (u'is', 3), (u'smart', 2), (u'jumped', 2), (u'fast', 1), (u'dog', 1)]
>>> lines.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortByKey(0).collect()
[(u'smart', 2), (u'jumped', 2), (u'is', 3), (u'fox', 4), (u'fast', 1), (u'dog', 1), (u'crazy', 3)]
>>> lines.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortByKey(1).collect()
[(u'crazy', 3), (u'dog', 1), (u'fast', 1), (u'fox', 4), (u'is', 3), (u'jumped', 2), (u'smart', 2)]
>>> lines.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda s:s[1]).collect()
[(u'fast', 1), (u'dog', 1), (u'smart', 2), (u'jumped', 2), (u'crazy', 3), (u'is', 3), (u'fox', 4)]
>>> lines.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda s:s[1]).takeOrdered(2)
[(u'crazy', 3), (u'dog', 1)]



>>> a = [('g1', 2), ('g2', 4), ('g3', 3), ('g4', 8)]
>>> a
[('g1', 2), ('g2', 4), ('g3', 3), ('g4', 8)]
>>> rdd = sc.parallelize(a);
>>> a
[('g1', 2), ('g2', 4), ('g3', 3), ('g4', 8)]
>>> rdd.collect()
[('g1', 2), ('g2', 4), ('g3', 3), ('g4', 8)]
>>> rdd
ParallelCollectionRDD[106] at parallelize at PythonRDD.scala:475
>>> sorted = rdd.sortByKey()
>>> sorted.collect()
[('g1', 2), ('g2', 4), ('g3', 3), ('g4', 8)]
>>> sorted = rdd.sortByKey(1)
>>> sorted.collect()
[('g1', 2), ('g2', 4), ('g3', 3), ('g4', 8)]
>>> sorted = rdd.sortByKey(0)
>>> sorted.collect()
[('g4', 8), ('g3', 3), ('g2', 4), ('g1', 2)]
>>> rdd.sortByKey().collect()
[('g1', 2), ('g2', 4), ('g3', 3), ('g4', 8)]
>>> rdd.sortByKey(1).collect()
[('g1', 2), ('g2', 4), ('g3', 3), ('g4', 8)]
>>> rdd.map(lambda x: x[1]).collect()
[2, 4, 3, 8]


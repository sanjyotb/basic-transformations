Basic Transformations

Work through the katas in this codebase to learn basic transformations, transformations on a single dataframe/dataset, with Spark + Scala.

Analyze diamonds dataset and write code for below operations:
- count number of records
- remove duplicates
- calculate average price
- calculate min and max price
- filter flawless diamonds
- groupBy clarity and calculate average price
- Add column "grade" with computation based on cut and clarity
- drop a column

Dataset: src/main/resources/diamonds.csv

Metadata: (Ref: https://www.kaggle.com/shivam2503/diamonds)
    
    Column   Description
1.  index:    counter
2.  carat:    Carat weight of the diamond
3.  cut:      Describe cut quality of the diamond. Quality in increasing order Fair, Good, Very Good, Premium, Ideal
4.  color:    Color of the diamond, with D being the best and J the worst
5.  clarity:  How obvious inclusions are within the diamond:(in order from best to worst, FL = flawless, I3= level 3 inclusions) FL,IF, VVS1, VVS2, VS1, VS2, SI1, SI2, I1, I2, I3
6.  depth:    depth % :The height of a diamond, measured from the culet to the table, divided by its average girdle diameter
7.  table:    table%: The width of the diamond's table expressed as a percentage of its average diameter
8.  price:    the price of the diamond
9.  x:        length mm
10. y:        width mm
11. z:        depth mm


How to run spark program through Intellij?
- Set main class as 
    org.apache.spark.deploy.SparkSubmit
- Set program arguments as
   --master local -- class <main_class> target/scala-2.12/<jar_name>
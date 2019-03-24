

from pyspark import SparkContext, SparkConf



def main(sc):
   print("My First Spark Application")
   

if __name__ == "__main__":

   # You may change the value for setMaster() and setAppName()
   conf = SparkConf().setMaster("local[*]").setAppName("My Spark Application")
   sc = SparkContext(conf=conf)

   #Execute Main functionality
   main(sc)




        
   

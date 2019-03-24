

from pyspark import SparkContext, SparkConf



def main(sc):
    sc.textFile("terorDev/terorTest1.csv").filter(lambda x:"TWITTER FOR IPHONE" in x.upper()).saveAsTextFile("terorDev/myFirstSpark1")
    

if __name__ == "__main__":

	
   conf = SparkConf().setMaster("local[2]").setAppName("My Spark Application")
   sc = SparkContext(conf=conf)

   #sen = "This is my first spark APPLICATION"     


   #Execute Main functionality
   main(sc)
        
   # Replace this line with your code/function:    
   #sen = "This is my first spark APPLICATION"  
   #print(testIt(sen) 	)

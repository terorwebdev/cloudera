from textblob import TextBlob
import matplotlib.pyplot as plt
from pyspark import SparkConf, SparkContext
import re
import string


##OTHER FUNCTIONS/CLASSES

def resolve_emoticon(line):
   emoticon = {
    	':-)' : 'smile',
        ':)'  : 'sad',
    	':))' : 'very happy',
    	':)'  : 'happy',
    	':((' : 'very sad',
    	':('  : 'sad',
    	':-P' : 'tongue',
    	':-o' : 'gasp',
    	'>:-)':'angry'
   }   
   for key in emoticon:
      line = line.replace(key, emoticon[key])
   return line

def abb_bm(line):
   abbreviation_bm = {
         'sy': 'saya',
         'sk': 'suka',
         'byk': 'banyak',
         'sgt' : 'sangat',
         'mcm' : 'macam',
         'bodo':'bodoh'
   }  
   abbrev = ' '.join (abbreviation_bm.get(word, word) for word in line.split())  
   return (resolve_emoticon(abbrev)) 

  

def abb_en(line):
   abbreviation_en = {
    'u': 'you',
    'thr': 'there',
    'asap': 'as soon as possible',
    'lv' : 'love',    
    'c' : 'see'
   } 
   abbrev = ' '.join (abbreviation_en.get(word, word) for word in line.split())
   return (resolve_emoticon(abbrev))  

def make_plot(pos,neg):
  
   #This function plots the counts of positive and negative words     

   Polarity = [1,2]
   LABELS = ["Positive", "Negative"]
   Count_polarity = [int(pos), int(neg)]

   plt.xlabel('Polarity')
   plt.ylabel('Count')
   plt.title('Sentiment Analysis - Lexical Based')

   plt.grid(True)

   plt.bar(Polarity, Count_polarity, align='center')
   plt.xticks(Polarity, LABELS)
   plt.show()



def remove_features():
   num_re = re.compile('(\d+)')    
   data_str = num_re.sub(' ', data_str)

   # Continue to CODE IT YOURSELF

   return data_str

   
  
   

def main(sc,filename):

   # CODE IT YOURSELF
 
   make_plot(int(pos),int(neg)) #the cast is just to ensure the value is in integer data type
   

  
   

if __name__ == "__main__":

   # Configure your Spark environment
   # CODE IT YOURSELF
  
   filename = "path/filename.txt"
  
   main(sc, filename)

   sc.stop()

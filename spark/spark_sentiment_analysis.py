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

def remove_features(data_str):
    num_re = re.compile('(\d+)')    
    data_str = num_re.sub(' ', data_str)
    url_re = re.compile(r'https?://(\S+)')
    num_re = re.compile(r'(\d+)')
    mention_re = re.compile(r'(@|#)(\w+)')
    RT_re = re.compile(r'RT(\s+)')
    data_str = str(data_str)
    data_str = RT_re.sub(' ', data_str) # remove RT
    data_str = url_re.sub(' ', data_str) # remove hyperlinks
    data_str = mention_re.sub(' ', data_str) # remove @mentions and hash
    data_str = num_re.sub(' ', data_str) # remove numerical digit
    return data_str

def toEnglish(text):
    Lang = TextBlob(text)
    Detected = Lang.detect_language()
    if (Detected == 'en'):
        return Lang
    else:
        return Lang.translate(from_lang=Detected, to='en') 
    
def toPolarity(text):
    return text.sentiment.polarity
    
   
def main(sc,filename):

    #CODE IT YOURSEL
    AllTweets = sc.textFile(filename).map(lambdax:x.split(',')).filter(lambda x:len(x) == 10).map(lambda x:x[1])
    
    TweetsRemovefeatures = AllTweets.map(lambda x:remove_features(x))
    TweetsRemoveEmoji = TweetsRemovefeatures.map(lambda x:resolve_emoticon(x))
    Tweetsabb_bm = TweetsRemoveEmoji.map(lambda x:abb_bm(line))
    Tweetsabb_en = Tweetsabb_bm.map(lambda x:abb_en(line))
    
    TweetClean = Tweetsabb_en.map(lambda x:toEnglish(x))
    
    checkPolarity = TweetClean.map(lambda x:toPolarity(x))
    
    pos = checkPolarity.filter(lambda x:x > 0.0).count()
    neg = checkPolarity.filter(lambda x:x < 0.0).count()
 
    make_plot(int(pos),int(neg)) #the cast is just to ensure the value is in integer data type
   

  
   

if __name__ == "__main__":

   # Configure your Spark environment
   # CODE IT YOURSELF
  
   filename = "terorDev/terorTest1.csv"
    conf = SparkConf().setMaster("local[2]").setAppName("My Spark Application")
    sc = SparkContext(conf=conf)
    main(sc, filename)
    sc.stop()

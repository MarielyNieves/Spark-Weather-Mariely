
# coding: utf-8

# In[7]:


import findspark
findspark.init()


# In[8]:


import pyspark


# In[11]:


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import SQLContext
from pyspark.sql.types import*


# In[ ]:


weather1= sc.textFile("C:/Users/52999/Documents/Weather/weather01.txt")
weather2= sc.textFile("C:/Users/52999/Documents/Weather/weather02.txt")
weather3= sc.textFile("C:/Users/52999/Documents/Weather/weather03.txt")
weather4= sc.textFile("C:/Users/52999/Documents/Weather/weather04.txt")
weather5= sc.textFile("C:/Users/52999/Documents/Weather/weather05.txt")
weather6= sc.textFile("C:/Users/52999/Documents/Weather/weather06.txt")


# In[ ]:


weather1.collect()


# In[ ]:


weather2.collect()


# In[55]:


from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

df = unionAll(*[weather1, weather2, weather3, weather4, weather5, weather6])


# In[56]:


df.collect()


# In[27]:





# In[26]:





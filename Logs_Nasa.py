
# coding: utf-8

# # Desafio Semantix - Logs Nasa #

# In[1]:


from pyspark.sql import SQLContext, Column
from pyspark import SparkContext
from pyspark.sql.functions import *


# In[2]:


sc = SparkContext(appName="RequestsNasa")


# In[3]:


sqlContext = SQLContext(sc)


# ## Arquivos de Log

# In[4]:


arq_log_Aug95 = sqlContext.read.text("access_log_Aug95")
arq_log_Jul95 = sqlContext.read.text("access_log_Jul95")


# In[6]:


arq_log_Aug95.show(5, truncate=False)
arq_log_Jul95.show(5, truncate=False)


# In[7]:


df_split_Aug95 = arq_log_Aug95.select(regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'),
                        regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('data'),
                        regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('path'),
                        regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('status'),
                        regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('tamanho'))

df_split_Jul95 = arq_log_Jul95.select(regexp_extract('value', r'^([^\s]+\s)', 1).alias('host'),
                        regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('data'),
                        regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('path'),
                        regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('status'),
                        regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('tamanho'))


# In[8]:


df_split_Aug95.show(2, truncate=False)
df_split_Jul95.show(2, truncate=False)


# ## df1 = Log Aug_95 & df2 = Log Jul_95

# In[9]:


df1 = df_split_Aug95.alias('df1')
df2 = df_split_Jul95.alias('df2')


# In[11]:


df1_rows = df1.filter(df1['host'].isNull() |
                            df1['data'].isNull() |
                            df1['path'].isNull() |
                            df1['status'].isNull() |
                            df1['tamanho'].isNull())

df2_rows = df2.filter(df2['host'].isNull() |
                            df2['data'].isNull() |
                            df2['path'].isNull() |
                            df2['status'].isNull() |
                            df2['tamanho'].isNull())


# In[12]:


def count_null_df1( col_name ):
  return df1.filter( df1[col_name].isNull() ).count()

def count_null_df2( col_name ):
  return df2.filter( df2[col_name].isNull() ).count()


# In[14]:


for col_name in df1.columns:
  print( col_name, " : ", count_null_df1( col_name ) )

for col_name in df2.columns:
  print( col_name, " : ", count_null_df2( col_name ) )


# In[15]:


( df1.filter(
  df1["tamanho"].isNull() )
  .groupby( "status").count() ).show()

( df2.filter(
  df2["tamanho"].isNull() )
  .groupby( "status").count() ).show()


# In[16]:


(arq_log_Aug95.filter(
      ~arq_log_Aug95["value"].rlike('''\d+$'''))
      .show( 5, truncate = False ) )

(arq_log_Jul95.filter(
      ~arq_log_Jul95["value"].rlike('''\d+$'''))
      .show( 5, truncate = False ) )


# In[23]:


df1_clean = df1.na.fill( 0 )
df1_clean.cache()

df2_clean = df2.na.fill( 0 )
df2_clean.cache()


# # Número de Hosts únicos

# In[34]:


df1_clean.select('host').distinct().count()


# In[36]:


df2_clean.select('host').distinct().count()


# # Total de erros 404

# In[38]:


df1_clean.select('status').where(df1_clean['status'] == '404').count()


# In[40]:


df2_clean.select('status').where(df2_clean['status'] == '404').count()


# # Os 5 URLs que mais causaram erro 404

# In[52]:


( df1_clean.filter(
  df1_clean["status"] == '404' )
  .groupby("host").count() ).sort(col("count").desc()).show(5)


# In[53]:


( df2_clean.filter(
  df2_clean["status"] == '404' )
  .groupby("host").count() ).sort(col("count").desc()).show(5)


# # Quantidades de erros 404 por dia

# In[98]:


(df1_clean.filter(
  df1_clean["status"] == '404' )).withColumn('Dia', concat(df1_clean.data.substr(1, 2)
                                   )).distinct().count()


# # Total de bytes retornados

# In[54]:


total_df1 = df1_clean.groupBy().sum('tamanho')
total_df1.show()


# In[55]:


total_df2 = df2_clean.groupBy().sum('tamanho')
total_df2.show()


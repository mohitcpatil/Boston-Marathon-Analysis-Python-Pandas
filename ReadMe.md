# Boston-Moscow Marathon Analysis 

## Project Datasets

Their are two sets of Datasets to complete this project Boston and Moscow Marathon.

### 1. Boston Marathon Results 2017

- **Below sample file marathon_results_2017.csv**


Columns :     {Bib	,Name,	Age,	M/F,	City,	State,	Country,	Citizen,		5K,	10K,	15K,	20K,	Half,	25K,	30K,	35K,	40K,	Pace,	Proj Time,	Official Time,	Overall,	Gender,	Division}

Data :        {11,	Kirui, Geoffrey,	24,	M,	Keringet,		KEN,			0:15:25,	0:30:28,	0:45:44,	1:01:15,	1:04:35,	1:16:59,	1:33:01,	1:48:19,	2:02:53,	0:04:57,	-	2:09:37,	1,	1,	1}
     
### 2. Moscow Marathon Full Results 2018

- **Below sample file 1_full_results_mm_2018.csv**

Columns :     {Bib,	finish_time_sec,	finish_time_result,	race,	pace_sec,	pace(minpkm),	pace(kmph),	half_pace_sec,	half_pace(minpkm),	half_pace(kmph),	gender_en,	agev	name_en,	location_city_ru,	location_city_en,	country_code_alpha_3,	flag_DNF,	flag_all_split_exist,	race_uniform_index}


Data :       {1,	8911,	2h 28min 31sec,	42.195 km,	211.1861595,	3:31 min/km	17.0 km/h,	208.3185212,	3:28 min/km,	17.3 km/h,	Female,	30,	Sardana Trofimova,	–Ø–∫—É—Ç—Å–∫,	Yakutsk,	RUS,	0,	1,	0.000132899}

### 3. Moscow Marathon Split Results 2018

- **Below sample file 1_split_results_mm_2018.csv**


Columns :     {bib,	split_name,	split,	split_time_sec,	split_time_result,	split_pace_sec,	split_pace(minpkm),	split_pace(kmph),	split_uniform_index}


Data :        {11,	Kirui, Geoffrey,	24,	M,	Keringet,		KEN,			0:15:25,	0:30:28,	0:45:44,	1:01:15,	1:04:35,	1:16:59,	1:33:01,	1:48:19,	2:02:53,	0:04:57,	-	2:09:37,	1,	1,	1}


# <center>Data Pipeline and Dashboards Assessment<center>

  
  This workspace gives a rudimentary view of how ETL pipeline gets build, transform and loaded in Spark Dataframes (temporary view) which acts as table/dataset and those dataset are used for further Analytics and Reporting. There are multiple approaches to build a pipeline and make the data ready for analysis. While making a decision I took all factors are taken into consideration to mention a few. data integrations, scalability, cost-efficiency, batch or stream processing, usabability. 

### Summary:
 1. Introduction
     - Spark Understanding 
     - ETL Architecture
     - Schema for Dataset
    
    
    
 2. Data Extraction 
     - Spark Partitions

    
    
 3. Data Transformation
    - Spark API's
    
    
    
 4. Data Loading 
    - Star Schema 
    
    
    
 5. Analytics and Dashboards
    - User Behavior Analytics
    - Campaign Analytics
    - Product Analytics

### 1. Introduction 
        
Spark popularity has grown in recent years reason being the in memory computation provided by RDD Data Model (Resilent Distributed Dataset) which eliminates the intermediate disk read/write task results in faster execution. Moreover, Spark uses (DAG) directed acyclic graph execution engine that optimizes iterative, iteractive, and on-demand computations which Hadoop's MapReduce system lacks on single dataset. Hadoop is more cost effective while it comes to handling massive dataset but the real time data processing capability gives Spark an upper hand as choice for Big data Analytics. 
    
I preferred Spark framework to implement data pipeline as our dataset isn't big enough, additionally getting the benefit of Spark having atleast 100 times faster than Hadoop MapR. In below project workspace, I will be creating a schema according to dataset column data types, this defines the structure for Dataframe (df_master) which is storing the data in tabular format in Spark dataframe and make data ready for transformation. Further making a use of temporary view to create tables, it does not persist the memory until we cache the dataset.Additionally, we can also saveAsTable and write the data into external data source.Thus, both ways dataframe and external data can be used for analysis purpose. 
    
With this project, I wanted to try a new approach which will be more efficient than what I implemented in my all previous approaches (Link references available at end of notebook), But the above approach really convinced me to go with Spark, Jupyter notebook, Plotly, matplotlib. For the Databricks community edition dataset upload size wasn't allowed beyond 2GB, So I stick to the plan and proceed with Spark framework.
    
Following architecture shows the implemented Data Pipeline flow: 
        
![ETL_Final.jpg](attachment:ETL_Final.jpg)


   


```python
#To use the RDD and creating clusters 
from pyspark import SparkContext  

#To use the Spark API's(Dataframe, Dataset, RDD)
from pyspark.sql import SparkSession 

#To use the SQL function and queries
from pyspark.sql import SQLContext 
from pyspark.sql.functions import *
from pyspark.sql.types import*
from pyspark.sql import functions as F
```


```python
# For visualization using plotly( Name converted to chart studio recently)

from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import plotly.express as px
import plotly.figure_factory as ff
import plotly.graph_objs as go
import chart_studio.plotly as py
import chart_studio
chart_studio.tools.set_credentials_file(username='mohitcpatil', api_key='M9kECA2ayXcvxG3Xdqvw')

# For plotting interactive charts 
import matplotlib.pyplot as plt 
import seaborn as sns
sns.set(context='notebook', style='whitegrid', rc={'figure.figsize': (18,4)})

# To interact with file and operating system  
import os

#To use pandas dataframe and set the column and row limits for display windows 
import pandas as pd
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)

# Visualization 
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"
```


```python
%autosave
```

    UsageError: %autosave requires an integer, got ''



```python
# For visuals to embed inside the notebook
init_notebook_mode(connected=True)
%matplotlib inline
```


<script type="text/javascript">
window.PlotlyConfig = {MathJaxConfig: 'local'};
if (window.MathJax) {MathJax.Hub.Config({SVG: {font: "STIX-Web"}});}
if (typeof require !== 'undefined') {
require.undef("plotly");
requirejs.config({
    paths: {
        'plotly': ['https://cdn.plot.ly/plotly-latest.min']
    }
});
require(['plotly'], function(Plotly) {
    window._Plotly = Plotly;
});
}
</script>




```python
# To filter and disable the warnings from modules
warnings.filterwarnings("ignore")
```

#### Spark Session


```python
# Creating the Spark session  which encapsulates all the API's (Dataframe, Dataset, RDD, SQL)
if __name__ == '__main__':
    scSpark = SparkSession.builder.appName("thredUP Assessment").getOrCreate()         
```


```python
#Apache Spark version
scSpark
```





    <div>
        <p><b>SparkSession - in-memory</b></p>

<div>
    <p><b>SparkContext</b></p>

    <p><a href="http://172.31.220.122:4044">Spark UI</a></p>

    <dl>
      <dt>Version</dt>
        <dd><code>v3.1.1</code></dd>
      <dt>Master</dt>
        <dd><code>local[*]</code></dd>
      <dt>AppName</dt>
        <dd><code>thredUP Assessment</code></dd>
    </dl>
</div>

    </div>




### User defined schema 

Our dataset does not contain header column row. Thus, we are going to create a user defined schema which provides the structure of the Dataframe. Other way can be inferring the schema from the input file and the one we are implementing is providing the Schema while reading the file. It's having added benefit making faster execution by placing the correct columns types in the dataframe.As in oppose Inferschema makes an extra pass over the file to infer the column types. 

StructType data type has collection of StructField as created in Master_Schema in below cell


```python
Master_schema = StructType([StructField("user_id",IntegerType(),True),StructField("device_id",StringType(),True),
                     StructField("event_type",StringType(),True),StructField("insert_id",StringType(),True),
                     StructField("event_time",TimestampType(),True),StructField("session_id",StringType(),True),
                     StructField("session_start",TimestampType(),True),StructField("session_end",TimestampType(),True),
                     StructField("platform",StringType(),True),StructField("uprop_app_download_ts",FloatType(),True),
                     StructField("revenue",FloatType(),True),StructField("location_lat",FloatType(),True),
                     StructField("location_lng",FloatType(),True),StructField("app_version",StringType(),True),
                     StructField("carrier",StringType(),True),StructField("os_version",StringType(),True),
                     StructField("uprop_item_order_count",IntegerType(),True),StructField("uprop_bag_req_count",IntegerType(),True),
                     StructField("uprop_last_purchase_ts",TimestampType(),True),StructField("uprop_first_purchase_ts",TimestampType(),True),
                     StructField("uprop_total_revenue",FloatType(),True),StructField("uprop_zip5",IntegerType(),True),
                     StructField("uprop_state",StringType(),True),StructField("uprop_msa_group_name",StringType(),True),
                     StructField("uprop_median_income",IntegerType(),True),StructField("uprop_hamlet_score",IntegerType(),True),
                     StructField("uprop_user_treatments",IntegerType(),True),StructField("uprop_orders_completed_lifetime",FloatType(),True),
                     StructField("uprop_user_credit_balance",IntegerType(),True),
                     StructField("uprop_user_promotion_code",IntegerType(),True),StructField("uprop_total_session_count",IntegerType(),True),
                     StructField("uprop_total_favorites",IntegerType(),True),StructField("uprop_dom_referrer",StringType(),True),
                     StructField("uprop_utm_term",StringType(),True),StructField("uprop_utm_source",StringType(),True),
                     StructField("uprop_utm_medium",StringType(),True),StructField("uprop_utm_content",StringType(),True),
                     StructField("uprop_utm_email",StringType(),True),StructField("uprop_utm_campaign",StringType(),True),
                     StructField("uprop_referral_code",IntegerType(),True),StructField("uprop_acq_signup_platform",StringType(),True),
                     StructField("uprop_acq_signup_channel",StringType(),True),StructField("uprop_acq_signup_sub_channel",StringType(),True),
                     StructField("uprop_acq_invitation_code",StringType(),True),StructField("uprop_acq_external_referrer",StringType(),True),
                     StructField("uprop_acq_signup_method_agg",StringType(),True),StructField("uprop_acq_signup_method",StringType(),True),
                     StructField("uprop_paid_acq_bucket",StringType(),True),
                     StructField("eprop_order_id",IntegerType(),True),StructField("eprop_order_type",StringType(),True),
                     StructField("eprop_pay_with",StringType(),True),StructField("eprop_order_contains_bag",FloatType(),True),
                     StructField("eprop_total_shipping_fees",IntegerType(),True),StructField("eprop_total_tax",IntegerType(),True),
                     StructField("eprop_total_discount",IntegerType(),True),StructField("eprop_item_qty",FloatType(),True),
                     StructField("eprop_order_asp",IntegerType(),True),StructField("eprop_total_cash_credits_used",FloatType(),True),
                     StructField("eprop_item_order_seq",IntegerType(),True),StructField("eprop_cart_id",IntegerType(),True),
                     StructField("eprop_app_id",StringType(),True),StructField("eprop_device_model",StringType(),True),
                     StructField("eprop_device_resolution",IntegerType(),True),StructField("eprop_link_name",StringType(),True),
                     StructField("eprop_event_mythredup_sections",StringType(),True),StructField("eprop_cleanout_section",StringType(),True),
                     StructField("eprop_prod_dept",StringType(),True),
                     StructField("eprop_pagination_no",IntegerType(),True),StructField("eprop_prod_merch_dept",StringType(),True),
                     StructField("eprop_prod_state",StringType(),True),StructField("eprop_prod_list_price",FloatType(),True),
                     StructField("eprop_prod_name",StringType(),True),StructField("eprop_prod_id",IntegerType(),True),
                     StructField("eprop_dept_tags",IntegerType(),True), 
                     StructField("eprop_sort_by",StringType(),True),StructField("eprop_prod_color",StringType(),True),
                     StructField("eprop_prod_condition",StringType(),True),StructField("eprop_prod_discount",StringType(),True),
                     StructField("eprop_prod_sizing_id",FloatType(),True),StructField("eprop_prod_category",IntegerType(),True),
                     StructField("eprop_search_keywords",StringType(),True),StructField("eprop_search_result_count",IntegerType(),True),
                     StructField("eprop_filter_type",StringType(),True),StructField("eprop_promo_code",StringType(),True),
                     StructField("eprop_promo_code_error",StringType(),True),StructField("eprop_atc_from",StringType(),True),
                     StructField("eprop_customer_status",StringType(),True),StructField("eprop_plp_url",StringType(),True)])    


```

------

### 2. Data Extraction


```python
#Read the file and infer the above created schema
#Here, we are working with tab separated value file. There are few other file formats more popular are JSON, parquet, and CSV.

df_master = scSpark.read.options(header='False', delimiter="\t")\
                        .schema(Master_schema)\
                        .csv("/Users/mohitpatil/Downloads/Chrome/thredUP/events.tsv")
```


```python
df_master.show(vertical=True)
```

    -RECORD 0-----------------------------------------------
     user_id                         | 16878712             
     device_id                       | 89ae640fa58e4f268... 
     event_type                      | View homepage        
     insert_id                       | 15c25174-96ee-467... 
     event_time                      | 2017-12-01 08:00:03  
     session_id                      | 1512115155000        
     session_start                   | 2017-12-01 07:59:15  
     session_end                     | 2017-12-01 08:02:58  
     platform                        | android              
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 4.7.9                
     carrier                         | Vodafone.de          
     os_version                      | 5.0.2                
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 3.0                  
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 450                  
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | null                 
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | null                 
     eprop_device_model              | HTCONE               
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | null                 
     eprop_prod_dept                 | null                 
     eprop_pagination_no             | null                 
     eprop_prod_merch_dept           | null                 
     eprop_prod_state                | null                 
     eprop_prod_list_price           | null                 
     eprop_prod_name                 | null                 
     eprop_prod_id                   | null                 
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | null                 
     eprop_prod_color                | null                 
     eprop_prod_condition            | null                 
     eprop_prod_discount             | null                 
     eprop_prod_sizing_id            | null                 
     eprop_prod_category             | null                 
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | null                 
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 1-----------------------------------------------
     user_id                         | 1612442              
     device_id                       | 015f605f6efc00034... 
     event_type                      | View Product list... 
     insert_id                       | 39fc5cde-2dd2-46a... 
     event_time                      | 2017-12-01 08:00:03  
     session_id                      | 1512112873000        
     session_start                   | 2017-12-01 07:21:13  
     session_end                     | 2017-12-01 08:13:54  
     platform                        | web                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 47.6801              
     location_lng                    | -122.1206            
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | 84                   
     uprop_orders_completed_lifetime | 4.0                  
     uprop_user_credit_balance       | 0                    
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 54                   
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | www.thredup.com      
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | null                 
     eprop_device_model              | null                 
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | women                
     eprop_prod_dept                 | null                 
     eprop_pagination_no             | 11                   
     eprop_prod_merch_dept           | null                 
     eprop_prod_state                | null                 
     eprop_prod_list_price           | null                 
     eprop_prod_name                 | null                 
     eprop_prod_id                   | null                 
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | Price Low to High    
     eprop_prod_color                | null                 
     eprop_prod_condition            | null                 
     eprop_prod_discount             | null                 
     eprop_prod_sizing_id            | null                 
     eprop_prod_category             | null                 
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | Sizes                
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | /products/women      
    -RECORD 2-----------------------------------------------
     user_id                         | 14204551             
     device_id                       | 015d7b2f98e500078... 
     event_type                      | View Product list... 
     insert_id                       | 6c1395ab-526b-443... 
     event_time                      | 2017-12-01 08:00:03  
     session_id                      | 1512113662000        
     session_start                   | 2017-12-01 07:34:22  
     session_end                     | 2017-12-01 08:01:07  
     platform                        | ios                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 7.7.1                
     carrier                         | Verizon              
     os_version                      | 10.1.1               
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 3.0                  
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 67                   
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | null                 
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | thredUP 7.7.1        
     eprop_device_model              | iPhone               
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | women                
     eprop_prod_dept                 | null                 
     eprop_pagination_no             | null                 
     eprop_prod_merch_dept           | null                 
     eprop_prod_state                | null                 
     eprop_prod_list_price           | null                 
     eprop_prod_name                 | null                 
     eprop_prod_id                   | null                 
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | null                 
     eprop_prod_color                | null                 
     eprop_prod_condition            | null                 
     eprop_prod_discount             | null                 
     eprop_prod_sizing_id            | null                 
     eprop_prod_category             | null                 
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | No Filters           
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 3-----------------------------------------------
     user_id                         | 24254812             
     device_id                       | 01600e16568400092... 
     event_type                      | View Product deta... 
     insert_id                       | 620db094-c933-431... 
     event_time                      | 2017-12-01 08:00:03  
     session_id                      | 1512108989000        
     session_start                   | 2017-12-01 06:16:29  
     session_end                     | 2017-12-01 08:07:10  
     platform                        | web                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 31.5105              
     location_lng                    | -97.2645             
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | 0                    
     uprop_orders_completed_lifetime | 5.0                  
     uprop_user_credit_balance       | 0                    
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 15                   
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | www.google.com       
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | null                 
     eprop_device_model              | null                 
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | null                 
     eprop_prod_dept                 | null                 
     eprop_pagination_no             | null                 
     eprop_prod_merch_dept           | women                
     eprop_prod_state                | listed               
     eprop_prod_list_price           | 70.99                
     eprop_prod_name                 | Banana Republic W... 
     eprop_prod_id                   | 27374860             
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | null                 
     eprop_prod_color                |  Gray                
     eprop_prod_condition            | Q1_only              
     eprop_prod_discount             | 0.76                 
     eprop_prod_sizing_id            | 795.0                
     eprop_prod_category             | 813                  
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | null                 
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 4-----------------------------------------------
     user_id                         | 12339891             
     device_id                       | 0160103a6c49001a1... 
     event_type                      | View Product list... 
     insert_id                       | f872f818-2d8a-493... 
     event_time                      | 2017-12-01 08:00:03  
     session_id                      | 1512114864000        
     session_start                   | 2017-12-01 07:54:24  
     session_end                     | 2017-12-01 09:51:46  
     platform                        | web                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 41.4737              
     location_lng                    | -81.5799             
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | 7                    
     uprop_orders_completed_lifetime | 4.0                  
     uprop_user_credit_balance       | 0                    
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 22                   
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | www.thredup.com      
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | null                 
     eprop_device_model              | null                 
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | shoes                
     eprop_prod_dept                 | null                 
     eprop_pagination_no             | 1                    
     eprop_prod_merch_dept           | null                 
     eprop_prod_state                | null                 
     eprop_prod_list_price           | null                 
     eprop_prod_name                 | null                 
     eprop_prod_id                   | null                 
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | Price Low to High    
     eprop_prod_color                | null                 
     eprop_prod_condition            | null                 
     eprop_prod_discount             | null                 
     eprop_prod_sizing_id            | null                 
     eprop_prod_category             | null                 
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | Sizes                
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | /products/shoes      
    -RECORD 5-----------------------------------------------
     user_id                         | 16878712             
     device_id                       | 89ae640fa58e4f268... 
     event_type                      | View homepage        
     insert_id                       | 94024fda-dd32-4d8... 
     event_time                      | 2017-12-01 08:00:03  
     session_id                      | 1512115155000        
     session_start                   | 2017-12-01 07:59:15  
     session_end                     | 2017-12-01 08:02:58  
     platform                        | android              
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 4.7.9                
     carrier                         | Vodafone.de          
     os_version                      | 5.0.2                
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 3.0                  
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 450                  
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | null                 
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | null                 
     eprop_device_model              | HTCONE               
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | null                 
     eprop_prod_dept                 | null                 
     eprop_pagination_no             | null                 
     eprop_prod_merch_dept           | null                 
     eprop_prod_state                | null                 
     eprop_prod_list_price           | null                 
     eprop_prod_name                 | null                 
     eprop_prod_id                   | null                 
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | null                 
     eprop_prod_color                | null                 
     eprop_prod_condition            | null                 
     eprop_prod_discount             | null                 
     eprop_prod_sizing_id            | null                 
     eprop_prod_category             | null                 
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | null                 
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 6-----------------------------------------------
     user_id                         | 577869               
     device_id                       | 015dc0c19e870072a... 
     event_type                      | Remove from cart     
     insert_id                       | b9feb53b-d2eb-4a7... 
     event_time                      | 2017-12-01 08:00:03  
     session_id                      | 1512108994000        
     session_start                   | 2017-12-01 06:16:34  
     session_end                     | 2017-12-01 08:40:00  
     platform                        | mobile_web           
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 37.3422              
     location_lng                    | -121.8833            
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | null                 
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 17                   
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | www.thredup.com      
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | null                 
     eprop_device_model              | null                 
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | null                 
     eprop_prod_dept                 | null                 
     eprop_pagination_no             | null                 
     eprop_prod_merch_dept           | null                 
     eprop_prod_state                | null                 
     eprop_prod_list_price           | null                 
     eprop_prod_name                 | null                 
     eprop_prod_id                   | null                 
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | null                 
     eprop_prod_color                | null                 
     eprop_prod_condition            | null                 
     eprop_prod_discount             | null                 
     eprop_prod_sizing_id            | null                 
     eprop_prod_category             | null                 
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | null                 
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 7-----------------------------------------------
     user_id                         | 91444761             
     device_id                       | 015db6f0277900139... 
     event_type                      | View Product deta... 
     insert_id                       | 09a983ac-bd80-4f8... 
     event_time                      | 2017-12-01 08:00:07  
     session_id                      | 1512110113000        
     session_start                   | 2017-12-01 06:35:13  
     session_end                     | 2017-12-01 09:28:31  
     platform                        | ios                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 7.7.4                
     carrier                         | AT&T                 
     os_version                      | 11.1.2               
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 5.0                  
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 600                  
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | null                 
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | thredUP 7.7.4        
     eprop_device_model              | iPhone               
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | null                 
     eprop_prod_dept                 | null                 
     eprop_pagination_no             | null                 
     eprop_prod_merch_dept           | women                
     eprop_prod_state                | null                 
     eprop_prod_list_price           | 7.99                 
     eprop_prod_name                 | null                 
     eprop_prod_id                   | 27746639             
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | null                 
     eprop_prod_color                | null                 
     eprop_prod_condition            | null                 
     eprop_prod_discount             | null                 
     eprop_prod_sizing_id            | null                 
     eprop_prod_category             | null                 
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | null                 
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 8-----------------------------------------------
     user_id                         | 13861851             
     device_id                       | 016010fe824b0060b... 
     event_type                      | View Product deta... 
     insert_id                       | 97be672f-d66f-4a8... 
     event_time                      | 2017-12-01 08:00:07  
     session_id                      | 1512113604000        
     session_start                   | 2017-12-01 07:33:24  
     session_end                     | 2017-12-01 11:59:36  
     platform                        | mobile_web           
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 41.9845              
     location_lng                    | -72.5571             
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | 0                    
     uprop_orders_completed_lifetime | 1.0                  
     uprop_user_credit_balance       | 0                    
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 2                    
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | www.thredup.com      
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | null                 
     eprop_device_model              | null                 
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | null                 
     eprop_prod_dept                 | null                 
     eprop_pagination_no             | null                 
     eprop_prod_merch_dept           | women                
     eprop_prod_state                | listed               
     eprop_prod_list_price           | 12.99                
     eprop_prod_name                 | Style&Co Cardigan    
     eprop_prod_id                   | 27574685             
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | null                 
     eprop_prod_color                |  Black               
     eprop_prod_condition            | Q1_only              
     eprop_prod_discount             | 0.78                 
     eprop_prod_sizing_id            | 778.0                
     eprop_prod_category             | 335                  
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | null                 
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 9-----------------------------------------------
     user_id                         | 2145675              
     device_id                       | 1EADC2EAE4E046379... 
     event_type                      | View Product deta... 
     insert_id                       | 3e600ac5-4737-413... 
     event_time                      | 2017-12-01 08:00:07  
     session_id                      | 1512114689000        
     session_start                   | 2017-12-01 07:51:29  
     session_end                     | 2017-12-01 08:16:28  
     platform                        | ios                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 7.7.8                
     carrier                         | AT&T                 
     os_version                      | 11.1.2               
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 1.0                  
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 294                  
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | null                 
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | null                 
     eprop_device_model              | iPhone9,4            
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | null                 
     eprop_prod_dept                 | null                 
     eprop_pagination_no             | null                 
     eprop_prod_merch_dept           | X                    
     eprop_prod_state                | null                 
     eprop_prod_list_price           | 50.99                
     eprop_prod_name                 | null                 
     eprop_prod_id                   | 27118744             
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | null                 
     eprop_prod_color                | null                 
     eprop_prod_condition            | null                 
     eprop_prod_discount             | null                 
     eprop_prod_sizing_id            | null                 
     eprop_prod_category             | null                 
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | null                 
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 10----------------------------------------------
     user_id                         | 36771861             
     device_id                       | 8686A9D00FE8496D8... 
     event_type                      | View Product deta... 
     insert_id                       | 2f44f560-7fcd-4ae... 
     event_time                      | 2017-12-01 08:00:07  
     session_id                      | 1512115140000        
     session_start                   | 2017-12-01 07:59:00  
     session_end                     | 2017-12-01 08:00:35  
     platform                        | ios                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 7.7.8                
     carrier                         | T-Mobile             
     os_version                      | 10.3.3               
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 3.0                  
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 63                   
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | null                 
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | null                 
     eprop_device_model              | iPhone 5c (GSM)      
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | null                 
     eprop_prod_dept                 | null                 
     eprop_pagination_no             | null                 
     eprop_prod_merch_dept           | plus                 
     eprop_prod_state                | null                 
     eprop_prod_list_price           | 15.99                
     eprop_prod_name                 | null                 
     eprop_prod_id                   | 24027609             
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | null                 
     eprop_prod_color                | null                 
     eprop_prod_condition            | null                 
     eprop_prod_discount             | null                 
     eprop_prod_sizing_id            | null                 
     eprop_prod_category             | null                 
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | null                 
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 11----------------------------------------------
     user_id                         | 3312971              
     device_id                       | 7DF597A0ACD94F80A... 
     event_type                      | View Product deta... 
     insert_id                       | 384ef8e8-5b77-480... 
     event_time                      | 2017-12-01 08:00:07  
     session_id                      | 1512112728000        
     session_start                   | 2017-12-01 07:18:48  
     session_end                     | 2017-12-01 09:34:01  
     platform                        | ios                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 7.7.8                
     carrier                         | null                 
     os_version                      | 11.0.3               
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 4.0                  
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 3                    
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | null                 
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | null                 
     eprop_device_model              | iPad Mini 2G (WiFi)  
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | null                 
     eprop_prod_dept                 | null                 
     eprop_pagination_no             | null                 
     eprop_prod_merch_dept           | X                    
     eprop_prod_state                | null                 
     eprop_prod_list_price           | 69.99                
     eprop_prod_name                 | null                 
     eprop_prod_id                   | 28562538             
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | null                 
     eprop_prod_color                | null                 
     eprop_prod_condition            | null                 
     eprop_prod_discount             | null                 
     eprop_prod_sizing_id            | null                 
     eprop_prod_category             | null                 
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | null                 
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 12----------------------------------------------
     user_id                         | 58885012             
     device_id                       | 01601095d06c0001d... 
     event_type                      | View Product list... 
     insert_id                       | d8c80316-994b-491... 
     event_time                      | 2017-12-01 08:00:07  
     session_id                      | 1512111432000        
     session_start                   | 2017-12-01 06:57:12  
     session_end                     | 2017-12-01 08:18:25  
     platform                        | mobile_web           
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 33.608               
     location_lng                    | -86.6472             
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | 0                    
     uprop_orders_completed_lifetime | 5.0                  
     uprop_user_credit_balance       | 0                    
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 7                    
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | www.thredup.com      
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | null                 
     eprop_device_model              | null                 
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | women                
     eprop_prod_dept                 | women-jeans-bootcut  
     eprop_pagination_no             | 32                   
     eprop_prod_merch_dept           | null                 
     eprop_prod_state                | null                 
     eprop_prod_list_price           | null                 
     eprop_prod_name                 | null                 
     eprop_prod_id                   | null                 
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | Newest First         
     eprop_prod_color                | null                 
     eprop_prod_condition            | null                 
     eprop_prod_discount             | null                 
     eprop_prod_sizing_id            | null                 
     eprop_prod_category             | null                 
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | No Filters           
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | /products/women/b... 
    -RECORD 13----------------------------------------------
     user_id                         | 8827704              
     device_id                       | 015ad239352a001e8... 
     event_type                      | Promo-code Error     
     insert_id                       | 2343e0ac-43ab-47f... 
     event_time                      | 2017-12-01 08:00:07  
     session_id                      | 1512113090000        
     session_start                   | 2017-12-01 07:24:50  
     session_end                     | 2017-12-01 08:24:32  
     platform                        | web                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 29.5548              
     location_lng                    | -98.6951             
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | null                 
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 120                  
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | www.thredup.com      
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | null                 
     eprop_device_model              | null                 
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | null                 
     eprop_prod_dept                 | null                 
     eprop_pagination_no             | null                 
     eprop_prod_merch_dept           | null                 
     eprop_prod_state                | null                 
     eprop_prod_list_price           | null                 
     eprop_prod_name                 | null                 
     eprop_prod_id                   | null                 
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | null                 
     eprop_prod_color                | null                 
     eprop_prod_condition            | null                 
     eprop_prod_discount             | null                 
     eprop_prod_sizing_id            | null                 
     eprop_prod_category             | null                 
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | null                 
     eprop_promo_code                | thrifty15            
     eprop_promo_code_error          | This promotion co... 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 14----------------------------------------------
     user_id                         | 3062301              
     device_id                       | 7B275388ADFA4439A... 
     event_type                      | View Product deta... 
     insert_id                       | e3a00ca7-6a1a-464... 
     event_time                      | 2017-12-01 08:00:08  
     session_id                      | 1512114236000        
     session_start                   | 2017-12-01 07:43:56  
     session_end                     | 2017-12-01 08:09:05  
     platform                        | ios                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 7.7.8                
     carrier                         | AT&T                 
     os_version                      | 11.1.2               
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 1.0                  
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 65                   
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | null                 
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | null                 
     eprop_device_model              | iPhone 6s            
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | null                 
     eprop_prod_dept                 | null                 
     eprop_pagination_no             | null                 
     eprop_prod_merch_dept           | women                
     eprop_prod_state                | null                 
     eprop_prod_list_price           | 26.99                
     eprop_prod_name                 | null                 
     eprop_prod_id                   | 28364450             
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | null                 
     eprop_prod_color                | null                 
     eprop_prod_condition            | null                 
     eprop_prod_discount             | null                 
     eprop_prod_sizing_id            | null                 
     eprop_prod_category             | null                 
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | null                 
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 15----------------------------------------------
     user_id                         | 59277201             
     device_id                       | 059C52DBEC45425A9... 
     event_type                      | View Product list... 
     insert_id                       | 551c8c11-6da0-4bd... 
     event_time                      | 2017-12-01 08:00:08  
     session_id                      | 1512114587000        
     session_start                   | 2017-12-01 07:49:47  
     session_end                     | 2017-12-01 08:12:55  
     platform                        | ios                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 7.7.8                
     carrier                         | Verizon              
     os_version                      | 11.0.3               
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 1.0                  
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 525                  
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | null                 
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | null                 
     eprop_device_model              | iPhone 6 Plus        
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | women                
     eprop_prod_dept                 | null                 
     eprop_pagination_no             | null                 
     eprop_prod_merch_dept           | null                 
     eprop_prod_state                | null                 
     eprop_prod_list_price           | null                 
     eprop_prod_name                 | null                 
     eprop_prod_id                   | null                 
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | null                 
     eprop_prod_color                | null                 
     eprop_prod_condition            | null                 
     eprop_prod_discount             | null                 
     eprop_prod_sizing_id            | null                 
     eprop_prod_category             | null                 
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | No Filters           
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 16----------------------------------------------
     user_id                         | 16878712             
     device_id                       | 89ae640fa58e4f268... 
     event_type                      | View Product list... 
     insert_id                       | 656a64b9-06d9-43a... 
     event_time                      | 2017-12-01 08:00:08  
     session_id                      | 1512115155000        
     session_start                   | 2017-12-01 07:59:15  
     session_end                     | 2017-12-01 08:02:58  
     platform                        | android              
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 4.7.9                
     carrier                         | Vodafone.de          
     os_version                      | 5.0.2                
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 3.0                  
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 450                  
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | null                 
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | null                 
     eprop_device_model              | HTCONE               
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | women                
     eprop_prod_dept                 | null                 
     eprop_pagination_no             | null                 
     eprop_prod_merch_dept           | null                 
     eprop_prod_state                | null                 
     eprop_prod_list_price           | null                 
     eprop_prod_name                 | null                 
     eprop_prod_id                   | null                 
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | null                 
     eprop_prod_color                | null                 
     eprop_prod_condition            | null                 
     eprop_prod_discount             | null                 
     eprop_prod_sizing_id            | null                 
     eprop_prod_category             | null                 
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | No Filters           
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 17----------------------------------------------
     user_id                         | 62334512             
     device_id                       | 015ec1ffc2f80011e... 
     event_type                      | View Product list... 
     insert_id                       | b4385e20-41f5-4d0... 
     event_time                      | 2017-12-01 08:00:08  
     session_id                      | 1512111731000        
     session_start                   | 2017-12-01 07:02:11  
     session_end                     | 2017-12-01 08:12:45  
     platform                        | web                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 35.7332              
     location_lng                    | 139.3418             
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | 0                    
     uprop_orders_completed_lifetime | 3.0                  
     uprop_user_credit_balance       | 0                    
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 11                   
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | www.thredup.com      
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | null                 
     eprop_device_model              | null                 
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | women                
     eprop_prod_dept                 | women-outerwear      
     eprop_pagination_no             | null                 
     eprop_prod_merch_dept           | null                 
     eprop_prod_state                | null                 
     eprop_prod_list_price           | null                 
     eprop_prod_name                 | null                 
     eprop_prod_id                   | null                 
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | Newest First         
     eprop_prod_color                | null                 
     eprop_prod_condition            | null                 
     eprop_prod_discount             | null                 
     eprop_prod_sizing_id            | null                 
     eprop_prod_category             | null                 
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | Sizes                
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | /products/women/o... 
    -RECORD 18----------------------------------------------
     user_id                         | 24254812             
     device_id                       | 01600e16568400092... 
     event_type                      | View Cart page       
     insert_id                       | 4654edff-73d3-4e0... 
     event_time                      | 2017-12-01 08:00:08  
     session_id                      | 1512108989000        
     session_start                   | 2017-12-01 06:16:29  
     session_end                     | 2017-12-01 08:07:10  
     platform                        | web                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 31.5105              
     location_lng                    | -97.2645             
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | 0                    
     uprop_orders_completed_lifetime | 5.0                  
     uprop_user_credit_balance       | 0                    
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 15                   
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | www.google.com       
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | 38676982             
     eprop_app_id                    | null                 
     eprop_device_model              | null                 
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | null                 
     eprop_prod_dept                 | null                 
     eprop_pagination_no             | null                 
     eprop_prod_merch_dept           | null                 
     eprop_prod_state                | null                 
     eprop_prod_list_price           | null                 
     eprop_prod_name                 | null                 
     eprop_prod_id                   | null                 
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | null                 
     eprop_prod_color                | null                 
     eprop_prod_condition            | null                 
     eprop_prod_discount             | null                 
     eprop_prod_sizing_id            | null                 
     eprop_prod_category             | null                 
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | null                 
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 19----------------------------------------------
     user_id                         | 9838422              
     device_id                       | 01601020af8d0020a... 
     event_type                      | View Product deta... 
     insert_id                       | 83c83f2a-8d8c-476... 
     event_time                      | 2017-12-01 08:00:08  
     session_id                      | 1512110625000        
     session_start                   | 2017-12-01 06:43:45  
     session_end                     | 2017-12-01 08:47:16  
     platform                        | mobile_web           
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 30.0378              
     location_lng                    | -95.5326             
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | 0                    
     uprop_orders_completed_lifetime | 5.0                  
     uprop_user_credit_balance       | 0                    
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 2                    
     uprop_total_favorites           | null                 
     uprop_dom_referrer              | www.thredup.com      
     uprop_utm_term                  | null                 
     uprop_utm_source                | null                 
     uprop_utm_medium                | null                 
     uprop_utm_content               | null                 
     uprop_utm_email                 | null                 
     uprop_utm_campaign              | null                 
     uprop_referral_code             | null                 
     uprop_acq_signup_platform       | null                 
     uprop_acq_signup_channel        | null                 
     uprop_acq_signup_sub_channel    | null                 
     uprop_acq_invitation_code       | null                 
     uprop_acq_external_referrer     | null                 
     uprop_acq_signup_method_agg     | null                 
     uprop_acq_signup_method         | null                 
     uprop_paid_acq_bucket           | null                 
     eprop_order_id                  | null                 
     eprop_order_type                | null                 
     eprop_pay_with                  | null                 
     eprop_order_contains_bag        | null                 
     eprop_total_shipping_fees       | null                 
     eprop_total_tax                 | null                 
     eprop_total_discount            | null                 
     eprop_item_qty                  | null                 
     eprop_order_asp                 | null                 
     eprop_total_cash_credits_used   | null                 
     eprop_item_order_seq            | null                 
     eprop_cart_id                   | null                 
     eprop_app_id                    | null                 
     eprop_device_model              | null                 
     eprop_device_resolution         | null                 
     eprop_link_name                 | null                 
     eprop_event_mythredup_sections  | null                 
     eprop_cleanout_section          | null                 
     eprop_prod_dept                 | null                 
     eprop_pagination_no             | null                 
     eprop_prod_merch_dept           | girls                
     eprop_prod_state                | listed               
     eprop_prod_list_price           | 7.99                 
     eprop_prod_name                 | Justice Capris       
     eprop_prod_id                   | 28590200             
     eprop_dept_tags                 | null                 
     eprop_sort_by                   | null                 
     eprop_prod_color                | null                 
     eprop_prod_condition            | null                 
     eprop_prod_discount             | null                 
     eprop_prod_sizing_id            | 61.0                 
     eprop_prod_category             | 112                  
     eprop_search_keywords           | null                 
     eprop_search_result_count       | null                 
     eprop_filter_type               | null                 
     eprop_promo_code                | null                 
     eprop_promo_code_error          | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    only showing top 20 rows
    


-----


```python
#Converting DataFrame to Pandas

df_master.limit(50).toPandas() 
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>device_id</th>
      <th>event_type</th>
      <th>insert_id</th>
      <th>event_time</th>
      <th>session_id</th>
      <th>session_start</th>
      <th>session_end</th>
      <th>platform</th>
      <th>uprop_app_download_ts</th>
      <th>revenue</th>
      <th>location_lat</th>
      <th>location_lng</th>
      <th>app_version</th>
      <th>carrier</th>
      <th>os_version</th>
      <th>uprop_item_order_count</th>
      <th>uprop_bag_req_count</th>
      <th>uprop_last_purchase_ts</th>
      <th>uprop_first_purchase_ts</th>
      <th>uprop_total_revenue</th>
      <th>uprop_zip5</th>
      <th>uprop_state</th>
      <th>uprop_msa_group_name</th>
      <th>uprop_median_income</th>
      <th>uprop_hamlet_score</th>
      <th>uprop_user_treatments</th>
      <th>uprop_orders_completed_lifetime</th>
      <th>uprop_user_credit_balance</th>
      <th>uprop_user_promotion_code</th>
      <th>uprop_total_session_count</th>
      <th>uprop_total_favorites</th>
      <th>uprop_dom_referrer</th>
      <th>uprop_utm_term</th>
      <th>uprop_utm_source</th>
      <th>uprop_utm_medium</th>
      <th>uprop_utm_content</th>
      <th>uprop_utm_email</th>
      <th>uprop_utm_campaign</th>
      <th>uprop_referral_code</th>
      <th>uprop_acq_signup_platform</th>
      <th>uprop_acq_signup_channel</th>
      <th>uprop_acq_signup_sub_channel</th>
      <th>uprop_acq_invitation_code</th>
      <th>uprop_acq_external_referrer</th>
      <th>uprop_acq_signup_method_agg</th>
      <th>uprop_acq_signup_method</th>
      <th>uprop_paid_acq_bucket</th>
      <th>eprop_order_id</th>
      <th>eprop_order_type</th>
      <th>eprop_pay_with</th>
      <th>eprop_order_contains_bag</th>
      <th>eprop_total_shipping_fees</th>
      <th>eprop_total_tax</th>
      <th>eprop_total_discount</th>
      <th>eprop_item_qty</th>
      <th>eprop_order_asp</th>
      <th>eprop_total_cash_credits_used</th>
      <th>eprop_item_order_seq</th>
      <th>eprop_cart_id</th>
      <th>eprop_app_id</th>
      <th>eprop_device_model</th>
      <th>eprop_device_resolution</th>
      <th>eprop_link_name</th>
      <th>eprop_event_mythredup_sections</th>
      <th>eprop_cleanout_section</th>
      <th>eprop_prod_dept</th>
      <th>eprop_pagination_no</th>
      <th>eprop_prod_merch_dept</th>
      <th>eprop_prod_state</th>
      <th>eprop_prod_list_price</th>
      <th>eprop_prod_name</th>
      <th>eprop_prod_id</th>
      <th>eprop_dept_tags</th>
      <th>eprop_sort_by</th>
      <th>eprop_prod_color</th>
      <th>eprop_prod_condition</th>
      <th>eprop_prod_discount</th>
      <th>eprop_prod_sizing_id</th>
      <th>eprop_prod_category</th>
      <th>eprop_search_keywords</th>
      <th>eprop_search_result_count</th>
      <th>eprop_filter_type</th>
      <th>eprop_promo_code</th>
      <th>eprop_promo_code_error</th>
      <th>eprop_atc_from</th>
      <th>eprop_customer_status</th>
      <th>eprop_plp_url</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>16878712.0</td>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>View homepage</td>
      <td>15c25174-96ee-4672-b247-d6069d628e34</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512115155000</td>
      <td>2017-12-01 07:59:15</td>
      <td>2017-12-01 08:02:58</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.9</td>
      <td>Vodafone.de</td>
      <td>5.0.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>450.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>HTCONE</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1612442.0</td>
      <td>015f605f6efc00034f45b017f70404073003306b00bd0</td>
      <td>View Product listing Page (PLP)</td>
      <td>39fc5cde-2dd2-46a8-bc39-26adda1a2f25</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512112873000</td>
      <td>2017-12-01 07:21:13</td>
      <td>2017-12-01 08:13:54</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>47.680099</td>
      <td>-122.120598</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>84.0</td>
      <td>4.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>54.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>11.0</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Price Low to High</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/women</td>
    </tr>
    <tr>
      <th>2</th>
      <td>14204551.0</td>
      <td>015d7b2f98e500078c82e2eea6790006f008a06700398</td>
      <td>View Product listing Page (PLP)</td>
      <td>6c1395ab-526b-443a-9101-5e0e7e8eeac6</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512113662000</td>
      <td>2017-12-01 07:34:22</td>
      <td>2017-12-01 08:01:07</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.1</td>
      <td>Verizon</td>
      <td>10.1.1</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>67.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>thredUP 7.7.1</td>
      <td>iPhone</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>No Filters</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>24254812.0</td>
      <td>01600e1656840009284f97c8becd04072001806a00978</td>
      <td>View Product details page (PDP)</td>
      <td>620db094-c933-431c-8f63-ad30f6bf9e5c</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512108989000</td>
      <td>2017-12-01 06:16:29</td>
      <td>2017-12-01 08:07:10</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>31.510500</td>
      <td>-97.264503</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>5.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>15.0</td>
      <td>NaN</td>
      <td>www.google.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>women</td>
      <td>listed</td>
      <td>70.989998</td>
      <td>Banana Republic Wool Coat</td>
      <td>27374860.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>Gray</td>
      <td>Q1_only</td>
      <td>0.76</td>
      <td>795.0</td>
      <td>813.0</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>12339891.0</td>
      <td>0160103a6c49001a1f2ca5264e0405066001805e00718</td>
      <td>View Product listing Page (PLP)</td>
      <td>f872f818-2d8a-493e-8028-28fd06f590ce</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512114864000</td>
      <td>2017-12-01 07:54:24</td>
      <td>2017-12-01 09:51:46</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>41.473701</td>
      <td>-81.579903</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.0</td>
      <td>4.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>22.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>shoes</td>
      <td>None</td>
      <td>1.0</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Price Low to High</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/shoes</td>
    </tr>
    <tr>
      <th>5</th>
      <td>16878712.0</td>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>View homepage</td>
      <td>94024fda-dd32-4d83-9046-b0954f15fede</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512115155000</td>
      <td>2017-12-01 07:59:15</td>
      <td>2017-12-01 08:02:58</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.9</td>
      <td>Vodafone.de</td>
      <td>5.0.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>450.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>HTCONE</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>6</th>
      <td>577869.0</td>
      <td>015dc0c19e870072ac9bb38628b800092001808a0063b</td>
      <td>Remove from cart</td>
      <td>b9feb53b-d2eb-4a75-9ad1-6a2c83d449ab</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512108994000</td>
      <td>2017-12-01 06:16:34</td>
      <td>2017-12-01 08:40:00</td>
      <td>mobile_web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>37.342201</td>
      <td>-121.883301</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>17.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>7</th>
      <td>91444761.0</td>
      <td>015db6f02779001392bcb7c0aec50006d008a06500432</td>
      <td>View Product details page (PDP)</td>
      <td>09a983ac-bd80-4f8b-9dcc-1bc39be6e65a</td>
      <td>2017-12-01 08:00:07</td>
      <td>1512110113000</td>
      <td>2017-12-01 06:35:13</td>
      <td>2017-12-01 09:28:31</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.4</td>
      <td>AT&amp;T</td>
      <td>11.1.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>5.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>600.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>thredUP 7.7.4</td>
      <td>iPhone</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>women</td>
      <td>None</td>
      <td>7.990000</td>
      <td>None</td>
      <td>27746639.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>8</th>
      <td>13861851.0</td>
      <td>016010fe824b0060bc06cc41443c0007f00c407700720</td>
      <td>View Product details page (PDP)</td>
      <td>97be672f-d66f-4a80-96c5-17c195f02cbd</td>
      <td>2017-12-01 08:00:07</td>
      <td>1512113604000</td>
      <td>2017-12-01 07:33:24</td>
      <td>2017-12-01 11:59:36</td>
      <td>mobile_web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>41.984501</td>
      <td>-72.557098</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>2.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>women</td>
      <td>listed</td>
      <td>12.990000</td>
      <td>Style&amp;Co Cardigan</td>
      <td>27574685.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>Black</td>
      <td>Q1_only</td>
      <td>0.78</td>
      <td>778.0</td>
      <td>335.0</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2145675.0</td>
      <td>1EADC2EAE4E04637939BE1265BE4C8C6</td>
      <td>View Product details page (PDP)</td>
      <td>3e600ac5-4737-4130-b475-e101e5b647d9</td>
      <td>2017-12-01 08:00:07</td>
      <td>1512114689000</td>
      <td>2017-12-01 07:51:29</td>
      <td>2017-12-01 08:16:28</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.8</td>
      <td>AT&amp;T</td>
      <td>11.1.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>294.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>iPhone9,4</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>X</td>
      <td>None</td>
      <td>50.990002</td>
      <td>None</td>
      <td>27118744.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>10</th>
      <td>36771861.0</td>
      <td>8686A9D00FE8496D87EE062F1C8FB9A2</td>
      <td>View Product details page (PDP)</td>
      <td>2f44f560-7fcd-4ae4-9a4a-9d47c60e5744</td>
      <td>2017-12-01 08:00:07</td>
      <td>1512115140000</td>
      <td>2017-12-01 07:59:00</td>
      <td>2017-12-01 08:00:35</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.8</td>
      <td>T-Mobile</td>
      <td>10.3.3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>63.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>iPhone 5c (GSM)</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>plus</td>
      <td>None</td>
      <td>15.990000</td>
      <td>None</td>
      <td>24027609.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>11</th>
      <td>3312971.0</td>
      <td>7DF597A0ACD94F80A635E0932FA4B460</td>
      <td>View Product details page (PDP)</td>
      <td>384ef8e8-5b77-480e-9363-dce90343ff7b</td>
      <td>2017-12-01 08:00:07</td>
      <td>1512112728000</td>
      <td>2017-12-01 07:18:48</td>
      <td>2017-12-01 09:34:01</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.8</td>
      <td>None</td>
      <td>11.0.3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>iPad Mini 2G (WiFi)</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>X</td>
      <td>None</td>
      <td>69.989998</td>
      <td>None</td>
      <td>28562538.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>12</th>
      <td>58885012.0</td>
      <td>01601095d06c0001d2cee7b05aca00089017d08100432</td>
      <td>View Product listing Page (PLP)</td>
      <td>d8c80316-994b-4911-969c-764e9ac8209d</td>
      <td>2017-12-01 08:00:07</td>
      <td>1512111432000</td>
      <td>2017-12-01 06:57:12</td>
      <td>2017-12-01 08:18:25</td>
      <td>mobile_web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>33.608002</td>
      <td>-86.647202</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>5.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>7.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>women-jeans-bootcut</td>
      <td>32.0</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Newest First</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>No Filters</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/women/bootcut-jeans</td>
    </tr>
    <tr>
      <th>13</th>
      <td>8827704.0</td>
      <td>015ad239352a001e8a273e6ce3e00107500d606d0093c</td>
      <td>Promo-code Error</td>
      <td>2343e0ac-43ab-47f9-ab98-278002fad7dd</td>
      <td>2017-12-01 08:00:07</td>
      <td>1512113090000</td>
      <td>2017-12-01 07:24:50</td>
      <td>2017-12-01 08:24:32</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>29.554800</td>
      <td>-98.695099</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>120.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>thrifty15</td>
      <td>This promotion couldn't be applied.</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>14</th>
      <td>3062301.0</td>
      <td>7B275388ADFA4439A48250C693E12417</td>
      <td>View Product details page (PDP)</td>
      <td>e3a00ca7-6a1a-4648-9a45-3c7c3b0bdd6f</td>
      <td>2017-12-01 08:00:08</td>
      <td>1512114236000</td>
      <td>2017-12-01 07:43:56</td>
      <td>2017-12-01 08:09:05</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.8</td>
      <td>AT&amp;T</td>
      <td>11.1.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>65.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>iPhone 6s</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>women</td>
      <td>None</td>
      <td>26.990000</td>
      <td>None</td>
      <td>28364450.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>15</th>
      <td>59277201.0</td>
      <td>059C52DBEC45425A90E390030C32B7E2</td>
      <td>View Product listing Page (PLP)</td>
      <td>551c8c11-6da0-4bdc-96a2-d7b6cdd2086e</td>
      <td>2017-12-01 08:00:08</td>
      <td>1512114587000</td>
      <td>2017-12-01 07:49:47</td>
      <td>2017-12-01 08:12:55</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.8</td>
      <td>Verizon</td>
      <td>11.0.3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>525.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>iPhone 6 Plus</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>No Filters</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>16</th>
      <td>16878712.0</td>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>View Product listing Page (PLP)</td>
      <td>656a64b9-06d9-43a9-adf0-1e6ed9c39a46</td>
      <td>2017-12-01 08:00:08</td>
      <td>1512115155000</td>
      <td>2017-12-01 07:59:15</td>
      <td>2017-12-01 08:02:58</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.9</td>
      <td>Vodafone.de</td>
      <td>5.0.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>450.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>HTCONE</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>No Filters</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>17</th>
      <td>62334512.0</td>
      <td>015ec1ffc2f80011ed34c8a4801904072002606a0086e</td>
      <td>View Product listing Page (PLP)</td>
      <td>b4385e20-41f5-4d03-a653-893d13b65c23</td>
      <td>2017-12-01 08:00:08</td>
      <td>1512111731000</td>
      <td>2017-12-01 07:02:11</td>
      <td>2017-12-01 08:12:45</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>35.733200</td>
      <td>139.341797</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>3.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>11.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>women-outerwear</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Newest First</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/women/outerwear</td>
    </tr>
    <tr>
      <th>18</th>
      <td>24254812.0</td>
      <td>01600e1656840009284f97c8becd04072001806a00978</td>
      <td>View Cart page</td>
      <td>4654edff-73d3-4e0e-a966-3e46524bbd91</td>
      <td>2017-12-01 08:00:08</td>
      <td>1512108989000</td>
      <td>2017-12-01 06:16:29</td>
      <td>2017-12-01 08:07:10</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>31.510500</td>
      <td>-97.264503</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>5.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>15.0</td>
      <td>NaN</td>
      <td>www.google.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>38676982.0</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>19</th>
      <td>9838422.0</td>
      <td>01601020af8d0020ad414f1c8380000ba00af0b200399</td>
      <td>View Product details page (PDP)</td>
      <td>83c83f2a-8d8c-4762-aa61-db580f8841de</td>
      <td>2017-12-01 08:00:08</td>
      <td>1512110625000</td>
      <td>2017-12-01 06:43:45</td>
      <td>2017-12-01 08:47:16</td>
      <td>mobile_web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>30.037800</td>
      <td>-95.532600</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>5.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>2.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>girls</td>
      <td>listed</td>
      <td>7.990000</td>
      <td>Justice Capris</td>
      <td>28590200.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>61.0</td>
      <td>112.0</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>20</th>
      <td>59277201.0</td>
      <td>059C52DBEC45425A90E390030C32B7E2</td>
      <td>View Product listing Page (PLP)</td>
      <td>82832e20-5328-49e5-b70b-b2861858e781</td>
      <td>2017-12-01 08:00:08</td>
      <td>1512114587000</td>
      <td>2017-12-01 07:49:47</td>
      <td>2017-12-01 08:12:55</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.8</td>
      <td>Verizon</td>
      <td>11.0.3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>525.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>iPhone 6 Plus</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Conditions</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>21</th>
      <td>16878712.0</td>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>View Product listing Page (PLP)</td>
      <td>3ff70ff3-61e3-49e9-99dd-3f8afe2e9aa2</td>
      <td>2017-12-01 08:00:08</td>
      <td>1512115155000</td>
      <td>2017-12-01 07:59:15</td>
      <td>2017-12-01 08:02:58</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.9</td>
      <td>Vodafone.de</td>
      <td>5.0.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>450.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>HTCONE</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>No Filters</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>22</th>
      <td>82679222.0</td>
      <td>01600c0d73e70050df46a70e14d400086002607e00720</td>
      <td>View Product listing Page (PLP)</td>
      <td>e206e53c-b815-4041-870d-fda90459bc0b</td>
      <td>2017-12-01 08:00:08</td>
      <td>1512105563000</td>
      <td>2017-12-01 05:19:23</td>
      <td>2017-12-01 10:05:15</td>
      <td>mobile_web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>42.926399</td>
      <td>-78.747902</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>3.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>7.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Newest First</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Brands, Sizes</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/women/lululemon-athletica</td>
    </tr>
    <tr>
      <th>23</th>
      <td>2123672.0</td>
      <td>015f02b5e18000191cd12fcbea7100086001807e00432</td>
      <td>Add to cart</td>
      <td>96cb6765-d366-428a-b5ec-176d4b23c722</td>
      <td>2017-12-01 08:00:08</td>
      <td>1512115086000</td>
      <td>2017-12-01 07:58:06</td>
      <td>2017-12-01 08:03:17</td>
      <td>mobile_web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>21.306900</td>
      <td>-157.858398</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>534.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>28572868.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>shop-details</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>24</th>
      <td>95156921.0</td>
      <td>39DF795AF89D4737A9DC730423E3B722</td>
      <td>View Product listing Page (PLP)</td>
      <td>854fab08-cf12-4cae-bb62-1ce39bcc54c1</td>
      <td>2017-12-01 08:00:38</td>
      <td>1512114434000</td>
      <td>2017-12-01 07:47:14</td>
      <td>2017-12-01 08:01:37</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.8</td>
      <td>AT&amp;T</td>
      <td>11.1.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>10.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>iPhone9,3</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes, Conditions</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>25</th>
      <td>15704211.0</td>
      <td>015e5a13b532001d8bee735577e10008801190800046c</td>
      <td>View Product listing Page (PLP)</td>
      <td>ec060cf5-6c7a-441e-8c5b-0aed3ca039d9</td>
      <td>2017-12-01 08:00:38</td>
      <td>1512115169000</td>
      <td>2017-12-01 07:59:29</td>
      <td>2017-12-01 08:12:34</td>
      <td>mobile_web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>28.008699</td>
      <td>-82.745399</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>6.0</td>
      <td>5.0</td>
      <td>7204.0</td>
      <td>NaN</td>
      <td>65.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>accessories</td>
      <td>women-accessories-scarves</td>
      <td>2.0</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Newest First</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes, Colors</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/accessories/scarves</td>
    </tr>
    <tr>
      <th>26</th>
      <td>311375.0</td>
      <td>015f6a1ff8c2003ad1042db7dc9800087001d07f00399</td>
      <td>Unfavorited</td>
      <td>bdda271b-9d99-470c-9afb-4b29c6a616e6</td>
      <td>2017-12-01 08:00:38</td>
      <td>1512115219000</td>
      <td>2017-12-01 08:00:19</td>
      <td>2017-12-01 08:13:56</td>
      <td>mobile_web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>33.932899</td>
      <td>-117.490799</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>475.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>27</th>
      <td>NaN</td>
      <td>016011176ac10016170f756dfd5e0008700d607f00432</td>
      <td>See Signup modal</td>
      <td>02726bb1-a13f-4d7e-82b3-cbfe8278c786</td>
      <td>2017-12-01 08:00:38</td>
      <td>1512115238000</td>
      <td>2017-12-01 08:00:38</td>
      <td>2017-12-01 08:00:38</td>
      <td>mobile_web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>40.792099</td>
      <td>-73.943901</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>www.google.com</td>
      <td>-</td>
      <td>google</td>
      <td>cpc</td>
      <td>-</td>
      <td>-</td>
      <td>adwords_search_802406051_44402193794_190622839...</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>28</th>
      <td>59558831.0</td>
      <td>015ab75014df00786c16974d22680006d008a06500432</td>
      <td>View Product details page (PDP)</td>
      <td>54a5952a-9fae-433a-9f22-2ac55e50f9b2</td>
      <td>2017-12-01 08:00:38</td>
      <td>1512114569000</td>
      <td>2017-12-01 07:49:29</td>
      <td>2017-12-01 08:01:22</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.2</td>
      <td>AT&amp;T</td>
      <td>11.1.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>135.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>thredUP 7.7.2</td>
      <td>iPhone</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>womenshoes</td>
      <td>None</td>
      <td>298.989990</td>
      <td>None</td>
      <td>28526371.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>29</th>
      <td>95156921.0</td>
      <td>39DF795AF89D4737A9DC730423E3B722</td>
      <td>View Product listing Page (PLP)</td>
      <td>94653523-8379-45b2-acc1-950d6be7bd2c</td>
      <td>2017-12-01 08:00:38</td>
      <td>1512114434000</td>
      <td>2017-12-01 07:47:14</td>
      <td>2017-12-01 08:01:37</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.8</td>
      <td>AT&amp;T</td>
      <td>11.1.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>10.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>iPhone9,3</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>No Filters</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>30</th>
      <td>8787563.0</td>
      <td>015eea9e042700124b1e2947ff1700084001807c00488</td>
      <td>View homepage</td>
      <td>9d9092db-63aa-4737-9ba7-b99d33d28579</td>
      <td>2017-12-01 08:00:38</td>
      <td>1512112607000</td>
      <td>2017-12-01 07:16:47</td>
      <td>2017-12-01 09:19:59</td>
      <td>mobile_web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>42.425701</td>
      <td>-114.382698</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>4.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>91.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>31</th>
      <td>5468621.0</td>
      <td>015ffb93a546001d5e66161d805004077001806f00838</td>
      <td>View Product listing Page (PLP)</td>
      <td>148dd9df-647a-4602-a12d-1f27e671fcf5</td>
      <td>2017-12-01 08:00:38</td>
      <td>1512115142000</td>
      <td>2017-12-01 07:59:02</td>
      <td>2017-12-01 08:00:59</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>31.549299</td>
      <td>-97.146698</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>6.0</td>
      <td>5.0</td>
      <td>3603.0</td>
      <td>NaN</td>
      <td>285.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/women</td>
    </tr>
    <tr>
      <th>32</th>
      <td>61506112.0</td>
      <td>0160110d4b8e00228266001f842000086010507e00408</td>
      <td>View Product listing Page (PLP)</td>
      <td>7e739960-0ff8-4f1c-99d8-ebf687f1ffaf</td>
      <td>2017-12-01 08:00:38</td>
      <td>1512114582000</td>
      <td>2017-12-01 07:49:42</td>
      <td>2017-12-01 08:30:29</td>
      <td>mobile_web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>38.978401</td>
      <td>-77.028702</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>3.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>42.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>juniors</td>
      <td>None</td>
      <td>13.0</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Newest First</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/juniors</td>
    </tr>
    <tr>
      <th>33</th>
      <td>91444761.0</td>
      <td>015db6f02779001392bcb7c0aec50006d008a06500432</td>
      <td>View Product listing Page (PLP)</td>
      <td>2a740571-4313-4cf4-9d91-8c29ad240bc9</td>
      <td>2017-12-01 08:00:38</td>
      <td>1512110113000</td>
      <td>2017-12-01 06:35:13</td>
      <td>2017-12-01 09:28:31</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.4</td>
      <td>AT&amp;T</td>
      <td>11.1.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>5.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>600.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>thredUP 7.7.4</td>
      <td>iPhone</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>No Filters</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>34</th>
      <td>4294579.0</td>
      <td>015807649dfa0030bde84d31929c0207503b806d00bd0</td>
      <td>View Product listing Page (PLP)</td>
      <td>1e31c002-c856-4bb5-a9b8-308c5b94d405</td>
      <td>2017-12-01 08:00:38</td>
      <td>1512115002000</td>
      <td>2017-12-01 07:56:42</td>
      <td>2017-12-01 08:11:14</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>40.054798</td>
      <td>-75.408302</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>5.0</td>
      <td>3.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>47.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>women-dresses</td>
      <td>1.0</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Price Low to High</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/women/dresses</td>
    </tr>
    <tr>
      <th>35</th>
      <td>94030322.0</td>
      <td>016011164f42000dbc0df01a819f00088002c0800049e</td>
      <td>View Product listing Page (PLP)</td>
      <td>e6ba50a9-cb4b-4a44-adbc-89af9cff218d</td>
      <td>2017-12-01 08:00:43</td>
      <td>1512115168000</td>
      <td>2017-12-01 07:59:28</td>
      <td>2017-12-01 08:01:47</td>
      <td>mobile_web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>33.948502</td>
      <td>-118.201698</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>plus</td>
      <td>women-dresses</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>No Filters</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/plus/dresses</td>
    </tr>
    <tr>
      <th>36</th>
      <td>7950761.0</td>
      <td>1f6f3d51c973458a8a9e873734eaf9a6</td>
      <td>View Product details page (PDP)</td>
      <td>ad2a2c14-e8e3-420a-bf2e-98d7a8d55d4b</td>
      <td>2017-12-01 08:00:43</td>
      <td>1512111924000</td>
      <td>2017-12-01 07:05:24</td>
      <td>2017-12-01 09:37:06</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.10</td>
      <td>GCI</td>
      <td>5.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1657.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>Samsung SAMSUNG-SM-G900A</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>X</td>
      <td>None</td>
      <td>54.990002</td>
      <td>None</td>
      <td>28592860.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>37</th>
      <td>2145675.0</td>
      <td>1EADC2EAE4E04637939BE1265BE4C8C6</td>
      <td>View Product details page (PDP)</td>
      <td>c454583e-c361-4b52-a540-167baac2403a</td>
      <td>2017-12-01 08:00:59</td>
      <td>1512114689000</td>
      <td>2017-12-01 07:51:29</td>
      <td>2017-12-01 08:16:28</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.8</td>
      <td>AT&amp;T</td>
      <td>11.1.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>294.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>iPhone9,4</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>women</td>
      <td>None</td>
      <td>30.990000</td>
      <td>None</td>
      <td>28143785.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>38</th>
      <td>9838422.0</td>
      <td>01601020af8d0020ad414f1c8380000ba00af0b200399</td>
      <td>Add to cart</td>
      <td>c13069cb-217c-4a7d-a2c0-893179491ee3</td>
      <td>2017-12-01 08:00:59</td>
      <td>1512110625000</td>
      <td>2017-12-01 06:43:45</td>
      <td>2017-12-01 08:47:16</td>
      <td>mobile_web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>30.037800</td>
      <td>-95.532600</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>28541102.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>shop-details</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>39</th>
      <td>5030322.0</td>
      <td>01601114cacb0015990b26f493e400087007b07f00408</td>
      <td>User Signup</td>
      <td>223030501512115259</td>
      <td>2017-12-01 08:00:59</td>
      <td>1512115071000</td>
      <td>2017-12-01 07:57:51</td>
      <td>2017-12-01 08:08:40</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>78413.0</td>
      <td>TX</td>
      <td>metro TX</td>
      <td>65271.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>mobile_web</td>
      <td>Organic</td>
      <td>shop</td>
      <td>no_valid_code</td>
      <td>www.youtube.com</td>
      <td>Facebook</td>
      <td>mobile_web_facebook_signup</td>
      <td>Organic</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>40</th>
      <td>5315612.0</td>
      <td>015edaa8af7e00426dfdaf4fd63c000a500ea09d00408</td>
      <td>View Product details page (PDP)</td>
      <td>f0ef468d-b06c-4679-b991-d0e04c14cf67</td>
      <td>2017-12-01 08:00:59</td>
      <td>1512114938000</td>
      <td>2017-12-01 07:55:38</td>
      <td>2017-12-01 08:11:19</td>
      <td>mobile_web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>35.989899</td>
      <td>-79.698502</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.0</td>
      <td>5.0</td>
      <td>6100.0</td>
      <td>NaN</td>
      <td>134.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>X</td>
      <td>listed</td>
      <td>32.990002</td>
      <td>Adriano Goldschmied Jeans</td>
      <td>27487108.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>806.0</td>
      <td>356.0</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>41</th>
      <td>67015012.0</td>
      <td>016010f3ba6a000b0ce94923e13504078001807000bd0</td>
      <td>View Product details page (PDP)</td>
      <td>c4e200c0-89ba-4991-9dca-1c6af44ff703</td>
      <td>2017-12-01 08:00:59</td>
      <td>1512112897000</td>
      <td>2017-12-01 07:21:37</td>
      <td>2017-12-01 08:30:59</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>38.408798</td>
      <td>-121.371597</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>19.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>85.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>women</td>
      <td>listed</td>
      <td>16.990000</td>
      <td>Ann Taylor Sleeveless Blouse</td>
      <td>28629430.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>812.0</td>
      <td>742.0</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>42</th>
      <td>5468621.0</td>
      <td>015ffb93a546001d5e66161d805004077001806f00838</td>
      <td>View Product listing Page (PLP)</td>
      <td>36b38466-5119-4413-9b75-3656d7552a64</td>
      <td>2017-12-01 08:00:59</td>
      <td>1512115142000</td>
      <td>2017-12-01 07:59:02</td>
      <td>2017-12-01 08:00:59</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>31.549299</td>
      <td>-97.146698</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>6.0</td>
      <td>5.0</td>
      <td>3603.0</td>
      <td>NaN</td>
      <td>285.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>1.0</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Price Low to High</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/women</td>
    </tr>
    <tr>
      <th>43</th>
      <td>7458238.0</td>
      <td>01600b818dd6000e8476778e5ea100087011907f00408</td>
      <td>View Product listing Page (PLP)</td>
      <td>bb8d3e9b-0aef-4bda-96ec-43b97314ec06</td>
      <td>2017-12-01 08:01:37</td>
      <td>1512114491000</td>
      <td>2017-12-01 07:48:11</td>
      <td>2017-12-01 08:07:11</td>
      <td>mobile_web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>38.005100</td>
      <td>-121.838699</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>4677.0</td>
      <td>NaN</td>
      <td>150.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>women-jeans-bootcut</td>
      <td>2.0</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Newest First</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/women/bootcut-jeans</td>
    </tr>
    <tr>
      <th>44</th>
      <td>9498665.0</td>
      <td>01543fccd0bb00038bff6dbfeb870006e008906600432</td>
      <td>View Product details page (PDP)</td>
      <td>28866d25-1bcf-4612-920a-9f120c028c3a</td>
      <td>2017-12-01 08:01:37</td>
      <td>1512115242000</td>
      <td>2017-12-01 08:00:42</td>
      <td>2017-12-01 08:14:12</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.4</td>
      <td>Sprint</td>
      <td>10.3.3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>75.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>thredUP 7.7.4</td>
      <td>iPhone</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>X</td>
      <td>None</td>
      <td>70.989998</td>
      <td>None</td>
      <td>28302351.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>45</th>
      <td>1863433.0</td>
      <td>377801C7E3044882B2D70784314940E4</td>
      <td>View Product details page (PDP)</td>
      <td>3d2adb49-9e93-4fd2-9588-3cce5f0adb5d</td>
      <td>2017-12-01 08:01:37</td>
      <td>1512112828000</td>
      <td>2017-12-01 07:20:28</td>
      <td>2017-12-01 08:18:59</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.8</td>
      <td>AT&amp;T</td>
      <td>11.1.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1047.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>iPhone9,3</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>women</td>
      <td>None</td>
      <td>24.990000</td>
      <td>None</td>
      <td>27823076.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>46</th>
      <td>25418902.0</td>
      <td>015b9afd2b55007fb01f28e03bac0008900ca08100432</td>
      <td>View Product details page (PDP)</td>
      <td>04bc18be-e63e-4574-96ba-8da3f611b62b</td>
      <td>2017-12-01 08:01:37</td>
      <td>1512115019000</td>
      <td>2017-12-01 07:56:59</td>
      <td>2017-12-01 08:42:03</td>
      <td>mobile_web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>52.250000</td>
      <td>21.000000</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>3.0</td>
      <td>895.0</td>
      <td>NaN</td>
      <td>7.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>women</td>
      <td>listed</td>
      <td>17.990000</td>
      <td>C. Wonder Turtleneck Sweater</td>
      <td>28438635.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>778.0</td>
      <td>795.0</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>47</th>
      <td>95156921.0</td>
      <td>39DF795AF89D4737A9DC730423E3B722</td>
      <td>View Product details page (PDP)</td>
      <td>ac035cca-b66b-44eb-9749-89ef1de6dc28</td>
      <td>2017-12-01 08:01:37</td>
      <td>1512114434000</td>
      <td>2017-12-01 07:47:14</td>
      <td>2017-12-01 08:01:37</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.8</td>
      <td>AT&amp;T</td>
      <td>11.1.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>10.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>iPhone9,3</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>women</td>
      <td>None</td>
      <td>8.990000</td>
      <td>None</td>
      <td>28488223.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>48</th>
      <td>2123672.0</td>
      <td>015f02b5e18000191cd12fcbea7100086001807e00432</td>
      <td>View Product listing Page (PLP)</td>
      <td>ac18fe7b-2175-48dc-9546-8c97be33aca4</td>
      <td>2017-12-01 08:01:37</td>
      <td>1512115086000</td>
      <td>2017-12-01 07:58:06</td>
      <td>2017-12-01 08:03:17</td>
      <td>mobile_web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>21.306900</td>
      <td>-157.858398</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>20.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>534.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>4.0</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Newest First</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/women</td>
    </tr>
    <tr>
      <th>49</th>
      <td>64920322.0</td>
      <td>016010e5b3570020de229ff37cf400096001808e00408</td>
      <td>View Product listing Page (PLP)</td>
      <td>f8d99677-7e05-4382-9d93-04a937b1d2c7</td>
      <td>2017-12-01 08:01:37</td>
      <td>1512111986000</td>
      <td>2017-12-01 07:06:26</td>
      <td>2017-12-01 08:32:52</td>
      <td>mobile_web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>35.236099</td>
      <td>-80.309799</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>5.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>designer</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/designer/luxe</td>
    </tr>
  </tbody>
</table>
</div>



#### A user defined schema for master dataframe


```python
df_master.printSchema()
```

    root
     |-- user_id: integer (nullable = true)
     |-- device_id: string (nullable = true)
     |-- event_type: string (nullable = true)
     |-- insert_id: string (nullable = true)
     |-- event_time: timestamp (nullable = true)
     |-- session_id: string (nullable = true)
     |-- session_start: timestamp (nullable = true)
     |-- session_end: timestamp (nullable = true)
     |-- platform: string (nullable = true)
     |-- uprop_app_download_ts: float (nullable = true)
     |-- revenue: float (nullable = true)
     |-- location_lat: float (nullable = true)
     |-- location_lng: float (nullable = true)
     |-- app_version: string (nullable = true)
     |-- carrier: string (nullable = true)
     |-- os_version: string (nullable = true)
     |-- uprop_item_order_count: integer (nullable = true)
     |-- uprop_bag_req_count: integer (nullable = true)
     |-- uprop_last_purchase_ts: timestamp (nullable = true)
     |-- uprop_first_purchase_ts: timestamp (nullable = true)
     |-- uprop_total_revenue: float (nullable = true)
     |-- uprop_zip5: integer (nullable = true)
     |-- uprop_state: string (nullable = true)
     |-- uprop_msa_group_name: string (nullable = true)
     |-- uprop_median_income: integer (nullable = true)
     |-- uprop_hamlet_score: integer (nullable = true)
     |-- uprop_user_treatments: integer (nullable = true)
     |-- uprop_orders_completed_lifetime: float (nullable = true)
     |-- uprop_user_credit_balance: integer (nullable = true)
     |-- uprop_user_promotion_code: integer (nullable = true)
     |-- uprop_total_session_count: integer (nullable = true)
     |-- uprop_total_favorites: integer (nullable = true)
     |-- uprop_dom_referrer: string (nullable = true)
     |-- uprop_utm_term: string (nullable = true)
     |-- uprop_utm_source: string (nullable = true)
     |-- uprop_utm_medium: string (nullable = true)
     |-- uprop_utm_content: string (nullable = true)
     |-- uprop_utm_email: string (nullable = true)
     |-- uprop_utm_campaign: string (nullable = true)
     |-- uprop_referral_code: integer (nullable = true)
     |-- uprop_acq_signup_platform: string (nullable = true)
     |-- uprop_acq_signup_channel: string (nullable = true)
     |-- uprop_acq_signup_sub_channel: string (nullable = true)
     |-- uprop_acq_invitation_code: string (nullable = true)
     |-- uprop_acq_external_referrer: string (nullable = true)
     |-- uprop_acq_signup_method_agg: string (nullable = true)
     |-- uprop_acq_signup_method: string (nullable = true)
     |-- uprop_paid_acq_bucket: string (nullable = true)
     |-- eprop_order_id: integer (nullable = true)
     |-- eprop_order_type: string (nullable = true)
     |-- eprop_pay_with: string (nullable = true)
     |-- eprop_order_contains_bag: float (nullable = true)
     |-- eprop_total_shipping_fees: integer (nullable = true)
     |-- eprop_total_tax: integer (nullable = true)
     |-- eprop_total_discount: integer (nullable = true)
     |-- eprop_item_qty: float (nullable = true)
     |-- eprop_order_asp: integer (nullable = true)
     |-- eprop_total_cash_credits_used: float (nullable = true)
     |-- eprop_item_order_seq: integer (nullable = true)
     |-- eprop_cart_id: integer (nullable = true)
     |-- eprop_app_id: string (nullable = true)
     |-- eprop_device_model: string (nullable = true)
     |-- eprop_device_resolution: integer (nullable = true)
     |-- eprop_link_name: string (nullable = true)
     |-- eprop_event_mythredup_sections: string (nullable = true)
     |-- eprop_cleanout_section: string (nullable = true)
     |-- eprop_prod_dept: string (nullable = true)
     |-- eprop_pagination_no: integer (nullable = true)
     |-- eprop_prod_merch_dept: string (nullable = true)
     |-- eprop_prod_state: string (nullable = true)
     |-- eprop_prod_list_price: float (nullable = true)
     |-- eprop_prod_name: string (nullable = true)
     |-- eprop_prod_id: integer (nullable = true)
     |-- eprop_dept_tags: integer (nullable = true)
     |-- eprop_sort_by: string (nullable = true)
     |-- eprop_prod_color: string (nullable = true)
     |-- eprop_prod_condition: string (nullable = true)
     |-- eprop_prod_discount: string (nullable = true)
     |-- eprop_prod_sizing_id: float (nullable = true)
     |-- eprop_prod_category: integer (nullable = true)
     |-- eprop_search_keywords: string (nullable = true)
     |-- eprop_search_result_count: integer (nullable = true)
     |-- eprop_filter_type: string (nullable = true)
     |-- eprop_promo_code: string (nullable = true)
     |-- eprop_promo_code_error: string (nullable = true)
     |-- eprop_atc_from: string (nullable = true)
     |-- eprop_customer_status: string (nullable = true)
     |-- eprop_plp_url: string (nullable = true)
    


### Spark Partitions

A spark job should be well distributed across the cluster and it depends on number of partition available for the dataframe. below we have master dataframe distributed across 25 partitions.In order to increase the partitions we can use repartition() method to redistribute the data across multiple partitions. 

#### Master dataframe current partitions


```python
print("Partitions present {0}".format(df_master.rdd.getNumPartitions()))
```

    Partitions present 25


#### Repartition the master dataframe with 50 partition


```python
#df_master = df_master.repartition(50).cache()

```


```python
#print("Partitions present {0}".format(df_master.rdd.getNumPartitions()))
```

-----

## 3. Data Transfomations

Before going ahead I need to decide on which Spark API framework will be utilizing among available (RDD, Dataset, Dataframe). RDD API is the one available right from start of Spark invention, It is most useful when it comes to unstructured data format (Media, row data). As our dataset is in tablular format we will be using Dataframe API. 

Spark framework API's such as spark dataset and dataframes are utilizing the SparkSession. whereas SparkContext is used specifically when working with RDD and creating a spark clusters.
With the recent update on Apache Spark 2.2.0 everything has been encapsulated with SparkSession, so we don't need to use sepecially Hive Context, Spark Conetxt, and SQL Context.

For the whole course of transformation we will use df_master and df_master_transform dataframe for transformations. Then, df_master dataframe is used for loading into fact and dimension tables in Star Schema format. 

Transformation usually have two steps:

   - Transformation : It performs the certain transformations on given RDD, Dataframe, Dataset such as filter(), withColumn(), groupby() just like we did below. This process generally does not change the data and creates a new RDD 
   - Actions : Few of the transformations we used below to mention by taking action such as counting the values and taking first 5 rows. Action creates the smaller dataset such as count(), aggragate(), distinct() etc. are few examples of them. 
   
Transformations performed as per the requirement and need of data, In our dataset values in few of the columns are null and missing, otherwise most of the present data is clean.


### Remove the decimal point 0 from customer orders lifetime and user_id column 


```python
# Using the withColumn function to manipulate the column values 

df_master = df_master\
                     .withColumn('uprop_orders_completed_lifetime', translate('uprop_orders_completed_lifetime', '.0', ''))

df_master = df_master\
                     .withColumn('user_id', translate('user_id', '.0', ''))

df_master.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>device_id</th>
      <th>event_type</th>
      <th>insert_id</th>
      <th>event_time</th>
      <th>session_id</th>
      <th>session_start</th>
      <th>session_end</th>
      <th>platform</th>
      <th>uprop_app_download_ts</th>
      <th>revenue</th>
      <th>location_lat</th>
      <th>location_lng</th>
      <th>app_version</th>
      <th>carrier</th>
      <th>os_version</th>
      <th>uprop_item_order_count</th>
      <th>uprop_bag_req_count</th>
      <th>uprop_last_purchase_ts</th>
      <th>uprop_first_purchase_ts</th>
      <th>uprop_total_revenue</th>
      <th>uprop_zip5</th>
      <th>uprop_state</th>
      <th>uprop_msa_group_name</th>
      <th>uprop_median_income</th>
      <th>uprop_hamlet_score</th>
      <th>uprop_user_treatments</th>
      <th>uprop_orders_completed_lifetime</th>
      <th>uprop_user_credit_balance</th>
      <th>uprop_user_promotion_code</th>
      <th>uprop_total_session_count</th>
      <th>uprop_total_favorites</th>
      <th>uprop_dom_referrer</th>
      <th>uprop_utm_term</th>
      <th>uprop_utm_source</th>
      <th>uprop_utm_medium</th>
      <th>uprop_utm_content</th>
      <th>uprop_utm_email</th>
      <th>uprop_utm_campaign</th>
      <th>uprop_referral_code</th>
      <th>uprop_acq_signup_platform</th>
      <th>uprop_acq_signup_channel</th>
      <th>uprop_acq_signup_sub_channel</th>
      <th>uprop_acq_invitation_code</th>
      <th>uprop_acq_external_referrer</th>
      <th>uprop_acq_signup_method_agg</th>
      <th>uprop_acq_signup_method</th>
      <th>uprop_paid_acq_bucket</th>
      <th>eprop_order_id</th>
      <th>eprop_order_type</th>
      <th>eprop_pay_with</th>
      <th>eprop_order_contains_bag</th>
      <th>eprop_total_shipping_fees</th>
      <th>eprop_total_tax</th>
      <th>eprop_total_discount</th>
      <th>eprop_item_qty</th>
      <th>eprop_order_asp</th>
      <th>eprop_total_cash_credits_used</th>
      <th>eprop_item_order_seq</th>
      <th>eprop_cart_id</th>
      <th>eprop_app_id</th>
      <th>eprop_device_model</th>
      <th>eprop_device_resolution</th>
      <th>eprop_link_name</th>
      <th>eprop_event_mythredup_sections</th>
      <th>eprop_cleanout_section</th>
      <th>eprop_prod_dept</th>
      <th>eprop_pagination_no</th>
      <th>eprop_prod_merch_dept</th>
      <th>eprop_prod_state</th>
      <th>eprop_prod_list_price</th>
      <th>eprop_prod_name</th>
      <th>eprop_prod_id</th>
      <th>eprop_dept_tags</th>
      <th>eprop_sort_by</th>
      <th>eprop_prod_color</th>
      <th>eprop_prod_condition</th>
      <th>eprop_prod_discount</th>
      <th>eprop_prod_sizing_id</th>
      <th>eprop_prod_category</th>
      <th>eprop_search_keywords</th>
      <th>eprop_search_result_count</th>
      <th>eprop_filter_type</th>
      <th>eprop_promo_code</th>
      <th>eprop_promo_code_error</th>
      <th>eprop_atc_from</th>
      <th>eprop_customer_status</th>
      <th>eprop_plp_url</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>16878712</td>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>View homepage</td>
      <td>15c25174-96ee-4672-b247-d6069d628e34</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512115155000</td>
      <td>2017-12-01 07:59:15</td>
      <td>2017-12-01 08:02:58</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.9</td>
      <td>Vodafone.de</td>
      <td>5.0.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>450</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>HTCONE</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1612442</td>
      <td>015f605f6efc00034f45b017f70404073003306b00bd0</td>
      <td>View Product listing Page (PLP)</td>
      <td>39fc5cde-2dd2-46a8-bc39-26adda1a2f25</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512112873000</td>
      <td>2017-12-01 07:21:13</td>
      <td>2017-12-01 08:13:54</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>47.680099</td>
      <td>-122.120598</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>84.0</td>
      <td>4</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>54</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>11.0</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Price Low to High</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/women</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1424551</td>
      <td>015d7b2f98e500078c82e2eea6790006f008a06700398</td>
      <td>View Product listing Page (PLP)</td>
      <td>6c1395ab-526b-443a-9101-5e0e7e8eeac6</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512113662000</td>
      <td>2017-12-01 07:34:22</td>
      <td>2017-12-01 08:01:07</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.1</td>
      <td>Verizon</td>
      <td>10.1.1</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>67</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>thredUP 7.7.1</td>
      <td>iPhone</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>No Filters</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>24254812</td>
      <td>01600e1656840009284f97c8becd04072001806a00978</td>
      <td>View Product details page (PDP)</td>
      <td>620db094-c933-431c-8f63-ad30f6bf9e5c</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512108989000</td>
      <td>2017-12-01 06:16:29</td>
      <td>2017-12-01 08:07:10</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>31.510500</td>
      <td>-97.264503</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>5</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>15</td>
      <td>NaN</td>
      <td>www.google.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>women</td>
      <td>listed</td>
      <td>70.989998</td>
      <td>Banana Republic Wool Coat</td>
      <td>27374860.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>Gray</td>
      <td>Q1_only</td>
      <td>0.76</td>
      <td>795.0</td>
      <td>813.0</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>12339891</td>
      <td>0160103a6c49001a1f2ca5264e0405066001805e00718</td>
      <td>View Product listing Page (PLP)</td>
      <td>f872f818-2d8a-493e-8028-28fd06f590ce</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512114864000</td>
      <td>2017-12-01 07:54:24</td>
      <td>2017-12-01 09:51:46</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>41.473701</td>
      <td>-81.579903</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.0</td>
      <td>4</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>22</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>shoes</td>
      <td>None</td>
      <td>1.0</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Price Low to High</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/shoes</td>
    </tr>
  </tbody>
</table>
</div>



--------

### Fill column eprop_prod_merch_dept with appropriate value 

- There are some irrelevant values present in the merchadise department column which we are going to replace with meaningful value.


```python
df_master = df_master\
                    .withColumn('eprop_prod_merch_dept', translate('eprop_prod_merch_dept', 'X', 'Others'))

df_master.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>device_id</th>
      <th>event_type</th>
      <th>insert_id</th>
      <th>event_time</th>
      <th>session_id</th>
      <th>session_start</th>
      <th>session_end</th>
      <th>platform</th>
      <th>uprop_app_download_ts</th>
      <th>revenue</th>
      <th>location_lat</th>
      <th>location_lng</th>
      <th>app_version</th>
      <th>carrier</th>
      <th>os_version</th>
      <th>uprop_item_order_count</th>
      <th>uprop_bag_req_count</th>
      <th>uprop_last_purchase_ts</th>
      <th>uprop_first_purchase_ts</th>
      <th>uprop_total_revenue</th>
      <th>uprop_zip5</th>
      <th>uprop_state</th>
      <th>uprop_msa_group_name</th>
      <th>uprop_median_income</th>
      <th>uprop_hamlet_score</th>
      <th>uprop_user_treatments</th>
      <th>uprop_orders_completed_lifetime</th>
      <th>uprop_user_credit_balance</th>
      <th>uprop_user_promotion_code</th>
      <th>uprop_total_session_count</th>
      <th>uprop_total_favorites</th>
      <th>uprop_dom_referrer</th>
      <th>uprop_utm_term</th>
      <th>uprop_utm_source</th>
      <th>uprop_utm_medium</th>
      <th>uprop_utm_content</th>
      <th>uprop_utm_email</th>
      <th>uprop_utm_campaign</th>
      <th>uprop_referral_code</th>
      <th>uprop_acq_signup_platform</th>
      <th>uprop_acq_signup_channel</th>
      <th>uprop_acq_signup_sub_channel</th>
      <th>uprop_acq_invitation_code</th>
      <th>uprop_acq_external_referrer</th>
      <th>uprop_acq_signup_method_agg</th>
      <th>uprop_acq_signup_method</th>
      <th>uprop_paid_acq_bucket</th>
      <th>eprop_order_id</th>
      <th>eprop_order_type</th>
      <th>eprop_pay_with</th>
      <th>eprop_order_contains_bag</th>
      <th>eprop_total_shipping_fees</th>
      <th>eprop_total_tax</th>
      <th>eprop_total_discount</th>
      <th>eprop_item_qty</th>
      <th>eprop_order_asp</th>
      <th>eprop_total_cash_credits_used</th>
      <th>eprop_item_order_seq</th>
      <th>eprop_cart_id</th>
      <th>eprop_app_id</th>
      <th>eprop_device_model</th>
      <th>eprop_device_resolution</th>
      <th>eprop_link_name</th>
      <th>eprop_event_mythredup_sections</th>
      <th>eprop_cleanout_section</th>
      <th>eprop_prod_dept</th>
      <th>eprop_pagination_no</th>
      <th>eprop_prod_merch_dept</th>
      <th>eprop_prod_state</th>
      <th>eprop_prod_list_price</th>
      <th>eprop_prod_name</th>
      <th>eprop_prod_id</th>
      <th>eprop_dept_tags</th>
      <th>eprop_sort_by</th>
      <th>eprop_prod_color</th>
      <th>eprop_prod_condition</th>
      <th>eprop_prod_discount</th>
      <th>eprop_prod_sizing_id</th>
      <th>eprop_prod_category</th>
      <th>eprop_search_keywords</th>
      <th>eprop_search_result_count</th>
      <th>eprop_filter_type</th>
      <th>eprop_promo_code</th>
      <th>eprop_promo_code_error</th>
      <th>eprop_atc_from</th>
      <th>eprop_customer_status</th>
      <th>eprop_plp_url</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>16878712</td>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>View homepage</td>
      <td>15c25174-96ee-4672-b247-d6069d628e34</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512115155000</td>
      <td>2017-12-01 07:59:15</td>
      <td>2017-12-01 08:02:58</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.9</td>
      <td>Vodafone.de</td>
      <td>5.0.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>450</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>HTCONE</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1612442</td>
      <td>015f605f6efc00034f45b017f70404073003306b00bd0</td>
      <td>View Product listing Page (PLP)</td>
      <td>39fc5cde-2dd2-46a8-bc39-26adda1a2f25</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512112873000</td>
      <td>2017-12-01 07:21:13</td>
      <td>2017-12-01 08:13:54</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>47.680099</td>
      <td>-122.120598</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>84.0</td>
      <td>4</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>54</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>11.0</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Price Low to High</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/women</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1424551</td>
      <td>015d7b2f98e500078c82e2eea6790006f008a06700398</td>
      <td>View Product listing Page (PLP)</td>
      <td>6c1395ab-526b-443a-9101-5e0e7e8eeac6</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512113662000</td>
      <td>2017-12-01 07:34:22</td>
      <td>2017-12-01 08:01:07</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.1</td>
      <td>Verizon</td>
      <td>10.1.1</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>67</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>thredUP 7.7.1</td>
      <td>iPhone</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>No Filters</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>24254812</td>
      <td>01600e1656840009284f97c8becd04072001806a00978</td>
      <td>View Product details page (PDP)</td>
      <td>620db094-c933-431c-8f63-ad30f6bf9e5c</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512108989000</td>
      <td>2017-12-01 06:16:29</td>
      <td>2017-12-01 08:07:10</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>31.510500</td>
      <td>-97.264503</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>5</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>15</td>
      <td>NaN</td>
      <td>www.google.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>women</td>
      <td>listed</td>
      <td>70.989998</td>
      <td>Banana Republic Wool Coat</td>
      <td>27374860.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>Gray</td>
      <td>Q1_only</td>
      <td>0.76</td>
      <td>795.0</td>
      <td>813.0</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>12339891</td>
      <td>0160103a6c49001a1f2ca5264e0405066001805e00718</td>
      <td>View Product listing Page (PLP)</td>
      <td>f872f818-2d8a-493e-8028-28fd06f590ce</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512114864000</td>
      <td>2017-12-01 07:54:24</td>
      <td>2017-12-01 09:51:46</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>41.473701</td>
      <td>-81.579903</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.0</td>
      <td>4</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>22</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>shoes</td>
      <td>None</td>
      <td>1.0</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Price Low to High</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/shoes</td>
    </tr>
  </tbody>
</table>
</div>



### Filter the column with specified value

- Filter transformation are used same as where clause in SQL to return the rows matched with condition.  


```python
#filter the dataframe for user having device model LG

df_master_transform = df_master.where((col("eprop_device_model") == "HTCONE" ))

df_master_transform.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>device_id</th>
      <th>event_type</th>
      <th>insert_id</th>
      <th>event_time</th>
      <th>session_id</th>
      <th>session_start</th>
      <th>session_end</th>
      <th>platform</th>
      <th>uprop_app_download_ts</th>
      <th>revenue</th>
      <th>location_lat</th>
      <th>location_lng</th>
      <th>app_version</th>
      <th>carrier</th>
      <th>os_version</th>
      <th>uprop_item_order_count</th>
      <th>uprop_bag_req_count</th>
      <th>uprop_last_purchase_ts</th>
      <th>uprop_first_purchase_ts</th>
      <th>uprop_total_revenue</th>
      <th>uprop_zip5</th>
      <th>uprop_state</th>
      <th>uprop_msa_group_name</th>
      <th>uprop_median_income</th>
      <th>uprop_hamlet_score</th>
      <th>uprop_user_treatments</th>
      <th>uprop_orders_completed_lifetime</th>
      <th>uprop_user_credit_balance</th>
      <th>uprop_user_promotion_code</th>
      <th>uprop_total_session_count</th>
      <th>uprop_total_favorites</th>
      <th>uprop_dom_referrer</th>
      <th>uprop_utm_term</th>
      <th>uprop_utm_source</th>
      <th>uprop_utm_medium</th>
      <th>uprop_utm_content</th>
      <th>uprop_utm_email</th>
      <th>uprop_utm_campaign</th>
      <th>uprop_referral_code</th>
      <th>uprop_acq_signup_platform</th>
      <th>uprop_acq_signup_channel</th>
      <th>uprop_acq_signup_sub_channel</th>
      <th>uprop_acq_invitation_code</th>
      <th>uprop_acq_external_referrer</th>
      <th>uprop_acq_signup_method_agg</th>
      <th>uprop_acq_signup_method</th>
      <th>uprop_paid_acq_bucket</th>
      <th>eprop_order_id</th>
      <th>eprop_order_type</th>
      <th>eprop_pay_with</th>
      <th>eprop_order_contains_bag</th>
      <th>eprop_total_shipping_fees</th>
      <th>eprop_total_tax</th>
      <th>eprop_total_discount</th>
      <th>eprop_item_qty</th>
      <th>eprop_order_asp</th>
      <th>eprop_total_cash_credits_used</th>
      <th>eprop_item_order_seq</th>
      <th>eprop_cart_id</th>
      <th>eprop_app_id</th>
      <th>eprop_device_model</th>
      <th>eprop_device_resolution</th>
      <th>eprop_link_name</th>
      <th>eprop_event_mythredup_sections</th>
      <th>eprop_cleanout_section</th>
      <th>eprop_prod_dept</th>
      <th>eprop_pagination_no</th>
      <th>eprop_prod_merch_dept</th>
      <th>eprop_prod_state</th>
      <th>eprop_prod_list_price</th>
      <th>eprop_prod_name</th>
      <th>eprop_prod_id</th>
      <th>eprop_dept_tags</th>
      <th>eprop_sort_by</th>
      <th>eprop_prod_color</th>
      <th>eprop_prod_condition</th>
      <th>eprop_prod_discount</th>
      <th>eprop_prod_sizing_id</th>
      <th>eprop_prod_category</th>
      <th>eprop_search_keywords</th>
      <th>eprop_search_result_count</th>
      <th>eprop_filter_type</th>
      <th>eprop_promo_code</th>
      <th>eprop_promo_code_error</th>
      <th>eprop_atc_from</th>
      <th>eprop_customer_status</th>
      <th>eprop_plp_url</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>16878712</td>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>View homepage</td>
      <td>15c25174-96ee-4672-b247-d6069d628e34</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512115155000</td>
      <td>2017-12-01 07:59:15</td>
      <td>2017-12-01 08:02:58</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.9</td>
      <td>Vodafone.de</td>
      <td>5.0.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>450</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>HTCONE</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>16878712</td>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>View homepage</td>
      <td>94024fda-dd32-4d83-9046-b0954f15fede</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512115155000</td>
      <td>2017-12-01 07:59:15</td>
      <td>2017-12-01 08:02:58</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.9</td>
      <td>Vodafone.de</td>
      <td>5.0.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>450</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>HTCONE</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>16878712</td>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>View Product listing Page (PLP)</td>
      <td>656a64b9-06d9-43a9-adf0-1e6ed9c39a46</td>
      <td>2017-12-01 08:00:08</td>
      <td>1512115155000</td>
      <td>2017-12-01 07:59:15</td>
      <td>2017-12-01 08:02:58</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.9</td>
      <td>Vodafone.de</td>
      <td>5.0.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>450</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>HTCONE</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>No Filters</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>16878712</td>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>View Product listing Page (PLP)</td>
      <td>3ff70ff3-61e3-49e9-99dd-3f8afe2e9aa2</td>
      <td>2017-12-01 08:00:08</td>
      <td>1512115155000</td>
      <td>2017-12-01 07:59:15</td>
      <td>2017-12-01 08:02:58</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.9</td>
      <td>Vodafone.de</td>
      <td>5.0.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>450</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>HTCONE</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>No Filters</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>16878712</td>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>View Product listing Page (PLP)</td>
      <td>7609d4ab-fd90-489c-88c5-ec557dff54ea</td>
      <td>2017-12-01 13:02:20</td>
      <td>1512133289000</td>
      <td>2017-12-01 13:01:29</td>
      <td>2017-12-01 13:03:06</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.9</td>
      <td>Vodafone.de</td>
      <td>5.0.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>454</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>HTCONE</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>No Filters</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



### Change data types of the column
- WithColumn function provides usage on dataframe such as adding the column, altering the values, deriving new columns from existing and in below case we are converting the datatype of column with cast function 


```python
df_master_transform = df_master\
                            .withColumn("eprop_total_tax", df_master["eprop_total_tax"].cast(FloatType()))

df_master_transform.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>device_id</th>
      <th>event_type</th>
      <th>insert_id</th>
      <th>event_time</th>
      <th>session_id</th>
      <th>session_start</th>
      <th>session_end</th>
      <th>platform</th>
      <th>uprop_app_download_ts</th>
      <th>revenue</th>
      <th>location_lat</th>
      <th>location_lng</th>
      <th>app_version</th>
      <th>carrier</th>
      <th>os_version</th>
      <th>uprop_item_order_count</th>
      <th>uprop_bag_req_count</th>
      <th>uprop_last_purchase_ts</th>
      <th>uprop_first_purchase_ts</th>
      <th>uprop_total_revenue</th>
      <th>uprop_zip5</th>
      <th>uprop_state</th>
      <th>uprop_msa_group_name</th>
      <th>uprop_median_income</th>
      <th>uprop_hamlet_score</th>
      <th>uprop_user_treatments</th>
      <th>uprop_orders_completed_lifetime</th>
      <th>uprop_user_credit_balance</th>
      <th>uprop_user_promotion_code</th>
      <th>uprop_total_session_count</th>
      <th>uprop_total_favorites</th>
      <th>uprop_dom_referrer</th>
      <th>uprop_utm_term</th>
      <th>uprop_utm_source</th>
      <th>uprop_utm_medium</th>
      <th>uprop_utm_content</th>
      <th>uprop_utm_email</th>
      <th>uprop_utm_campaign</th>
      <th>uprop_referral_code</th>
      <th>uprop_acq_signup_platform</th>
      <th>uprop_acq_signup_channel</th>
      <th>uprop_acq_signup_sub_channel</th>
      <th>uprop_acq_invitation_code</th>
      <th>uprop_acq_external_referrer</th>
      <th>uprop_acq_signup_method_agg</th>
      <th>uprop_acq_signup_method</th>
      <th>uprop_paid_acq_bucket</th>
      <th>eprop_order_id</th>
      <th>eprop_order_type</th>
      <th>eprop_pay_with</th>
      <th>eprop_order_contains_bag</th>
      <th>eprop_total_shipping_fees</th>
      <th>eprop_total_tax</th>
      <th>eprop_total_discount</th>
      <th>eprop_item_qty</th>
      <th>eprop_order_asp</th>
      <th>eprop_total_cash_credits_used</th>
      <th>eprop_item_order_seq</th>
      <th>eprop_cart_id</th>
      <th>eprop_app_id</th>
      <th>eprop_device_model</th>
      <th>eprop_device_resolution</th>
      <th>eprop_link_name</th>
      <th>eprop_event_mythredup_sections</th>
      <th>eprop_cleanout_section</th>
      <th>eprop_prod_dept</th>
      <th>eprop_pagination_no</th>
      <th>eprop_prod_merch_dept</th>
      <th>eprop_prod_state</th>
      <th>eprop_prod_list_price</th>
      <th>eprop_prod_name</th>
      <th>eprop_prod_id</th>
      <th>eprop_dept_tags</th>
      <th>eprop_sort_by</th>
      <th>eprop_prod_color</th>
      <th>eprop_prod_condition</th>
      <th>eprop_prod_discount</th>
      <th>eprop_prod_sizing_id</th>
      <th>eprop_prod_category</th>
      <th>eprop_search_keywords</th>
      <th>eprop_search_result_count</th>
      <th>eprop_filter_type</th>
      <th>eprop_promo_code</th>
      <th>eprop_promo_code_error</th>
      <th>eprop_atc_from</th>
      <th>eprop_customer_status</th>
      <th>eprop_plp_url</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>16878712</td>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>View homepage</td>
      <td>15c25174-96ee-4672-b247-d6069d628e34</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512115155000</td>
      <td>2017-12-01 07:59:15</td>
      <td>2017-12-01 08:02:58</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.9</td>
      <td>Vodafone.de</td>
      <td>5.0.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>450</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>HTCONE</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1612442</td>
      <td>015f605f6efc00034f45b017f70404073003306b00bd0</td>
      <td>View Product listing Page (PLP)</td>
      <td>39fc5cde-2dd2-46a8-bc39-26adda1a2f25</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512112873000</td>
      <td>2017-12-01 07:21:13</td>
      <td>2017-12-01 08:13:54</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>47.680099</td>
      <td>-122.120598</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>84.0</td>
      <td>4</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>54</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>11.0</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Price Low to High</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/women</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1424551</td>
      <td>015d7b2f98e500078c82e2eea6790006f008a06700398</td>
      <td>View Product listing Page (PLP)</td>
      <td>6c1395ab-526b-443a-9101-5e0e7e8eeac6</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512113662000</td>
      <td>2017-12-01 07:34:22</td>
      <td>2017-12-01 08:01:07</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.1</td>
      <td>Verizon</td>
      <td>10.1.1</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>67</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>thredUP 7.7.1</td>
      <td>iPhone</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>No Filters</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>24254812</td>
      <td>01600e1656840009284f97c8becd04072001806a00978</td>
      <td>View Product details page (PDP)</td>
      <td>620db094-c933-431c-8f63-ad30f6bf9e5c</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512108989000</td>
      <td>2017-12-01 06:16:29</td>
      <td>2017-12-01 08:07:10</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>31.510500</td>
      <td>-97.264503</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>5</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>15</td>
      <td>NaN</td>
      <td>www.google.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>women</td>
      <td>listed</td>
      <td>70.989998</td>
      <td>Banana Republic Wool Coat</td>
      <td>27374860.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>Gray</td>
      <td>Q1_only</td>
      <td>0.76</td>
      <td>795.0</td>
      <td>813.0</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>12339891</td>
      <td>0160103a6c49001a1f2ca5264e0405066001805e00718</td>
      <td>View Product listing Page (PLP)</td>
      <td>f872f818-2d8a-493e-8028-28fd06f590ce</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512114864000</td>
      <td>2017-12-01 07:54:24</td>
      <td>2017-12-01 09:51:46</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>41.473701</td>
      <td>-81.579903</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.0</td>
      <td>4</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>22</td>
      <td>NaN</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>shoes</td>
      <td>None</td>
      <td>1.0</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Price Low to High</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/shoes</td>
    </tr>
  </tbody>
</table>
</div>



### Drop duplicates from column


```python
df_master_transform.select("event_type").dropDuplicates().sort("event_type").limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>event_type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>-</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Add to cart</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Address update failed</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Address update successfull</td>
    </tr>
    <tr>
      <th>4</th>
      <td>App Download</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Click from email to reset password</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Clicks on Place order button</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Credit card info validation failed</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Favorited</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Login Failure</td>
    </tr>
  </tbody>
</table>
</div>



### Count Null Values


```python
#df_master_transform.select([count(when(col(c).isNull(), c)).alias(c) for c in df_master.columns])
```


```python
#df_master_transform.limit(10).toPandas()
```


```python
# Drop the column or row if any NA values found

#df_master_transform = df_master.dropna(how='any')
```

### Describe column Product list price

To get the statistical data such as mean, max, standard daviation for mixed data types. 


```python
%%time
df_master_transform.describe(['eprop_prod_list_price']).show()
```

    +-------+---------------------+
    |summary|eprop_prod_list_price|
    +-------+---------------------+
    |  count|              2331425|
    |   mean|   31.376294601424313|
    | stddev|    74.25558852877613|
    |    min|                  0.0|
    |    max|              3599.99|
    +-------+---------------------+
    
    CPU times: user 3.14 ms, sys: 3.54 ms, total: 6.68 ms
    Wall time: 14.5 s


### Query with Keyword ,contains and multiple conditions


```python
# Startswith function search the specified keyword to match

df_master_transform.filter(df_master["app_version"].startswith("4")).limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>device_id</th>
      <th>event_type</th>
      <th>insert_id</th>
      <th>event_time</th>
      <th>session_id</th>
      <th>session_start</th>
      <th>session_end</th>
      <th>platform</th>
      <th>uprop_app_download_ts</th>
      <th>revenue</th>
      <th>location_lat</th>
      <th>location_lng</th>
      <th>app_version</th>
      <th>carrier</th>
      <th>os_version</th>
      <th>uprop_item_order_count</th>
      <th>uprop_bag_req_count</th>
      <th>uprop_last_purchase_ts</th>
      <th>uprop_first_purchase_ts</th>
      <th>uprop_total_revenue</th>
      <th>uprop_zip5</th>
      <th>uprop_state</th>
      <th>uprop_msa_group_name</th>
      <th>uprop_median_income</th>
      <th>uprop_hamlet_score</th>
      <th>uprop_user_treatments</th>
      <th>uprop_orders_completed_lifetime</th>
      <th>uprop_user_credit_balance</th>
      <th>uprop_user_promotion_code</th>
      <th>uprop_total_session_count</th>
      <th>uprop_total_favorites</th>
      <th>uprop_dom_referrer</th>
      <th>uprop_utm_term</th>
      <th>uprop_utm_source</th>
      <th>uprop_utm_medium</th>
      <th>uprop_utm_content</th>
      <th>uprop_utm_email</th>
      <th>uprop_utm_campaign</th>
      <th>uprop_referral_code</th>
      <th>uprop_acq_signup_platform</th>
      <th>uprop_acq_signup_channel</th>
      <th>uprop_acq_signup_sub_channel</th>
      <th>uprop_acq_invitation_code</th>
      <th>uprop_acq_external_referrer</th>
      <th>uprop_acq_signup_method_agg</th>
      <th>uprop_acq_signup_method</th>
      <th>uprop_paid_acq_bucket</th>
      <th>eprop_order_id</th>
      <th>eprop_order_type</th>
      <th>eprop_pay_with</th>
      <th>eprop_order_contains_bag</th>
      <th>eprop_total_shipping_fees</th>
      <th>eprop_total_tax</th>
      <th>eprop_total_discount</th>
      <th>eprop_item_qty</th>
      <th>eprop_order_asp</th>
      <th>eprop_total_cash_credits_used</th>
      <th>eprop_item_order_seq</th>
      <th>eprop_cart_id</th>
      <th>eprop_app_id</th>
      <th>eprop_device_model</th>
      <th>eprop_device_resolution</th>
      <th>eprop_link_name</th>
      <th>eprop_event_mythredup_sections</th>
      <th>eprop_cleanout_section</th>
      <th>eprop_prod_dept</th>
      <th>eprop_pagination_no</th>
      <th>eprop_prod_merch_dept</th>
      <th>eprop_prod_state</th>
      <th>eprop_prod_list_price</th>
      <th>eprop_prod_name</th>
      <th>eprop_prod_id</th>
      <th>eprop_dept_tags</th>
      <th>eprop_sort_by</th>
      <th>eprop_prod_color</th>
      <th>eprop_prod_condition</th>
      <th>eprop_prod_discount</th>
      <th>eprop_prod_sizing_id</th>
      <th>eprop_prod_category</th>
      <th>eprop_search_keywords</th>
      <th>eprop_search_result_count</th>
      <th>eprop_filter_type</th>
      <th>eprop_promo_code</th>
      <th>eprop_promo_code_error</th>
      <th>eprop_atc_from</th>
      <th>eprop_customer_status</th>
      <th>eprop_plp_url</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>16878712</td>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>View homepage</td>
      <td>15c25174-96ee-4672-b247-d6069d628e34</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512115155000</td>
      <td>2017-12-01 07:59:15</td>
      <td>2017-12-01 08:02:58</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.9</td>
      <td>Vodafone.de</td>
      <td>5.0.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>450</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>HTCONE</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>16878712</td>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>View homepage</td>
      <td>94024fda-dd32-4d83-9046-b0954f15fede</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512115155000</td>
      <td>2017-12-01 07:59:15</td>
      <td>2017-12-01 08:02:58</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.9</td>
      <td>Vodafone.de</td>
      <td>5.0.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>450</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>HTCONE</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>16878712</td>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>View Product listing Page (PLP)</td>
      <td>656a64b9-06d9-43a9-adf0-1e6ed9c39a46</td>
      <td>2017-12-01 08:00:08</td>
      <td>1512115155000</td>
      <td>2017-12-01 07:59:15</td>
      <td>2017-12-01 08:02:58</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.9</td>
      <td>Vodafone.de</td>
      <td>5.0.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>450</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>HTCONE</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>No Filters</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>16878712</td>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>View Product listing Page (PLP)</td>
      <td>3ff70ff3-61e3-49e9-99dd-3f8afe2e9aa2</td>
      <td>2017-12-01 08:00:08</td>
      <td>1512115155000</td>
      <td>2017-12-01 07:59:15</td>
      <td>2017-12-01 08:02:58</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.9</td>
      <td>Vodafone.de</td>
      <td>5.0.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>450</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>HTCONE</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>No Filters</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>795761</td>
      <td>1f6f3d51c973458a8a9e873734eaf9a6</td>
      <td>View Product details page (PDP)</td>
      <td>ad2a2c14-e8e3-420a-bf2e-98d7a8d55d4b</td>
      <td>2017-12-01 08:00:43</td>
      <td>1512111924000</td>
      <td>2017-12-01 07:05:24</td>
      <td>2017-12-01 09:37:06</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.10</td>
      <td>GCI</td>
      <td>5.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1657</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>Samsung SAMSUNG-SM-G900A</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>O</td>
      <td>None</td>
      <td>54.990002</td>
      <td>None</td>
      <td>28592860.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



### Search with values contains (Product name with jeans)


```python
#df_master_transform.filter(df_master["eprop_prod_name"].contains("jacket")).limit(10).toPandas()
```

### Query with multiple condition (user with app version 3 and device model iPhone)


```python
#df_master_transform.filter((col("app_version").endswith("3")) & (col("eprop_device_model") == "iPhone") & col("platform").startswith("mobile")).limit(10).toPandas()
```

--------

## 4. Data Loading 

Data loading is the part either occurs before transform (ELT) or after transform (ETL). In our case, we are going with later. ELT is most suitable for massive dataset, whereas ETL can be used performing operations on smaller datasets. We will be using the df_master dataframe to load the data into fact and dimension tables which are created using temporary view.

## <center>  Star Schema <center>

![StarSchema.png](attachment:StarSchema.png)

Created the dimension and fact table with temporary view and kept ready for analysis. 

#### Fact table :
    
1. customer_analytics_table

#### Dimension tables:

1. user_details_table
2. product_details_table
3. order_details_table
4. campaign_details_table
5. user_session_details_table
6. user_search_details_table
7. user_device_details_table
8. time_table
9. location_table


### Dimension 1. user_details 


```python
user_details_table = df_master.select('user_id',
                                      'device_id',
                                      'event_type',
                                      'insert_id',
                                      'event_time',
                                      'session_id',
                                      'platform',
                                      'uprop_app_download_ts',
                                      'revenue',
                                      'location_lat',
                                      'location_lng',
                                      'app_version',
                                      'carrier',
                                      'os_version',
                                      'uprop_item_order_count',
                                      'uprop_bag_req_count',
                                      'uprop_last_purchase_ts',
                                      'uprop_first_purchase_ts',
                                      'uprop_total_revenue',
                                      'uprop_zip5',
                                      'uprop_state',
                                      'uprop_msa_group_name',
                                      'uprop_median_income',
                                      'uprop_hamlet_score',
                                      'uprop_user_treatments',
                                      'uprop_orders_completed_lifetime',
                                      'uprop_user_credit_balance',
                                      'uprop_user_promotion_code',
                                      'uprop_total_session_count',
                                      'uprop_total_favorites',
                                      'eprop_atc_from',
                                      'eprop_customer_status',
                                      'eprop_plp_url')
```

#### Count NaN Values


```python
#user_details_table.select([count(when(col(c).isNull(), c)).alias(c) for c in user_details_table.columns]).show(vertical=True)

```

##### Remove rows without user_id


```python
user_details_table.createOrReplaceTempView("user_details_temp") #creates a temporary view
```


```python
df_user_notnull = scSpark.sql("SELECT * FROM user_details_temp where user_id IS NOT NULL")

df_user_notnull.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>device_id</th>
      <th>event_type</th>
      <th>insert_id</th>
      <th>event_time</th>
      <th>session_id</th>
      <th>platform</th>
      <th>uprop_app_download_ts</th>
      <th>revenue</th>
      <th>location_lat</th>
      <th>location_lng</th>
      <th>app_version</th>
      <th>carrier</th>
      <th>os_version</th>
      <th>uprop_item_order_count</th>
      <th>uprop_bag_req_count</th>
      <th>uprop_last_purchase_ts</th>
      <th>uprop_first_purchase_ts</th>
      <th>uprop_total_revenue</th>
      <th>uprop_zip5</th>
      <th>uprop_state</th>
      <th>uprop_msa_group_name</th>
      <th>uprop_median_income</th>
      <th>uprop_hamlet_score</th>
      <th>uprop_user_treatments</th>
      <th>uprop_orders_completed_lifetime</th>
      <th>uprop_user_credit_balance</th>
      <th>uprop_user_promotion_code</th>
      <th>uprop_total_session_count</th>
      <th>uprop_total_favorites</th>
      <th>eprop_atc_from</th>
      <th>eprop_customer_status</th>
      <th>eprop_plp_url</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>16878712</td>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>View homepage</td>
      <td>15c25174-96ee-4672-b247-d6069d628e34</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512115155000</td>
      <td>android</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4.7.9</td>
      <td>Vodafone.de</td>
      <td>5.0.2</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>450</td>
      <td>NaN</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1612442</td>
      <td>015f605f6efc00034f45b017f70404073003306b00bd0</td>
      <td>View Product listing Page (PLP)</td>
      <td>39fc5cde-2dd2-46a8-bc39-26adda1a2f25</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512112873000</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>47.680099</td>
      <td>-122.120598</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>84.0</td>
      <td>4</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>54</td>
      <td>NaN</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/women</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1424551</td>
      <td>015d7b2f98e500078c82e2eea6790006f008a06700398</td>
      <td>View Product listing Page (PLP)</td>
      <td>6c1395ab-526b-443a-9101-5e0e7e8eeac6</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512113662000</td>
      <td>ios</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.7.1</td>
      <td>Verizon</td>
      <td>10.1.1</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>67</td>
      <td>NaN</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>24254812</td>
      <td>01600e1656840009284f97c8becd04072001806a00978</td>
      <td>View Product details page (PDP)</td>
      <td>620db094-c933-431c-8f63-ad30f6bf9e5c</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512108989000</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>31.510500</td>
      <td>-97.264503</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>5</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>15</td>
      <td>NaN</td>
      <td>None</td>
      <td>logged_in</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>12339891</td>
      <td>0160103a6c49001a1f2ca5264e0405066001805e00718</td>
      <td>View Product listing Page (PLP)</td>
      <td>f872f818-2d8a-493e-8028-28fd06f590ce</td>
      <td>2017-12-01 08:00:03</td>
      <td>1512114864000</td>
      <td>web</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>41.473701</td>
      <td>-81.579903</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7.0</td>
      <td>4</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>22</td>
      <td>NaN</td>
      <td>None</td>
      <td>logged_in</td>
      <td>/products/shoes</td>
    </tr>
  </tbody>
</table>
</div>



##### Dimension table: user_details


```python
df_user_notnull.createOrReplaceTempView("user_details") 
```


```python
scSpark.sql("SELECT * FROM user_details").show(vertical=True)
```

    -RECORD 0-----------------------------------------------
     user_id                         | 16878712             
     device_id                       | 89ae640fa58e4f268... 
     event_type                      | View homepage        
     insert_id                       | 15c25174-96ee-467... 
     event_time                      | 2017-12-01 08:00:03  
     session_id                      | 1512115155000        
     platform                        | android              
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 4.7.9                
     carrier                         | Vodafone.de          
     os_version                      | 5.0.2                
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 3                    
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 450                  
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 1-----------------------------------------------
     user_id                         | 1612442              
     device_id                       | 015f605f6efc00034... 
     event_type                      | View Product list... 
     insert_id                       | 39fc5cde-2dd2-46a... 
     event_time                      | 2017-12-01 08:00:03  
     session_id                      | 1512112873000        
     platform                        | web                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 47.6801              
     location_lng                    | -122.1206            
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | 84                   
     uprop_orders_completed_lifetime | 4                    
     uprop_user_credit_balance       | 0                    
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 54                   
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | /products/women      
    -RECORD 2-----------------------------------------------
     user_id                         | 1424551              
     device_id                       | 015d7b2f98e500078... 
     event_type                      | View Product list... 
     insert_id                       | 6c1395ab-526b-443... 
     event_time                      | 2017-12-01 08:00:03  
     session_id                      | 1512113662000        
     platform                        | ios                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 7.7.1                
     carrier                         | Verizon              
     os_version                      | 10.1.1               
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 3                    
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 67                   
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 3-----------------------------------------------
     user_id                         | 24254812             
     device_id                       | 01600e16568400092... 
     event_type                      | View Product deta... 
     insert_id                       | 620db094-c933-431... 
     event_time                      | 2017-12-01 08:00:03  
     session_id                      | 1512108989000        
     platform                        | web                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 31.5105              
     location_lng                    | -97.2645             
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | 0                    
     uprop_orders_completed_lifetime | 5                    
     uprop_user_credit_balance       | 0                    
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 15                   
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 4-----------------------------------------------
     user_id                         | 12339891             
     device_id                       | 0160103a6c49001a1... 
     event_type                      | View Product list... 
     insert_id                       | f872f818-2d8a-493... 
     event_time                      | 2017-12-01 08:00:03  
     session_id                      | 1512114864000        
     platform                        | web                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 41.4737              
     location_lng                    | -81.5799             
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | 7                    
     uprop_orders_completed_lifetime | 4                    
     uprop_user_credit_balance       | 0                    
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 22                   
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | /products/shoes      
    -RECORD 5-----------------------------------------------
     user_id                         | 16878712             
     device_id                       | 89ae640fa58e4f268... 
     event_type                      | View homepage        
     insert_id                       | 94024fda-dd32-4d8... 
     event_time                      | 2017-12-01 08:00:03  
     session_id                      | 1512115155000        
     platform                        | android              
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 4.7.9                
     carrier                         | Vodafone.de          
     os_version                      | 5.0.2                
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 3                    
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 450                  
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 6-----------------------------------------------
     user_id                         | 577869               
     device_id                       | 015dc0c19e870072a... 
     event_type                      | Remove from cart     
     insert_id                       | b9feb53b-d2eb-4a7... 
     event_time                      | 2017-12-01 08:00:03  
     session_id                      | 1512108994000        
     platform                        | mobile_web           
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 37.3422              
     location_lng                    | -121.8833            
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | null                 
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 17                   
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 7-----------------------------------------------
     user_id                         | 91444761             
     device_id                       | 015db6f0277900139... 
     event_type                      | View Product deta... 
     insert_id                       | 09a983ac-bd80-4f8... 
     event_time                      | 2017-12-01 08:00:07  
     session_id                      | 1512110113000        
     platform                        | ios                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 7.7.4                
     carrier                         | AT&T                 
     os_version                      | 11.1.2               
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 5                    
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 600                  
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 8-----------------------------------------------
     user_id                         | 13861851             
     device_id                       | 016010fe824b0060b... 
     event_type                      | View Product deta... 
     insert_id                       | 97be672f-d66f-4a8... 
     event_time                      | 2017-12-01 08:00:07  
     session_id                      | 1512113604000        
     platform                        | mobile_web           
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 41.9845              
     location_lng                    | -72.5571             
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | 0                    
     uprop_orders_completed_lifetime | 1                    
     uprop_user_credit_balance       | 0                    
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 2                    
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 9-----------------------------------------------
     user_id                         | 2145675              
     device_id                       | 1EADC2EAE4E046379... 
     event_type                      | View Product deta... 
     insert_id                       | 3e600ac5-4737-413... 
     event_time                      | 2017-12-01 08:00:07  
     session_id                      | 1512114689000        
     platform                        | ios                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 7.7.8                
     carrier                         | AT&T                 
     os_version                      | 11.1.2               
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 1                    
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 294                  
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 10----------------------------------------------
     user_id                         | 36771861             
     device_id                       | 8686A9D00FE8496D8... 
     event_type                      | View Product deta... 
     insert_id                       | 2f44f560-7fcd-4ae... 
     event_time                      | 2017-12-01 08:00:07  
     session_id                      | 1512115140000        
     platform                        | ios                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 7.7.8                
     carrier                         | T-Mobile             
     os_version                      | 10.3.3               
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 3                    
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 63                   
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 11----------------------------------------------
     user_id                         | 3312971              
     device_id                       | 7DF597A0ACD94F80A... 
     event_type                      | View Product deta... 
     insert_id                       | 384ef8e8-5b77-480... 
     event_time                      | 2017-12-01 08:00:07  
     session_id                      | 1512112728000        
     platform                        | ios                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 7.7.8                
     carrier                         | null                 
     os_version                      | 11.0.3               
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 4                    
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 3                    
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 12----------------------------------------------
     user_id                         | 5888512              
     device_id                       | 01601095d06c0001d... 
     event_type                      | View Product list... 
     insert_id                       | d8c80316-994b-491... 
     event_time                      | 2017-12-01 08:00:07  
     session_id                      | 1512111432000        
     platform                        | mobile_web           
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 33.608               
     location_lng                    | -86.6472             
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | 0                    
     uprop_orders_completed_lifetime | 5                    
     uprop_user_credit_balance       | 0                    
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 7                    
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | /products/women/b... 
    -RECORD 13----------------------------------------------
     user_id                         | 882774               
     device_id                       | 015ad239352a001e8... 
     event_type                      | Promo-code Error     
     insert_id                       | 2343e0ac-43ab-47f... 
     event_time                      | 2017-12-01 08:00:07  
     session_id                      | 1512113090000        
     platform                        | web                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 29.5548              
     location_lng                    | -98.6951             
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | null                 
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 120                  
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 14----------------------------------------------
     user_id                         | 36231                
     device_id                       | 7B275388ADFA4439A... 
     event_type                      | View Product deta... 
     insert_id                       | e3a00ca7-6a1a-464... 
     event_time                      | 2017-12-01 08:00:08  
     session_id                      | 1512114236000        
     platform                        | ios                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 7.7.8                
     carrier                         | AT&T                 
     os_version                      | 11.1.2               
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 1                    
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 65                   
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 15----------------------------------------------
     user_id                         | 5927721              
     device_id                       | 059C52DBEC45425A9... 
     event_type                      | View Product list... 
     insert_id                       | 551c8c11-6da0-4bd... 
     event_time                      | 2017-12-01 08:00:08  
     session_id                      | 1512114587000        
     platform                        | ios                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 7.7.8                
     carrier                         | Verizon              
     os_version                      | 11.0.3               
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 1                    
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 525                  
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 16----------------------------------------------
     user_id                         | 16878712             
     device_id                       | 89ae640fa58e4f268... 
     event_type                      | View Product list... 
     insert_id                       | 656a64b9-06d9-43a... 
     event_time                      | 2017-12-01 08:00:08  
     session_id                      | 1512115155000        
     platform                        | android              
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | null                 
     location_lng                    | null                 
     app_version                     | 4.7.9                
     carrier                         | Vodafone.de          
     os_version                      | 5.0.2                
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | null                 
     uprop_orders_completed_lifetime | 3                    
     uprop_user_credit_balance       | null                 
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 450                  
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 17----------------------------------------------
     user_id                         | 62334512             
     device_id                       | 015ec1ffc2f80011e... 
     event_type                      | View Product list... 
     insert_id                       | b4385e20-41f5-4d0... 
     event_time                      | 2017-12-01 08:00:08  
     session_id                      | 1512111731000        
     platform                        | web                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 35.7332              
     location_lng                    | 139.3418             
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | 0                    
     uprop_orders_completed_lifetime | 3                    
     uprop_user_credit_balance       | 0                    
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 11                   
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | /products/women/o... 
    -RECORD 18----------------------------------------------
     user_id                         | 24254812             
     device_id                       | 01600e16568400092... 
     event_type                      | View Cart page       
     insert_id                       | 4654edff-73d3-4e0... 
     event_time                      | 2017-12-01 08:00:08  
     session_id                      | 1512108989000        
     platform                        | web                  
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 31.5105              
     location_lng                    | -97.2645             
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | 0                    
     uprop_orders_completed_lifetime | 5                    
     uprop_user_credit_balance       | 0                    
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 15                   
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    -RECORD 19----------------------------------------------
     user_id                         | 9838422              
     device_id                       | 01601020af8d0020a... 
     event_type                      | View Product deta... 
     insert_id                       | 83c83f2a-8d8c-476... 
     event_time                      | 2017-12-01 08:00:08  
     session_id                      | 1512110625000        
     platform                        | mobile_web           
     uprop_app_download_ts           | null                 
     revenue                         | null                 
     location_lat                    | 30.0378              
     location_lng                    | -95.5326             
     app_version                     | null                 
     carrier                         | null                 
     os_version                      | null                 
     uprop_item_order_count          | null                 
     uprop_bag_req_count             | null                 
     uprop_last_purchase_ts          | null                 
     uprop_first_purchase_ts         | null                 
     uprop_total_revenue             | null                 
     uprop_zip5                      | null                 
     uprop_state                     | null                 
     uprop_msa_group_name            | null                 
     uprop_median_income             | null                 
     uprop_hamlet_score              | null                 
     uprop_user_treatments           | 0                    
     uprop_orders_completed_lifetime | 5                    
     uprop_user_credit_balance       | 0                    
     uprop_user_promotion_code       | null                 
     uprop_total_session_count       | 2                    
     uprop_total_favorites           | null                 
     eprop_atc_from                  | null                 
     eprop_customer_status           | logged_in            
     eprop_plp_url                   | null                 
    only showing top 20 rows
    


---------

### Dimension 2: product_details 


```python
product_details_table = df_master.select('eprop_prod_id',
                                         'eprop_prod_dept',
                                         'eprop_prod_merch_dept',
                                         'eprop_prod_state',
                                         'eprop_prod_list_price',
                                         'eprop_prod_name',
                                         'eprop_dept_tags',
                                         'eprop_prod_color',
                                         'eprop_prod_condition',
                                         'eprop_prod_discount',
                                         'eprop_prod_sizing_id',
                                         'eprop_prod_category',
                                         'eprop_order_id',
                                         'uprop_first_purchase_ts',
                                         'uprop_last_purchase_ts',
                                         'revenue',
                                         'user_id',
                                         'session_id')
```


```python
product_details_table.limit(5).toPandas()

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>eprop_prod_id</th>
      <th>eprop_prod_dept</th>
      <th>eprop_prod_merch_dept</th>
      <th>eprop_prod_state</th>
      <th>eprop_prod_list_price</th>
      <th>eprop_prod_name</th>
      <th>eprop_dept_tags</th>
      <th>eprop_prod_color</th>
      <th>eprop_prod_condition</th>
      <th>eprop_prod_discount</th>
      <th>eprop_prod_sizing_id</th>
      <th>eprop_prod_category</th>
      <th>eprop_order_id</th>
      <th>uprop_first_purchase_ts</th>
      <th>uprop_last_purchase_ts</th>
      <th>revenue</th>
      <th>user_id</th>
      <th>session_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>16878712</td>
      <td>1512115155000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>1612442</td>
      <td>1512112873000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>1424551</td>
      <td>1512113662000</td>
    </tr>
    <tr>
      <th>3</th>
      <td>27374860.0</td>
      <td>None</td>
      <td>women</td>
      <td>listed</td>
      <td>70.989998</td>
      <td>Banana Republic Wool Coat</td>
      <td>NaN</td>
      <td>Gray</td>
      <td>Q1_only</td>
      <td>0.76</td>
      <td>795.0</td>
      <td>813.0</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>24254812</td>
      <td>1512108989000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>12339891</td>
      <td>1512114864000</td>
    </tr>
  </tbody>
</table>
</div>



##### Remove rows without product_id


```python
product_details_table.createOrReplaceTempView("product_details_temp") 
```


```python
df_product_notnull = scSpark.sql("SELECT * FROM product_details_temp where eprop_prod_id IS NOT NULL")

df_product_notnull.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>eprop_prod_id</th>
      <th>eprop_prod_dept</th>
      <th>eprop_prod_merch_dept</th>
      <th>eprop_prod_state</th>
      <th>eprop_prod_list_price</th>
      <th>eprop_prod_name</th>
      <th>eprop_dept_tags</th>
      <th>eprop_prod_color</th>
      <th>eprop_prod_condition</th>
      <th>eprop_prod_discount</th>
      <th>eprop_prod_sizing_id</th>
      <th>eprop_prod_category</th>
      <th>eprop_order_id</th>
      <th>uprop_first_purchase_ts</th>
      <th>uprop_last_purchase_ts</th>
      <th>revenue</th>
      <th>user_id</th>
      <th>session_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>27374860</td>
      <td>None</td>
      <td>women</td>
      <td>listed</td>
      <td>70.989998</td>
      <td>Banana Republic Wool Coat</td>
      <td>NaN</td>
      <td>Gray</td>
      <td>Q1_only</td>
      <td>0.76</td>
      <td>795.0</td>
      <td>813.0</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>24254812</td>
      <td>1512108989000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>27746639</td>
      <td>None</td>
      <td>women</td>
      <td>None</td>
      <td>7.990000</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>91444761</td>
      <td>1512110113000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>27574685</td>
      <td>None</td>
      <td>women</td>
      <td>listed</td>
      <td>12.990000</td>
      <td>Style&amp;Co Cardigan</td>
      <td>NaN</td>
      <td>Black</td>
      <td>Q1_only</td>
      <td>0.78</td>
      <td>778.0</td>
      <td>335.0</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>13861851</td>
      <td>1512113604000</td>
    </tr>
    <tr>
      <th>3</th>
      <td>27118744</td>
      <td>None</td>
      <td>O</td>
      <td>None</td>
      <td>50.990002</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>2145675</td>
      <td>1512114689000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>24027609</td>
      <td>None</td>
      <td>plus</td>
      <td>None</td>
      <td>15.990000</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>36771861</td>
      <td>1512115140000</td>
    </tr>
  </tbody>
</table>
</div>



##### Dimension table: product_details


```python
df_product_notnull.createOrReplaceTempView("product_details") 
```


```python
scSpark.sql("SELECT * FROM product_details").show(vertical=True)
```

    -RECORD 0---------------------------------------
     eprop_prod_id           | 27374860             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | women                
     eprop_prod_state        | listed               
     eprop_prod_list_price   | 70.99                
     eprop_prod_name         | Banana Republic W... 
     eprop_dept_tags         | null                 
     eprop_prod_color        |  Gray                
     eprop_prod_condition    | Q1_only              
     eprop_prod_discount     | 0.76                 
     eprop_prod_sizing_id    | 795.0                
     eprop_prod_category     | 813                  
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 24254812             
     session_id              | 1512108989000        
    -RECORD 1---------------------------------------
     eprop_prod_id           | 27746639             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | women                
     eprop_prod_state        | null                 
     eprop_prod_list_price   | 7.99                 
     eprop_prod_name         | null                 
     eprop_dept_tags         | null                 
     eprop_prod_color        | null                 
     eprop_prod_condition    | null                 
     eprop_prod_discount     | null                 
     eprop_prod_sizing_id    | null                 
     eprop_prod_category     | null                 
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 91444761             
     session_id              | 1512110113000        
    -RECORD 2---------------------------------------
     eprop_prod_id           | 27574685             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | women                
     eprop_prod_state        | listed               
     eprop_prod_list_price   | 12.99                
     eprop_prod_name         | Style&Co Cardigan    
     eprop_dept_tags         | null                 
     eprop_prod_color        |  Black               
     eprop_prod_condition    | Q1_only              
     eprop_prod_discount     | 0.78                 
     eprop_prod_sizing_id    | 778.0                
     eprop_prod_category     | 335                  
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 13861851             
     session_id              | 1512113604000        
    -RECORD 3---------------------------------------
     eprop_prod_id           | 27118744             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | O                    
     eprop_prod_state        | null                 
     eprop_prod_list_price   | 50.99                
     eprop_prod_name         | null                 
     eprop_dept_tags         | null                 
     eprop_prod_color        | null                 
     eprop_prod_condition    | null                 
     eprop_prod_discount     | null                 
     eprop_prod_sizing_id    | null                 
     eprop_prod_category     | null                 
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 2145675              
     session_id              | 1512114689000        
    -RECORD 4---------------------------------------
     eprop_prod_id           | 24027609             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | plus                 
     eprop_prod_state        | null                 
     eprop_prod_list_price   | 15.99                
     eprop_prod_name         | null                 
     eprop_dept_tags         | null                 
     eprop_prod_color        | null                 
     eprop_prod_condition    | null                 
     eprop_prod_discount     | null                 
     eprop_prod_sizing_id    | null                 
     eprop_prod_category     | null                 
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 36771861             
     session_id              | 1512115140000        
    -RECORD 5---------------------------------------
     eprop_prod_id           | 28562538             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | O                    
     eprop_prod_state        | null                 
     eprop_prod_list_price   | 69.99                
     eprop_prod_name         | null                 
     eprop_dept_tags         | null                 
     eprop_prod_color        | null                 
     eprop_prod_condition    | null                 
     eprop_prod_discount     | null                 
     eprop_prod_sizing_id    | null                 
     eprop_prod_category     | null                 
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 3312971              
     session_id              | 1512112728000        
    -RECORD 6---------------------------------------
     eprop_prod_id           | 28364450             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | women                
     eprop_prod_state        | null                 
     eprop_prod_list_price   | 26.99                
     eprop_prod_name         | null                 
     eprop_dept_tags         | null                 
     eprop_prod_color        | null                 
     eprop_prod_condition    | null                 
     eprop_prod_discount     | null                 
     eprop_prod_sizing_id    | null                 
     eprop_prod_category     | null                 
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 36231                
     session_id              | 1512114236000        
    -RECORD 7---------------------------------------
     eprop_prod_id           | 28590200             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | girls                
     eprop_prod_state        | listed               
     eprop_prod_list_price   | 7.99                 
     eprop_prod_name         | Justice Capris       
     eprop_dept_tags         | null                 
     eprop_prod_color        | null                 
     eprop_prod_condition    | null                 
     eprop_prod_discount     | null                 
     eprop_prod_sizing_id    | 61.0                 
     eprop_prod_category     | 112                  
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 9838422              
     session_id              | 1512110625000        
    -RECORD 8---------------------------------------
     eprop_prod_id           | 28572868             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | null                 
     eprop_prod_state        | null                 
     eprop_prod_list_price   | null                 
     eprop_prod_name         | null                 
     eprop_dept_tags         | null                 
     eprop_prod_color        | null                 
     eprop_prod_condition    | null                 
     eprop_prod_discount     | null                 
     eprop_prod_sizing_id    | null                 
     eprop_prod_category     | null                 
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 2123672              
     session_id              | 1512115086000        
    -RECORD 9---------------------------------------
     eprop_prod_id           | 28526371             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | womenshoes           
     eprop_prod_state        | null                 
     eprop_prod_list_price   | 298.99               
     eprop_prod_name         | null                 
     eprop_dept_tags         | null                 
     eprop_prod_color        | null                 
     eprop_prod_condition    | null                 
     eprop_prod_discount     | null                 
     eprop_prod_sizing_id    | null                 
     eprop_prod_category     | null                 
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 59558831             
     session_id              | 1512114569000        
    -RECORD 10--------------------------------------
     eprop_prod_id           | 28592860             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | O                    
     eprop_prod_state        | null                 
     eprop_prod_list_price   | 54.99                
     eprop_prod_name         | null                 
     eprop_dept_tags         | null                 
     eprop_prod_color        | null                 
     eprop_prod_condition    | null                 
     eprop_prod_discount     | null                 
     eprop_prod_sizing_id    | null                 
     eprop_prod_category     | null                 
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 795761               
     session_id              | 1512111924000        
    -RECORD 11--------------------------------------
     eprop_prod_id           | 28143785             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | women                
     eprop_prod_state        | null                 
     eprop_prod_list_price   | 30.99                
     eprop_prod_name         | null                 
     eprop_dept_tags         | null                 
     eprop_prod_color        | null                 
     eprop_prod_condition    | null                 
     eprop_prod_discount     | null                 
     eprop_prod_sizing_id    | null                 
     eprop_prod_category     | null                 
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 2145675              
     session_id              | 1512114689000        
    -RECORD 12--------------------------------------
     eprop_prod_id           | 28541102             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | null                 
     eprop_prod_state        | null                 
     eprop_prod_list_price   | null                 
     eprop_prod_name         | null                 
     eprop_dept_tags         | null                 
     eprop_prod_color        | null                 
     eprop_prod_condition    | null                 
     eprop_prod_discount     | null                 
     eprop_prod_sizing_id    | null                 
     eprop_prod_category     | null                 
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 9838422              
     session_id              | 1512110625000        
    -RECORD 13--------------------------------------
     eprop_prod_id           | 27487108             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | O                    
     eprop_prod_state        | listed               
     eprop_prod_list_price   | 32.99                
     eprop_prod_name         | Adriano Goldschmi... 
     eprop_dept_tags         | null                 
     eprop_prod_color        | null                 
     eprop_prod_condition    | null                 
     eprop_prod_discount     | null                 
     eprop_prod_sizing_id    | 806.0                
     eprop_prod_category     | 356                  
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 5315612              
     session_id              | 1512114938000        
    -RECORD 14--------------------------------------
     eprop_prod_id           | 28629430             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | women                
     eprop_prod_state        | listed               
     eprop_prod_list_price   | 16.99                
     eprop_prod_name         | Ann Taylor Sleeve... 
     eprop_dept_tags         | null                 
     eprop_prod_color        | null                 
     eprop_prod_condition    | null                 
     eprop_prod_discount     | null                 
     eprop_prod_sizing_id    | 812.0                
     eprop_prod_category     | 742                  
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 671512               
     session_id              | 1512112897000        
    -RECORD 15--------------------------------------
     eprop_prod_id           | 28302351             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | O                    
     eprop_prod_state        | null                 
     eprop_prod_list_price   | 70.99                
     eprop_prod_name         | null                 
     eprop_dept_tags         | null                 
     eprop_prod_color        | null                 
     eprop_prod_condition    | null                 
     eprop_prod_discount     | null                 
     eprop_prod_sizing_id    | null                 
     eprop_prod_category     | null                 
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 9498665              
     session_id              | 1512115242000        
    -RECORD 16--------------------------------------
     eprop_prod_id           | 27823076             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | women                
     eprop_prod_state        | null                 
     eprop_prod_list_price   | 24.99                
     eprop_prod_name         | null                 
     eprop_dept_tags         | null                 
     eprop_prod_color        | null                 
     eprop_prod_condition    | null                 
     eprop_prod_discount     | null                 
     eprop_prod_sizing_id    | null                 
     eprop_prod_category     | null                 
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 1863433              
     session_id              | 1512112828000        
    -RECORD 17--------------------------------------
     eprop_prod_id           | 28438635             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | women                
     eprop_prod_state        | listed               
     eprop_prod_list_price   | 17.99                
     eprop_prod_name         | C. Wonder Turtlen... 
     eprop_dept_tags         | null                 
     eprop_prod_color        | null                 
     eprop_prod_condition    | null                 
     eprop_prod_discount     | null                 
     eprop_prod_sizing_id    | 778.0                
     eprop_prod_category     | 795                  
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 2541892              
     session_id              | 1512115019000        
    -RECORD 18--------------------------------------
     eprop_prod_id           | 28488223             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | women                
     eprop_prod_state        | null                 
     eprop_prod_list_price   | 8.99                 
     eprop_prod_name         | null                 
     eprop_dept_tags         | null                 
     eprop_prod_color        | null                 
     eprop_prod_condition    | null                 
     eprop_prod_discount     | null                 
     eprop_prod_sizing_id    | null                 
     eprop_prod_category     | null                 
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 95156921             
     session_id              | 1512114434000        
    -RECORD 19--------------------------------------
     eprop_prod_id           | 27967821             
     eprop_prod_dept         | null                 
     eprop_prod_merch_dept   | null                 
     eprop_prod_state        | null                 
     eprop_prod_list_price   | null                 
     eprop_prod_name         | null                 
     eprop_dept_tags         | null                 
     eprop_prod_color        | null                 
     eprop_prod_condition    | null                 
     eprop_prod_discount     | null                 
     eprop_prod_sizing_id    | null                 
     eprop_prod_category     | null                 
     eprop_order_id          | null                 
     uprop_first_purchase_ts | null                 
     uprop_last_purchase_ts  | null                 
     revenue                 | null                 
     user_id                 | 595656               
     session_id              | 1512113208000        
    only showing top 20 rows
    


-------

### Dimension 3: order_details 
 


```python
order_details_table = df_master.select('eprop_order_id',
                                        'eprop_order_type',
                                        'eprop_pay_with',
                                        'eprop_order_contains_bag',
                                        'eprop_total_shipping_fees',
                                        'eprop_total_tax',
                                        'eprop_total_discount',
                                        'eprop_item_qty',
                                        'eprop_order_asp',
                                        'eprop_total_cash_credits_used',
                                        'eprop_item_order_seq',
                                        'uprop_first_purchase_ts',
                                        'uprop_last_purchase_ts',
                                        'eprop_cart_id',
                                        'eprop_prod_id',
                                        'eprop_prod_list_price',
                                        'user_id',
                                        'session_id')
```


```python
order_details_table.limit(5).toPandas() #Dataframe for order table
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>eprop_order_id</th>
      <th>eprop_order_type</th>
      <th>eprop_pay_with</th>
      <th>eprop_order_contains_bag</th>
      <th>eprop_total_shipping_fees</th>
      <th>eprop_total_tax</th>
      <th>eprop_total_discount</th>
      <th>eprop_item_qty</th>
      <th>eprop_order_asp</th>
      <th>eprop_total_cash_credits_used</th>
      <th>eprop_item_order_seq</th>
      <th>uprop_first_purchase_ts</th>
      <th>uprop_last_purchase_ts</th>
      <th>eprop_cart_id</th>
      <th>eprop_prod_id</th>
      <th>eprop_prod_list_price</th>
      <th>user_id</th>
      <th>session_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>16878712</td>
      <td>1512115155000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1612442</td>
      <td>1512112873000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1424551</td>
      <td>1512113662000</td>
    </tr>
    <tr>
      <th>3</th>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>27374860.0</td>
      <td>70.989998</td>
      <td>24254812</td>
      <td>1512108989000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>12339891</td>
      <td>1512114864000</td>
    </tr>
  </tbody>
</table>
</div>



##### Remove rows without order_id


```python
order_details_table.createOrReplaceTempView("order_details_temp") 
```


```python
df_order_notnull = scSpark.sql("SELECT * FROM order_details_temp where eprop_order_id IS NOT NULL")

df_order_notnull.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>eprop_order_id</th>
      <th>eprop_order_type</th>
      <th>eprop_pay_with</th>
      <th>eprop_order_contains_bag</th>
      <th>eprop_total_shipping_fees</th>
      <th>eprop_total_tax</th>
      <th>eprop_total_discount</th>
      <th>eprop_item_qty</th>
      <th>eprop_order_asp</th>
      <th>eprop_total_cash_credits_used</th>
      <th>eprop_item_order_seq</th>
      <th>uprop_first_purchase_ts</th>
      <th>uprop_last_purchase_ts</th>
      <th>eprop_cart_id</th>
      <th>eprop_prod_id</th>
      <th>eprop_prod_list_price</th>
      <th>user_id</th>
      <th>session_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>10157853</td>
      <td>regular</td>
      <td>braintree_apple_pay_card</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>100</td>
      <td>18.0</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>2.0</td>
      <td>NaT</td>
      <td>2017-12-01 08:04:10</td>
      <td>38705309.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>38457851</td>
      <td>1512115312000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>10157854</td>
      <td>regular</td>
      <td>braintree_credit_card</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>11.0</td>
      <td>NaT</td>
      <td>2017-12-01 08:04:25</td>
      <td>38601857.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>87166291</td>
      <td>1512115420000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>10157882</td>
      <td>regular</td>
      <td>None</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>2017-12-01 08:31:01</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>44295841</td>
      <td>1512116771000</td>
    </tr>
    <tr>
      <th>3</th>
      <td>10157910</td>
      <td>regular</td>
      <td>braintree_apple_pay_card</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>0</td>
      <td>3.0</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>3.0</td>
      <td>NaT</td>
      <td>2017-12-01 08:54:55</td>
      <td>38297109.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>289241</td>
      <td>1512116214000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>10157916</td>
      <td>regular</td>
      <td>None</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>2017-12-01 09:03:23</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4855111</td>
      <td>1512118700000</td>
    </tr>
  </tbody>
</table>
</div>



##### Dimension table:  order_details 


```python
df_order_notnull.createOrReplaceTempView("order_details")
```


```python
df_temp = scSpark.sql("SELECT * FROM order_details")

df_temp.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>eprop_order_id</th>
      <th>eprop_order_type</th>
      <th>eprop_pay_with</th>
      <th>eprop_order_contains_bag</th>
      <th>eprop_total_shipping_fees</th>
      <th>eprop_total_tax</th>
      <th>eprop_total_discount</th>
      <th>eprop_item_qty</th>
      <th>eprop_order_asp</th>
      <th>eprop_total_cash_credits_used</th>
      <th>eprop_item_order_seq</th>
      <th>uprop_first_purchase_ts</th>
      <th>uprop_last_purchase_ts</th>
      <th>eprop_cart_id</th>
      <th>eprop_prod_id</th>
      <th>eprop_prod_list_price</th>
      <th>user_id</th>
      <th>session_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>10157853</td>
      <td>regular</td>
      <td>braintree_apple_pay_card</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>100</td>
      <td>18.0</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>2.0</td>
      <td>NaT</td>
      <td>2017-12-01 08:04:10</td>
      <td>38705309.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>38457851</td>
      <td>1512115312000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>10157854</td>
      <td>regular</td>
      <td>braintree_credit_card</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>11.0</td>
      <td>NaT</td>
      <td>2017-12-01 08:04:25</td>
      <td>38601857.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>87166291</td>
      <td>1512115420000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>10157882</td>
      <td>regular</td>
      <td>None</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>2017-12-01 08:31:01</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>44295841</td>
      <td>1512116771000</td>
    </tr>
    <tr>
      <th>3</th>
      <td>10157910</td>
      <td>regular</td>
      <td>braintree_apple_pay_card</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>0</td>
      <td>3.0</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>3.0</td>
      <td>NaT</td>
      <td>2017-12-01 08:54:55</td>
      <td>38297109.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>289241</td>
      <td>1512116214000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>10157916</td>
      <td>regular</td>
      <td>None</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>NaT</td>
      <td>2017-12-01 09:03:23</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>4855111</td>
      <td>1512118700000</td>
    </tr>
  </tbody>
</table>
</div>



-------

### Dimension 4 : campaign_details


```python
campaign_details_table = df_master.select('insert_id',
                                          'uprop_dom_referrer',
                                          'uprop_utm_term',
                                          'uprop_utm_source',
                                          'uprop_utm_medium',
                                          'uprop_utm_content',
                                          'uprop_utm_email',
                                          'uprop_utm_campaign',
                                          'uprop_referral_code',
                                          'uprop_acq_signup_platform',
                                          'uprop_acq_signup_channel',
                                          'uprop_acq_signup_sub_channel',
                                          'uprop_acq_invitation_code',
                                          'uprop_acq_external_referrer',
                                          'uprop_acq_signup_method_agg',
                                          'uprop_acq_signup_method',
                                          'uprop_paid_acq_bucket',
                                          'eprop_order_id',
                                          'eprop_prod_id',
                                          'session_id')

```


```python
campaign_details_table.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>insert_id</th>
      <th>uprop_dom_referrer</th>
      <th>uprop_utm_term</th>
      <th>uprop_utm_source</th>
      <th>uprop_utm_medium</th>
      <th>uprop_utm_content</th>
      <th>uprop_utm_email</th>
      <th>uprop_utm_campaign</th>
      <th>uprop_referral_code</th>
      <th>uprop_acq_signup_platform</th>
      <th>uprop_acq_signup_channel</th>
      <th>uprop_acq_signup_sub_channel</th>
      <th>uprop_acq_invitation_code</th>
      <th>uprop_acq_external_referrer</th>
      <th>uprop_acq_signup_method_agg</th>
      <th>uprop_acq_signup_method</th>
      <th>uprop_paid_acq_bucket</th>
      <th>eprop_order_id</th>
      <th>eprop_prod_id</th>
      <th>session_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>15c25174-96ee-4672-b247-d6069d628e34</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1512115155000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>39fc5cde-2dd2-46a8-bc39-26adda1a2f25</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1512112873000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>6c1395ab-526b-443a-9101-5e0e7e8eeac6</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1512113662000</td>
    </tr>
    <tr>
      <th>3</th>
      <td>620db094-c933-431c-8f63-ad30f6bf9e5c</td>
      <td>www.google.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>27374860.0</td>
      <td>1512108989000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>f872f818-2d8a-493e-8028-28fd06f590ce</td>
      <td>www.thredup.com</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1512114864000</td>
    </tr>
  </tbody>
</table>
</div>




```python
campaign_details_table.createOrReplaceTempView("campaign_details")
```

------

### Dimension 5 : user_session_details 


```python
user_session_details_table = df_master.select('session_id',
                                              'session_start',
                                              'session_end',
                                              'user_id')
```


```python
user_session_details_table.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>session_id</th>
      <th>session_start</th>
      <th>session_end</th>
      <th>user_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1512115155000</td>
      <td>2017-12-01 07:59:15</td>
      <td>2017-12-01 08:02:58</td>
      <td>16878712</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1512112873000</td>
      <td>2017-12-01 07:21:13</td>
      <td>2017-12-01 08:13:54</td>
      <td>1612442</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1512113662000</td>
      <td>2017-12-01 07:34:22</td>
      <td>2017-12-01 08:01:07</td>
      <td>1424551</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1512108989000</td>
      <td>2017-12-01 06:16:29</td>
      <td>2017-12-01 08:07:10</td>
      <td>24254812</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1512114864000</td>
      <td>2017-12-01 07:54:24</td>
      <td>2017-12-01 09:51:46</td>
      <td>12339891</td>
    </tr>
  </tbody>
</table>
</div>




```python
user_session_details_table.createOrReplaceTempView("user_session_details")
```

-------

### Dimension 6 : user_search_details 


```python
user_search_details_table = df_master.select('eprop_pagination_no',
                                             'eprop_sort_by',
                                             'eprop_search_keywords',
                                             'eprop_search_result_count',
                                             'eprop_filter_type',
                                             'user_id',
                                             'session_id')
```


```python
user_search_details_table.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>eprop_pagination_no</th>
      <th>eprop_sort_by</th>
      <th>eprop_search_keywords</th>
      <th>eprop_search_result_count</th>
      <th>eprop_filter_type</th>
      <th>user_id</th>
      <th>session_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>16878712</td>
      <td>1512115155000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>11.0</td>
      <td>Price Low to High</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes</td>
      <td>1612442</td>
      <td>1512112873000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>No Filters</td>
      <td>1424551</td>
      <td>1512113662000</td>
    </tr>
    <tr>
      <th>3</th>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>24254812</td>
      <td>1512108989000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1.0</td>
      <td>Price Low to High</td>
      <td>None</td>
      <td>NaN</td>
      <td>Sizes</td>
      <td>12339891</td>
      <td>1512114864000</td>
    </tr>
  </tbody>
</table>
</div>




```python
user_search_details_table.createOrReplaceTempView("user_search_details")
```

------

### Dimension 7 : user_device_details 


```python
user_device_details_table = df_master.select('device_id',
                                             'eprop_app_id',
                                             'eprop_device_model',
                                             'eprop_device_resolution',
                                             'eprop_link_name',
                                             'eprop_event_mythredup_sections',
                                             'eprop_cleanout_section',
                                             'session_id')
```


```python
user_device_details_table.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>device_id</th>
      <th>eprop_app_id</th>
      <th>eprop_device_model</th>
      <th>eprop_device_resolution</th>
      <th>eprop_link_name</th>
      <th>eprop_event_mythredup_sections</th>
      <th>eprop_cleanout_section</th>
      <th>session_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>None</td>
      <td>HTCONE</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>1512115155000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>015f605f6efc00034f45b017f70404073003306b00bd0</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>1512112873000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>015d7b2f98e500078c82e2eea6790006f008a06700398</td>
      <td>thredUP 7.7.1</td>
      <td>iPhone</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>women</td>
      <td>1512113662000</td>
    </tr>
    <tr>
      <th>3</th>
      <td>01600e1656840009284f97c8becd04072001806a00978</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>1512108989000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0160103a6c49001a1f2ca5264e0405066001805e00718</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>shoes</td>
      <td>1512114864000</td>
    </tr>
  </tbody>
</table>
</div>




```python
user_device_details_table.createOrReplaceTempView("user_device_details")
```

------

### Dimension 8 : time_details


```python
time_details_table = df_master.select('event_time')\
                                                   .withColumn("hour", hour(col('event_time')))\
                                                   .withColumn("day", dayofmonth(col('event_time')))\
                                                   .withColumn("week", weekofyear(col('event_time')))\
                                                   .withColumn("month", month(col('event_time')))\
                                                   .withColumn("year", year(col('event_time')))\
                                                   .withColumn("weekday", dayofweek(col('event_time')))
```


```python
time_details_table.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>event_time</th>
      <th>hour</th>
      <th>day</th>
      <th>week</th>
      <th>month</th>
      <th>year</th>
      <th>weekday</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2017-12-01 08:00:03</td>
      <td>8</td>
      <td>1</td>
      <td>48</td>
      <td>12</td>
      <td>2017</td>
      <td>6</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2017-12-01 08:00:03</td>
      <td>8</td>
      <td>1</td>
      <td>48</td>
      <td>12</td>
      <td>2017</td>
      <td>6</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2017-12-01 08:00:03</td>
      <td>8</td>
      <td>1</td>
      <td>48</td>
      <td>12</td>
      <td>2017</td>
      <td>6</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2017-12-01 08:00:03</td>
      <td>8</td>
      <td>1</td>
      <td>48</td>
      <td>12</td>
      <td>2017</td>
      <td>6</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2017-12-01 08:00:03</td>
      <td>8</td>
      <td>1</td>
      <td>48</td>
      <td>12</td>
      <td>2017</td>
      <td>6</td>
    </tr>
  </tbody>
</table>
</div>




```python
time_details_table.createOrReplaceTempView("time_details")
```

-------

### Dimension 9: location_details


```python
location_details_table = df_master.select('location_lat',
                                            'location_lng',
                                            'uprop_zip5',
                                            'uprop_state',
                                            'session_id')
```


```python
location_details_table.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>location_lat</th>
      <th>location_lng</th>
      <th>uprop_zip5</th>
      <th>uprop_state</th>
      <th>session_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>1512115155000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>47.680099</td>
      <td>-122.120598</td>
      <td>NaN</td>
      <td>None</td>
      <td>1512112873000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>1512113662000</td>
    </tr>
    <tr>
      <th>3</th>
      <td>31.510500</td>
      <td>-97.264503</td>
      <td>NaN</td>
      <td>None</td>
      <td>1512108989000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>41.473701</td>
      <td>-81.579903</td>
      <td>NaN</td>
      <td>None</td>
      <td>1512114864000</td>
    </tr>
  </tbody>
</table>
</div>




```python
location_details_table.createOrReplaceTempView("location_details")
```

--------

### Fact table : customer_analytics


```python
customer_analytics_fact_table = df_master.select('user_id',
                                                'eprop_prod_id',
                                                'insert_id',
                                                'eprop_order_id',
                                                'device_id',
                                                'location_lat',
                                                'eprop_pagination_no',
                                                'session_id',
                                                'event_time').withColumn("customer_key",monotonically_increasing_id())
```


```python
customer_analytics_fact_table.limit(5).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>eprop_prod_id</th>
      <th>insert_id</th>
      <th>eprop_order_id</th>
      <th>device_id</th>
      <th>location_lat</th>
      <th>eprop_pagination_no</th>
      <th>session_id</th>
      <th>event_time</th>
      <th>customer_key</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>16878712</td>
      <td>NaN</td>
      <td>15c25174-96ee-4672-b247-d6069d628e34</td>
      <td>NaN</td>
      <td>89ae640fa58e4f26897354a8d880df42</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1512115155000</td>
      <td>2017-12-01 08:00:03</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1612442</td>
      <td>NaN</td>
      <td>39fc5cde-2dd2-46a8-bc39-26adda1a2f25</td>
      <td>NaN</td>
      <td>015f605f6efc00034f45b017f70404073003306b00bd0</td>
      <td>47.680099</td>
      <td>11.0</td>
      <td>1512112873000</td>
      <td>2017-12-01 08:00:03</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1424551</td>
      <td>NaN</td>
      <td>6c1395ab-526b-443a-9101-5e0e7e8eeac6</td>
      <td>NaN</td>
      <td>015d7b2f98e500078c82e2eea6790006f008a06700398</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1512113662000</td>
      <td>2017-12-01 08:00:03</td>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>24254812</td>
      <td>27374860.0</td>
      <td>620db094-c933-431c-8f63-ad30f6bf9e5c</td>
      <td>NaN</td>
      <td>01600e1656840009284f97c8becd04072001806a00978</td>
      <td>31.510500</td>
      <td>NaN</td>
      <td>1512108989000</td>
      <td>2017-12-01 08:00:03</td>
      <td>3</td>
    </tr>
    <tr>
      <th>4</th>
      <td>12339891</td>
      <td>NaN</td>
      <td>f872f818-2d8a-493e-8028-28fd06f590ce</td>
      <td>NaN</td>
      <td>0160103a6c49001a1f2ca5264e0405066001805e00718</td>
      <td>41.473701</td>
      <td>1.0</td>
      <td>1512114864000</td>
      <td>2017-12-01 08:00:03</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>




```python
customer_analytics_fact_table.createOrReplaceTempView("customer_analytics")
```

-----

## 5. Analytics and Dashboards

There are many ways analytics can be generated one way is to model the data after pipeline to apply ML algorithms. Some dashboards are prescriptive, predicive and descriptive analytics which helps management team with future trends and recommendations. Few Visualization tools such as Tableau, Looker, PowerBI makes it easy to present the data.

I am going to use the SQL, matplotlib, seaborn, ans plotly for querying the data and creating the chart/graphs.

Note: I tried using zepplin and databricks notebook but file size upload issue restrain me from that approach. 

- User behavior analytics
- Campaign analytics
- Product analytics


## 1. User Behavior Analytics

These kind of analytics helps to understand the user actions by analysing the behavior of their activities.

### 1A. Count of events in general 

Event are the pages which user visits during their session while login, below query calculates the most number of pages visitied by the user.




```python
df_event_count = scSpark.sql('''
          SELECT
                COUNT(event_type) AS Count_of_event,
                event_type
          FROM
                user_details 
          GROUP BY 
                event_type ''')
```


```python
%%time
event_data = df_event_count.limit(5).toPandas()
```

    CPU times: user 7.28 ms, sys: 3.06 ms, total: 10.3 ms
    Wall time: 14.2 s


-------


```python
#Define the structure of the chart using figure and horizontal bar takes x,y axis parameters
plt.figure(figsize=(9,5), dpi = 100)
event_data.plot.barh(x='event_type', y='Count_of_event', align='center', rot=0, color='teal')

#To define X,Y axis and title name
plt.xlabel('Event Page')  
plt.ylabel('Count of events')
plt.title('Event count for pages', fontsize=14) 
```




    <Figure size 900x500 with 0 Axes>






    <AxesSubplot:ylabel='event_type'>






    Text(0.5, 0, 'Event Page')






    Text(0, 0.5, 'Count of events')






    Text(0.5, 1.0, 'Event count for pages')




    <Figure size 900x500 with 0 Axes>



    
![png](thredUP_Final-new_files/thredUP_Final-new_132_6.png)
    


-------

### 1B. Count of page views for particular day

This analysis can help to find the general stats of the users daily page views on the platform. 


```python
%%time
df_page_count = scSpark.sql('''
          SELECT
                COUNT(event_type) AS page_views_count,
                DATE(event_time) 
          FROM
                user_details 
          GROUP BY 
                DATE(user_details.event_time)
        ORDER BY 
                DATE(user_details.event_time)''')
```

    CPU times: user 745 µs, sys: 1.1 ms, total: 1.85 ms
    Wall time: 13.5 ms



```python
df_page_count.limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>page_views_count</th>
      <th>event_time</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1327365</td>
      <td>2017-12-01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2257544</td>
      <td>2017-12-02</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2065298</td>
      <td>2017-12-03</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2030055</td>
      <td>2017-12-04</td>
    </tr>
    <tr>
      <th>4</th>
      <td>904246</td>
      <td>2017-12-05</td>
    </tr>
  </tbody>
</table>
</div>




```python
pdf = df_page_count.toPandas()
```


```python
#Plot takes the page count views aggregated to their categorial column 

df_page_chart = sns.barplot(x='event_time', y='page_views_count', data=pdf)
df_page_chart.set_title('Count of page views');
```


    
![png](thredUP_Final-new_files/thredUP_Final-new_138_0.png)
    


-------

### 1C. Count of user device models

Analysis can help to find which mobile platform to more focus on considering the growth in users or in opposite if seen the slide decline in the user growth can rectify where things are going wrong.


```python
df_device_model = scSpark.sql('''
                  SELECT 
                         eprop_device_model, 
                         COUNT(eprop_device_model) as device_count
                  FROM 
                         user_device_details 
                  GROUP BY
                         eprop_device_model
                  ORDER BY
                         device_count DESC''')
```


```python
df_device = df_device_model\
                           .groupBy('eprop_device_model')\
                           .count()\
                           .orderBy('count', ascending=False)\
                           .limit(10)
```


```python
df_device_model.limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>eprop_device_model</th>
      <th>device_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>iPhone</td>
      <td>512123</td>
    </tr>
    <tr>
      <th>1</th>
      <td>iPhone 6s</td>
      <td>383582</td>
    </tr>
    <tr>
      <th>2</th>
      <td>iPhone 6</td>
      <td>281981</td>
    </tr>
    <tr>
      <th>3</th>
      <td>iPhone9,1</td>
      <td>242016</td>
    </tr>
    <tr>
      <th>4</th>
      <td>iPhone9,2</td>
      <td>162354</td>
    </tr>
    <tr>
      <th>5</th>
      <td>iPhone 6s Plus</td>
      <td>160594</td>
    </tr>
    <tr>
      <th>6</th>
      <td>iPhone9,3</td>
      <td>130938</td>
    </tr>
    <tr>
      <th>7</th>
      <td>iPhone8,4</td>
      <td>117396</td>
    </tr>
    <tr>
      <th>8</th>
      <td>iPhone9,4</td>
      <td>115821</td>
    </tr>
    <tr>
      <th>9</th>
      <td>iPhone 6 Plus</td>
      <td>103658</td>
    </tr>
  </tbody>
</table>
</div>



-------

### 1D. Count of daily active users 

Here we used the group by function by DataFrame


```python
df_active_user = scSpark.sql('''
          SELECT
                COUNT(session_id) AS active_users,
                DATE(event_time) 
          FROM
                user_details 
          GROUP BY 
                DATE(user_details.event_time)
        ORDER BY 
                DATE(user_details.event_time)''')
```


```python
pdf1 = df_active_user.limit(10).toPandas()
```


```python
%%time
df_active_user = sns.barplot(x='event_time', y='active_users', data=pdf1)
df_active_user.set_title('Count of daily active users');
```

    CPU times: user 49.6 ms, sys: 30.1 ms, total: 79.7 ms
    Wall time: 119 ms





    Text(0.5, 1.0, 'Count of daily active users')




    
![png](thredUP_Final-new_files/thredUP_Final-new_149_2.png)
    


-------

## 2. Campaign analytics

Campaign is one of the important side to focus on while analysing the data. As it gives the further view on how much we should invest in campaigns and marketing for particular or group of customers. This also helps to define the budget and target the more leads on prospective customers.

Below are the few simple metrics for campaign analytics:

### 2A. Customer sign-up methods (TOP 10)


```python
df_campaign_signup = scSpark.sql('''SELECT * FROM campaign_details WHERE uprop_acq_signup_method IS NOT NULL''')
```


```python
df_sign_count = df_campaign_signup\
                                    .groupBy('uprop_acq_signup_method')\
                                    .count()\
                                    .orderBy('count', ascending=False)\
                                    .limit(10)
```


```python
df_sign_count.cache().show()
```

    +-----------------------+-----+
    |uprop_acq_signup_method|count|
    +-----------------------+-----+
    |   mobile_web_signup...| 8206|
    |            email_modal| 5625|
    |   mobile_web_signup...| 3769|
    |   mobile_web_facebo...| 1962|
    |                 iPhone| 1670|
    |   mobile_web_facebo...| 1547|
    |        facebook_signup| 1187|
    |   mobile_web_onboar...|  975|
    |     android_smartphone|  847|
    |   onboarding_modal_...|  803|
    +-----------------------+-----+
    



```python
df_sign_count.toPandas().plot.barh(x='uprop_acq_signup_method',
                                     align='center', 
                                     color="skyblue")

plt.xlabel('Count of users')
plt.ylabel('Sign-Up method')
plt.title('Customer Sign up Methods', fontsize=14)
```




    <AxesSubplot:ylabel='uprop_acq_signup_method'>






    Text(0.5, 0, 'Count of users')






    Text(0, 0.5, 'Sign-Up method')






    Text(0.5, 1.0, 'Customer Sign up Methods')




    
![png](thredUP_Final-new_files/thredUP_Final-new_156_4.png)
    


-------

### 2B. Percentage of customers by platform


```python
df_campaign_platform = scSpark.sql('''SELECT * FROM campaign_details WHERE uprop_acq_signup_platform IS NOT NULL''')
```


```python
df_platform_count = df_campaign_platform\
                                        .groupBy('uprop_acq_signup_platform')\
                                        .count()\
                                        .orderBy('count', ascending=False)\
                                        .limit(6)
```


```python
df_platform_count.cache().show(10)
```

    +-------------------------+-----+
    |uprop_acq_signup_platform|count|
    +-------------------------+-----+
    |               mobile_web|16779|
    |                      web| 8147|
    |                      iOS| 2528|
    |                  android| 1183|
    |                    other|  879|
    |                     luxe|   10|
    +-------------------------+-----+
    



```python
my_labels = 'Mobile_web','web','iOS','android','other','luxe',

df_platform_count.toPandas().plot(kind='pie', 
                                  labels=my_labels,
                                  y= 'count', 
                                  subplots=True,
                                  shadow = True,
                                  startangle=90,
                                  figsize=(15,10), 
                                  autopct='%1.1f%%')

plt.title('Customers % by Platform', fontsize=14)
```




    array([<AxesSubplot:ylabel='count'>], dtype=object)






    Text(0.5, 1.0, 'Customers % by Platform')




    
![png](thredUP_Final-new_files/thredUP_Final-new_162_2.png)
    


-------

### 2C. Customer count by zip code


```python
df_campaign_zip = scSpark.sql('''SELECT * FROM user_details WHERE uprop_zip5 IS NOT NULL''') 
```


```python
df_user_zip = df_campaign_zip\
                             .groupBy('uprop_zip5')\
                             .count()\
                             .orderBy('count', ascending=False)\
                             .limit(5)
```


```python
df_user_zip.cache().show()

```

    +----------+-----+
    |uprop_zip5|count|
    +----------+-----+
    |     75270|  160|
    |     77002|  149|
    |     60602|  147|
    |     19107|   79|
    |     10029|   76|
    +----------+-----+
    



```python
df_user_zip.toPandas().plot.bar(x='uprop_zip5', align='center', color='orange')
plt.ylabel('Customer count')
plt.xlabel('ZIP Codes')
plt.title('Top 5 customers by ZIP code', fontsize=14)
```




    <AxesSubplot:xlabel='uprop_zip5'>






    Text(0, 0.5, 'Customer count')






    Text(0.5, 0, 'ZIP Codes')






    Text(0.5, 1.0, 'Top 5 customers by ZIP code')




    
![png](thredUP_Final-new_files/thredUP_Final-new_168_4.png)
    


-------

## 3. Product Analytics

### 3A. Ranking the products

Dense rank window function creates a parition on columns and rank the rows based on the partition results set. 

Note: Revenue column had null values but I fill in with random values and try to rank the products


```python
df_product_rev = scSpark.sql('''
             SELECT 
                   eprop_prod_id,
                   eprop_prod_name, 
                   eprop_prod_category,
                   revenue, 
                   dense_rank
             FROM 
                  (SELECT
                         DISTINCT(eprop_prod_id),
                         eprop_prod_name, 
                         eprop_prod_category, 
                         revenue,
                         DENSE_RANK(revenue)
                                    OVER (PARTITION BY eprop_prod_category 
                                    ORDER BY revenue DESC) AS dense_rank
                   FROM 
                         product_details  
                   )a
             WHERE 
                    dense_rank <= 3
                 ''').withColumn("revenue",when(rand() > 0.5, 1).otherwise(0))
```


```python
df_product_rev.limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>eprop_prod_id</th>
      <th>eprop_prod_name</th>
      <th>eprop_prod_category</th>
      <th>revenue</th>
      <th>dense_rank</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>28595651</td>
      <td>Tykes Short Sleeve Onesie</td>
      <td>148</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>28261817</td>
      <td>Baby Gap Short Sleeve Onesie</td>
      <td>148</td>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>28211366</td>
      <td>Baby Gap Short Sleeve Onesie</td>
      <td>148</td>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>20583384</td>
      <td>The Children's Place Short Sleeve Onesie</td>
      <td>148</td>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>28390258</td>
      <td>Carter's Short Sleeve Onesie</td>
      <td>148</td>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>5</th>
      <td>28295707</td>
      <td>Wrangler Jeans Co Short Sleeve Onesie</td>
      <td>148</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>6</th>
      <td>28535659</td>
      <td>Nfl Short Sleeve Onesie</td>
      <td>148</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>7</th>
      <td>28053532</td>
      <td>Cat &amp; Jack Short Sleeve Onesie</td>
      <td>148</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>8</th>
      <td>28535959</td>
      <td>Nfl Short Sleeve Onesie</td>
      <td>148</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>9</th>
      <td>27276824</td>
      <td>Carter's Short Sleeve Onesie</td>
      <td>148</td>
      <td>1</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



------

### 3B. Product Purchase date (check for returning customer)

We are trying to find out the user is a returning customer, using last purchase date column we find the first order date for the user and then partition on user_id to see if user has order any product after that


```python
df_purchase_return = scSpark.sql('''
                     SELECT 
                           user_id,
                           uprop_last_purchase_ts, 
                           FIRST(uprop_last_purchase_ts) 
                                 OVER (PARTITION BY user_id ORDER BY uprop_last_purchase_ts)
                                 AS first_order_date, 
                      CASE 
                           WHEN FIRST(uprop_last_purchase_ts) 
                                      OVER (PARTITION BY user_id 
                                      ORDER BY uprop_last_purchase_ts) = uprop_last_purchase_ts then 1 else 0 
                      END AS
                           returning_customer
                      FROM 
                           order_details
                      ORDER BY 
                           uprop_first_purchase_ts, 
                           uprop_last_purchase_ts''')
```


```python
df_purchase_return.limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>uprop_last_purchase_ts</th>
      <th>first_order_date</th>
      <th>returning_customer</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>882774</td>
      <td>2017-12-01 08:01:00</td>
      <td>2017-12-01 08:01:00</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>7875861</td>
      <td>2017-12-01 08:01:50</td>
      <td>2017-12-01 08:01:50</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>959394</td>
      <td>2017-12-01 08:02:58</td>
      <td>2017-12-01 08:02:58</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1999461</td>
      <td>2017-12-01 08:03:13</td>
      <td>2017-12-01 08:03:13</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>38457851</td>
      <td>2017-12-01 08:04:10</td>
      <td>2017-12-01 08:04:10</td>
      <td>1</td>
    </tr>
    <tr>
      <th>5</th>
      <td>87166291</td>
      <td>2017-12-01 08:04:25</td>
      <td>2017-12-01 08:04:25</td>
      <td>1</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2832941</td>
      <td>2017-12-01 08:05:23</td>
      <td>2017-12-01 08:05:23</td>
      <td>1</td>
    </tr>
    <tr>
      <th>7</th>
      <td>7458238</td>
      <td>2017-12-01 08:06:35</td>
      <td>2017-12-01 08:06:35</td>
      <td>1</td>
    </tr>
    <tr>
      <th>8</th>
      <td>413322</td>
      <td>2017-12-01 08:06:55</td>
      <td>2017-12-01 08:06:55</td>
      <td>1</td>
    </tr>
    <tr>
      <th>9</th>
      <td>852531</td>
      <td>2017-12-01 08:08:02</td>
      <td>2017-12-01 08:08:02</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



------

### 3C. User first and last purchase difference in days and months

We have first_purchase_date column null but still we can find the maximum days of time user has not ordered anything from the merchandise


```python
df_purchase_diff = scSpark.sql('''
                   SELECT 
                         user_id,
                         uprop_first_purchase_ts,
                         uprop_last_purchase_ts, 
                         DATEDIFF( uprop_last_purchase_ts, uprop_first_purchase_ts ) AS diff_in_days,
                                   CAST( months_between( uprop_last_purchase_ts, uprop_first_purchase_ts ) AS INT) AS diff_in_months      
                   FROM 
                         order_details
                   ORDER BY 1''')
```


```python
df_purchase_diff.limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>uprop_first_purchase_ts</th>
      <th>uprop_last_purchase_ts</th>
      <th>diff_in_days</th>
      <th>diff_in_months</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>111131</td>
      <td>NaT</td>
      <td>2017-12-01 15:43:04</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>111212</td>
      <td>NaT</td>
      <td>2017-12-01 16:55:41</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>111238</td>
      <td>NaT</td>
      <td>2017-12-03 00:12:21</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>111322</td>
      <td>2017-12-02 17:40:59</td>
      <td>2017-12-02 17:40:59</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>11136931</td>
      <td>NaT</td>
      <td>2017-12-04 04:53:29</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>5</th>
      <td>111376</td>
      <td>NaT</td>
      <td>2017-12-02 11:35:43</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>6</th>
      <td>1114241</td>
      <td>NaT</td>
      <td>2017-12-04 18:32:51</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>7</th>
      <td>111434</td>
      <td>NaT</td>
      <td>2017-12-05 02:04:20</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>8</th>
      <td>11148112</td>
      <td>2017-12-01 16:46:27</td>
      <td>2017-12-01 16:46:27</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>1115332</td>
      <td>NaT</td>
      <td>2017-12-04 01:52:04</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>



-------

### 3D. Cumulative amount spent by each customer


```python
df_product_cumulate = scSpark.sql('''
                     SELECT
                             * 
                     FROM 
                            product_details
                     WHERE
                            user_id 
                     IS NOT NULL  ''') 
```


```python
df_product_cumulate.createOrReplaceTempView("product_details_amount") 
```


```python
df_purchase_cum = scSpark.sql('''
              SELECT 
                     user_id,
                     eprop_prod_id, 
                     eprop_prod_name,
                     uprop_last_purchase_ts, 
                     eprop_prod_list_price, 
                     ROUND(SUM(eprop_prod_list_price) 
                           OVER (PARTITION BY user_id 
                                 ORDER BY uprop_last_purchase_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS cum_expense
              FROM 
                     product_details_amount
              GROUP BY
                     user_id,
                     eprop_prod_id,
                     eprop_prod_name,
                     uprop_last_purchase_ts,
                     eprop_prod_list_price
              ORDER BY 
                     user_id,
                     uprop_last_purchase_ts''')
```


```python
df_purchase_cum.limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>eprop_prod_id</th>
      <th>eprop_prod_name</th>
      <th>uprop_last_purchase_ts</th>
      <th>eprop_prod_list_price</th>
      <th>cum_expense</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>11111322</td>
      <td>27737876</td>
      <td>Anna Sui Faux Fur Jacket</td>
      <td>NaT</td>
      <td>94.989998</td>
      <td>131.98</td>
    </tr>
    <tr>
      <th>1</th>
      <td>11111322</td>
      <td>27127751</td>
      <td>Adrienne Vittadini Faux Fur Jacket</td>
      <td>NaT</td>
      <td>33.990002</td>
      <td>240.95</td>
    </tr>
    <tr>
      <th>2</th>
      <td>11111322</td>
      <td>28643839</td>
      <td>Express Faux Fur Jacket</td>
      <td>NaT</td>
      <td>41.990002</td>
      <td>324.93</td>
    </tr>
    <tr>
      <th>3</th>
      <td>11111322</td>
      <td>27233987</td>
      <td>Express Faux Fur Jacket</td>
      <td>NaT</td>
      <td>41.990002</td>
      <td>282.94</td>
    </tr>
    <tr>
      <th>4</th>
      <td>11111322</td>
      <td>27854363</td>
      <td>Roaman's Faux Fur Jacket</td>
      <td>NaT</td>
      <td>36.990002</td>
      <td>36.99</td>
    </tr>
    <tr>
      <th>5</th>
      <td>11111322</td>
      <td>28616226</td>
      <td>H&amp;M Faux Fur Jacket</td>
      <td>NaT</td>
      <td>13.990000</td>
      <td>206.96</td>
    </tr>
    <tr>
      <th>6</th>
      <td>11111322</td>
      <td>25063726</td>
      <td>Banana Republic Faux Fur Jacket</td>
      <td>NaT</td>
      <td>60.990002</td>
      <td>192.97</td>
    </tr>
    <tr>
      <th>7</th>
      <td>1111171</td>
      <td>28568574</td>
      <td>Victoria's Secret Pink Sweatshirt</td>
      <td>NaT</td>
      <td>20.990000</td>
      <td>20.99</td>
    </tr>
    <tr>
      <th>8</th>
      <td>1111194</td>
      <td>26317630</td>
      <td>Coach Crossbody Bag</td>
      <td>NaT</td>
      <td>88.989998</td>
      <td>393.98</td>
    </tr>
    <tr>
      <th>9</th>
      <td>1111194</td>
      <td>28024004</td>
      <td>Baggallini Crossbody Bag</td>
      <td>NaT</td>
      <td>42.990002</td>
      <td>436.97</td>
    </tr>
  </tbody>
</table>
</div>



### 3E. Moving Average of price for each customer


```python
df_mov_avg = scSpark.sql('''
             SELECT  
                   user_id,
                   session_id,
                   eprop_prod_list_price, 
                   ROUND(AVG(eprop_prod_list_price) 
                         OVER (PARTITION BY user_id 
                         ORDER BY session_id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), 2) AS moving_avg
             FROM 
                   product_details_amount
             ORDER BY
                   user_id,
                   session_id''')
```


```python
df_mov_avg.limit(10).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>session_id</th>
      <th>eprop_prod_list_price</th>
      <th>moving_avg</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>11111322</td>
      <td>1512235554000</td>
      <td>33.990002</td>
      <td>37.99</td>
    </tr>
    <tr>
      <th>1</th>
      <td>11111322</td>
      <td>1512235554000</td>
      <td>94.989998</td>
      <td>57.99</td>
    </tr>
    <tr>
      <th>2</th>
      <td>11111322</td>
      <td>1512235554000</td>
      <td>13.990000</td>
      <td>37.49</td>
    </tr>
    <tr>
      <th>3</th>
      <td>11111322</td>
      <td>1512235554000</td>
      <td>41.990002</td>
      <td>56.99</td>
    </tr>
    <tr>
      <th>4</th>
      <td>11111322</td>
      <td>1512235554000</td>
      <td>36.990002</td>
      <td>57.99</td>
    </tr>
    <tr>
      <th>5</th>
      <td>11111322</td>
      <td>1512235554000</td>
      <td>41.990002</td>
      <td>46.66</td>
    </tr>
    <tr>
      <th>6</th>
      <td>11111322</td>
      <td>1512235554000</td>
      <td>60.990002</td>
      <td>38.99</td>
    </tr>
    <tr>
      <th>7</th>
      <td>1111171</td>
      <td>1512328271000</td>
      <td>20.990000</td>
      <td>20.99</td>
    </tr>
    <tr>
      <th>8</th>
      <td>1111194</td>
      <td>1512182631000</td>
      <td>88.989998</td>
      <td>65.99</td>
    </tr>
    <tr>
      <th>9</th>
      <td>1111194</td>
      <td>1512212945000</td>
      <td>42.990002</td>
      <td>145.66</td>
    </tr>
  </tbody>
</table>
</div>



### Write data to Parquet File


```python
df_mov_avg.write.parquet("platform_count/df_mov_avg.parquet")
```

### Few approaches, I have experienced implementing before for ETL data pipelines and further analytics:
    
   1. Data Pipeline with MySQL and Pentaho
      - Database - MySQL
      - Integration tool - Pentaho
      - Reporting and Dashboards - Tableau 
        
    Link reference:  [Port and Shipment Data Analysis](https://github.com/mohitcpatil/Ports-and-Shipment-Data-Analytics)
    
       
   2. Data Pipeline with Cassandra and Redshift 
      - Database - Apache Cassandra
      - Integration tool - AWS Redshift
      - Storage - AWS S3
        
    Link reference:  [Sparkify Data Warehouse AWS Redshift](https://github.com/mohitcpatil/Data-Engineering-Nanodegree-Udacity-Program-Projects-and-Excercise/tree/master/M2%20Cloud%20Data%20Warehouse/Project%203%20Cloud%20Data%20Warehouse%20with%20AWS)     
 
    
   3. Data Analysis and Visualization in Pandas & NumPy 
      
   Link reference : [Boston Marathon Analysis](https://github.com/mohitcpatil/Boston-Marathon-Analysis)     
 
         
    
   4. Data modeling with PostgreSQL and Apache Cassandra
        
   Link reference:  [Data modeling in SQL & NoSQL database](https://github.com/mohitcpatil/Data-Engineering-Nanodegree-Udacity-Program-Projects-and-Excercise/tree/master/M1%20Data%20Modeling)     
   
    
   5. Data pipeline with AWS Step-functions and Lambda
    
   Link reference:  [Step-functions and Lambda](-)     

    

### Learning and Key takeaways:




```python

```

Futuristic step:
    
Spark provides MLlib a machine learning library for developing the ML models. This libary provides the whole framework from preprocessing and munging then data and training the models for predictions.

Right now, we have build our pipeline and data is ready for developing the ML models  


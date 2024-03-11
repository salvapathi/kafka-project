import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

import pandas as pd
import numpy as np
import re
import mysql.connector
import pandas as pd
import numpy as np
import psycopg2
from minio import Minio
#from minio.error import ResponseError
import warnings
# Suppress all warnings
warnings.filterwarnings("ignore")
import boto3
from io import StringIO

#mark libraries 
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
spark = SparkSession.builder .appName("spark_kakfa").getOrCreate()
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import StringIndexer, VectorAssembler,OneHotEncoder
from pyspark.ml import Pipeline
import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from hyperopt import Trials
from hyperopt import hp
target_column="churn"

from hyperopt import hp, fmin, tpe, Trials

#ML-flow
import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
import mlflow.sklearn
from pymongo import MongoClient
from sqlalchemy import create_engine

#kafka
from confluent_kafka import Producer, Consumer, KafkaError
import json
from confluent_kafka.admin import AdminClient, NewTopic

#data time 
from datetime import datetime
import time


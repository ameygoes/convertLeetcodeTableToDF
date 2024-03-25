def getDataFrame(input_structure):
    table_name = None
    lines = input_structure.split("\n")

    # Iterate through each table definition and create DataFrames
    data = []
    for idx, line in enumerate(lines):
        # Skip empty lines
        if not line.strip() or line.startswith('+'):
            continue
        
        if ':' in line.strip():
            table_name = line.split(':')[0].split()[0]
            continue

        # Split the table definition into lines
        parts = line.strip().split('|')
        # Extract column names and data types from subsequent parts
        columns = []
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if part.isdigit():
                part = int(part)
            columns.append(part)
        data.append(tuple(columns))
    return [data[1:], table_name]

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Create DataFrames") \
    .getOrCreate()


def get_spark_data_type(python_type):
    type_mapping = {
        'int': IntegerType(),
        'float': FloatType(),
        'str': StringType(),
        'bool': BooleanType(),
        'bytes': BinaryType(),
        'bytearray': BinaryType(),
        'complex': StringType(), 
        'dict': MapType(StringType(), StringType()), 
        'list': ArrayType(StringType()),  
    }
    return type_mapping.get(python_type, StringType())  

def getDF(input_structure, schema_details):
    data, table_name = getDataFrame(input_structure)
    schema = getSchema(schema_details)
    return spark.createDataFrame(data, schema)

def getSchema(schema_details):
    data, table_name = getDataFrame(schema_details)
    return StructType([StructField(col_name, get_spark_data_type(col_type), nullable=True) for col_name, col_type in data])




# Define the input structure
input_structure = """
Employee table:
+----+--------+
| id | salary |
+----+--------+
| 1  | 100    |
| 2  | 200    |
| 3  | 300    |
+----+--------+
"""

schema_details = """
+-------------+------+
| Column Name | Type |
+-------------+------+
| id          | int  |
| salary      | int  |
+-------------+------+
"""

df = getDF(input_structure, schema_details)
df.show()


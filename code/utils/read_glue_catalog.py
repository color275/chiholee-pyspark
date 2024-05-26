import boto3
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType, DoubleType

# AWS Glue Catalog에서 테이블 정보를 가져오는 함수
def get_table_from_glue(database_name, table_name, region_name):
    client = boto3.client('glue', region_name=region_name)
    response = client.get_table(DatabaseName=database_name, Name=table_name)
    return response['Table']

# Glue 테이블 정보를 기반으로 동적 스키마 생성 함수
def create_schema(table_info):
    columns = table_info['StorageDescriptor']['Columns']
    partition_keys = table_info['PartitionKeys']

    fields = []
    for col in columns:
        col_name = col['Name']
        col_type = col['Type']
        if col_type == 'string':
            fields.append(StructField(col_name, StringType(), True))
        elif col_type == 'int':
            fields.append(StructField(col_name, IntegerType(), True))
        elif col_type == 'bigint':
            fields.append(StructField(col_name, LongType(), True))
        elif col_type == 'double':
            fields.append(StructField(col_name, DoubleType(), True))
        elif col_type == 'timestamp':
            fields.append(StructField(col_name, TimestampType(), True))
        # 추가적인 데이터 타입이 필요하면 여기에 추가
        else:
            raise ValueError(f"Unsupported column type: {col_type}")

    for part in partition_keys:
        part_name = part['Name']
        part_type = part['Type']
        if part_type == 'string':
            fields.append(StructField(part_name, StringType(), True))
        elif part_type == 'int':
            fields.append(StructField(part_name, IntegerType(), True))
        elif part_type == 'timestamp':
            fields.append(StructField(part_name, TimestampType(), True))
        else:
            raise ValueError(f"Unsupported partition type: {part_type}")

    return StructType(fields)

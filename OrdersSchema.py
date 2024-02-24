from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

orders_struct_fields = [StructField("order_id", IntegerType()),
                        StructField("order_date", DateType()),
                        StructField("order_customer_id", IntegerType()),
                        StructField("order_status", StringType())]

orders_schema = StructType(orders_struct_fields)
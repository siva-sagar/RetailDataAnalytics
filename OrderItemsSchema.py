from pyspark.sql.types import StructField, IntegerType, DoubleType, StructType

order_items_struct_fields = [StructField("order_item_id", IntegerType()),
                             StructField("order_item_order_id", IntegerType()),
                             StructField("order_items_product_id", IntegerType()),
                             StructField("order_item_quantity", IntegerType()),
                             StructField("order_item_subtotal", DoubleType()),
                             StructField("order_item_product_price", DoubleType())]

order_items_schema = StructType(order_items_struct_fields)
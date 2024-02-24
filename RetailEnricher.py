import sys
import OrderItemsSchema
from DevConstants import INPUT_BASE_DIR, OUTPUT_BASE_DIR
from ProdConstants import INPUT_BASE_DIR_S3, OUTPUT_BASE_DIR_S3
from Reader import read_from_source
from RetailProcessor import get_daily_revenue
from SinkConstants import SINK_FILE_FORMAT, SINK_OUTPUT_MODE, SINK_OUTPUT_PROCESSED_FOLDER_NAME
from SourceConstants import SOURCE_FILE_FORMAT, ORDERS_DATA_PATH, ORDER_ITEMS_DATA_PATH
from SparkSessionHelperUtil import get_spark_session
import configparser as cp
import OrdersSchema
from Writer import write_to_sink



def main():
    # get environment, source and sink variables from the arguments
    environment = sys.argv[1]
    source = sys.argv[2]
    sink = sys.argv[3]
    app_name = "RetailExpertAnalytics"

    # creating SparkSession in a different file
    spark = get_spark_session(app_name)

    # getting the properties from a different file using configparser
    props = cp.RawConfigParser()
    props.read("application.properties")

    spark.conf.set("spark.sql.shuffle.partitions", "1")

    # input_base_dir = props.get(environment, INPUT_BASE_DIR)
    input_base_dir = props.get(environment, INPUT_BASE_DIR_S3)
    source_file_format = props.get(source, SOURCE_FILE_FORMAT)

    # fetching orders data
    order_data_folder = props.get(source, ORDERS_DATA_PATH)
    order_custom_schema = OrdersSchema.orders_schema
    orders_df = read_from_source(spark, input_base_dir, source_file_format,
                                 order_data_folder, order_custom_schema)

    # fetching order_items data
    order_items_data_folder = props.get(source, ORDER_ITEMS_DATA_PATH)
    order_items_custom_schema = OrderItemsSchema.order_items_schema
    order_items_df = read_from_source(spark, input_base_dir, source_file_format,
                                      order_items_data_folder, order_items_custom_schema)

    # getting processed dataFrame
    final_processed_df = get_daily_revenue(orders_df, order_items_df)

    # final_processing.show()

    # output_base_dir = props.get(environment, OUTPUT_BASE_DIR)
    output_base_dir = props.get(environment, OUTPUT_BASE_DIR_S3)
    sink_file_format = props.get(sink, SINK_FILE_FORMAT)
    sink_output_mode = props.get(sink, SINK_OUTPUT_MODE)
    output_processed_folder_name = props.get(sink, SINK_OUTPUT_PROCESSED_FOLDER_NAME)

    # saving data to the output folder
    write_to_sink(final_processed_df, sink_file_format, sink_output_mode, output_processed_folder_name, output_base_dir)


if __name__ == "__main__":
    main()

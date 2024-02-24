from pyspark.sql.functions import round
from pyspark.sql.functions import sum as _sum


def get_daily_revenue(orders_df, order_items_df):
    # filtering the completed or closed orders
    filtered_df = orders_df.filter("order_status in ('CLOSED', 'COMPLETE')")

    # joining the two dataFrames
    joined_df = filtered_df.join(order_items_df, filtered_df.order_id == order_items_df.order_item_order_id)

    # applying the aggregate operations
    grouping_df = joined_df.groupBy('order_date', 'order_items_product_id') \
        .agg(round(_sum('order_item_subtotal'), 2).alias("revenue"))

    # final data frame
    final_df = grouping_df.orderBy(grouping_df.order_date, grouping_df.revenue.desc())

    return final_df

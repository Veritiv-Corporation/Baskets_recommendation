import pandas as pd

def multi_aggregate_data(df):
    """
    Perform a three-level aggregation of transaction data:
     - First aggregated by 'bill_to', 'invc_date', 'item_cde', and 'so_key' to sum 'qty_ship'.
     - Second aggregated by 'bill_to' and 'invc_date' to collect lists of items, keys, and quantities,
       and to count unique 'so_key'. 
     - Third, it aggregates everything by 'bill_to'.

    :param df: DataFrame containing the transaction data.
    :return: Aggregated DataFrame with three-level summary.
    """
    # First level of aggregation
    first_agg = df.groupby(['bill_to', 'invc_date', 'item_cde', 'so_key']).agg(
                           qty_ship=('qty_ship', 'sum')
                          ).reset_index()

    # Second level of aggregation
    second_agg = first_agg.groupby(['bill_to', 'invc_date']).agg(
                                   item_cde=('item_cde', list),
                                   so_key=('so_key', list),
                                   qty_ship=('qty_ship', list),
                                   so_key_frequency=('so_key', lambda x: len(set(x)))
                                  ).reset_index()

    # Final level of aggregation
    third_agg = second_agg.groupby('bill_to').agg(
                                    invc_date=('invc_date', list),
                                    item_cde=('item_cde', list),
                                    so_key=('so_key', list),
                                    qty_ship=('qty_ship', list),
                                    so_key_frequency=('so_key_frequency', 'sum')
                                  ).reset_index()

    return third_agg


def apply_custom_calculations(df):
    """
    Apply custom calculations to the DataFrame.

    :param df: DataFrame after aggregation.
    :return: DataFrame with custom calculations applied.
    """
    # Add calculated fields for item-date dictionary and average interval
    df['invc_date_TO_item_cde'] = df.apply(combine_invc_item, axis=1)
    df['time_span_min_max'] = df['invc_date'].apply(lambda x: (max(x) - min(x)).days)
    df['avg_interval'] = df.apply(calculate_avg_time_interval, axis=1)
    df['avg_interval'] = df['avg_interval'].apply(lambda x: x if x >= 7 else 7)
    return df

def combine_invc_item(x):
    """
    Aggregates invoice dates and item codes into a list of tuples.

    Args:
    x (DataFrame row): The row of a DataFrame from which to extract and format data.

    Returns:
    list: A list of tuples, each containing the invoice date as a formatted string ('YYYY-MM-DD')
          and the corresponding list of item codes.
    """
    return [(date.strftime('%Y-%m-%d'), items) for date, items in zip(x['invc_date'], x['item_cde'])]


def calculate_avg_time_interval(row):
    """
    Calculate the average time interval based on the 'so_key_frequency'.

    :param row: Row of DataFrame.
    :return: Average time interval as integer.
    """
    return 1 if row['so_key_frequency'] == 1 else row['time_span_min_max'] / row['so_key_frequency']

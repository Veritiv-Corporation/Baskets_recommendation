import pandas as pd

def flatten_baskets(df):
    """
    Flatten the baskets into a single list of items.

    :param df: DataFrame with 'baskets' column.
    :return: List containing all items from all baskets.
    """
    return [item for sublist in df['baskets'] for item in sublist]

def create_baskets(df):
    """
    Create baskets of items based on the interval around each date and return the DataFrame with a new 'baskets' column.

    :param df: DataFrame with 'invc_date_TO_item_cde' as a list of tuples (date, items) and 'avg_interval'.
    :return: DataFrame augmented with a 'baskets' column, where each basket is a list of combined items within the interval.
    """
    # Initialize the 'baskets' column
    df['baskets'] = None

    for index, row in df.iterrows():
        # Extract the list of tuples (date, items) and sort by date
        date_items_list = row['invc_date_TO_item_cde']
        avg_interval = round(row['avg_interval'])  # Round the average interval to the nearest whole number
        
        # Sort the tuples by date
        date_items_list.sort(key=lambda x: x[0])
        
        # Initialize the process to create baskets
        baskets = []
        if not date_items_list:
            df.at[index, 'baskets'] = baskets
            continue

        # Start the first basket with the first date's items
        current_basket = set(date_items_list[0][1])
        current_date = pd.to_datetime(date_items_list[0][0])

        # Process subsequent dates
        for date, items in date_items_list[1:]:
            date = pd.to_datetime(date)
            # Check if the date is within the interval from the current_date
            if (date - current_date).days <= avg_interval:
                current_basket.update(items)
            else:
                # Add the current basket to baskets and start a new one
                baskets.append(list(current_basket))
                current_basket = set(items)
                current_date = date
        
        # Don't forget to add the last basket
        baskets.append(list(current_basket))
        
        # Assign the list of baskets to the 'baskets' column for this row
        df.at[index, 'baskets'] = baskets
    
    return df

def create_baskets_365(df):
    """
    Create baskets of items based on the interval around each date and return the DataFrame with a new 'baskets' column.
    Each basket may contain overlapping items.

    :param df: DataFrame with 'invc_date_TO_item_cde' as a list of tuples (date, items) and 'avg_interval'.
    :return: DataFrame augmented with a 'baskets' column, where each basket is a list of combined items within the interval.
    """
    # Input validation
    if 'invc_date_TO_item_cde' not in df.columns or 'avg_interval' not in df.columns:
        raise ValueError("Input DataFrame must contain 'invc_date_TO_item_cde' and 'avg_interval' columns.")
    
    # Initialize the 'baskets' column
    df['baskets'] = None

    # Process each row
    for idx, row in df.iterrows():
        date_items_list = row['invc_date_TO_item_cde']
        avg_interval = round(row['avg_interval'])
        
        # Sort the date-items list by date
        date_items_list.sort(key=lambda x: x[0])
        
        baskets = []
        if not date_items_list:
            df.at[idx, 'baskets'] = baskets
            continue

        # Process each date
        for i in range(len(date_items_list)):
            current_date, items = date_items_list[i]
            current_date = pd.to_datetime(current_date)
            basket_end_date = current_date + pd.Timedelta(days=avg_interval)  # End date of the basket
            
            # Collect items within the interval from the current date
            basket_items = set()
            for j in range(i, len(date_items_list)):
                date, items = date_items_list[j]
                date = pd.to_datetime(date)
                if date <= basket_end_date:
                    basket_items.update(items)
                else:
                    break  # Stop collecting items if the date exceeds the basket end date
            
            baskets.append(list(basket_items))
        
        df.at[idx, 'baskets'] = baskets
    
    return df


import pandas as pd

def create_baskets_365_qty(df):
    """
    Create baskets of items along with corresponding quantity shipped based on the interval around each date 
    and return the DataFrame with a new 'baskets' column.
    Each basket may contain overlapping items.

    :param df: DataFrame with 'invc_date_TO_item_cde' as a list of tuples (date, items), 'qty_ship', and 'avg_interval'.
    :return: DataFrame augmented with a 'baskets' column, where each basket is a list of dictionaries containing item and corresponding quantity shipped.
    """
    # Input validation
    required_columns = ['invc_date_TO_item_cde', 'qty_ship', 'avg_interval']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"Input DataFrame must contain columns: {required_columns}")
    
    # Initialize the 'baskets' column
    df['baskets'] = None

    # Process each row
    for idx, row in df.iterrows():
        date_items_list = row['invc_date_TO_item_cde']
        qty_ship_list = row['qty_ship']
        avg_interval = round(row['avg_interval'])
        
        # Sort the date-items list by date
        sorted_indices = sorted(range(len(date_items_list)), key=lambda k: pd.to_datetime(date_items_list[k][0]))
        
        baskets = []
        if not date_items_list:
            df.at[idx, 'baskets'] = baskets
            continue

        # Process each date
        for i in sorted_indices:
            current_date, items = date_items_list[i]
            current_date = pd.to_datetime(current_date)
            basket_end_date = current_date + pd.Timedelta(days=avg_interval)  # End date of the basket
            
            # Collect items and corresponding quantity shipped within the interval from the current date
            basket_items = {}
            for j in range(i, len(date_items_list)):
                date, items = date_items_list[j]
                date = pd.to_datetime(date)
                if date <= basket_end_date:
                    for item, qty in zip(items, qty_ship_list[j]):
                        basket_items[item] = basket_items.get(item, 0) + qty
                else:
                    break  # Stop collecting items if the date exceeds the basket end date
            
            baskets.append(basket_items)
        
        df.at[idx, 'baskets'] = baskets
    
    return df

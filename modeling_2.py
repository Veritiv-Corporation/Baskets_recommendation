import pandas as pd
def replace_low_values(df,threshold=100):
    # Create the list of frequency columns
    frequency_columns = [f'frequency {i}' for i in range(1, 16)]
    
    # Replace values < threshold with 'replace_with'
    df[frequency_columns] = df[frequency_columns].applymap(lambda x: 'CAT' if x < threshold else x)
    
    return df

def get_top_5_cat3_items(trx_df):
    # Step 1: Group by 'item_cde' and 'cat', then aggregate 'qty_ship' and 'sls'
    item_cde_qty = trx_df.groupby(['item_cde', 'cat']).agg({'qty_ship': 'sum', 'sls': 'sum'}).reset_index()

    # Step 2: Sort each group within 'cat' by 'sls' and 'qty_ship' in descending order
    sorted_df = item_cde_qty.groupby('cat').apply(lambda x: x.sort_values(by=['sls', 'qty_ship'], ascending=[False, False]))
    sorted_df.reset_index(drop=True, inplace=True)

    # Step 3: Select the top 5 items for each category
    top_5_items = sorted_df.groupby('cat').head(5)

    # Step 4: Create a list of the top 5 'item_cde' for each 'cat'
    top_5_items_list = top_5_items.groupby('cat')['item_cde'].apply(lambda x: x.tolist()).reset_index()

    return top_5_items_list


def get_top_5_cat1_items(trx_df):
    # Step 1: Group by 'item_cde' and 'cat', then aggregate 'qty_ship' and 'sls'
    item_cde_qty = trx_df.groupby(['item_cde', 'cat1']).agg({'qty_ship': 'sum', 'sls': 'sum'}).reset_index()

    # Step 2: Sort each group within 'cat' by 'sls' and 'qty_ship' in descending order
    sorted_df = item_cde_qty.groupby('cat1').apply(lambda x: x.sort_values(by=['sls', 'qty_ship'], ascending=[False, False]))
    sorted_df.reset_index(drop=True, inplace=True)

    # Step 3: Select the top 5 items for each category
    top_5_items = sorted_df.groupby('cat1').head(5)

    # Step 4: Create a list of the top 5 'item_cde' for each 'cat'
    top_5_items_list = top_5_items.groupby('cat1')['item_cde'].apply(lambda x: x.tolist()).reset_index()

    return top_5_items_list

def create_cat3_to_top_item_map(df, cat3_df, top_5_items_cat3):
    # Step 1: Create a map from item_cde to Category (cat3)
    item_to_cat3 = pd.Series(cat3_df.Category.values, index=cat3_df.item_cde).to_dict()
    #print(len(item_to_cat3))
    # Step 2: Form a map from cat to the first item_cde in the list of top 5 items
    cat_to_first_item = top_5_items_cat3.set_index('cat')['item_cde'].apply(lambda x: x[0]).to_dict()
    #print(len(cat_to_first_item))

    # Step 3: Create the resulting map from item_cde to its corresponding top item_cde in cat3
    result_map = {}
    for item in df['item_cde']:
        cat3 = item_to_cat3[str(item)]  #item_cde in df is int, while in dict it is str
        #print(cat3)
        if cat3:
            top_item = cat_to_first_item.get(cat3)
            #print(top_item)
            if top_item:
                result_map[item] = top_item

    return result_map

def replace_recommendations(df, item_to_cat3_top_map):
    frequency_columns = [f'frequency {i}' for i in range(1, 6)]
    recommendation_columns = [f'Recommendation {i}' for i in range(1, 6)]
    
    for freq_col, rec_col in zip(frequency_columns, recommendation_columns):
        #df[rec_col] = df.apply(lambda row: item_to_cat3_top_map[row[freq_col]] if row[freq_col] == 'CAT' else row[rec_col], axis=1)
        df[rec_col] = df.apply(
            lambda row: item_to_cat3_top_map.get(int(row[rec_col]), row[rec_col]) if row[freq_col] == 'CAT' and pd.notna(row[rec_col]) else row[rec_col], axis=1
        )
    return df

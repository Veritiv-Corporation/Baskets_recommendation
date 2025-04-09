import pandas as pd
def replace_item_cde_with_cat3_set(flattened_basket_list_365, cat3_df):
    # Create a map from item_cde to Category (cat3)
    item_to_cat3 = pd.Series(cat3_df.Category.values, index=cat3_df.item_cde).to_dict()
    
    # Initialize a list to hold the new baskets
    new_basket_list = []

    # Iterate over each basket in the list
    for basket in flattened_basket_list_365:
        # Initialize a dictionary to hold category quantities
        cat3_quantities = {}
        
        # Iterate over each item in the basket
        for item, qty in basket.items():
            cat3 = item_to_cat3.get(item)
            if cat3:
                # Add quantity to corresponding category
                cat3_quantities[cat3] = cat3_quantities.get(cat3, 0) + qty
        
        # Append the new basket (as a dictionary with category quantities) to the list
        new_basket_list.append(cat3_quantities)
    
    return new_basket_list

def map_and_add_recommendations(df, cat3_df, new_recommendation_df, cat3_recommendation_df, top_5_items_cat3):
    # Create a map from item_cde to Category
    item_to_cat3 = pd.Series(cat3_df['Category'].values, index=cat3_df['item_cde']).to_dict()

    # Create a map from each category to its top item code
    category_to_top5_items = top_5_items_cat3.set_index('cat')['item_cde'].apply(lambda x: x[0]).to_dict()

    # Initialize a list to hold new rows to add
    rows_to_add = []

    # Iterate over each item in df
    for item in df['item_cde']:
        item = str(item)
        category = item_to_cat3.get(item)
        
        # Check if the item is in new_recommendation_df
        if item not in new_recommendation_df['item_cde'].values:
            # Get recommendations for the category
            if category in cat3_recommendation_df.index:
                recommendations = cat3_recommendation_df.loc[category].to_dict()
                new_row = {'item_cde': item}

                # Replace category recommendations with item codes from the map
                for i in range(1, 16):
                    recommendation_col = f'Recommendation {i}'
                    cat_recommendation = recommendations.get(recommendation_col)
                    if cat_recommendation in category_to_top5_items:
                        new_row[recommendation_col] = category_to_top5_items[cat_recommendation]
                    else:
                        new_row[recommendation_col] = ''

                rows_to_add.append(new_row)

    # Create a DataFrame for the new rows
    if rows_to_add:
        rows_to_add_df = pd.DataFrame(rows_to_add)
    else:
        rows_to_add_df = pd.DataFrame(columns=new_recommendation_df.columns)

    # Concatenate the new rows to new_recommendation_df
    updated_recommendation_df = pd.concat([new_recommendation_df, rows_to_add_df], ignore_index=True)

    return updated_recommendation_df


def map_and_add_recommendations_top3(df, cat3_df, new_recommendation_df, cat3_recommendation_df, top_5_items_cat3):
    # Create a map from item_cde to Category
    item_to_cat3 = pd.Series(cat3_df['Category'].values, index=cat3_df['item_cde']).to_dict()

    # Create a map from each category to its top 3 item codes
    category_to_top_items = (
        top_5_items_cat3.groupby('cat')['item_cde']
        .apply(lambda x: list(x[:3]))  # Extract top 3 items per category
        .to_dict()
    )

    # Initialize a list to hold new rows to add
    rows_to_add = []

    # Iterate over each item in df
    for item in df['item_cde']:
        item = str(item)
        category = item_to_cat3.get(item)
        
        # Check if the item is in new_recommendation_df
        if item not in new_recommendation_df['item_cde'].values:
            # Get recommendations for the category
            if category in cat3_recommendation_df.index:
                recommendations = cat3_recommendation_df.loc[category].to_dict()
                new_row = {'item_cde': item}
                recommendation_list = []

                # Retrieve 3 items for each recommended category
                for i in range(1, 6):  # 5 categories
                    recommendation_col = f'Recommendation {i}'
                    recommended_category = recommendations.get(recommendation_col)

                    if recommended_category in category_to_top_items:
                        top_items = category_to_top_items[recommended_category]
                        recommendation_list.extend(top_items)  # Append top 3 items
                
                # Ensure we only take the first 15 items
                recommendation_list = recommendation_list[:15]

                # Fill the row with recommendations
                for i in range(15):
                    new_row[f'Recommendation {i+1}'] = recommendation_list[i] if i < len(recommendation_list) else ''

                rows_to_add.append(new_row)

    return pd.DataFrame(rows_to_add)

def map_and_add_recommendations_cat1_top3(df, cat3_df, new_recommendation_df, cat3_recommendation_df, top_5_items_cat3):
    # Create a map from item_cde to Category
    item_to_cat3 = pd.Series(cat3_df['Category'].values, index=cat3_df['item_cde']).to_dict()

    # Create a map from each category to its top 3 item codes
    category_to_top_items = (
        top_5_items_cat3.groupby('cat1')['item_cde']
        .apply(lambda x: list(x[:3]))  # Extract top 3 items per category
        .to_dict()
    )

    # Initialize a list to hold new rows to add
    rows_to_add = []

    # Iterate over each item in df
    for item in df['item_cde']:
        item = str(item)
        category = item_to_cat3.get(item)
        
        # Check if the item is in new_recommendation_df
        if item not in new_recommendation_df['item_cde'].values:
            # Get recommendations for the category
            if category in cat3_recommendation_df.index:
                recommendations = cat3_recommendation_df.loc[category].to_dict()
                new_row = {'item_cde': item}
                recommendation_list = []

                # Retrieve 3 items for each recommended category
                for i in range(1, 6):  # 5 categories
                    recommendation_col = f'Recommendation {i}'
                    recommended_category = recommendations.get(recommendation_col)

                    if recommended_category in category_to_top_items:
                        top_items = category_to_top_items[recommended_category]
                        recommendation_list.extend(top_items)  # Append top 3 items
                
                # Ensure we only take the first 15 items
                recommendation_list = recommendation_list[:15]

                # Fill the row with recommendations
                for i in range(15):
                    new_row[f'Recommendation {i+1}'] = recommendation_list[i] if i < len(recommendation_list) else ''

                rows_to_add.append(new_row)

    return pd.DataFrame(rows_to_add)

def map_and_add_recommendations_cat1(df, cat3_df, new_recommendation_df, cat3_recommendation_df, top_5_items_cat3):
    # Create a map from item_cde to Category
    item_to_cat3 = pd.Series(cat3_df['Category'].values, index=cat3_df['item_cde']).to_dict()

    # Create a map from each category to its top item code
    category_to_top5_items = top_5_items_cat3.set_index('cat1')['item_cde'].apply(lambda x: x[0]).to_dict()

    # Initialize a list to hold new rows to add
    rows_to_add = []

    # Iterate over each item in df
    for item in df['item_cde']:
        item = str(item)
        category = item_to_cat3.get(item)
        
        # Check if the item is in new_recommendation_df
        if item not in new_recommendation_df['item_cde'].values:
            # Get recommendations for the category
            if category in cat3_recommendation_df.index:
                recommendations = cat3_recommendation_df.loc[category].to_dict()
                new_row = {'item_cde': item}

                # Replace category recommendations with item codes from the map
                for i in range(1, 16):
                    recommendation_col = f'Recommendation {i}'
                    cat_recommendation = recommendations.get(recommendation_col)
                    if cat_recommendation in category_to_top5_items:
                        new_row[recommendation_col] = category_to_top5_items[cat_recommendation]
                    else:
                        new_row[recommendation_col] = ''

                rows_to_add.append(new_row)

    # Create a DataFrame for the new rows
    if rows_to_add:
        rows_to_add_df = pd.DataFrame(rows_to_add)
    else:
        rows_to_add_df = pd.DataFrame(columns=new_recommendation_df.columns)

    # Concatenate the new rows to new_recommendation_df
    updated_recommendation_df = pd.concat([new_recommendation_df, rows_to_add_df], ignore_index=True)

    return updated_recommendation_df

def transform_recommendations(df):
    # Initialize an empty list to store the new rows
    rows = []
    
    # Iterate over each row in the original DataFrame
    for _, row in df.iterrows():
        item_cde = row['item_cde']
        # Iterate through each recommendation
        for i in range(1, 6):
            recommendation = row[f'Recommendation {i}']
            # Append the new row to the list
            rows.append({'Primary Item Number': item_cde, 'Related Item Number': recommendation})
    
    # Convert the list to a new DataFrame
    transformed_df = pd.DataFrame(rows)
    return transformed_df

def reorder_sustainable(df, sustainable_df):
    """
    Reorders the recommendations for each item_cde based on private_label_sw
    and adds columns indicating whether each recommendation is private branded.
    Parameters:
        df: A DataFrame containing 'item_cde' and 'Recommendation 1' to 'Recommendation 15'.
        private_label_df: A DataFrame containing 'item_cde' and 'private_label_sw'.
    Returns:
        reordered_df: A DataFrame with reordered recommendations and private branding info.
    """
    private_label_dict = dict(zip(sustainable_df['item_cde'], sustainable_df['sustainable']))
    
    reordered_recommendations = []

    for _, row in df.iterrows():
        item_cde = row['item_cde']
        recommendations = [(row[f'Recommendation {i}'], private_label_dict.get(row[f'Recommendation {i}'], 'N')) 
                           for i in range(1, 16)]
        
        # Sort recommendations based on private_label_sw ('Y' should come first)
        recommendations.sort(key=lambda x: x[1] != 'Y')
        
        reordered_row = {'item_cde': item_cde}
        for i, (rec, private_label) in enumerate(recommendations):
            reordered_row[f'Recommendation {i+1}'] = rec
            reordered_row[f'Recommendation {i+1}_private'] = private_label
        
        reordered_recommendations.append(reordered_row)
    
    reordered_df = pd.DataFrame(reordered_recommendations)
    return reordered_df

def reorder_private(df, private_df):
    """
    Reorders the recommendations for each item_cde based on private_label_sw
    and adds columns indicating whether each recommendation is private branded.
    Parameters:
        df: A DataFrame containing 'item_cde' and 'Recommendation 1' to 'Recommendation 15'.
        private_label_df: A DataFrame containing 'item_cde' and 'private_label_sw'.
    Returns:
        reordered_df: A DataFrame with reordered recommendations and private branding info.
    """
    private_label_dict = dict(zip(private_df['item_cde'], private_df['private_label_sw']))
    
    reordered_recommendations = []

    for _, row in df.iterrows():
        item_cde = row['item_cde']
        recommendations = [(row[f'Recommendation {i}'], private_label_dict.get(row[f'Recommendation {i}'], 'N')) 
                           for i in range(1, 16)]
        
        # Sort recommendations based on private_label_sw ('Y' should come first)
        recommendations.sort(key=lambda x: x[1] != 'Y')
        
        reordered_row = {'item_cde': item_cde}
        for i, (rec, private_label) in enumerate(recommendations):
            reordered_row[f'Recommendation {i+1}'] = rec
            reordered_row[f'Recommendation {i+1}_private'] = private_label
        
        reordered_recommendations.append(reordered_row)
    
    reordered_df = pd.DataFrame(reordered_recommendations)
    return reordered_df

def reorder_alliance(df, pvt_a_label_df):
    """
    Reorders the recommendations for each item_cde based on private_label_sw
    and adds columns indicating whether each recommendation is private branded.
    Parameters:
        df: A DataFrame containing 'item_cde' and 'Recommendation 1' to 'Recommendation 15'.
        private_label_df: A DataFrame containing 'item_cde' and 'private_label_sw'.
    Returns:
        reordered_df: A DataFrame with reordered recommendations and private branding info.
    """
    alliance_brand_dict = {item: 1 if item in pvt_a_label_df['Item Number'].values else 0 for item in df['item_cde']}

    
    reordered_recommendations = []

    for _, row in df.iterrows():
        item_cde = row['item_cde']
        recommendations = [(row[f'Recommendation {i}'], alliance_brand_dict.get(row[f'Recommendation {i}'])) 
                           for i in range(1, 16)]
        
        # Sort recommendations based on private_label_sw ('Y' should come first)
        recommendations.sort(key=lambda x: x[1] != 1)
        
        reordered_row = {'item_cde': item_cde}
        for i, (rec, private_label) in enumerate(recommendations):
            reordered_row[f'Recommendation {i+1}'] = rec
            reordered_row[f'Recommendation {i+1}_private'] = private_label
        
        reordered_recommendations.append(reordered_row)
    
    reordered_df = pd.DataFrame(reordered_recommendations)
    return reordered_df


def filter_print_segment(df):
    # Filter the DataFrame where Segment is 'print'
    filtered_df = df[df['segment'] == 'Print']
    return filtered_df

def filter_non_print_segment(df):
    # Filter the DataFrame where Segment is 'print'
    filtered_df = df[df['segment'] != 'Print']
    return filtered_df

def filter_pkg_segment(df):
    # Filter the DataFrame where Segment is 'print'
    filtered_df = df[df['segment'].str.contains('Packaging')]
    return filtered_df


def filter_fs_segment(df):
    # Filter the DataFrame where Segment is 'print'
    filtered_df = df[df['segment'].str.contains('Facility')]
    return filtered_df

def add_descriptions(trx_df: pd.DataFrame, reorder_private_df: pd.DataFrame) -> pd.DataFrame:
    # Create a mapping dictionary from item_cde to description
    description_mapping = dict(zip(trx_df['item_cde'], trx_df['description']))

    # Add description for the main item_cde
    reorder_private_df['item_cde Description'] = reorder_private_df['item_cde'].map(description_mapping)

    # Add description for each recommendation column
    for i in range(1, 6):
        recom_col = f'Recommendation {i}'
        recom_desc_col = f'{recom_col} Description'
        reorder_private_df[recom_desc_col] = reorder_private_df[recom_col].map(description_mapping)

    return reorder_private_df

def drop_duplicate_rows(transformed_df):
    """
    Drops duplicate rows in the DataFrame based on 'Primary Item Number' and 'Related Item Number' columns.
    
    Parameters:
        transformed_df: A DataFrame containing the columns 'Primary Item Number' and 'Related Item Number'.
    
    Returns:
        A DataFrame with duplicate rows removed.
    """
    # Drop duplicates based on 'Primary Item Number' and 'Related Item Number'
    transformed_df = transformed_df.drop_duplicates(subset=['Primary Item Number', 'Related Item Number'])

    return transformed_df

def remove_empty_related_item_rows(transformed_df_new):
    """
    Removes rows in the DataFrame where the 'Related Item Number' column is empty or NaN.
    
    Parameters:
        transformed_df_new: A DataFrame containing the column 'Related Item Number'.
    
    Returns:
        A DataFrame with rows removed where 'Related Item Number' is empty or NaN.
    """
    # Remove rows where 'Related Item Number' is NaN or empty string
    transformed_df_new = transformed_df_new.dropna(subset=['Related Item Number'])
    transformed_df_new = transformed_df_new[transformed_df_new['Related Item Number'] != '']

    return transformed_df_new

def filter_mfg_name(df, nam):
    # Filter the DataFrame where mfg name is given
    filtered_df = df[df['mfg_name'] == nam]
    return filtered_df
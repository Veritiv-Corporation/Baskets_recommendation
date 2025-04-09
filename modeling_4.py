import pandas as pd
def remove_duplicate_and_self_references(reorder_with_desc):
    """
    Sets duplicate recommendations and self-references to None in the DataFrame.
    
    Parameters:
        reorder_with_desc: A DataFrame containing columns for item codes and their recommendations.
    
    Returns:
        A DataFrame with duplicate recommendations and self-references set to None, keeping only the earliest occurrence.
    """
    recommendation_cols = [
        'Recommendation 1', 'Recommendation 2', 
        'Recommendation 3', 'Recommendation 4', 'Recommendation 5'
    ]

    def remove_duplicates_and_self_refs(row):
        seen = set()
        unique_recommendations = []
        item_cde = row['item_cde']
        
        # Collect unique recommendations
        for col in recommendation_cols:
            recommendation = row[col]
            if recommendation not in seen and recommendation != item_cde:
                seen.add(recommendation)
                unique_recommendations.append(recommendation)
        
        # Fill the row with unique recommendations and shift left if needed
        result = []
        for recommendation in unique_recommendations:
            result.append(recommendation)
        
        # Fill remaining slots with None
        while len(result) < len(recommendation_cols):
            result.append(None)
        
        return pd.Series(result, index=recommendation_cols)

    # Apply the function to each row
    reorder_with_desc[recommendation_cols] = reorder_with_desc.apply(remove_duplicates_and_self_refs, axis=1)

    return reorder_with_desc


def shift_recommendations_left(reorder_with_desc_cleaned):
    """
    Push non-empty recommendations left when there are empty slots before them.
    
    Parameters:
        reorder_with_desc_cleaned: A DataFrame containing columns for item codes and their recommendations.
    
    Returns:
        A DataFrame with non-empty recommendations shifted left.
    """
    recommendation_cols = [
        'Recommendation 1', 'Recommendation 2', 
        'Recommendation 3', 'Recommendation 4', 'Recommendation 5'
    ]

    def shift_left(row):
        # Extract recommendations into a list
        recommendations = [row[col] for col in recommendation_cols]
        
        # Filter out empty recommendations
        recommendations = [rec for rec in recommendations if pd.notnull(rec)]
        
        # Extend the list to maintain the length of the recommendation columns
        while len(recommendations) < len(recommendation_cols):
            recommendations.append(None)
        
        return pd.Series(recommendations, index=recommendation_cols)
    
    # Apply the function to each row
    reorder_with_desc_cleaned[recommendation_cols] = reorder_with_desc_cleaned.apply(shift_left, axis=1)

    return reorder_with_desc_cleaned



def copy_rows_with_0_or_1_recommendation(df):
    """
    Copies rows where there are 0 or 1 recommendations.
    
    Parameters:
        df: A DataFrame containing columns for item codes and their recommendations.
    
    Returns:
        A DataFrame containing only the rows with 0 or 1 recommendations.
    """
    recommendation_cols = [
        'Recommendation 1', 'Recommendation 2', 
        'Recommendation 3', 'Recommendation 4', 'Recommendation 5'
    ]
    
    def count_non_none(row):
        count = 0
        for col in recommendation_cols:
            if pd.notna(row[col]) and row[col] != None:
                count += 1
        return count
    
    # Filter rows where count of non-None recommendations is 0 or 1
    filtered_df = df[df.apply(count_non_none, axis=1) <= 1]
    
    return filtered_df

def copy_rows_with_0_to_2_recommendation(df):
    """
    Copies rows where there are 0 or 1 recommendations.
    
    Parameters:
        df: A DataFrame containing columns for item codes and their recommendations.
    
    Returns:
        A DataFrame containing only the rows with 0 or 1 recommendations.
    """
    recommendation_cols = [
        'Recommendation 1', 'Recommendation 2', 
        'Recommendation 3', 'Recommendation 4', 'Recommendation 5'
    ]
    
    def count_non_none(row):
        count = 0
        for col in recommendation_cols:
            if pd.notna(row[col]) and row[col] != None:
                count += 1
        return count
    
    # Filter rows where count of non-None recommendations is 0 or 1
    filtered_df = df[df.apply(count_non_none, axis=1) <= 8]
    
    return filtered_df

def add_recommendations2(reorder_df, cat3_df, top_5_items_cat3, cat1_df, top_5_items_cat1):
    # Create dictionaries to map item codes to categories
    item_to_category_dict_cat3 = dict(zip(cat3_df['item_cde'], cat3_df['Category']))
    item_to_category_dict_cat1 = dict(zip(cat1_df['item_cde'], cat1_df['Category']))

    # Function to find rows with 0 or 1 recommendations
    def copy_rows_with_0_or_1_recommendation(df):
        recommendation_cols = ['Recommendation 1', 'Recommendation 2', 'Recommendation 3', 'Recommendation 4', 'Recommendation 5']
        
        def count_non_none(row):
            return sum(pd.notna(row[col]) for col in recommendation_cols)
        
        return df[df.apply(count_non_none, axis=1) <= 1]

    rows_with_0_or_1_recommendation = copy_rows_with_0_or_1_recommendation(reorder_df)

    for idx, row in rows_with_0_or_1_recommendation.iterrows():
        current_item = row['item_cde']
        category_cat3 = item_to_category_dict_cat3.get(current_item)
        category_cat1 = item_to_category_dict_cat1.get(current_item)
        
        if category_cat3:
            # Get top items for the category from cat3
            top_items_cat3 = top_5_items_cat3[top_5_items_cat3['cat'] == category_cat3]['item_cde'].values
            # Get top items for the category from cat1
            top_items_cat1 = top_5_items_cat1[top_5_items_cat1['cat1'] == category_cat1]['item_cde'].values if category_cat1 else []
            
            # Flatten the lists if they are not already
            top_items_cat3 = [item for sublist in top_items_cat3 for item in sublist] if isinstance(top_items_cat3[0], list) else top_items_cat3
            top_items_cat1 = [item for sublist in top_items_cat1 for item in sublist] if isinstance(top_items_cat1[0], list) else top_items_cat1

            # Concatenate the top items lists
            combined_top_items = list(top_items_cat3) + list(top_items_cat1)
            
            # Ensure items are unique and not the current item
            unique_top_items = []
            seen_items = set()
            for item in combined_top_items:
                if item != current_item and item not in seen_items:
                    unique_top_items.append(item)
                    seen_items.add(item)
            
            # Check if Recommendation 1 is empty
            if pd.isna(row['Recommendation 1']) or row['Recommendation 1'] is None:
                if len(unique_top_items) > 0:
                    reorder_df.at[idx, 'Recommendation 1'] = unique_top_items[0]
                if len(unique_top_items) > 1:
                    reorder_df.at[idx, 'Recommendation 2'] = unique_top_items[1]
            else:
                # If Recommendation 1 is not empty, add the next available unique item as Recommendation 2
                for item in unique_top_items:
                    if item != row['Recommendation 1']:
                        reorder_df.at[idx, 'Recommendation 2'] = item
                        break

    return reorder_df
def minimum_three_recommendations(reorder_df, cat3_df, top_5_items_cat3, cat1_df, top_5_items_cat1):
    # Create dictionaries to map item codes to categories
    item_to_category_dict_cat3 = dict(zip(cat3_df['item_cde'], cat3_df['Category']))
    item_to_category_dict_cat1 = dict(zip(cat1_df['item_cde'], cat1_df['Category']))

    # Define the recommendation columns
    recommendation_cols = ['Recommendation 1', 'Recommendation 2', 'Recommendation 3', 'Recommendation 4', 'Recommendation 5',
    'Recommendation 6', 'Recommendation 7', 'Recommendation 8', 'Recommendation 9', 'Recommendation 10']

    # Function to find rows with fewer than 3 recommendations
    def copy_rows_with_fewer_than_3_recommendations(df):
        def count_non_none(row):
            return sum(pd.notna(row[col]) for col in recommendation_cols)
        return df[df.apply(count_non_none, axis=1) < 8]

    # Filter rows that need more recommendations
    rows_with_fewer_than_3_recommendations = copy_rows_with_fewer_than_3_recommendations(reorder_df)

    for idx, row in rows_with_fewer_than_3_recommendations.iterrows():
        current_item = row['item_cde']
        category_cat3 = item_to_category_dict_cat3.get(current_item)
        category_cat1 = item_to_category_dict_cat1.get(current_item)
        
        if category_cat3:
            # Get top items for the category from cat3
            top_items_cat3 = top_5_items_cat3[top_5_items_cat3['cat'] == category_cat3]['item_cde'].values
            # Get top items for the category from cat1
            top_items_cat1 = top_5_items_cat1[top_5_items_cat1['cat1'] == category_cat1]['item_cde'].values if category_cat1 else []
            
            # Flatten the lists if they are not already
            if top_items_cat3 and isinstance(top_items_cat3[0], list):
                top_items_cat3 = [item for sublist in top_items_cat3 for item in sublist]

            if top_items_cat1 and isinstance(top_items_cat1[0], list):
                top_items_cat1 = [item for sublist in top_items_cat1 for item in sublist]

            # Concatenate the top items lists
            combined_top_items = list(top_items_cat3) + list(top_items_cat1)
            
            # Ensure items are unique and not the current item
            unique_top_items = []
            seen_items = set()
            for item in combined_top_items:
                if item != current_item and item not in seen_items:
                    unique_top_items.append(item)
                    seen_items.add(item)
            
            # Fill recommendations to ensure at least 3
            filled_recommendations = []
            for col in recommendation_cols:
                if pd.notna(row[col]) and row[col] not in filled_recommendations:
                    filled_recommendations.append(row[col])
            
            # Add additional items to fill up to 3 recommendations
            for item in unique_top_items:
                if len(filled_recommendations) >= 8:
                    break
                if item not in filled_recommendations:
                    filled_recommendations.append(item)
            
            # Update the recommendations in the DataFrame
            for i, recommendation in enumerate(filled_recommendations[:8]):
                reorder_df.at[idx, recommendation_cols[i]] = recommendation

    return reorder_df

def are_values_unique(row):     
 
    # Filter out empty values    
    non_empty_values = [value for value in row if value is not None and value != '']         
 
    # Check if the length of the list is the same as the length of the set (i.e., all values are unique)
    return len(non_empty_values) == len(set(non_empty_values))

def remove_empty_related_items(df):
    # Filter out rows where 'Related Item Number' is null or empty
    filtered_df = df[df['Related Item Number'].notna() & (df['Related Item Number'] != '')]
    return filtered_df

def drop_spaces(df):
    result_df = pd.DataFrame({
    'item_cde': df.item_cde,
    'reco': df.drop(columns=['item_cde'])  # Apply to all other columns
        .apply(lambda row: [
            int(x) if isinstance(x, (str, float, int)) and str(x).strip() != '' else x 
            for x in row.dropna()
        ], axis=1)})
    return result_df
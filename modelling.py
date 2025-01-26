import pandas as pd
from collections import defaultdict
from tqdm import tqdm
from decimal import Decimal
from itertools import combinations


def create_cooccurrence_matrix_with_recommendations(baskets, top_n=5):
    # Initialize the dictionary of dictionaries
    co_occurrence = defaultdict(lambda: defaultdict(int))
    recommendations = defaultdict(lambda: defaultdict(int))

    # Loop through each basket
    for basket in tqdm(baskets):
        # Extract items and their quantities from the basket
        items = list(basket.keys())
        quantities = list(basket.values())
        
        # Sort the items to ensure that each pair is counted once
        sorted_items = sorted(items)
        
        # Count each pair in the basket
        for i in range(len(sorted_items)):
            for j in range(i + 1, len(sorted_items)):
                item1, item2 = sorted_items[i], sorted_items[j]
                quantity1, quantity2 = quantities[items.index(item1)], quantities[items.index(item2)]
                
                # Increment the co-occurrence count by the minimum quantity of the two items
                co_occurrence[item1][item2] +=  min(quantity1, quantity2)
                co_occurrence[item2][item1] += min(quantity1, quantity2)

    # Convert to DataFrame
    # Extract items and sort them to ensure DataFrame columns and rows are aligned
    items = sorted(co_occurrence.keys())
    df = pd.DataFrame(index=items, columns=items).fillna(0)
    
    # Fill the DataFrame and make recommendations
    for item1, neighbors in tqdm(co_occurrence.items()):
        for item2, count in neighbors.items():
            df.at[item1, item2] = count
            recommendations[item1][item2] = count
            recommendations[item2][item1] = count
    
    # Sort the recommendations based on count and take top-N
    rec_df = pd.DataFrame(index=items, columns=[f"Recommendation {i+1}" for i in range(top_n)])
    for item, recs in recommendations.items():
        # Remove the queried item from recommendations
        recs.pop(item, None)
        # Sort by count and take top-N
        sorted_recs = sorted(recs.items(), key=lambda x: x[1], reverse=True)
        total_count = sum(count for _, count in sorted_recs)
        #print(item)
        #print(sorted_recs)
        for i, (rec_item, fre) in enumerate(sorted_recs[:top_n]):
            rec_df.at[item, f"Recommendation {i+1}"] = rec_item
            #rec_df.at[item, f"prop {i+1}"] = fre/total_count

    return df, rec_df


def create_pair_frequency_matrix(baskets):
    # Extract unique products from all baskets
    unique_products = set()
    for basket in baskets:
        unique_products.update(basket.keys())
    unique_products = sorted(unique_products)
    num_products = len(unique_products)
    
    # Initialize nested dictionary to store pair frequencies
    pair_freq_dict = defaultdict(lambda: defaultdict(int))
    
    # Iterate over each basket
    for basket in baskets:
        products = sorted(basket.keys())
        
        # Get all pairs of items in the basket
        pairs = list(combinations(products, 2))
        
        # Update frequencies in the pair frequency dictionary
        for pair in pairs:
            pair_freq_dict[pair[0]][pair[1]] += 1
            pair_freq_dict[pair[1]][pair[0]] += 1
    
    # Convert nested dictionary to DataFrame
    pair_freq_matrix = pd.DataFrame(pair_freq_dict).reindex(index=unique_products, columns=unique_products).fillna(0)
    
    return pair_freq_matrix

def add_freq(recommendation_df,pair_freq_matrix):
    recommendation_df.index.name = 'item_cde'

    recommendation_df= recommendation_df.reset_index()

    # Convert the 'item_cde' column to integers
    #recommendation_df['item_cde'] = recommendation_df['item_cde'].astype(int)

    # Now, iterate over each recommendation column
    for i in range(1, 6):
        recommendation_col = 'Recommendation {}'.format(i)
        freq_col = 'frequency {}'.format(i)
    
        # Compute frequencies using the pair frequency matrix
        recommendation_df[freq_col] = recommendation_df.apply(lambda row: pair_freq_matrix.get(str(row['item_cde']), {}).get(row[recommendation_col], 0), axis=1)
    
    return recommendation_df



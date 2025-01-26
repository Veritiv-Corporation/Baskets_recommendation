import os
import pandas as pd
from dotenv import load_dotenv
import redshift_connector

# Load environment variables from .env file
load_dotenv()

# Function for db connection
def connect_db():
    """
    Establishes a connection to the database using environment variables.
    Returns:
        conn: A connection object to the database.
    """
    try:
        conn = redshift_connector.connect(
                host = os.getenv('DB_HOST'),
                database = os.getenv('DB_NAME'),
                port = 5439, #os.getenv('DB_PORT'),
                user = os.getenv('DB_USER'),
                password = os.getenv('DB_PASSWORD')
                )
        #print("Database connection established.")
        return conn
    except Exception as e:
        print(f"An error occurred while connecting to the database: {e}")
        return None

# Function to fetch transaction data (for the specific items) for the past 1 year 
def fetch_trx_data(conn, df):
    """
    Fetches past 1 year transaction data from the database based on item numbers in the provided DataFrame.
    Parameters:
        conn: The database connection object.
        df: A DataFrame containing 'item_cde' column.
    Returns:
        trx_df: A DataFrame containing past 1 year transactions data for item codes provided.
        It contains: 'so_key','item_cde','mfg_name','description','segment', 'cat1','cat2','cat3','cat','bill_to',
                     'sls','qty_ship','ord_date','invc_date'
    """
    if conn is None:
        print("Database connection is not established.")
        return None
    
    try:
        with conn.cursor() as cursor:
            # Generate the placeholders string based on the number of items
            placeholders = ', '.join(['%s'] * len(df['item_cde']))

            # SQL query to fetch transaction data based on item_cde in the DataFrame
            query = f"""
                    SELECT 	f.so_key,
                            i.item_cde,
                            im.mfg_name,
                            im.prod_desc AS description,
                            im.prod_seg_name segment,
                            TRIM(im.prod_cat_1_name) cat1,
                            TRIM(im.prod_cat_2_name) cat2,
                            TRIM(im.prod_cat_3_name) cat3,
                            im.prod_cat_1_name||'.'||im.prod_cat_2_name||'.'||im.prod_cat_3_name AS cat,
                            c.sys_cde + c.cust_cde AS bill_to,
                            f.sls,
                            f.qty_ship,   	
                            DATE(f.ord_date) AS ord_date,
                            DATE(f.invc_date) AS invc_date
                    FROM edm.f_sls_item_v f
                            LEFT JOIN edm.d_item i 
                                ON (f.item_key = i.item_key)
                            LEFT JOIN edm.d_item_mst_v im 
                                ON (im.item_mst_key = i.item_mst_key)
                            LEFT JOIN edm.d_sls_item_type sit 
                                ON (f.sls_item_type_key = sit.sls_item_type_key)
                            LEFT JOIN edm.d_cust_ship_to st 
                                ON (f.cust_ship_to_key = st.cust_ship_to_key)
                            LEFT JOIN edm.d_cust c 
                                ON (st.cust_key = c.cust_key)
                    WHERE 1=1
                    AND sit.trans_type_desc in ('ORDER', 'INVOICE')
                    AND f.incl_fin_rpt_sw = 'Y' /*FILTER OUT INTERNAL TRANSFERS AND NON-CUSTOMER SALES*/
                    AND f.invc_date >= '2023-10-01' and f.invc_date <= '2024-10-01' 
                        --BETWEEN CURRENT_DATE - INTERVAL '1 year' AND CURRENT_DATE
                    AND f.sys_cde in ('03','MA','NU','PC')
                    AND ISNULL(i.item_class_cde,'') <> '99'
                    AND ISNULL(i.item_cde,'') NOT LIKE '%/%'
                    AND ISNULL(i.item_cde,'') NOT LIKE '%&%'
                    AND i.item_vrsn_nbr = '0'
                    AND c.parent_party_id NOT IN ('172298134', '1000017298')
                    AND ISNULL(c.cust_acct_type,'') <> '3'
                    AND i.item_cde IN ({placeholders});
                    """

            # Execute the query
            cursor.execute(query, tuple(df['item_cde']))
            result = cursor.fetchall()

            # Convert query result to DataFrame
            trx_df = pd.DataFrame(result, columns=['so_key','item_cde','mfg_name','description', 
                                                   'segment', 'cat1','cat2','cat3','cat','bill_to',
                                                   'sls','qty_ship','ord_date','invc_date'])
            trx_df['invc_date'] = pd.to_datetime(trx_df['invc_date'])

        conn.commit()

        return trx_df

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
        return None

    finally:
        conn.close()

import pandas as pd

def fetch_cat_data(conn, df):
    """
    Fetches category data from the database based on item codes in the provided DataFrame.
    Parameters:
        conn: The database connection object.
        df: A DataFrame containing 'item_cde' column.
    Returns:
        cat_df: A DataFrame containing category data for item codes provided.
        It contains: 'item_cde', 'prod_cat_1_name', 'prod_cat_2_name', 'prod_cat_3_name'
    """
    if conn is None:
        print("Database connection is not established.")
        return None
    
    try:
        with conn.cursor() as cursor:
            # Generate the placeholders string based on the number of items
            placeholders = ', '.join(['%s'] * len(df['item_cde']))
            print(placeholders)
            # SQL query to fetch category data based on item_cde in the DataFrame
            query = f"""
                    SELECT item_cde,
                           prod_cat_1_name||'.'||prod_cat_2_name||'.'||prod_cat_3_name AS Category
                    FROM edm.d_item_mst_v dimv
                    WHERE item_cde IN ({placeholders});
                    """

            # Execute the query
            cursor.execute(query, tuple(df['item_cde']))
            result = cursor.fetchall()

            # Convert query result to DataFrame
            cat_df = pd.DataFrame(result, columns=['item_cde', 'Category'])

        conn.commit()

        return cat_df

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
        return None

    finally:
        conn.close()


def fetch_cat1_data(conn, df):
    """
    Fetches category data from the database based on item codes in the provided DataFrame.
    Parameters:
        conn: The database connection object.
        df: A DataFrame containing 'item_cde' column.
    Returns:
        cat_df: A DataFrame containing category data for item codes provided.
        It contains: 'item_cde', 'prod_cat_1_name', 'prod_cat_2_name', 'prod_cat_3_name'
    """
    if conn is None:
        print("Database connection is not established.")
        return None
    
    try:
        with conn.cursor() as cursor:
            # Generate the placeholders string based on the number of items
            placeholders = ', '.join(['%s'] * len(df['item_cde']))
            print(placeholders)
            # SQL query to fetch category data based on item_cde in the DataFrame
            query = f"""
                    SELECT item_cde,
                           prod_cat_1_name Category
                    FROM edm.d_item_mst_v dimv
                    WHERE item_cde IN ({placeholders});
                    """

            # Execute the query
            cursor.execute(query, tuple(df['item_cde']))
            result = cursor.fetchall()

            # Convert query result to DataFrame
            cat_df = pd.DataFrame(result, columns=['item_cde', 'Category'])

        conn.commit()

        return cat_df
    
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
        return None

    finally:
        conn.close()


def fetch_item_descriptions(conn, df):
    """
    Fetches item descriptions from the database based on item codes in the provided DataFrame.
    
    Parameters:
        conn: The database connection object.
        df: A DataFrame containing 'item_cde' column.
    
    Returns:
        desc_df: A DataFrame containing item descriptions for item codes provided.
        It contains: 'item_cde', 'description'
    """
    if conn is None:
        print("Database connection is not established.")
        return None
    
    try:
        with conn.cursor() as cursor:
            # Generate the placeholders string based on the number of items
            placeholders = ', '.join(['%s'] * len(df['item_cde']))

            # SQL query to fetch item descriptions based on item_cde in the DataFrame
            query = f"""
                    SELECT item_cde,
                           prod_desc AS description
                    FROM edm.d_item_mst_v
                    WHERE item_cde IN ({placeholders});
                    """

            # Execute the query
            cursor.execute(query, tuple(df['item_cde']))
            result = cursor.fetchall()

            # Convert query result to DataFrame
            desc_df = pd.DataFrame(result, columns=['item_cde', 'description'])

        conn.commit()

        return desc_df

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
        return None

    finally:
        conn.close()


def fetch_private_label_data(conn, df):
    """
    Fetches private label data from the database based on item codes in the provided DataFrame.
    Parameters:
        conn: The database connection object.
        df: A DataFrame containing 'item_cde' column.
    Returns:
        private_label_df: A DataFrame containing private label data for item codes provided.
        It contains: 'item_cde', 'private_label_sw'
    """
    if conn is None:
        print("Database connection is not established.")
        return None
    
    try:
        with conn.cursor() as cursor:
            # Generate the placeholders string based on the number of items
            placeholders = ', '.join(['%s'] * len(df['item_cde']))
            
            # SQL query to fetch private label data based on item_cde in the DataFrame
            query = f"""
                    SELECT item_cde,
                           private_label_sw
                    FROM veritiv.edm.d_item_mst
                    WHERE item_cde IN ({placeholders});
                    """

            # Execute the query
            cursor.execute(query, tuple(df['item_cde']))
            result = cursor.fetchall()

            # Convert query result to DataFrame
            private_label_df = pd.DataFrame(result, columns=['item_cde', 'private_label_sw'])

        conn.commit()

        return private_label_df

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
        return None

    finally:
        conn.close()

def fetch_brand_data(conn, df):
    """
    Fetches category data from the database based on item codes in the provided DataFrame.
    Parameters:
        conn: The database connection object.
        df: A DataFrame containing 'item_cde' column.
    Returns:
        cat_df: A DataFrame containing category data for item codes provided.
        It contains: 'item_cde', 'prod_cat_1_name', 'prod_cat_2_name', 'prod_cat_3_name'
    """
    if conn is None:
        print("Database connection is not established.")
        return None
    
    try:
        with conn.cursor() as cursor:
            # Generate the placeholders string based on the number of items
            placeholders = ', '.join(['%s'] * len(df['item_cde']))
            print(placeholders)
            # SQL query to fetch category data based on item_cde in the DataFrame
            query = f"""
                    SELECT item_cde, 
                    prod_seg_name, 
                    CASE 
                        WHEN prod_seg_name = 'Print' 
                                AND (UPPER(brand) LIKE '%ENDURANCE%' 
                                    OR UPPER(brand) LIKE '%STARBRITE%' 
                                    OR UPPER(brand) LIKE '%SEVILLE%' 
                                    OR UPPER(brand) LIKE '%ECONOSOURCE%' 
                                    OR UPPER(brand) LIKE '%COMET%' 
                                    OR UPPER(brand) LIKE '%POLIPRINT%' 
                                    OR UPPER(brand) LIKE '%GALAXY%' 
                                    OR UPPER(brand) LIKE '%NORDIC%' 
                                    OR UPPER(brand) LIKE '%SHOWCASE%') 
                        THEN 'Y' 
                        ELSE private_label_sw 
                    END AS private_label_sw, 
                    brand
                FROM veritiv.edm.d_item_mst
                WHERE item_cde IN ({placeholders})
                AND prod_seg_name = 'Print';
                    """

            # Execute the query
            cursor.execute(query, tuple(df['item_cde']))
            result = cursor.fetchall()

            # Convert query result to DataFrame
            cat_df = pd.DataFrame(result, columns=['item_cde', 'prod_seg_name', 'private_label_sw','brand'])

        conn.commit()

        return cat_df

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
        return None

    finally:
        conn.close()

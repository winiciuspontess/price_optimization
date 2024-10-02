import os
import pandas as pd

def filter_products_users(file_name: str):
    raw_path = '../raw_data/'
    file_uci = f'UCI-Oneline_retail/{file_name}.csv'
    uci_df = pd.read_csv(raw_path + file_uci)    
    products_purc = uci_df[uci_df['event_type'] == 'purchase']['product_id'].value_counts()
    
    products_threshold = int(0.3 * len(products_purc))
    top_products = products_purc.nlargest(products_threshold).index
    
    users_relevants = uci_df['user_id'].value_counts()
    
    user_limit = int(0.4 * len(users_relevants))
    top_users = users_relevants.nlargest(user_limit).index
    
    df_filtered = uci_df[(uci_df['product_id'].isin(top_products)) & (uci_df['user_id'].isin(top_users))]
    
    path_dir = "../processed_data/redux/"
    os.makedirs(path_dir, exist_ok=True)
    
    path_file = f"{path_dir}{file_name}.parquet"
    df_filtered.to_parquet(path_file, index=False)
    
    print(f"Path of filted datasets in parquet format: {path_file}")
    
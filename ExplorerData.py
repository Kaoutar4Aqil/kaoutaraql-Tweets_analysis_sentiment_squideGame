import pandas as pd
import seaborn as sns
import missingno as msno
from pymongo import MongoClient

def read_from_mongodb(database_name, collection_name):
    # Établir une connexion à MongoDB (assurez-vous que MongoDB est en cours d'exécution)
    client = MongoClient('mongodb://localhost:27017/')
    
    # Accéder à la base de données
    db = client[database_name]
    
    # Accéder à la collection
    collection = db[collection_name]
    
    # Lire les données à partir de la collection
    cursor = collection.find()
    
    # Convertir les résultats en liste de dictionnaires
        # Convert the cursor to a list
    data_list = list(cursor)
    
    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(data_list)
     
    return df
def save_to_mongodb(df, basedonne,collection_name):
    # Établir une connexion à MongoDB (assurez-vous que MongoDB est en cours d'exécution)
    client = MongoClient('mongodb://localhost:27017/')
    
    # Accéder à la base de données
    db = client[basedonne]
    
    # Accéder à la collection
    collection =db.create_collection(collection_name)

    
    # Convertir les données DataFrame en format de dictionnaire pour l'insertion dans MongoDB
    data_dict = df.to_dict(orient='records')
    
    # Insérer les données dans la collection
    collection.insert_many(data_dict)
    print("Données insérées avec succès dans MongoDB.")


def DataInfo(data):
    print(data)
    print(data.shape)
    print(data.dtypes)
    columns = data.columns
    print(columns)
def rename_columns(df, columns_rename):
    return df.rename(columns=columns_rename)

def drop_columns(df, columns_to_drop):
    return df.drop(columns=columns_to_drop, errors='ignore')

def explorer_data(data, columns_to_drop, columns_rename):
    print(data)
    #print(data.isnull().sum())
    data = drop_columns(data, columns_to_drop) 
    data = rename_columns(data, columns_rename)
    msno.bar(data)
    return data


database_name = "databse_name"
collection_name = "collection_name"
data = read_from_mongodb(database_name, collection_name)
columns_to_drop = ['col1', 'col2']
columns_rename = {'text': 'rename_name'}
DataInfo(data)
data1 = explorer_data(data, columns_to_drop, columns_rename)
DataInfo(data1)

new_collection="new_collection_for_save"
save_to_mongodb(data1,database_name,new_collection)
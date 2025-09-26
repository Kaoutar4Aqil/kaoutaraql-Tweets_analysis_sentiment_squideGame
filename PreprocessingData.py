import pandas as pd
import re
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
def format_datetime(df, column_name):
        df[column_name] = pd.to_datetime(df[column_name])
        df[column_name] = df[column_name].dt.strftime('%d/%m/%Y %H:%M:%S')
        return df
def remove_extra_spaces(text):
    # Remove extra spaces and strip leading/trailing spaces
    return re.sub(r'\s+', ' ', text).strip()

def remove_urls(text):
    # Define regex pattern to match URLs
    url_pattern = re.compile(r'https?://\S+|www\.\S+')
    # Replace URLs with empty string
    return url_pattern.sub(r'', text)
def remove_hashtag_sign(text):
    # Replace '#' sign with empty string
    return text.replace('#', '')

def remove_special_chars(text):
    # Remove special characters except spaces and strip leading/trailing spaces
    return re.sub(r'[!@#$%^&*()_+}~{\]["?><`//\\|;“”]', '', text).strip()

def remove_emojis(text):
    # Définir le modèle regex pour faire correspondre les emojis
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # émoticônes
                               u"\U0001F300-\U0001F5FF"  # symboles et pictogrammes
                               u"\U0001F680-\U0001F6FF"  # symboles de transport et de carte
                               u"\U0001F700-\U0001F77F"  # symboles alchimiques
                               u"\U0001F780-\U0001F7FF"  # formes géométriques étendues
                               u"\U0001F800-\U0001F8FF"  # flèches supplémentaires-C
                               u"\U0001F900-\U0001F9FF"  # symboles et pictogrammes supplémentaires
                               u"\U0001FA00-\U0001FA6F"  # Symboles d'échecs
                               u"\U0001FA70-\U0001FAFF"  # Symboles et pictogrammes étendus-A
                               u"\U00002702-\U000027B0"  # Dingbats
                               u"\U000024C2-\U0001F251" 
                               "]+", flags=re.UNICODE)
    # Remplacer les emojis par une chaîne vide
    return emoji_pattern.sub(r'', text)


def Preprocessing(df, column):

    if df[column].dtype == 'object':
        df[column] = df[column].apply(remove_urls)
        df[column] = df[column].apply(remove_hashtag_sign)
        df[column] = df[column].apply(remove_special_chars)
        df[column] = df[column].apply(remove_emojis)
        df[column] = df[column].apply(remove_extra_spaces)
        df[column] = df[column].str.lower()    
    else:
        print("La colonne spécifiée ne contient pas des chaînes de caractères et ne peut pas être prétraitée.")
    
    return df



database_name = "databse_name"
collection_name = "collection_name"
data = read_from_mongodb(database_name, collection_name)
DataInfo(data)

# Nom de la colonne à prétraiter
column = 'nom_column'

# Effectuer le prétraitement des données
data1 = Preprocessing(data, column)

new_collection="new_collection_for_save"
save_to_mongodb(data1,database_name,new_collection)
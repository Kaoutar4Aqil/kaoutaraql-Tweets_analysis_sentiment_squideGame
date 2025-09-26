import pandas as pd
from translate import Translator

# Charger votre dataframe


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
# Initialiser le traducteur


database_name = "databse_name"
collection_name = "collection_name"
data = read_from_mongodb(database_name, collection_name)
translator = Translator(to_lang='en')  # fr=french en=english ar=arabe

# Fonction pour traduire une colonne
def traduire_colonne(texte):
    return translator.translate(texte)
column="nom_column"
# Remplacer la colonne 'nom_de_votre_colonne' par sa traduction
data['tweets_en'] = data['column'].apply(traduire_colonne)

# Enregistrer votre dataframe avec la colonne traduite
data.to_csv('TweetsTra.csv')

print(data)

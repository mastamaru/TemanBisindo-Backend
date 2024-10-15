from flask import Flask, request, jsonify
from flask_pymongo import PyMongo
from dotenv import load_dotenv
import os
from azure.storage.blob import BlobServiceClient
from bson import ObjectId
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from flask_cors import CORS


load_dotenv(dotenv_path='.env')
kamus_list = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'halo', 'saya', 'selamat']
# buat kategori gestur yaitu angka atau huruf dengan memanfaatkan lambda yang memanfaatkan kamus_list assign 2 nilai bro "angka" atau "huruf" tergantung dari nilai yang ada di kamus_list
kategori_gestur = {k: "Angka" if k.isdigit() else "Kata" if len(k) > 1 else "Huruf" for k in kamus_list}


uri = os.environ.get('URI')


app = Flask(__name__)
app.config['AZURE_STORAGE_CONNECTION_STRING'] = os.environ.get('CONN_STRING')
app.config['CONTAINER_NAME'] = os.environ.get('CONTAINER_NAME')
client = MongoClient(uri)

blob_service_client = BlobServiceClient.from_connection_string(app.config['AZURE_STORAGE_CONNECTION_STRING'])
container_client = blob_service_client.get_container_client(app.config['CONTAINER_NAME'])


db = client[os.environ.get('DB_NAME')]
collection = db[os.environ.get('COLLECTION_NAME')]
CORS(app)




@app.route('/db', methods=['GET'])
def get_db():
    # check connection to database
    # check list of databases
    list_db = client.list_database_names()
    return "Connected to database: " + list_db[0]

@app.route('/')
def home():
    return "Hello, this is the home page"



# Dapatkan container client setelah memastikan container ada
container_client = blob_service_client.get_container_client(app.config['CONTAINER_NAME'])

def upload_to_azure_and_mongodb(local_path, terjemahan):
    try:
        # Upload file ke Azure Blob Storage
        file_name = os.path.basename(local_path)
        blob_name = f"{terjemahan}/{file_name}"
        with open(local_path, "rb") as data:
            blob_client = container_client.upload_blob(name=blob_name, data=data, overwrite=True)

        # Dapatkan URL video
        video_url = blob_client.url

        # Cek apakah dokumen dengan terjemahan yang sama sudah ada
        existing_doc = collection.find_one({"Terjemahan": terjemahan})
        if existing_doc:
            app.logger.info(f"Dokumen untuk {terjemahan} sudah ada, melewati penambahan.")
            return None

        # Buat dokumen untuk MongoDB jika belum ada
        document = {
            "Category": kategori_gestur[terjemahan],
            "Terjemahan": terjemahan,
            "Link_Video": video_url
        }

        # Tambahkan dokumen ke MongoDB
        if collection is None:
            raise Exception("MongoDB collection is not initialized")
        result = collection.insert_one(document)

        app.logger.info(f"Berhasil menambahkan data untuk {terjemahan}")
        return str(result.inserted_id)

    except Exception as e:
        app.logger.error(f"Error saat menambahkan data untuk {terjemahan}: {str(e)}")
        return None


@app.route('/add_all_gestur', methods=['POST'])
def add_all_gestur():
    results = []
    for terjemahan in kamus_list:
        folder_path = os.path.join('./data', terjemahan)
        
        # Cek apakah folder ada
        if os.path.exists(folder_path):
            
            # Cek apakah folder kosong
            if not os.listdir(folder_path):
                app.logger.warning(f"Folder untuk {terjemahan} kosong, melewati.")
                continue
            
            # Iterasi jika folder tidak kosong
            for file_name in os.listdir(folder_path):
                app.logger.info(f"Memproses file {file_name}")
                if file_name.lower().endswith(('.mp4', '.avi')):  # Sesuaikan dengan format video yang kamu gunakan
                    file_path = os.path.join(folder_path, file_name)
                    result_id = upload_to_azure_and_mongodb(file_path, terjemahan)
                    if result_id:
                        results.append({"terjemahan": terjemahan, "id": result_id, "Category": kategori_gestur[terjemahan]})
                else:
                    app.logger.warning(f"File {file_name} tidak didukung, melewati.")
        else:
            app.logger.warning(f"Folder untuk {terjemahan} tidak ditemukan")

    return jsonify({"message": "Proses penambahan data selesai", "results": results})



@app.route('/get_gestur', methods=['GET'])
def get_gestur():
    terjemahan = request.args.get('terjemahan')
    if not terjemahan:
        return jsonify({"error": "Parameter 'terjemahan' diperlukan"}), 400

    try:
        gestur = collection.find_one({"Terjemahan": terjemahan})
        if gestur:
            # Konversi ObjectId ke string jika ingin menampilkan _id
            gestur['_id'] = str(gestur['_id'])
            return jsonify(gestur), 200
        else:
            return jsonify({"error": "Gestur tidak ditemukan"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get_all_gestur', methods=['GET'])
def get_all_gestur():
    try:
        gestur = collection.find()
        result = []
        for g in gestur:
            g['_id'] = str(g['_id'])
            result.append(g)
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# delete all data
@app.route('/delete_all_gestur', methods=['DELETE'])
def delete_all_gestur():
    try:
        collection.delete_many({})
        return jsonify({"message": "Berhasil menghapus semua data"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route('/test_gestur', methods=['GET'])
#return a div with the video
def test_gestur():
    terjemahan = request.args.get('terjemahan')
    if not terjemahan:
        return jsonify({"error": "Parameter 'terjemahan' diperlukan"}), 400

    try:
        gestur = collection.find_one({"Terjemahan": terjemahan})
        if gestur:
            return f'<video width="320" height="240" controls><source src="{gestur["Link_Video"]}" type="video/mp4"></video>'
        else:
            return jsonify({"error": "Gestur tidak ditemukan"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


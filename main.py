from flask import Flask, jsonify, request
from flask_cors import CORS
import base64
from PIL import Image
import boto3
import uuid
import io
import psycopg2

app = Flask(__name__)
CORS(app)



# Establish a connection to the database
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

# Route to create a new dataset
@app.route('/datasets', methods=['POST'])
def create_dataset():
    try:
        data = request.get_json()
        dataset_name = data.get('name')

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT INTO dataset (name) VALUES (%s) RETURNING dataset_id, name;', (dataset_name,))
        dataset = cursor.fetchone()
        conn.commit()
        conn.close()

        if dataset:
            dataset_details = {'id': dataset[0], 'name': dataset[1]}
            return jsonify(dataset_details), 201
        else:
            return jsonify({'error': 'Failed to create dataset'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Route to get all datasets
@app.route('/datasets', methods=['GET'])
def get_all_datasets():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM dataset;')
        datasets = cursor.fetchall()
        conn.close()

        dataset_list = [{'id': dataset[0], 'name': dataset[1]} for dataset in datasets]
        return jsonify(dataset_list), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/datasettt', methods=['GET'])
def get_all_datasets_noofentries():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT dataset.dataset_id, dataset.name, COUNT(entry.image_id) AS num_entries FROM dataset LEFT JOIN entry ON dataset.dataset_id = entry.dataset_id GROUP BY dataset.dataset_id, dataset.name;')
        datasets = cursor.fetchall()
        conn.close()

        dataset_list = [{'id': dataset[0], 'name': dataset[1], 'num_entries': dataset[2]} for dataset in datasets]
        return jsonify(dataset_list), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    
# Route to get entries by dataset ID
@app.route('/datasets/<int:dataset_id>/entries', methods=['GET'])
def get_entries_by_dataset(dataset_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT image_id, file_url, dataset_id, text_extraction, text_classification, visual_classification, attachment_type, labreport_extraction, nutrition_extraction, output, history FROM entry WHERE dataset_id = %s;', (dataset_id,))
        entries = cursor.fetchall()
        # Format data as list of dictionaries
        entries_list = []
        for row in entries:
            entry_dict = {
                "image_id": row[0],
                "file_url": row[1],
                "dataset_id": row[2],
                "text_extraction": row[3],
                "text_classification": row[4],
                "visual_classification": row[5],
                "attachment_type": row[6],
                "labreport_extraction": row[7],
                "nutrition_extraction": row[8],
                "output": row[9],
                "history": row[10]
            }
            entries_list.append(entry_dict)
        
        conn.close()
        return jsonify(entries_list), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    


# Route to add a new entry to a dataset
@app.route('/datasets/<int:dataset_id>/entries', methods=['POST'])
def add_entry_to_dataset(dataset_id):
    try:
        # Extract data from the request body
        data = request.json
        image_data_base64 = data.get('file_url')
        text_extraction = data.get('text_extraction')
        text_classification = data.get('text_classification')
        visual_classification = data.get('visual_classification')
        attachment_type = data.get('attachment_type')
        labreport_extraction = data.get('labreport_extraction')
        nutrition_extraction = data.get('nutrition_extraction')
        output = data.get('output')
        history = data.get('history')
        
        # Decode base64 data into image bytes
        image_bytes = base64.b64decode(image_data_base64.split(',')[1])  # Extract the base64 data part

        # Upload image to S3 bucket
        bucket_name = 'image-testing-pipeline'
        object_key = str(uuid.uuid4()) + '.jpg'  # Example: 'images/entry_' + str(uuid.uuid4()) + '.jpg'
        s3.upload_fileobj(io.BytesIO(image_bytes), bucket_name, object_key)

        # Generate custom file URL
        file_url = f'https://{bucket_name}.s3.amazonaws.com/{object_key}'

        # Insert the new entry into the database
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT INTO entry (file_url, dataset_id, text_extraction, text_classification, visual_classification, attachment_type, labreport_extraction, nutrition_extraction, output, history) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);', (file_url, dataset_id, text_extraction, text_classification, visual_classification, attachment_type, labreport_extraction, nutrition_extraction, output, history))
        conn.commit()
        conn.close()

        return jsonify({'message': 'Entry added successfully'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500




@app.route('/datasets/<int:dataset_id>/pageEntry', methods=['GET'])
def get_entries_by_dataset_paginated(dataset_id):
    try:
        # Get pagination parameters from the request query string
        page = int(request.args.get('page', 1))
        page_size = 4

        conn = get_db_connection()
        cursor = conn.cursor()

        # Count total number of entries for the dataset
        cursor.execute('SELECT COUNT(*) FROM entry WHERE dataset_id = %s;', (dataset_id,))
        total_entries = cursor.fetchone()[0]

        # Calculate total pages
        total_pages = (total_entries + page_size - 1) // page_size

        # Calculate offset and limit for pagination
        offset = (page - 1) * page_size
        limit = page_size

        cursor.execute('SELECT image_id, file_url, dataset_id, text_extraction, text_classification, visual_classification, attachment_type, labreport_extraction, nutrition_extraction, output, history FROM entry WHERE dataset_id = %s ORDER BY image_id OFFSET %s LIMIT %s;', (dataset_id, offset, limit))
        entries = cursor.fetchall()

        # Format data as list of dictionaries
        entries_list = []
        for row in entries:
            entry_dict = {
                "image_id": row[0],
                "file_url": row[1],
                "dataset_id": row[2],
                "text_extraction": row[3],
                "text_classification": row[4],
                "visual_classification": row[5],
                "attachment_type": row[6],
                "labreport_extraction": row[7],
                "nutrition_extraction": row[8],
                "output": row[9],
                "history": row[10],
                "totalPages": total_pages
            }
            entries_list.append(entry_dict)
        
        conn.close()
        
        return jsonify(entries_list), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Route to get all entries
@app.route('/entries', methods=['GET'])
def get_entries():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT image_id, file_url, dataset_id, text_extraction, text_classification, visual_classification, attachment_type, labreport_extraction, nutrition_extraction, output, history FROM entry;')
        entries = cursor.fetchall()
        
        # Format data as list of dictionaries
        entries_list = []
        for row in entries:
            entry_dict = {
                "image_id": row[0],
                "file_url": row[1],
                "dataset_id": row[2],
                "text_extraction": row[3],
                "text_classification": row[4],
                "visual_classification": row[5],
                "attachment_type": row[6],
                "labreport_extraction": row[7],
                "nutrition_extraction": row[8],
                "output": row[9],
                "history": row[10]
            }
            entries_list.append(entry_dict)
        
        conn.close()
        return jsonify(entries_list), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Route to add a new entry
@app.route('/entries', methods=['POST'])
def add_entry():
    try:
        data = request.get_json()

        attachment_type = data.get("attachment_type", "")
        dataset_id = data.get("dataset_id", "")
        file_url = data.get("file_url", "")
        history = data.get("history", "")
        labreport_extraction = data.get("labreport_extraction", "")
        nutrition_extraction = data.get("nutrition_extraction", "")
        output = data.get("output", "")
        text_classification = data.get("text_classification", "")
        text_extraction = data.get("text_extraction", "")
        visual_classification = data.get("visual_classification", "")

        # Assuming 'name' is a column in your 'entry' table

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT INTO entry (attachment_type, dataset_id, file_url, history, labreport_extraction, nutrition_extraction, output, text_classification, text_extraction, visual_classification) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);',
                       (attachment_type, dataset_id, file_url, history, labreport_extraction, nutrition_extraction, output, text_classification, text_extraction, visual_classification))
        conn.commit()
        conn.close()
        return jsonify({'message': 'Entry added successfully'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
# Route to update an entry
@app.route('/entries/<int:entry_id>', methods=['PUT'])
def update_entry(entry_id):
    try:
        data = request.get_json()
        # Extract updated values from the request body
        attachment_type = data.get("attachment_type", "")
        dataset_id = data.get("dataset_id", "")
        file_url = data.get("file_url", "")
        history = data.get("history", "")
        labreport_extraction = data.get("labreport_extraction", "")
        nutrition_extraction = data.get("nutrition_extraction", "")
        output = data.get("output", "")
        text_classification = data.get("text_classification", "")
        text_extraction = data.get("text_extraction", "")
        visual_classification = data.get("visual_classification", "")
        image_id = entry_id

        dataset_id = int(dataset_id) if dataset_id else None
        image_id = int(image_id) if image_id else None
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE entry SET attachment_type = %s, dataset_id = %s, file_url = %s, history = %s, labreport_extraction = %s, nutrition_extraction = %s, output = %s, text_classification = %s, text_extraction = %s, visual_classification = %s WHERE image_id = %s;',
               (attachment_type, dataset_id, file_url, history, labreport_extraction, nutrition_extraction, output, text_classification, text_extraction, visual_classification, image_id))

        conn.commit()
        conn.close()

        return jsonify({'message': 'Entry updated successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)


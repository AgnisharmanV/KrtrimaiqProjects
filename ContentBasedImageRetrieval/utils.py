import numpy as np
import cv2
from scipy import spatial
import tensorflow as tf
import pickle
import psycopg2
import base64
import json
import sys
import requests


class PGdB:

    def __init__(self, db_cred):
        self.hostName = db_cred['hostName']
        self.dbName = db_cred['dbName']
        self.userName = db_cred['userName']
        self.password = db_cred['password']
        self.conn = psycopg2.connect(host=self.hostName,
                                     database=self.dbName,
                                     user=self.userName,
                                     password=self.password)
        self.cur = self.conn.cursor()

    def reconnect(self):
        self.conn = psycopg2.connect(host=self.hostName,
                                     database=self.dbName,
                                     user=self.userName,
                                     password=self.password)
        self.cur = self.conn.cursor()

    def select_rows(self, keys):
        keys = tuple(keys)
        sql = f"SELECT * FROM metadata WHERE MediaNumber in {keys}"

        self.cur.execute(sql)
        rows = self.cur.fetchall()
        return rows

    def close_connection(self):
        try:
            self.conn.close()
            self.cur.close()
        except Exception as e:
            pass


def load_pkl_from_bytes(b):
    return pickle.loads(b)


def load_pickle(path):
    with open(path, 'rb') as pkl:
        x = pickle.load(pkl)
    return x


def euclidean_distance(query, x):
    """ Measures euclidean distances between 2 vectors """
    return np.linalg.norm(query - x)


def cosine_similarity(query, x):
    """ Measures cosine similarity between 2 vectors """
    return spatial.distance.cosine(query, x)


def load_model(path):
    return tf.keras.models.load_model(path, compile=False)


def load_image(query, shape=None, resize_image=True):
    """ Reads image as numpy array in RGB format """
    image = np.frombuffer(query, np.uint8)
    image = cv2.imdecode(image, cv2.IMREAD_COLOR)
    image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
    if shape and resize_image:
        if image.shape != shape:
            image = resize(image, (shape[0], shape[1]))
    return image


def get_features(query, encoder, input_shape):
    """ Returns features : (1, 1024) """
    image = standardize(load_image(query, input_shape))
    image = np.expand_dims(image, axis=0)
    image_features = np.stack(encoder.predict(image), axis=0).astype('float16')
    return image_features.astype('float16')


def get_img_string(image_url):
    img = requests.get(image_url)
    return base64.b64encode(img.content).decode('ascii')


def display_results(query, results, db_cred):
    search_result = {'searchresult': []}
    DB = PGdB(db_cred)

    for i in range(len(query)):
        result_keys = [r[1] for r in results[i]]

        q_string = base64.b64encode(query[i]).decode('ascii')

        try:
            rows = DB.select_rows(result_keys)
        except psycopg2.InternalError:
            DB.reconnect()
            rows = DB.select_rows(result_keys)
        except psycopg2.InterfaceError:
            DB.reconnect()
            rows = DB.select_rows(result_keys)
        except Exception as e:
            print(e)
            sys.exit(-1)
        sub_results = [''] * len(result_keys)
        for r in rows:
            img_string = get_img_string(r[8])
            formatted_row = {'MediaNumber': r[0],
                             'CollectionTitle': r[1],
                             'WorfRef': r[2],
                             'OriginalFilename': r[3],
                             'FileName': r[4],
                             'LegacyAssetId': r[5],
                             'AssetLibraryId': r[6],
                             'ClusterName': r[7],
                             'ImageUrl': r[8],
                             'ImageContent': img_string}

            sub_results[result_keys.index(r[0])] = formatted_row
        search_result['searchresult'].append({'searchImage': q_string, 'similarImages': sub_results})
        DB.close_connection()
    return json.dumps(search_result)


def resize(image, shape, keep_aspect_ratio=True):
    """ Resizes image to 'shape' """
    x_shape = shape[0]
    y_shape = shape[1]

    if keep_aspect_ratio:
        if image.shape[0] > image.shape[1]:
            y_shape = int(y_shape * (image.shape[1] / image.shape[0]))
            image_resized = cv2.resize(image, (y_shape, x_shape))
            pad1 = (shape[1] - y_shape) // 2
            pad2 = shape[1] - y_shape - pad1
            img_padded = np.pad(image_resized, ((0, 0), (pad1, pad2), (0, 0)))
        else:
            x_shape = int(x_shape * (image.shape[0] / image.shape[1]))
            image_resized = cv2.resize(image, (y_shape, x_shape))
            pad1 = (shape[0] - x_shape) // 2
            pad2 = shape[0] - x_shape - pad1
            img_padded = np.pad(image_resized, ((pad1, pad2), (0, 0), (0, 0)))
        return img_padded
    else:
        image_resized = cv2.resize(image, (y_shape, x_shape))
    return image_resized


def standardize(image):
    """ Returns uniformly standardized array """
    image = (image - np.min(image)) / (np.max(image) - np.min(image))
    return image


def kmeans_cluster(kmeans, img_features):
    img_features = img_features.reshape((1, img_features.size))
    cluster = kmeans.predict(img_features)
    return cluster[0]


def search(query, features, encoder, input_shape, max_results, sim_func,
                     user_input=None, cate=None, kmeans=None, kclusters=None):
    keys = []

    for i in range(len(query)):
        if user_input:
            for ui in user_input:
                keys.append(cate[ui])

        elif kmeans:
            img_features = get_features(query[i], encoder, input_shape)
            cl = kmeans_cluster(kmeans, img_features)
            keys.append(kclusters[cl])

    else:
        keys = features.keys()

    results = []
    query_features = []
    for q in query:
        x = get_features(q, encoder, input_shape)
        query_features.append(x)

    for q_f in query_features:
        r = []
        for key in keys:
            feature = np.stack(features[key], axis=0)
            if sim_func == 'cosine':
                result = cosine_similarity(q_f.flatten(), feature.flatten())
            else:
                result = euclidean_distance(q_f, feature)
            r.append((result, key))
        results.append(sorted(r)[:max_results])
    return results

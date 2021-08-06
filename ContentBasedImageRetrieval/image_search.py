""" Performs search using an image. Top N similar images are returned. """
import utils
import config
import sys
import os

ROOT_DIR = config.root_dir
KMEANS_PATH = os.path.join(ROOT_DIR, config.kmeans_path)
ENCODER_PATH = os.path.join(ROOT_DIR, config.encoder_path)
INPUT_SHAPE = config.input_shape
KCLUSTER_PATH = os.path.join(ROOT_DIR, config.kcluster_path)
FEATURE_PATH = os.path.join(ROOT_DIR, config.features_path)
DB_CRED = config.db_cred

try:
    KMEANS = utils.load_pickle(KMEANS_PATH)
    ENCODER = utils.load_model(ENCODER_PATH)
    KCLUSTER = utils.load_pickle(KCLUSTER_PATH)
    FEATURES = utils.load_pickle(FEATURE_PATH)
except Exception as e:
    print(e)
    sys.exit(-1)


# sample for testing
sample_query = os.path.join(ROOT_DIR, 'sample_images/00083472.jpg')

with open(sample_query, 'rb') as f:
    query = f.read()

queries = [query]  # list of images to search for eg. [img1, img2, img3]
# sample for testing

try:
    results = utils.search(query=queries,
                           features=FEATURES,
                           encoder=ENCODER,
                           input_shape=INPUT_SHAPE,
                           max_results=5,
                           sim_func='cosine',
                           kmeans=KMEANS,
                           kclusters=KCLUSTER
                           )

except Exception as e:
    print(e)
    sys.exit(-1)


try:
    json_dump = utils.display_results(query=queries,
                                      results=results,
                                      db_cred=DB_CRED
                                      )
    print(json_dump)
except Exception as e:
    print(e)
    sys.exit(-1)

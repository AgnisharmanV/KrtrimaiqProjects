""" Transcribes an audio/video file """
import sys
import utils
import config
import os

BUCKET = config.BUCKET  # s3 bucket
JOB_NAME = config.JOB_NAME  # should be unique for each transcription job

KEY = 'key'
ROOT_DIR =   config.ROOT_DIR
ABS_PATH = os.path.join(ROOT_DIR, KEY)
with open(ABS_PATH, 'rb') as f:
    FILE_OBJ = f.read()

file_format = KEY.split('.')[-1]

# Converts file to supported path if not so
if file_format not in config.SUPPORTED_FORMAT:
    print('Converting file to a supported format...')
    KEY = KEY.split('.')[0] + '.wav'
    FILE_OBJ = utils.convert_file_to_wav(FILE_OBJ)

aws_ref = utils.AwsRef(BUCKET, JOB_NAME)

try:
    aws_ref.upload_to_s3(FILE_OBJ, KEY)
except Exception as e:
    print(e)
    print('Upload Failed')
    sys.exit(-1)

try:
    aws_ref.transcribe_file(KEY)
except Exception as e:
    print(e)
    print('Transcription Failed')
    sys.exit(-1)

# raw_transcript = aws_ref.get_raw_transcript()
speaker_tagged_transcript = aws_ref.get_speaker_tagged_transcript()

# Uncomment the block below to upload results to cloud
# raw_transcript_obj = bytes(raw_transcript, 'utf8')
# speaker_tagged_transcript = bytes(speaker_tagged_transcript, 'utf8')
#
# transcript_key1 = KEY.split('.')[0] + '_raw.txt'
# transcript_key2 = KEY.split('.')[0] + '_tagged.txt'
# try:
#     aws_ref.upload_to_s3(raw_transcript_obj, transcript_key1)
#     aws_ref.upload_to_s3(speaker_tagged_transcript, transcript_key2)
# except Exception as e:
#     print(e)
#     print('Upload Failed')
#     sys.exit(-1)

with open(os.path.join(ROOT_DIR, '/transcripts.txt'), 'w') as f:
    f.write(speaker_tagged_transcript)

# delete objects from s3
objs_to_delete = [KEY,  # audio file
                  JOB_NAME + '.json',  # json output of transcribe
                  ]
aws_ref.cleanup(objs_to_delete)

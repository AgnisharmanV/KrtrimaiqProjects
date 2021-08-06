import boto3
import json
import time
import sys
import io
from pydub import AudioSegment


s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
transcribe = boto3.client('transcribe')


class AwsRef:

    def __init__(self, bucket, job_name):
        self.bucket = bucket
        self.job_name = job_name

    def upload_to_s3(self, obj, key):
        """Uploads the file specified by file_name to s3 bucket as file_bucket"""
        print('Uploading to s3...')
        s3_client.put_object(Body=obj, Bucket=self.bucket, Key=key)
        print('Upload complete.')

    def cleanup(self, objs):
        for ob in objs:
            try:
                s3_client.delete_object(
                    Bucket=self.bucket,
                    Key=ob
                )
            except Exception as e:
                print(e)
                print(f'Failed to delete {ob}. Please use console.')

    def transcribe_file(self, key):
        """Starts transcription job"""
        file_format = key.split('.')[-1]
        file_uri = 's3://' + self.bucket + '/' + key

        transcribe.start_transcription_job(TranscriptionJobName=self.job_name,
                                           Media={'MediaFileUri': file_uri},
                                           MediaFormat=file_format,
                                           LanguageCode='en-IN',
                                           OutputBucketName=self.bucket,
                                           Settings={
                                              'MaxSpeakerLabels': 10,
                                              'ShowSpeakerLabels': True
                                           })

        print(f'{self.job_name} started')
        while True:
            try:
                status = transcribe.get_transcription_job(TranscriptionJobName=self.job_name)
                if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
                    print(f"Transcription job {self.job_name} {status['TranscriptionJob']['TranscriptionJobStatus']}")
                    break
                print('In progress...')
                time.sleep(30)
            except Exception as e:
                print(e)
                print('Connection lost')
                sys.exit(-1)
        result_uri = str(status['TranscriptionJob']['Transcript']['TranscriptFileUri'])
        return result_uri

    def get_raw_transcript(self):
        filename = self.job_name + '.json'
        obj = s3_resource.Object(self.bucket, filename).get()['Body'].read()
        transcript = obj.decode('utf-8')
        # transcript = json.load(obj.decode('utf8').replace("'", '"'))

        text = transcript['results']['transcripts'][0]['transcript']
        return transcript

    def get_speaker_tagged_transcript(self):
        filename = self.job_name + '.json'
        obj = s3_resource.Object(self.bucket, filename).get()['Body'].read()
        # transcript = json.loads(obj.decode('utf8').replace("'", '"'))
        transcript = obj.decode('utf-8')
        segments = transcript['results']['speaker_labels']['segments']
        items = transcript['results']['items']

        seg = []
        for i in segments:
            seg.extend(i['items'])

        time_speaker = {}
        for i in seg:
            key = i['start_time']
            value = i['speaker_label']

            time_speaker[key] = value

        curr_spk = None
        text = ''

        for item in items:
            if item['type'] == 'pronunciation':
                speaker = time_speaker[item['start_time']]
                if curr_spk != speaker:
                    text += f'\n{speaker}: '
                    curr_spk = speaker
                text = text + ' ' + item['alternatives'][0]['content']
            else:
                text += item['alternatives'][0]['content']

        return text


def convert_file_to_wav(obj):
    try:
        x = io.BytesIO()
        AudioSegment.from_file(obj).export(x, format='wav')

    except Exception as e:
        print(e)
        print('Conversion failed. File format not supported.')
        sys.exit(-1)
    return x

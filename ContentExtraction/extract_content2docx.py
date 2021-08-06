""" Extracts tables and other text from pdfs.
Returns a Word doc retaining the structure of data present (such as tables).
Input file could be normal text pdfs, scanned image pdfs or images (with minor modifications to the code)."""

import utils
import config
import io
import os


BUCKET = config.bucket_name
TIMELIM = config.timelim
REGION = config.region
os.environ['AWS_ACCESS_KEY'] = config.access_key
os.environ['AWS_DEFAULT_REGION'] = REGION
os.environ['AWS_SECRET_ACCESS_KEY'] = config.secret_key

#  User input
TAGS = ['extract start', 'extract end']  # None if no tags are present
OUTPUT_FORMAT = 'docx'  # docx | txt

Aws = utils.AWSRef(BUCKET, REGION)

# input pdf
###########################
pdf_path = r'C:\Users\agnis\PycharmProjects\ContentExtractionFromPetition\sample_pdfs\Lambasingi 1 voter list.pdf'
input_filename = 'votlist2.pdf'
with open(pdf_path, 'rb') as f:
    pdf_bytes = f.read()
###########################

Aws.upload_obj(pdf_bytes, input_filename, 'private')
job_id = Aws.start_analysis(input_filename)
response = Aws.check_progress(job_id, None)
blocks = response['Blocks']

while True:
    if 'NextToken' in response.keys():
        next_token = response['NextToken']
        response = Aws.check_progress(job_id, next_token)
        blocks.extend(response['Blocks'])
    else:
        break


if OUTPUT_FORMAT == 'docx':
    Docx = utils.Docx()
    utils.write_to_docx(blocks, Docx, TAGS)

    output_filename = input_filename.split('.')[0] + '.docx'

    with io.BytesIO() as fileobj:
        Docx.save_doc(fileobj)
        fileobj.seek(0)
        Aws.upload_obj(fileobj.read(), output_filename, 'public-read')

else:
    text = utils.write_to_text(blocks, TAGS)

    output_filename = input_filename.split('.')[0] + '.txt'
    with io.StringIO() as fileobj:
        fileobj.write(text)
        fileobj.seek(0)

        Aws.upload_obj(fileobj.read(), output_filename, 'public-read')

public_url = Aws.get_url(output_filename)
print(public_url)  # location of output file

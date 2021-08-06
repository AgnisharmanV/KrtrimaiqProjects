""" Extracts key information from a voters list and stores it in an excel or csv file. """
import json
import re
import pandas as pd
import sys

FILEPATH = 'path/to/file'  # path to json file containing a parsed pdf.
                           # File contains text and location of the text

with open(FILEPATH, 'r') as f:
    blocks = json.load(f)


def order_list(line, bb1, bb2):
    ord_l = []
    ord_b = []
    for b1 in bb1:
        min2 = 1000000.0001
        for idx, b2 in enumerate(bb2):
            b1x_avg = b1[0]['X'] + b1[1]['X'] + b1[2]['X'] + b1[3]['X']
            b1y_avg = b1[0]['Y'] + b1[1]['Y'] + b1[2]['Y'] + b1[3]['Y']
            b2x_avg = b2[0]['X'] + b2[1]['X'] + b2[2]['X'] + b2[3]['X']
            b2y_avg = b2[0]['Y'] + b2[1]['Y'] + b2[2]['Y'] + b2[3]['Y']
            dist = (b1x_avg - b2x_avg) ** 2 + (b1y_avg - b2y_avg) ** 2
            if dist < min2:
                min2 = dist
                i2 = idx
        ord_l.append(line[i2])
        ord_b.append(bb2[i2])
    return ord_l, ord_b


def read_blocks(response):

    line_blocks = []
    for block in response:
        if block['BlockType'] == 'LINE':
            line_blocks.append(block)

    list_text = []
    i = 0
    line_1, line_2, line_3, line_4 = [], [], [], []
    bb1, bb2, bb3, bb4 = [], [], [], []

    l1, l2, l3, l4 = 0, 0, 0, 0
    curr_page = 0
    while i < len(line_blocks):
        t = line_blocks[i]['Text'].strip()
        if 'section no and name' in t.lower():
            regex = re.compile("section no and name ?:? ([A-Za-z0-9 -]*)", flags=re.IGNORECASE)
            section_name = re.findall(regex, t)
        if t[:4] == 'Name':
            line_1.append(line_blocks[i]['Text'])
            bb1.append(line_blocks[i]['Geometry']['Polygon'])
            l1+=1
        if (t[:9].lower() == "husband's") or \
                (t[:8].lower() == "father's") or \
                (t[:8].lower() == "mother's") or \
                (t[:6].lower() == "wife's") or \
                (t[:7].lower() == "other's"):
            regex = re.compile("Name ?:? ([A-Za-z\. ]*)")
            names = [x for x in re.findall(regex, t) if len(x) > 1]

            if len(names) == 0:
                t = t.strip() + ' ' + line_blocks[i+3]['Text'].strip()
            line_2.append(t)
            bb2.append(line_blocks[i]['Geometry']['Polygon'])
            l2+=1
        if t[:5] == 'House':
            regex = re.compile("House Number ?:? (.*)")
            hns = re.findall(regex, t)
            if len(hns) == 0:
                t = t.strip() + ' : ' + line_blocks[i+1]['Text'].strip()
            line_3.append(t)
            bb3.append(line_blocks[i]['Geometry']['Polygon'])
            l3+=1
        if ('Age' in t) and ('Gender' in t):
            line_4.append(t)
            bb4.append(line_blocks[i]['Geometry']['Polygon'])
            l4+=1
        i += 1

        page_no = line_blocks[i-1]['Page']
        if (page_no != curr_page) or (i == len(line_blocks)):
            if (len(line_4)) > 0 and (len(line_1) == len(line_4)) and (len(line_2) == len(line_1)):
                ord_l2, ord_b2 = order_list(line_2, bb1, bb2)
                ord_l3, ord_b3 = order_list(line_3, bb1, bb3)
                ord_l4, ord_b4 = order_list(line_4, bb1, bb4)

                try:
                    for k in range(len(line_4)):
                        list_text.append([line_1[k], ord_l2[k], ord_l3[k], ord_l4[k], section_name])
                except Exception as e:
                    print(e)
                    sys.exit()

                line_1, line_2, line_3, line_4 = [], [], [], []
                bb1, bb2, bb3, bb4 = [], [], [], []

    text_ = ''
    print(l1, l2, l3, l4)
    for l in line_blocks:
        text_ = text_ + l['Text'] + '\n'

    return text_, list_text


def find_names(text):
    regex = re.compile("Name ?:? ([A-Za-z\. ]*)")
    names = [x for x in re.findall(regex, text) if len(x)>1]
    return names


def find_fh_names(text):
    regex = re.compile("(?!Husband's|Father's) Name ?:? ([A-Za-z ]*)\n")
    names = [x for x in re.findall(regex, text) if len(x) > 1]
    return names


def find_ages(text):
    regex = re.compile("Age ?:? (\d{1,3})")
    ages = re.findall(regex, text)
    return ages


def find_id(text):
    regex = re.compile("([A-Z]{2,3}) ?[A-Z]?(\d{5,9})")
    ids = re.findall(regex, text)
    return ids


def find_hn(text):
    regex = re.compile("House Number ?:? (.*)")
    hns = re.findall(regex, text)
    return hns


def find_gender(text):
    regex = re.compile("Gender ?:? (.*)")
    gender = re.findall(regex, text)
    return gender


text, list_text = read_blocks(blocks)

names, fh_names, ages, hns, genders, section_names = [], [], [], [], [], []

ids = find_id(text)
ids = [str(x) + str(y) for x, y in ids]

for ix, i in enumerate(list_text):
    names.append(find_names(i[0].strip())[0].strip())
    try:
        fh_names.append(find_names(i[1].strip())[0].strip())
        hns.append(find_hn(i[2].strip())[0].strip())
        ages.append(find_ages(i[3].strip())[0].strip())
        genders.append(find_gender(i[3].strip())[0].strip())
        section_names.append(i[4][0].strip())
    except Exception as e:
        print(e)
        sys.exit()

# print(len(names), len(fh_names), len(ages), len(ids), len(hns), len(genders))
df = pd.DataFrame()
df['sl.no'] = range(1, len(ids)+1)
df['id'] = ids
df['Name'] = names
df["Father's Name/ Husband's Name/ Mother's Name/ Wife's Name"] = fh_names
df['Age'] = ages
df['Gender'] = genders
df['House Number'] = hns
df['Section No and Name'] = section_names
# print(text)
df.to_excel('path/to/excel', index=False)



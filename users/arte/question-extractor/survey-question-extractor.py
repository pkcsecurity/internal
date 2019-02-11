import pandas as pd
import pprint
import json

pp = pprint.PrettyPrinter(indent=4)

df = pd.read_csv('healthy-church-surveys.csv')

current_category = None
results = []

for idx, row in df.iterrows():
    if idx != 0:
        new_question = {}
        category = row[0]
        question = row[1]

        if not pd.isnull(category):
            current_category = category

        if pd.isnull(question) or question.lower() == 'sub-total':
            continue

        new_question['question'] = question
        new_question['category'] = current_category

        results.append(new_question)

with open('result.json', 'w') as f:
    json.dump(results, f)

import json
from pandas.io.json import json_normalize
import pandas as pd


class order_json:
    def read_json(self, arq):
        with open(arq) as json_file:
            return json.load(json_file)

    def distinct_json(self, arq):
        json_string = pd.DataFrame()
        for data in arq:
            for managers in data['managers']:
                for watchers in data['watchers']:
                    job = data['name']
                    priotiy = data['priority']
                    text = pd.Series({"name": job, "managers": managers,
                                      "watchers": watchers, "priority": priotiy})
                    json_string = json_string.append(text, ignore_index=True)
        return json_string

    def export_df(self, json_string, json_filter):
        export = []
        json_string = json_string.sort_values([json_filter, 'priority'])
        for item in json_string[json_filter].unique():
            lista = list(json_string.loc[json_string[json_filter] == item]['name'].unique())
            export.append(json.loads(json.dumps({item: lista})))
        with open(f'{json_filter}.json', 'w') as outfile:
            json.dump(export, outfile)


oj = order_json()
arq = oj.read_json('data.json')
json_string = oj.distinct_json(arq)
oj.export_df(json_string, 'managers')
oj.export_df(json_string, 'watchers')

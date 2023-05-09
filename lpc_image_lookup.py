import flask
from flask import Flask, redirect
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd
import os
from fuzzywuzzy import fuzz

os.chdir('/home/seng3/seng3_nfs/LPC_IMAGE_LOOKUP')

app = Flask(__name__)

def uppercase(x):
    return x.upper()

def stripper(x):
    return x.strip()

def find_max_simscore(df, search_term):
    df['simscore'] = df['item_name'].map(lambda x: fuzz.token_sort_ratio(x, search_term))
    return df.iloc[df['simscore'].idxmax()]

def Sheet_to_df(sheet_name,Tab_name):
    scope = ['https://spreadsheets.google.com/feeds']
    creds = ServiceAccountCredentials.from_json_keyfile_name('anz-nv-lpc-a2e63f0ab584.json', scope)
    client = gspread.authorize(creds)
    Form_df = client.open_by_key(sheet_name)
    Form_df_data = Form_df.worksheet(Tab_name)
    Form_df = pd.DataFrame(Form_df_data.get_all_values())
    Form_df.columns = Form_df.iloc[0]
    Form_df = Form_df[1:].reset_index(drop=True)
    return Form_df    


def Df_to_sheet(sheet_name,Tab_name,df):
    scope = ['https://spreadsheets.google.com/feeds']
    creds = ServiceAccountCredentials.from_json_keyfile_name('anz-nv-lpc-a2e63f0ab584.json', scope)
    client = gspread.authorize(creds)
    # dataframe (create or import it)
    Form_df = client.open_by_key(sheet_name)
    Form_df_data = Form_df.worksheet(Tab_name)
    Form_df_data.clear()
    Form_df_data.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option='USER_ENTERED')

    
def Reset_input(sheet_name,Tab_name):
    scope = ['https://spreadsheets.google.com/feeds']
    creds = ServiceAccountCredentials.from_json_keyfile_name('anz-nv-lpc-a2e63f0ab584.json', scope)
    client = gspread.authorize(creds)
    # dataframe (create or import it)
    Form_df = client.open_by_key(sheet_name)
    Form_df_data = Form_df.worksheet(Tab_name)
    Form_df_data.clear()
    df = pd.DataFrame({'Item Name': ['Item Name']})
    Form_df_data.update([df.columns.values.tolist()])# + df.values.tolist())

    
def search_main(input_sheet, output_sheet):
    lpc = pd.read_csv('anz_lpc_raw.csv')
    lpc = lpc[lpc['item_name'].notna()]
    lpc = lpc[lpc['image'].notna()]
    lpc = lpc.loc[lpc['classification'] != 'incomplete']
    lpc = lpc.loc[lpc['classification'] != 'complete']
    lpc = lpc.drop(lpc[lpc.NA_Flag == 1].index)
    lpc['item_name'] = lpc['item_name'].apply(uppercase)
    lpc['item_name'] = lpc['item_name'].apply(stripper)
    lpc = lpc.sort_values(['category', 'item_name']).reset_index().drop('index', axis=1)
    
    threshold = 0
    found = 1
    
    storeage = pd.DataFrame({'item_name':[], 'image':[], 'simscore':[], 'image_preview':[]})
    input_df = Sheet_to_df(input_sheet, 'input')
    
    for i in input_df['Item Name']:
        temp = find_max_simscore(lpc, i).drop(labels = ['gtin', 'category', 'subcategory', 'gtin_source', 'name_source', 'category_source',
                                                        'subcategory_source', 'image_source', 'classification', 'NA_Flag', 'UPT', 'LastUpdated'])
        if temp['simscore'] >= threshold:
            found += 1
            temp['image_preview'] = '=image(b'+str(found)+')'
            storeage = storeage.append(temp)
    
    Df_to_sheet(output_sheet,'output',storeage)

@app.route('/', methods=['GET', 'POST'])
def index():
    return flask.render_template('home.html')

@app.route('/background')
def background():
    input_sheet = '1vyKsxr2A12Pa5HwmWFq3Fq5Iw3XLE5i9zOreGGqwJ5U'
    output_sheet = '1BTI9EU36Pi-9JSJhoANof4Oi8qHD8JLcAzQkaZCVdOU'
    search_main(input_sheet, output_sheet)
    return redirect('/')

if __name__ == '__main__':
    app.run()
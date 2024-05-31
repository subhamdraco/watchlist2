import json, gspread

def main():
    f = open('data.json')
    files = json.load(f)
    final_data_future = []
    final_data_recent = []
    # stock_ = []
    # index_ = []
    gc = gspread.service_account(filename='keys.json')
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    global_index = gc.open_by_url(config['global_index'])
    worksheet = global_index.worksheet('Supporting Data')
    stock_list = worksheet.get('C4:C2000')
    stock_list = [
        s[0].replace('.SZ', '').replace('.SS', '').replace('.SI', '').replace('.T', '').replace('.PA', '').replace(
            '.BR', '').replace('.JK', '').replace('.F', '') for s in stock_list]
    row = 4
    worksheet.batch_clear([f'AB{row}:AB2000', f'AC{row}:AC2000'])
    for stock_data in files:
        try:
            row = stock_list.index(stock_data['stock']) + 4
        except:
            continue
        final_data_future.append({'range': f'AB{row}', 'values': [[stock_data['future_date']]]})
        final_data_recent.append({'range': f'AC{row}', 'values': [[stock_data['recent_date']]]})
        row += 1

    worksheet.batch_update(final_data_future)
    worksheet.batch_update(final_data_recent)

task = True
while task:
    try:
        main()
        task = False
    except:
        continue
# worksheet.batch_update(stock_)
# worksheet.batch_update(index_)

from service import connect_worksheet, transform_data


def main_func():
    data = transform_data()
    sheet = connect_worksheet('Theo dõi công nợ KT')
    sheet.clear()
    sheet.update([data.columns.values.tolist()] + data.values.tolist())
    print('success')


if __name__ == '__main__':
    main_func()

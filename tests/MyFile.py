from annotate import annotate

@annotate('before')
def load_data(context):
    return "A"

@annotate('before')
def load_data1(context):
    return "V"

@annotate('before')
def load_data2(context):
    return "V"

@annotate('before')
def load_data3(context):
    return "FG"

@annotate('after')
def write_data(context):
    return context

@annotate('factor', rule=1, order='desc')
def price_book(ticker, date, context):
    print(ticker+date)
    return {}
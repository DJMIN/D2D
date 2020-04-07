import pandas


gegu = pandas.read_csv(open(r'C:\Users\Chen-PC\Desktop\ssclient\peTTM_sz.300032_data.csv', 'r'))
dapan = pandas.read_csv(open(r'C:\Users\Chen-PC\Desktop\ssclient\peTTM_sh.000300_data.csv', 'r'))

res = []

for line in gegu['date']:
    print(
        line,
        float(gegu[gegu['date'] == line]['close']),
        float(dapan[dapan['date'] == line]['close'])
        )

from utils.db import ElasticSearchD, MySqlD, CsvD, JsonListD, XlsIbyFileD, XlsxIbyFileD, MongoDBD
from task import Migration

mysqlm = MySqlD(host='localhost', port=3306, database='gu',
                 # user='root', passwd='root')
                 user='debian-sys-maint', passwd='BjOtjlf6bDqypoH1')
t = Migration(
    database_from=MySqlD(host='localhost', port=3306, database='gu',
                 # user='root', passwd='root')
                 user='debian-sys-maint', passwd='BjOtjlf6bDqypoH1'),

    database_to=XlsxIbyFileD(path='dataa'),
    # database_to=MySqlD(host='localhost', port=3306, database='gu',
    #              # user='root', passwd='root')
    #              user='debian-sys-maint', passwd='BjOtjlf6bDqypoH1'),
    table_from='SELECT * FROM `gu` where car21 > 50 and car21 <200 and time_date_int > 1373904000',
    table_to='gu',

    # database_to=CsvD(path='./dataa'),
    # table_from='select uuid, tag, priority, value from task where tasktype!=1',
    # table_to='url_tg',
)

def format_gu(d):
    mysqlm.get_data('SELECT * FROM `gu` where car21 > 50 and car21 <200 and time_date_int > 1373904000')
t.run()

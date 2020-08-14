from utils.db import ElasticSearchD, MySqlD, CsvD, JsonListD, XlsIbyFileD, XlsxIbyFileD, MongoDBD
from task import Migration

t = Migration(
    database_from=ElasticSearchD(hosts=["192.168.0.213:9200", "192.168.0.212:9200", "192.168.0.211:9200", ]),
    database_to=CsvD(path='tgdata'),
    table_from=('tgusers_re', {
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                {
                                    "term": {
                                        "last_name": ""
                                    }
                                },
                                {
                                    "bool": {
                                        "must_not": [
                                            {
                                                "exists": {
                                                    "field": "last_name"
                                                }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    },
                    {
                        "bool": {
                            "should": [
                                {
                                    "term": {
                                        "first_name": ""
                                    }
                                },
                                {
                                    "bool": {
                                        "must_not": [
                                            {
                                                "exists": {
                                                    "field": "first_name"
                                                }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    },
                    {
                        "wildcard": {
                            "phone": {
                                "value": "*"
                            }
                        }
                    }
                ]
            }
        },
        "_source": {"includes": ["phone"],
                    "excludes": []},

    }),
    table_to='导出tguser姓名缺失数据',

    # database_to=CsvD(path='./dataa'),
    # table_from='select uuid, tag, priority, value from task where tasktype!=1',
    # table_to='url_tg',
)


def formatd(s):

    s = {'phone':s['phone']}
    return s


t.format_data = formatd

t.run()

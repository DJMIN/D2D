from d22d import Migration, CsvD


def tet5():
    """
    同上test1 Migration 可以全量迁移单表数据，不指定表名可以全量迁移整个数据库

    将本地csv文件./data/user1全量保存到本地csv文件./data/user2。同时修改部分字段

    ./data/user1.csv
    user_id sex
    a111    1

    ./data/user2.csv
    uuid sex time       tag
    a111 1   1638410972 xs

    """
    import time
    t = Migration(
        database_from=CsvD(path='./data'),
        database_to=CsvD(path='./data1'),
        table_from='user',
        table_to='user2'
    )

    def self_format(data):
        data['uuid'] = data.pop('userid')
        data['time'] = int(time.time())
        data['tag'] = 'xs'
        return data

    t.format_data = self_format
    t.run()


if __name__ == '__main__':
    tet5()
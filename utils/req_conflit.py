# /usr/bin/env python
# coding: utf-8


all_line = set([])


def req_conflit(path):
    with open(path, 'r', encoding='utf8') as rf:
        with open(path + '1', 'w', encoding='utf8') as wf:
            for line in rf:
                if line not in all_line:
                    wf.write(line)
                    all_line.add(line)


def main():
    setting = r'C:\Users\Chen\PycharmProjects\D2D\requirements.txt'
    data = req_conflit(setting)
    print(data)


if __name__ == '__main__':
    main()

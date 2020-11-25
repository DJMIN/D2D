# /usr/bin/env python
# coding: utf-8


all_line = set([])
all_line_3 = set([])
all_line__ = set([])
all_line_base = set([])


def req_conflit(path):
    with open(path, 'r', encoding='utf8') as rf, \
            open(path + '.txt', 'w', encoding='utf8') as wf, \
            open(path.rsplit('/', maxsplit=1)[0] + '/setup.new.txt', 'w', encoding='utf8') as wsf:
        for line in rf:
            line = line.strip()
            if not line.strip():
                continue
            if line.startswith('#'):
                all_line_3.add(line)
            else:
                if line.find('=') > -1:
                    if line.find('>') > -1:
                        all_line__.add(line)
                        all_line.add(line.split('>')[0])
                    elif line.find('<') > -1:
                        all_line__.add(line)
                        all_line.add(line.split('<')[0])
                    else:
                        all_line__.add(line)
                        all_line.add(line.split('=')[0])
                elif line not in all_line:
                    all_line.add(line)
                    all_line_base.add(line)
        for line in all_line_base:
            wf.write('''{}\n'''.format(line.replace('\n', '')))
            wsf.write('''"{}",\n'''.format(line.replace('\n', '')))
        for line in all_line__:
            wf.write('''{}\n'''.format(line.replace('\n', '')))
            wsf.write('''"{}",\n'''.format(line.replace('\n', '')))
        for line in all_line_3:
            wf.write('''{}\n'''.format(line.replace('\n', '')))


def main():
    setting = r'../requirements.txt'
    req_conflit(setting)


if __name__ == '__main__':
    main()

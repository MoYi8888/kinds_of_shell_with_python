import json
import re
import sys
import traceback
from java.nio.charset import StandardCharsets
from org.apache.commons.io import IOUtils
from org.apache.nifi.processor.io import InputStreamCallback, OutputStreamCallback
from org.python.core.util import StringUtil


class WriteCallback(OutputStreamCallback):
    def __init__(self):
        self.content = None
        self.charset = StandardCharsets.UTF_8

    def process(self, outputStream):
        bytes = bytearray(self.content.encode('utf-8'))
        outputStream.write(bytes)


class SplitCallback(InputStreamCallback):
    def __init__(self):
        self.parentFlowFile = None

    def get_relate_tab_name(self, tab_name, all_tab_fore_tab):
        # 递归获取层级表
        name_list = []
        if len(all_tab_fore_tab[tab_name]) == 0:
            return [tab_name]
        for x in all_tab_fore_tab[tab_name]:
            tab_name_list = self.get_relate_tab_name(x, all_tab_fore_tab)
            name_list.append(x)
            name_list = name_list + tab_name_list
        return name_list

    def process(self, inputStream):
        splits = []

        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)

        pattern = re.compile('{.*?}')
        res_list = re.findall(pattern, text)
        tab_name_list = set()
        all_fore_tab = {}
        order_create_list = []

        for x in res_list:
            data = json.loads(x)
            if data['TABLE_NAME'] not in all_fore_tab.keys():
                all_fore_tab[data['TABLE_NAME']] = []

            if data['REFERENCED_TABLE_NAME']:  # 存在外键

                all_fore_tab[data['TABLE_NAME']].append(data['REFERENCED_TABLE_NAME'])

                if data['REFERENCED_TABLE_NAME'] not in all_fore_tab.keys():
                    all_fore_tab[data['REFERENCED_TABLE_NAME']] = []
                    tab_name_list.add(data['REFERENCED_TABLE_NAME'])

            tab_name_list.add(data['TABLE_NAME'])

        for tab_name in tab_name_list:
            res = self.get_relate_tab_name(tab_name, all_fore_tab)
            new_res = sorted(set(res), key=res.index)
            if len(new_res) != 1 or (len(new_res) == 1 and tab_name != new_res[0]):
                for x in new_res[::-1]:
                    if x not in order_create_list:
                        order_create_list.append(x)
                if tab_name not in order_create_list:
                    order_create_list.append(tab_name)
            else:
                if tab_name not in order_create_list:
                    order_create_list.append(tab_name)

        # index=len(order_create_list)
        index = 1
        for x in order_create_list:
            splitFlowFile = session.create(self.parentFlowFile)
            writeCallback = WriteCallback()
            xx = {'tab_name': x, 'all_tab': len(tab_name_list), 'order': ','.join(order_create_list[::-1])}
            writeCallback.content = json.dumps(xx)

            splitFlowFile = session.write(splitFlowFile, writeCallback)
            splitFlowFile = session.putAllAttributes(splitFlowFile, {
                'tab_name': x.upper(), 'priority': str(index), 'filename': x.upper()
            })
            splits.append(splitFlowFile)
            index += 1
            # index-=1

        for splitFlowFile in splits:
            session.transfer(splitFlowFile, REL_SUCCESS)

        # except:
        #     traceback.print_exc(file=sys.stdout)
        #     raise


parentFlowFile = session.get()

if parentFlowFile != None:
    splitCallback = SplitCallback()
    splitCallback.parentFlowFile = parentFlowFile
    session.read(parentFlowFile, splitCallback)
    session.remove(parentFlowFile)
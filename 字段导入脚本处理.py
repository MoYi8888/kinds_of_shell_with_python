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

    def process(self, outputStream):
        bytes = self.content
        outputStream.write(bytes)


class SplitCallback(InputStreamCallback):
    def __init__(self):
        self.parentFlowFile = None

    def process(self, inputStream):
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        data = json.loads(text)
        res = ""
        if isinstance(data, list):
            for x in data[:-1]:
                try:
                    column_name = x['column_name']
                except Exception as e:
                    column_name = x['COLUMN_NAME']
                res = res + column_name + ','
            try:
                res = res + data[-1]['column_name']
            except Exception as e:
                res = res + data[-1]['COLUMN_NAME']
        else:
            try:
                res = res + data['column_name']
            except Exception as e:
                res = res + data['COLUMN_NAME']

        # 目的表非空字段不在源表的与源表字段对应关系,用于提取共同字段替换
        # 外部key:目的表名，内部key:目的表的非空字段，内部value:源表对应得字段
        all_tab_match_col = {"by_shb_bk_copy": {"SHFNM": "SHFZHH"}}

        tab_name = self.parentFlowFile.getAttribute('tab_name')
        # tab_name='by_shb'
        # key源表名，value目的表名，源表名与目的表名对应表
        update_tab_name = {"by_shb": "by_shb_bk_copy"}

        splitFlowFile = session.create(self.parentFlowFile)
        if tab_name in update_tab_name.keys():
            splitFlowFile = session.putAttribute(splitFlowFile, 'start_tab_name', tab_name)
            splitFlowFile = session.putAttribute(splitFlowFile, 'end_tab_name', update_tab_name[tab_name])
        else:
            splitFlowFile = session.putAttribute(splitFlowFile, 'start_tab_name', tab_name)
            splitFlowFile = session.putAttribute(splitFlowFile, 'end_tab_name', tab_name)

        writeCallback = WriteCallback()
        writeCallback.content = text
        splitFlowFile = session.write(splitFlowFile, writeCallback)
        res.lower()

        splitFlowFile = session.putAttribute(splitFlowFile, 'start_tab_columns', res)
        splitFlowFile = session.putAttribute(splitFlowFile, 'all_tab_match_col',
                                             json.dumps(all_tab_match_col, ensure_ascii=False))
        session.transfer(splitFlowFile, REL_SUCCESS)


parentFlowFile = session.get()

if parentFlowFile != None:
    splitCallback = SplitCallback()
    splitCallback.parentFlowFile = parentFlowFile
    session.read(parentFlowFile, splitCallback)
    session.remove(parentFlowFile)
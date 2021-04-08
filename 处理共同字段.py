import json
import re
import sys
import traceback
import copy
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
        # text= IOUtils.toString(inputStream)
        # pattern = re.compile('{.*?}')
        # res_list = re.findall(pattern, text)
        # res_list  = literal_eval(text)
        res_list = json.loads(text)
        if isinstance(res_list, dict):
            res_list = [res_list]
        end_tab_column = set()
        not_null_field = set()
        start_tab_column_str = self.parentFlowFile.getAttribute('start_tab_columns')
        all_tab_match_col_str = self.parentFlowFile.getAttribute('all_tab_match_col')
        all_tab_match_col = json.loads(all_tab_match_col_str)
        # all_tab_match_col={"by_shb_bk":{"SHFNM":"SHFZHH"}}
        start_tab_column = set(start_tab_column_str.split(','))

        for x in res_list:
            # x=json.loads(x)
            if x:
                end_tab_column.add(x['column_name'])
                if x['is_nullable'] == 'NO':
                    not_null_field.add(x['column_name'])

        # 共同字段
        common_column = end_tab_column & start_tab_column
        # 目的表不在源表的非空字段
        not_null_not_in_start = not_null_field - common_column
        # 不在原始表的字段
        not_in_start = end_tab_column - start_tab_column

        end_tab_name = self.parentFlowFile.getAttribute('end_tab_name')
        if end_tab_name in all_tab_match_col.keys():
            match_cols_dict = all_tab_match_col[end_tab_name]
        else:
            match_cols_dict = {}

        # 目的字段对应得源字段
        res_dict = dict()  # KEY:目的表非空字段，value:源表对应字段
        if len(not_null_not_in_start) != 0:
            for x in not_null_not_in_start:
                if x in match_cols_dict.keys():
                    res_dict[x] = match_cols_dict[x]
                    not_in_start.remove(x)  # 对应后从不在源表中的移除
                    not_null_not_in_start.remove(x)  # 对应后从非空字段中移除

        common_column_str = (',').join(list(common_column))
        for end_col, start_col in res_dict.items():
            common_column_str = common_column_str + ',' + start_col + ' as ' + end_col

        not_in_start_str = (',').join(list(not_in_start))
        content_json = {'common': common_column_str, 'not_in_start': not_in_start_str,
                        'not_null_not_in_start': (',').join(list(not_null_not_in_start))}

        if len(common_column_str) != 0 and len(not_null_not_in_start) == 0:
            content_json['tab_in_end'] = 'yes'

        splitFlowFile = session.create(self.parentFlowFile)
        writeCallback = WriteCallback()
        writeCallback.content = json.dumps(content_json)
        splitFlowFile = session.write(splitFlowFile, writeCallback)
        splitFlowFile = session.putAllAttributes(splitFlowFile, {'common_column': common_column_str,
                                                                 'end_tab_columns': (',').join(list(end_tab_column)),
                                                                 'match_cols_dict': json.dumps(match_cols_dict)})
        session.transfer(splitFlowFile, REL_SUCCESS)


parentFlowFile = session.get()

if parentFlowFile != None:
    splitCallback = SplitCallback()
    splitCallback.parentFlowFile = parentFlowFile
    session.read(parentFlowFile, splitCallback)
    session.remove(parentFlowFile)
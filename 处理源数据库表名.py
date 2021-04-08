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
        splits = []
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        data = json.loads(text)

        for x in data:
            splitFlowFile = session.create(self.parentFlowFile)
            writeCallback = WriteCallback()
            try:
                table_name = x['table_name']
            except Exception as e:
                table_name = x['TABLE_NAME']
            writeCallback.content = table_name
            splitFlowFile = session.write(splitFlowFile, writeCallback)
            splitFlowFile = session.putAllAttributes(splitFlowFile, {'tab_name': table_name, 'filename': table_name})
            session.transfer(splitFlowFile, REL_SUCCESS)


parentFlowFile = session.get()

if parentFlowFile != None:
    splitCallback = SplitCallback()
    splitCallback.parentFlowFile = parentFlowFile
    session.read(parentFlowFile, splitCallback)
    session.remove(parentFlowFile)
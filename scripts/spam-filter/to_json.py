#!/usr/bin/python3

import sys
import json
import uuid

from os import listdir
from os.path import isfile, join

path   = sys.argv[1]
output = open(sys.argv[2], 'w')

files = [ f for f in listdir(path) if isfile(join(path,f)) ]

total = len(files)
count = 0

print('Number of files:', total)

for f in files:
    if count % 1000 == 0:
        print('Read', count, 'of', total)
        
    content = ' '.join([line.strip() for line in open(join(path,f), encoding="ISO-8859-1")])
    encoded = json.dumps({'id': str(uuid.uuid4()), 'message': content})
    
    output.write(encoded + '\n')
    count += 1
    
output.close()
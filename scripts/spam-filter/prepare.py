#!/usr/bin/python

import os
import sys
import shutil

input_dir  = sys.argv[1]
output_dir = sys.argv[2]

# traverse root directory, and list directories as dirs and files as files
for root, dirs, files in os.walk(input_dir):
    path = root.split('/')
    
    for file in files:
        print os.path.join(root, file)
        shutil.copy2(os.path.join(root, file), os.path.join(output_dir, '_'.join(path[2:]) + file))
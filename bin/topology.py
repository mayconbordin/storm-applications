#! /usr/bin/python

import urllib
import json
import time

from tabulate import tabulate
from datetime import datetime

def get_json(url):
    response = urllib.urlopen(url);
    return json.loads(response.read())

def print_summary(data):
    info = []
    for spout in data['spouts']:
        info.append([spout['spoutId'], spout['emitted'], spout['acked']])
    for bolt in data['bolts']:
        info.append([bolt['boltId'], bolt['emitted'], bolt['acked']])
        
    print "Summary at", datetime.now().isoformat(), ":\n"
    print tabulate(info, headers=["component_id", "emitted", "acked"]) + "\n"
    
def update_spout_info(data):
    spout_info = {'emitted': 0, 'acked': 0}
    
    for spout in data['spouts']:
        if spout['emitted'] is not None:
            spout_info['emitted'] += int(spout['emitted'])
        if spout['acked'] is not None:
            spout_info['acked'] += int(spout['acked'])
        
    return spout_info
    
def has_finished(curr_info, new_info):
    return (new_info['emitted'] == curr_info['emitted'] and 
            new_info['acked'] == curr_info['acked'])

def is_beginning(data):
    for spout in data['spouts']:
        if spout['emitted'] is None:
            return True
    return False

def get_topology_id(name, host):
    data = get_json(host + "/api/v1/topology/summary")
    topology = get_topology(data, name)
    
    if topology is not None:
        return topology['id']
    else:
        return None

def get_topology(data, name):
    if name is not None:
        for topology in data['topologies']:
            if topology['name'] == name:
                return topology
    elif len(data['topologies']) > 0:
        return data['topologies'][-1]
    else:
        return None

def topology_progress(name, host, interval):
    data = get_json(host + "/api/v1/topology/summary")
    topology = get_topology(data, name)
    
    if topology is not None:
        id = topology['id']

        spout_info = {'emitted': 0, 'acked': 0}

        while True:
            data = get_json(host + "/api/v1/topology/" + id)

            if is_beginning(data):
                continue

            print_summary(data)
            new_info = update_spout_info(data)

            if has_finished(spout_info, new_info):
                print "Topology seems to have finished ("+str(interval)+"s of inactivity)."
                break

            spout_info = new_info
            time.sleep(interval)
    
if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(description='Get topology status')
    parser.add_argument('command', choices=['progress', 'get_id'])
    parser.add_argument('--name', required=True)
    parser.add_argument('--host', default='http://localhost:8080')
    parser.add_argument('--interval', default=8, type=int)

    args = parser.parse_args()

    if args.command == 'progress':
        topology_progress(args.name, args.host, args.interval)
    elif args.command == 'get_id':
        print get_topology_id(args.name, args.host)
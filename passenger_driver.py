import json
import csv
from queue import PriorityQueue
from datetime import datetime
import math
import os

from min_distance import build_kdtree, closest_point

class Passenger:
    def __init__(self, date_time, source_lat, source_lon, dest_lat, dest_lon, source_node, dest_node):
        self.request_date_time = date_time
        self.source_lat = source_lat
        self.source_lon = source_lon
        self.dest_lat = dest_lat
        self.dest_lon = dest_lon
        self.source_node = source_node
        self.dest_node = dest_node

        self.date_time = date_time

    def __lt__(self, other):
        return self.date_time < other.date_time

class Driver:
    def __init__(self, date_time, source_lat, source_lon, node):
        self.date_time = date_time
        self.source_lat = source_lat
        self.source_lon = source_lon
        self.node = node
        self.profit = 0
        self.rides = 0

    def __lt__(self, other):
        return self.date_time < other.date_time

def load_nodes():
    with open(os.path.join('data', 'node_data.json'), 'r') as file:
        node_data = json.load(file)
    return node_data

def load_nodes_t4():
    data = load_nodes()
    node_data = []

    for entry in data:
        new_entry = [entry, [data[entry]['lat'], data[entry]['lon']]]
        node_data.append(new_entry)

    return node_data

def find_closest_node(node_data, p_lat, p_lon):
    # Load node data from the JSON file
    min_dist = math.inf
    best_node = None
    # Iterate through each node
    for node_id, coordinates in node_data.items():
        lon = coordinates['lon']
        lat = coordinates['lat']
        d = math.dist((lon, lat), (p_lon, p_lat))
        if d < min_dist:
            min_dist = d
            best_node = node_id
    return int(best_node)

# Function to read passengers from CSV and put them in a priority queue
def read_passengers(nodes):
    # Passengers with the earliest date_time have the highest priority
    passenger_queue = PriorityQueue()
    with open(os.path.join('data', 'passengers.csv'), 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            date_time = datetime.strptime(row['Date/Time'], '%m/%d/%Y %H:%M:%S')
            source_lat = float(row['Source Lat'])
            source_lon = float(row['Source Lon'])
            dest_lat = float(row['Dest Lat'])
            dest_lon = float(row['Dest Lon'])
            source_node = find_closest_node(nodes, source_lat, source_lon)
            dest_node = find_closest_node(nodes, dest_lat, dest_lon)
            passenger = Passenger(date_time, source_lat, source_lon, dest_lat, dest_lon, source_node, dest_node)
            passenger_queue.put(passenger)
    return passenger_queue


# Function to read drivers from CSV and put them in a priority queue
def read_drivers(nodes):
    # Drivers with the earliest date_time have the highest priority
    driver_queue = PriorityQueue()
    with open(os.path.join('data', 'drivers.csv'), 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            date_time = datetime.strptime(row['Date/Time'], '%m/%d/%Y %H:%M:%S')
            source_lat = float(row['Source Lat'])
            source_lon = float(row['Source Lon'])
            s_node = find_closest_node(nodes, source_lat, source_lon)
            driver = Driver(date_time, source_lat, source_lon, s_node)
            driver_queue.put(driver)

    return driver_queue

def read_passengers_t4(nodes, node_tree):
    # Passengers with the earliest date_time have the highest priority
    passenger_queue = PriorityQueue()
    with open(os.path.join('data', 'passengers.csv'), 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            date_time = datetime.strptime(row['Date/Time'], '%m/%d/%Y %H:%M:%S')
            source_lat = float(row['Source Lat'])
            source_lon = float(row['Source Lon'])
            dest_lat = float(row['Dest Lat'])
            dest_lon = float(row['Dest Lon'])
            source_node = closest_point(node_tree, source_lat, source_lon)
            dest_node = closest_point(node_tree, dest_lat, dest_lon)
            passenger = Passenger(date_time, source_lat, source_lon, dest_lat, dest_lon, int(source_node[0]), int(dest_node[0]))
            passenger_queue.put(passenger)
    return passenger_queue


# Function to read drivers from CSV and put them in a priority queue
def read_drivers_t4(nodes, node_tree):
    # Drivers with the earliest date_time have the highest priority
    driver_queue = PriorityQueue()
    with open(os.path.join('data', 'drivers.csv'), 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            date_time = datetime.strptime(row['Date/Time'], '%m/%d/%Y %H:%M:%S')
            source_lat = float(row['Source Lat'])
            source_lon = float(row['Source Lon'])
            s_node = closest_point(node_tree, source_lat, source_lon)
            driver = Driver(date_time, source_lat, source_lon, int(s_node[0]))
            driver_queue.put(driver)

    return driver_queue

# Testing: "42860618": {"lon": -73.9209487, "lat": 40.7413716}
# find_closest_node(nodes, 40.7413716,  -73.9209488)
def passenger_driver_queues():
    # Returns priority queues for passengers and drivers ordered by datetime
    nodes = load_nodes()
    driver_q = read_drivers(nodes)
    passenger_q = read_passengers(nodes)
    return driver_q, passenger_q

def passenger_driver_queues_t4():
    # Returns priority queues for passengers and drivers ordered by datetime
    nodes = load_nodes_t4()
    node_tree = build_kdtree(nodes)

    driver_q = read_drivers_t4(nodes, node_tree)
    passenger_q = read_passengers_t4(nodes, node_tree)
    return driver_q, passenger_q


# if __name__ == "__main__":
#   passenger_driver_queues_t4()
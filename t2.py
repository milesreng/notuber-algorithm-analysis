import os
import csv
import time
import math
from queue import Queue
from datetime import timedelta
from random import random
import csv

from buildgraph import build_graph
from passenger_driver import passenger_driver_queues
from djikstra import travel_times
from probability_distributions import exponential

def calculate_distance(lat1, lon1, lat2, lon2):
    return math.sqrt((lat2 - lat1) ** 2 + (lon2 - lon1) ** 2)

def T2():
    start_time = time.time()

    driver_Q, passenger_Q = passenger_driver_queues()

    queue_time = time.time() - start_time

    weekdays, weekends = build_graph()

    build_graph_time = time.time() - start_time

    # this tracks whether the current passenger still needs to be matched
    passenger_is_unmatched = False

    filename = "results/t2rides.csv"

    with open(filename, 'w') as file:
        writer = csv.writer(file)

        writer.writerow(["passenger_start_datetime", "ride_match_datetime", "driver_to_passenger", "passenger_to_dest", "match_runtime_sec"])

        start_match_time = time.time()
        time_to_match = 0

        while not passenger_Q.empty():

            # if we matched the last passenger we get hte next one, otherwise we still use the last one
            if not passenger_is_unmatched:
                curr_passenger = passenger_Q.get()
            
            closest_driver = None
            min_distance = float('inf')

            temp_driver_Q = Queue()

            while not driver_Q.empty():
                curr_driver = driver_Q.get()
                if curr_driver.date_time <= curr_passenger.date_time:
                    distance = calculate_distance(curr_passenger.source_lat, curr_passenger.source_lon, curr_driver.source_lat, curr_driver.source_lon)
                    if distance < min_distance:
                        if closest_driver is not None:
                            temp_driver_Q.put(closest_driver)
                        closest_driver = curr_driver
                        min_distance = distance
                    else:
                        temp_driver_Q.put(curr_driver)
                else:
                    temp_driver_Q.put(curr_driver)
            
            while not temp_driver_Q.empty():
                driver_Q.put(temp_driver_Q.get())

            if closest_driver:
                print(f"Passenger requesting at {curr_passenger.request_date_time} is matched with driver at {curr_passenger.date_time} with distance {min_distance}")
            
                # get traffic data
                hour = curr_passenger.date_time.hour
                graph = weekdays[hour] if curr_passenger.date_time.weekday() < 5 else weekends[hour]

                # run dijkstra's to get driver to passenger, then passenger to destination
                driver_to_passenger = travel_times(graph, closest_driver.node, curr_passenger.source_node)
                passenger_to_dest = travel_times(graph, curr_passenger.source_node, curr_passenger.dest_node)
                
                elapsed_time = driver_to_passenger + passenger_to_dest

                # print(f"Wait time: {wait_time + driver_to_passenger} hours")
                # print(f"Ride time: {passenger_to_dest} hours")

                # update driver attributes
                closest_driver.node = curr_passenger.dest_node
                closest_driver.date_time = curr_passenger.date_time + timedelta(hours = elapsed_time)
                closest_driver.profit = closest_driver.profit + passenger_to_dest - driver_to_passenger
                closest_driver.source_lat = curr_passenger.dest_lat
                closest_driver.source_lon = curr_passenger.dest_lon
                closest_driver.rides = closest_driver.rides + 1

                # put driver back in queue

                if random() > 0.075 or driver_Q.qsize() < 100:
                    driver_Q.put(closest_driver)                  

                # we matched the passenger, so we should grab the next one from the queue
                passenger_is_unmatched = False

                time_to_match = time.time() - start_match_time
                start_match_time = time.time()

                writer.writerow([curr_passenger.request_date_time, curr_passenger.date_time, driver_to_passenger, passenger_to_dest, time_to_match])

            else:
                # if there are not available drivers, then iterate the date and time of the passenger and put them back in the queue
                curr_passenger.date_time = curr_passenger.date_time + timedelta(minutes = 1)

                # we will use this passenger again rather than grab a new one
                passenger_is_unmatched = True

    total_runtime = time.time() - start_time
    print(f"total runtime: {total_runtime}")
    print(f"queue build runtime: {queue_time}")
    print(f"build_graph runtime: {build_graph_time}")

    # print('Wait average:' + str(sum(wait_times)/len(wait_times)) + ' hours')
    # print('Ride average:' + str(sum(ride_times)/len(ride_times)) + ' hours')
    # print('Total average:' + str(sum(total_times)/len(total_times)) + ' hours')


if __name__ == "__main__":

    T2()

#T1 baseline queue
#First come first serve
import time
import csv
from random import random
from queue import PriorityQueue
from datetime import datetime, timedelta
from passenger_driver import passenger_driver_queues
from buildgraph import build_graph
from djikstra import travel_times

#after driver pickup have to use node and road information from adjacency, edges, and node_data to get time
def T1():
    start_time = time.time()
    total_wait_time = timedelta()
    total_drive_time = timedelta()

    driver_queue, passenger_queue = passenger_driver_queues()

    queue_time = time.time() - start_time
    #update graph based on driver time
    weekdays, weekends = build_graph() ## -- assume build_graph returns list of all graphs for every hour
    #all_graphs[(0,1)][hour] where 0 = weekday and 1 = weekend

    #test counter
    passenger_count = 0
    build_graph_time = time.time() - start_time
    filename = "results/t1rides.csv"

    with open(filename, 'w') as file:
      writer = csv.writer(file)

      writer.writerow(["passenger_start_datetime", "ride_match_datetime", "driver_to_passenger", "passenger_to_dest", "match_runtime_sec"])

      start_match_time = time.time()
      time_to_match = 0

      while not passenger_queue.empty() and not driver_queue.empty():
         # Get the current passenger and driver
         curr_passenger = passenger_queue.get()
         curr_driver = driver_queue.get()
         
         #if passenger request before driver is ready
         if curr_passenger.date_time < curr_driver.date_time:
               curr_passenger.date_time = curr_driver.date_time

         #Pickup time 
         # Get the weekday as an integer (Monday is 0, Sunday is 6)
         hour = curr_passenger.date_time.hour
         graph = weekdays[hour] if curr_passenger.date_time.weekday() < 5 else weekends[hour]

         # run dijkstra's to get driver to passenger, then passenger to destination
         driver_to_passenger = travel_times(graph, curr_driver.node, curr_passenger.source_node)
         passenger_to_dest = travel_times(graph, curr_passenger.source_node, curr_passenger.dest_node)

         elapsed_time = driver_to_passenger + passenger_to_dest

         writer.writerow([curr_passenger.request_date_time, curr_passenger.date_time, driver_to_passenger, passenger_to_dest])
         print(f"Passenger requesting at {curr_passenger.request_date_time} is matched with driver at {curr_passenger.date_time} with distance {driver_to_passenger}")
         
         # update driver attributes
         curr_driver.node = curr_passenger.dest_node
         curr_driver.date_time = curr_passenger.date_time + timedelta(hours = elapsed_time)
         curr_driver.profit = curr_driver.profit + passenger_to_dest - driver_to_passenger
         curr_driver.source_lat = curr_passenger.dest_lat
         curr_driver.source_lon = curr_passenger.dest_lon
         curr_driver.rides = curr_driver.rides + 1

         if random() > 0.075 or driver_queue.qsize() < 100:
            driver_queue.put(curr_driver)
            
         passenger_count += 1

         time_to_match = time.time() - start_match_time
         start_match_time = time.time()

         writer.writerow([curr_passenger.request_date_time, curr_passenger.date_time, driver_to_passenger, passenger_to_dest, time_to_match])

    total_runtime = time.time() - start_time

    print(f"total runtime: {total_runtime}")
    print(f"queue build time: {queue_time}")
    print(f"build_graph runtime: {build_graph_time}")

if __name__ == "__main__":
  T1()
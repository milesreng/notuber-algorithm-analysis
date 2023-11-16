import math
import json
import csv
import os

def load_node_data(file_path):
    with open(file_path, 'r') as node_file:
        return json.load(node_file)

def load_edge_data(file_path):
  with open(file_path, 'r') as edges_file:
    edges_reader = csv.reader(edges_file)
    next(edges_reader)  # Skip header row
    road_segments = {}
        
    for row in edges_reader:
      start_id, end_id = map(int, row[:2])
      length = float(row[2])
      weekday_speeds = list(map(float, row[3:27]))
      weekend_speeds = list(map(float, row[27:]))

      weekday_times = []

      weekend_times = []

      for i in range(24):
        weekday_times.append(length / weekday_speeds[i])
        weekend_times.append(length / weekend_speeds[i])
            
      road_segments[(start_id, end_id)] = {
                'length': length,
                'weekday_times': weekday_times,
                'weekend_times': weekend_times,
                'weekday_speeds':weekday_speeds,
                'weekend_speeds':weekend_speeds
            }
      
  return road_segments

def build_graph():

  nodes = load_node_data(os.path.join("data", "node_data.json"))

  road_segments = load_edge_data(os.path.join("data", "edges.csv"))
  
  weekdays = []
  weekends = []

  for i in range(24):
    weekdays.append({})
    weekends.append({})

  for segment in road_segments:
    for i in range(24):
      if segment[0] not in weekdays[i]:
        lat = nodes[str(segment[0])]['lat']
        lon = nodes[str(segment[0])]['lon']
        weekdays[i][segment[0]] = ((lat, lon), [])
        weekends[i][segment[0]] = ((lat, lon, road_segments.get(segment)['weekday_speeds'][i]), [])

      weekdays[i][segment[0]][1].append((segment[1], road_segments.get(segment)['weekday_times'][i], 
                                         road_segments.get(segment)['weekday_speeds'][i]))
      
      if segment[0] not in weekends[i]:
        weekends[i][segment[0]] = []
      
      weekends[i][segment[0]][1].append((segment[1], road_segments.get(segment)['weekend_times'][i],
                                         road_segments.get(segment)['weekend_speeds'][i]))

  return weekdays, weekends

  # print(weekdays[0][39076461])

if __name__ == "__main__":
  build_graph()
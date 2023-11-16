import heapq

def travel_times(graph, start, end):
    # Initialize distances and predecessors
    distances = {node: float('infinity') for node in graph}
    predecessors = {node: None for node in graph}
    distances[start] = 0
    predecessors[start] = start
    # Priority queue to keep track of nodes with their tentative distances
    priority_queue = [(0, start)]

    while priority_queue:
        current_distance, current_node = heapq.heappop(priority_queue)

        # If the current node has been visited with a shorter distance, skip
        if current_distance > distances[current_node]:
            continue

        # Explore neighbors of the current node
        for neighbor, travel_time in graph[current_node][1]:
            distance = current_distance + travel_time

            # If a shorter path is found, update distance and predecessor
            if distance < distances[neighbor]:
                distances[neighbor] = distance
                predecessors[neighbor] = current_node
                heapq.heappush(priority_queue, (distance, neighbor))

    total_travel_time = distances[end]

    return total_travel_time

def travel_times_and_path(graph, start, end):
    # Initialize distances and predecessors
    distances = {node: float('infinity') for node in graph}
    predecessors = {node: None for node in graph}
    distances[start] = 0

    # Priority queue to keep track of nodes with their tentative distances
    priority_queue = [(0, start)]

    while priority_queue:
        current_distance, current_node = heapq.heappop(priority_queue)

        # If the current node has been visited with a shorter distance, skip
        if current_distance > distances[current_node]:
            continue

        # Explore neighbors of the current node
        for neighbor, travel_time in graph[current_node][1]:
            distance = current_distance + travel_time

            # If a shorter path is found, update distance and predecessor
            if distance < distances[neighbor]:
                distances[neighbor] = distance
                predecessors[neighbor] = current_node
                heapq.heappush(priority_queue, (distance, neighbor))

    # Reconstruct the path from start to end
    path = []
    current_node = end
    while current_node is not None:
        path.insert(0, current_node)
        current_node = predecessors[current_node]

    total_travel_time = distances[end]

    return path, total_travel_time

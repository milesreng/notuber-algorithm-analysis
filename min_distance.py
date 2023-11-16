import math

class Node:
  def __init__(self, point, id=None, left=None, right=None):
    self.point = point
    self.node_id = id
    self.left = left
    self.right = right

def calculate_distance(lat1, lon1, best):
   return math.sqrt((best[1][1] - lat1) ** 2 + (best[1][0] - lon1) ** 2)

def build_kdtree(points, depth = 0):
  if not points:
    return None
  
  axis = depth % 2

  sorted_points = sorted(points, key=lambda x : x[1][axis])
  median = len(sorted_points) // 2

  return Node(
        point=sorted_points[median],
        left=build_kdtree(sorted_points[:median], depth + 1),
        right=build_kdtree(sorted_points[median + 1:], depth + 1)
    )

def closest_point(root, t_lat, t_lon, depth=0, best=None):
    if root is None:
        return best

    axis = depth % 2

    next_best = None
    next_branch = None

    if best is None or calculate_distance(t_lat, t_lon, best) < calculate_distance(t_lat, t_lon, root.point):
        next_best = root.point
    else:
        next_best = best

    if axis == 0:
       if t_lat < root.point[1][0]:
          next_branch = root.left
       else:
          next_branch = root.right
    else:
       if t_lon < root.point[1][1]:
          next_branch = root.left
       else:
          next_branch = root.right

    return closest_point(next_branch, t_lon, t_lat, depth + 1, next_best)

# By da.li on 20180510
import time
import json
from file_operation import *
from py2neo import Graph

def connectGraph(bolt, usr, passwd):
    graph = Graph(bolt, user=usr, password=passwd)
    return graph

def checkNode(video_url, frame_index, boundingbox_info, data_type, \
    robust, graph, IOU_threshold):
    # Get necessary information
    video_url_spilted = video_url.split('-')
    cam_id = video_url_spilted[0]
    video_start_time = video_url_spilted[1]
    # calculate start time
    temporal_info = calObjectStartTime(video_start_time, frame_index)
    ID = getNodeID(graph, temporal_info, boundingbox_info, frame_index, \
        video_url, cam_id, robust, data_type, IOU_threshold)
    return ID
    
def calObjectStartTime(video_start_time, frame_index):
    fps_denominator = 2
    fps_numerator = 25
    time_interval = frame_index * fps_denominator / fps_numerator
    # sec ...
    sec_old = int(video_start_time[-2:])
    minu = (sec_old + time_interval) / 60
    sec = (sec_old + time_interval) % 60
    if sec < 10:
        sec_new = "0" + str(sec)
    else:
        sec_new = str(sec)
    # min ...
    min_old = int(video_start_time[-4:-2])
    hour = (min_old + minu) / 60
    minu = (min_old + minu) % 60
    if minu < 10:
        min_new = "0" + str(minu)
    else:
        min_new = str(minu)
    # hour ...
    hour_old = int(video_start_time[0:-4])
    hour = hour_old + hour
    # Combine
    time = str(hour) + min_new + sec_new
    return int(time)

def getNodeID(graph, temporal_info, boundingbox_info, frame_index, \
        video_url, cam_id, robust, data_type, IOU_threshold):
    # Time parsing ...
    minu, hour, day, mon, year = parseTime(temporal_info)

    # Query stream
    query_stream = """
    MATCH (r:Root)-[:HAS_YEAR]->(y:Year {year: %d})-[:HAS_MONTH]->
    (mon:Month {month: %d})-[:HAS_DAY]->(d:Day {day: %d})-[:HAS_HOUR]->
    (h:Hour {hour: %d})-[:HAS_MIN]->(min:Minute)
    WHERE min.start <= %d AND min.end >= %d
    WITH min
    MATCH (min:Minute)<-[:NEXT*0..1]-(before:Minute)-[:INCLUDES_PERSON]->
    (p:%s {camID: "%s", videoURL: "%s"}) WHERE p.startTime <= %d
    """ % (year, mon, day, hour, minu, minu, data_type, cam_id, \
        video_url, temporal_info)
    query_stream = query_stream + "RETURN p.trackletID as trackletID,\
        p.boundingBoxes as boundingBoxes, p.startIndex as startIndex,\
        p.startTime as startTime"
    res = graph.data(query_stream)
    # Get the matched id
    ids_with_iou = match(res, boundingbox_info, frame_index, IOU_threshold, \
        robust)
    return ids_with_iou

def parseTime(standard_time):
    minu = standard_time / 100
    hour = standard_time / 10000
    day  = standard_time / 1000000
    mon  = standard_time / 100000000
    year = standard_time / 10000000000
    return minu, hour, day, mon, year

def match(bbs_with_tracklet_id, bb_input, frame_index, \
    IOU_threshold, roubust):
    bingo_id = []
    for record in bbs_with_tracklet_id:
        tracklet_id = record["trackletID"]
        bbs = json.loads(record["boundingBoxes"])
        index = record["startIndex"]
        start_time = record["startTime"]
        lowerbound = max(frame_index - 10, 0)
        upperbound = frame_index + 10
        count = 0
        cal_iou = []
        if index > upperbound or (index+len(bbs)) < lowerbound:
            continue
        for bb in bbs:
            iou = calIOU(bb, bb_input)
            #min_iou = min(min_iou, iou)
            if iou > IOU_threshold:
                count = count + 1
                cal_iou.append(iou)
        if len(cal_iou) > 0:
            min_iou = min(cal_iou)
            avg_iou = sum(cal_iou)/len(cal_iou)
        else:
            min_iou = 0.0
            avg_iou = 0.0
        if count > roubust and min_iou > IOU_threshold:
            id_with_iou = {'trackletID': tracklet_id, \
                'iou': avg_iou, 'startTime': start_time}
            bingo_id.append(id_with_iou)
            print "  CHECK ONE (IoU = %s)!" % str(avg_iou)
    return bingo_id

def calIOU(rect1, rect2):
    # Rect1
    x1 = rect1['x']
    y1 = rect1['y']
    width1 = rect1['width']
    height1= rect1['height']

    # Rect2
    x2 = rect2['x']
    y2 = rect2['y']
    width2 = rect2['width']
    height2 = rect2['height']

    # Calculate bound
    end_x = max(x1+width1, x2+width2)
    start_x = min(x1, x2)
    width = width1 + width2 - (end_x-start_x)
    end_y = max(y1+height1, y2+height2)
    start_y = min(y1, y2)
    height= height1 + height2 - (end_y-start_y)

    # Calculate area and ratio
    if width <= 0 or height <= 0:
        ratio = 0
    else:
        area = width * height
        area1= width1 * height1
        area2= width2 * height2
        ratio = area * 1.0 / (area1 + area2 - area)
    return ratio

def run(data_type, IOU_threshold, data, graph):
    robust = 1
    
    matched_ids = []
    count = 0
    for item in data:
        count = count + 1
        print "==== INDEX: %d, %d/%d" % (int(item['index']), count, len(data))

        filename_splited = item['filename'].split('-')
        video_url = filename_splited[0]+'-'+filename_splited[4]+'-'+\
            filename_splited[5]
        frame_index_str = filename_splited[-1].split('.')[0]
        frame_index = int(frame_index_str[5:])
        x = int(item['x'])
        y = int(item['y'])
        width = int(item['width'])
        height= int(item['height'])
        bb = {'x':x, 'y':y, 'width':width, 'height':height}
        t0 = time.time()
        ids = checkNode(video_url, frame_index, bb, data_type, robust, graph, \
            IOU_threshold)
        if len(ids) > 1:
            ids = sorted(ids, key=lambda item: item['iou'], reverse=True)
        if len(ids) > 0:
            ids[0]['pid'] = int(item['index'])
            matched_ids.append(ids[0])
        t1 = time.time()
        print "  Elapsed time is %f s" % (t1 - t0)
    print "#Checked:", len(matched_ids)
    return matched_ids

if __name__=='__main__':
    bolt = 'bolt://xxx.xx.xx.xx:xxxx'
    user = 'xxxx'
    passwd = 'xxxxxxxx'
    graph = connectGraph(bolt, user, passwd)

    data_filename = "query_data/positive_boundingboxes.csv"
    IOU_thresholds = [0.5, 0.6, 0.7, 0.8, 0.9]
    datatypes = ['SSD20171031']
    data = loadCSV(data_filename)
    for datatype in datatypes:
        for iou in IOU_thresholds:
            res = run(datatype, iou, data, graph)
            save_path = 'plot_data/'+ datatype + '_' + str(iou) + '.csv'
            saveCSV(save_path, res)

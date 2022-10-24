# which factor strokes are
# Role of work type strokes are Occured
# Gender type
# Age Group and Gender
from pyspark import SparkContext

sc = SparkContext(master="local", appName="Health Care Analytics")

def work_type_stroke(health_data):
    health_data_list = health_data.split(",")
    work_type = health_data_list[6]
    stroke = int(health_data_list[11])
    return (work_type, stroke)

def getNumofStrokesAndTotalParticipation(x,y):
    if isinstance(x,tuple):
        stroke_count, person_count = x
        if isinstance(y, tuple):
            stroke_count =  stroke_count + y[0]
            person_count = person_count + y[1]
        else:

            person_count = person_count+1
            stroke_count = stroke_count + y
    else:
        person_count = 2
        stroke_count = x + y
    return (stroke_count, person_count)

def getPercentagePerKey(work_type_and_stroke_person_pair):
    work_type, stroke_person_pair = work_type_and_stroke_person_pair
    if isinstance(stroke_person_pair, tuple):
        stroke, total_person_count = stroke_person_pair
    else:
        stroke, total_person_count = (stroke_person_pair,1)
    return (work_type, stroke, total_person_count, float(stroke *100)/total_person_count)



health_data_rdd = sc.textFile("./inputData/healthcare_dataset_stroke_data.csv")
header = health_data_rdd.first()
data_with_out_header_rdd = health_data_rdd.filter( lambda x: x != header)
work_stroke = data_with_out_header_rdd.map(work_type_stroke)\
    .reduceByKey(getNumofStrokesAndTotalParticipation)\
    .map(getPercentagePerKey)

for result in work_stroke.collect():
    print(result)
print(work_stroke)


#work_type and count of heartstrokes

# def skip_header(index, iter):
#     current_row_num = -1
#     for record in iter:
#         current_row_num +=1
#         if index == 0 and current_row_num == 0:
#             continue
#         yield record

#health_data_skip_record = health_data_rdd.mapPartitionsWithIndex(skip_header)
#print(health_data_skip_record.take(3))

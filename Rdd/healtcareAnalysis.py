# which factor strokes are
# Role of work type strokes are Occured
# Gender type
# Age Group and Gender
from pyspark import SparkContext
from pyspark.storagelevel import StorageLevel

sc = SparkContext(master="local", appName="Health Care Analytics")

def work_type_stroke(health_data):
    health_data_list = health_data.split(",")
    work_type = str(health_data_list[6])
    stroke = int(health_data_list[11])
    return (work_type, stroke)

def getNumofStrokesAndTotalParticipation(x,y):
    """

    :param x:
    :param y:
    :return: (stroke_count, total_count)
    """

    if isinstance(x, tuple): #is x is tuple bcz first time return will be a tuple
        stroke_count, total_count = x  # extracting stroke_count, person_count based on return statement

        if isinstance(y, tuple): # y can be tuple after shuffle eg: partition 1 and two are collected
            # i.e., (2,3) -> x and partition 2  (2,5) -> y
            stroke_count = stroke_count + y[0] #x[0] is extracted as stroke_count and in y , y[0] is stroke_count so adding
            total_count = total_count + y[1] #similarly another value is tuple is person count. so y[0] is added to total_count
            # (extracted from x)
        else:
            stroke_count = stroke_count + y # y is new value x is tuple , so stroke_count (x[0]) + y (new value)
            total_count = total_count + 1 # so new person value is increased
    else:
        stroke_count = x + y #Adds like reduce
        total_count = 2 #total_person count for work type is 2 initially

    return(stroke_count, total_count) #returning as tuple, so that next iteration x is tuple y is next val

def getPercentagePerKey(work_type_and_stroke_person_pair):
    """
    Returns percentage for each record which is computed
    :param work_type_and_stroke_person_pair:
    :return:
    """
    work_type, stroke_person_pair = work_type_and_stroke_person_pair #tuple work_tyoe, (stroke_count, total_count)
    if isinstance(stroke_person_pair, tuple):
        stroke, total_person_count = stroke_person_pair # extractinhgstorke, total_count
    else:
        stroke, total_person_count = (stroke_person_pair,1) #if only value present , worktype , one person , so no tuple
                                                #w3, [0] -> reduce can't find other val returns only zero, one added to tuple
    return (work_type, stroke, total_person_count, float(stroke *100)/total_person_count)

#
health_data_rdd = sc.textFile("./inputData/healthcare_dataset_stroke_data.csv")
header = health_data_rdd.first()
#removing Header and caching the RDD of health data
data_with_out_header_rdd = health_data_rdd.filter( lambda x: x != header).persist(storageLevel=StorageLevel.MEMORY_ONLY)
work_stroke = data_with_out_header_rdd.map(work_type_stroke)\
    .reduceByKey(getNumofStrokesAndTotalParticipation)\
    .map(getPercentagePerKey)
print("************** Analysis on the role of Work Type on Stroke ****************")
for result in work_stroke.collect():
    print(result)


# Analysis on the role of gender on stroke

def get_gender_stroke(health_record):
    health_record_list = health_record.split(",")
    gender = str(health_record_list[1])
    stroke = int(health_record_list[11])
    return (gender,stroke)

gender_stroke_results = data_with_out_header_rdd.map(get_gender_stroke)\
    .reduceByKey(getNumofStrokesAndTotalParticipation)\
    .map(getPercentagePerKey)
print("************** Analysis on the role of gender on stroke ****************")
for result in gender_stroke_results.collect():
    print(result)


#Analysis on impact of age group and gender on stroke

def getAgeGroup(age):
    if age < 5:
        return "kid"
    elif 5<= age < 13:
        return "young"
    elif 13 <= age < 20:
        return "adsolent"
    elif 20 <= age < 40:
        return "adult"
    elif 40 <= age < 50:
        return "Midage"
    elif 50 <= age < 60:
        return "senior"
    else:
        return "old"

def getAgeGroupAndStroke(record):
    record_list = record.split(",")
    age_group = getAgeGroup(float(record_list[2]))
    gender = str(record_list[1])
    stroke = int(record_list[11])
    return ((age_group, gender), stroke)

age_group_gender_results = data_with_out_header_rdd\
    .map(getAgeGroupAndStroke)\
    .reduceByKey(getNumofStrokesAndTotalParticipation)\
    .map(getPercentagePerKey)
print("*********** Analysis on impact of age group and gender on stroke ************")
for result in age_group_gender_results.collect():
    print(result)
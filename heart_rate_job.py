# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# 
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# or in the "license" file accompanying this file. This file is distributed 
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either 
# express or implied. See the License for the specific language governing 
# permissions and limitations under the License.


import re
import boto3
import awswrangler as wr
from awsglue.utils import getResolvedOptions
import sys
import json
import traceback

# the filename should be of format <patient_id.yyyymmdd.metric.parquet/csv>

#  convert the minute of the day in hour, min


def getTimeFromMinute(minx):
    hour1 = int(minx/60)
    min1 = minx % 60
    t1 = [hour1, min1, 0]
    return t1


# get the date from the file_key. It should be 2nd part of the filename


def getDate(file_key):
    s1_split = re.split("/", file_key)
    patientInfo = s1_split[-1]
    filenamesplit = re.split("\.", patientInfo)
    filedate = filenamesplit[1]
    t1 = [filedate[0:4], filedate[4:6], filedate[6:8]]
    return t1


# get the Patient Id from the file_key. It should be 1st part of the filename


def getPatientId(file_key):
    s1_split = re.split("/", file_key)
    patientInfo = s1_split[-1]
    # print (patientInfo)
    filenamesplit = re.split("\.", patientInfo)
    patientId = filenamesplit[0]
    # print(patientId)
    return patientId

# read parameters from ssm


def getParameter(paramName):
    parameter = ssm.get_parameter(Name=paramName, WithDecryption=True)
    return parameter["Parameter"]["Value"]

# move the file to processed location


def moveFile(bucket_name, file_key):
    copy_source = {
        "Bucket": bucket_name,
        "Key": file_key
    }

    target_prefix = getParameter("DL-processed_location_prefix")
    target_bucket = getParameter("DL-processed_bucket")
    s1_split = re.split("/", file_key)
    object_name = s1_split[-1]

    otherkey = target_prefix + "/" + object_name
    print("Processed File bucket  is " + target_bucket)
    print("Processed target key is " + otherkey)

    s3.copy(copy_source, target_bucket, otherkey)
    s3.delete_object(Bucket=bucket_name, Key=file_key)
    return


# handler function that would be triggered


def glueHandler(buketname, filename):
    bucket_name = bucketname
    file_key = filename

    s3_read_url = "s3://" + bucket_name + "/" + file_key
    print("reading from : " + s3_read_url)

    patient_id = getPatientId(file_key)

    print("the patient info is " + patient_id)

    dataframe = ""
    # either parquet or csv
    if file_key.find("parquet") > -1:
        dataframe = wr.s3.read_parquet(path=s3_read_url)
    else:
        dataframe = wr.s3.read_csv(path=s3_read_url)

    # print(dataframe)

    patient_id = getPatientId(file_key)
    dateTuple = (getDate(file_key))
    metric_type = "heart_rate"
    # print(dateTuple)

    dataframe["year_value"] = 0
    dataframe["hour_value"] = 0
    dataframe["min_value"] = 0
    dataframe["sec_value"] = 0
    dataframe["year_value"] = int(dateTuple[0])
    dataframe["month_value"] = int(dateTuple[1])
    dataframe["day_value"] = int(dateTuple[2])
    dataframe["patient_id"] = patient_id
    dataframe["metric"] = metric_type

    rows = dataframe.shape[0]
    # cols = dataframe.shape[1]
    # print(rows)
    # print(cols)

    for rowId in range(rows):
        timeTuple = getTimeFromMinute(dataframe["minute_in_day"][rowId])
        dataframe["hour_value"][rowId] = timeTuple[0]
        dataframe["min_value"][rowId] = timeTuple[1]

    print("new rows " + str(dataframe.shape[0]))
    print("new cols " + str(dataframe.shape[1]))

    # print (dataframe)

    path = "s3://" + getParameter("DL-datalake_target_bucket") + "/"
    path = path + getParameter("DL-datalake_bucket_prefix") + "/"
    partition_cols = ["metric", "year_value", "month_value",
                      "day_value", "patient_id"]
    print("the location in the datalake is " + path)
    print("the partition information is " + str(partition_cols))
    athenaTable = "heart_rate_metric"
    databaseName = getParameter("DL-datalake_athena_database")
    print("the glue database " + databaseName)

    wr.s3.to_parquet(
        df=dataframe,
        path=path,
        dataset=True,
        mode="append",
        partition_cols=partition_cols,
        database=databaseName,
        table=athenaTable
    )

    moveFile(bucket_name, file_key)
    return


filename = ""
s3 = boto3.client("s3")
ssm = boto3.client("ssm")
sns = boto3.client("sns")
snsArn = getParameter("DL-datalake_failure_arn")

try:
    args = getResolvedOptions(sys.argv, ["bucketname", "filename"])
    print(args)
    bucketname = args["bucketname"]
    filename = args["filename"]
    print("The data is to be sourced from : " + args["bucketname"])
    print("The data key  is: " + args["filename"])

    glueHandler(bucketname, filename)
except Exception as inst:
    print(type(inst))
    print(inst)
    print(inst.args)
    track = traceback.format_exc()
    print(track)

    message = {"error ": "Unable to process file ", "filename": filename}
    response = sns.publish(
        TargetArn=snsArn,
        Message=json.dumps({"default": json.dumps(message)}),
        Subject="Failure in processing file " + filename,
        MessageStructure="json"
    )
    print("message : " + json.dumps(message) + " to ARN : " + snsArn)

print("\r\n processing done")
from pyspark.sql import SparkSessionspark = SparkSession.builder.getOrCreate()
#dbutils.widgets.text("Number_of_files", "1")

#num_files = int(dbutils.widgets.get("Number_of_files")) 


def path_exists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    msg = str(e)
    if ("com.databricks.sql.io.CloudFileNotFoundException" in msg
        or "java.io.FileNotFoundException" in msg):
      return False
    else:
      raise

def get_index(raw_dir):
    try:
        files = dbutils.fs.ls(raw_dir)
        file = max(f.name for f in files if f.name.endswith(".json"))
        index = int(file.split(".", maxsplit=1)[0])
        print(f"Current index is {index} and {file}")
    except:
        index = 0
    return index + 1
    
def load_json_file(current_idex, src_dir, raw_dir): 
    latest_file = f"{str(current_idex).zfill(2)}.json"
    print(latest_file)
    source = f"{src_dir}/{latest_file}"
    target = f"{raw_dir}/{latest_file}" 
    print(f"Streaming prefix is {src_dir}")
    prefix = src_dir.split("/")[-1]

    if path_exists(source):
        print(f"Loading {prefix}-{latest_file} file to the bookstore dataset")
        dbutils.fs.cp(source, target)
   
def __load_data(max, src_dir, raw_dir, all = False):
    index = get_index(raw_dir)
    if index > max:
        print("No more data to load \n")
        return 0
    elif all == True:
        while index <= max:
            load_json_file(index, src_dir, raw_dir)
            index += 1
    else:
        load_json_file(index, src_dir, raw_dir)
        index += 1
    return 1

def load_new_data(src_dir, raw_dir, num_files = 1):
    max_ = 10 # This approache is very regide -- so need to have a better way to implement an ingestion from aws s3 bucket
    for file in range(num_files):
        __load_data(max_, src_dir, raw_dir)



src_dir = "s3://dalhussein-courses/DE-Pro/datasets/bookstore/v1/kafka-streaming/"
dataset_path = "dbfs:/Volumes/bookstore_ldp_catalog/landing/kafka_source"



raw_dir = f"{dataset_path}/kafka-raw/"
print(f"Loading new data to {raw_dir}") 
load_new_data(src_dir, raw_dir)
#dbutils.fs.cp(src_dir, raw_dir, recurse=True)
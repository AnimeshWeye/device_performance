import fastavro
import pandas as pd
import s3_module
file_name = "/home/ubuntu/vibhor/IoT/device_performance/device_performance/avro/1929092_2022-03-09-002110.avro"
data = s3_module.get_avro_reader(file_name)
final_data = []
for record in data:
    final_data.append(record)
    data_df = pd.DataFrame(final_data)
print(data_df)

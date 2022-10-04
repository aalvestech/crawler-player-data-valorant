from aws_s3 import AwsS3
import pandas as pd
import json


# AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
# AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
# AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")

class DataCleaner():
    
    def data_cleaner_trackergg():

        path_read = 'raw/trackergg/matches_report/'
        path_write = 'cleaned/trackergg/matches_report/'
        
        df_aux = pd.DataFrame()

        files = AwsS3.get_files_list(path_read)

        for file in files:
            file = file.key

            data_s3 = AwsS3.get_file(path_read, file)

            data_json = json.loads(data_s3)
            
            expiryDate : str = data_json["data"]["expiryDate"]
            requestingPlayerAttributes : dict = data_json["data"]["requestingPlayerAttributes"]
            paginationType : str = data_json["data"]["paginationType"]
            metadata : dict = data_json["data"]["metadata"]
            matches : list = data_json["data"]["matches"]

            data = []

            for match in matches:
                # data
                attributes : dict = match["attributes"]
                match_metadata : dict = match["metadata"]
                expiryDate : str = match["expiryDate"]
                
                # more data
                segments : list = match["segments"]
                for segment in segments:
                    # data
                    segment_type: str = segment["type"]
                    attributes : dict = segment["attributes"]
                    segment_metadata : dict = segment["metadata"]
                    expiryDate : str = segment["expiryDate"]
                    
                    # more data
                    stat_dict = {}
                    stats : dict = segment["stats"]
                    for stat, stat_data in stats.items():
                        stat_keys = stat_data.keys()
                        stat_columns = [f'{stat}_{col}' for col in stat_keys]
                        stat_values = stat_data.values()
                        _stat_dict = {k: v for k, v in zip(stat_columns, stat_values)}
                        stat_dict.update(_stat_dict)

                    row = {}
                    row.update(attributes)
                    row.update(match_metadata)
                    row["expiryDate"] = expiryDate

                    row["segment_type"] = segment_type
                    row.update(attributes)
                    row.update(segment_metadata)
                    row["expiryDate"] = expiryDate

                    row.update(stat_dict)

                    data.append(row)

            
            columns = data[0].keys()
            df = pd.DataFrame(data, columns=columns)
            df_aux = pd.concat([df_aux, df], axis = 0)


        df_final = pd.concat([df_aux, df_aux['rank_metadata'].apply(pd.Series)], axis=1)

        df_final.to_csv('matches.csv')

        data_final_parquet = df_final.to_csv()

        file_format = '.csv'

        AwsS3.upload_file(data_final_parquet, path_write, file_format)
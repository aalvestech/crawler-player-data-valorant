from aws_s3 import AwsS3
import pandas as pd
import json


class DataCleaner():
    
    def data_cleaner_matches():

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
                attributes : dict = match["attributes"]
                match_metadata : dict = match["metadata"]
                expiryDate : str = match["expiryDate"]
                
                segments : list = match["segments"]
                for segment in segments:
                    segment_type: str = segment["type"]
                    attributes : dict = segment["attributes"]
                    segment_metadata : dict = segment["metadata"]
                    expiryDate : str = segment["expiryDate"]
                    
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
                    row["match_id"] = match["attributes"]["id"]
                    row.update(stat_dict)

                    data.append(row)

            columns = data[0].keys()
            df = pd.DataFrame(data, columns=columns)
            df_aux = pd.concat([df_aux, df], axis = 0)

        df_final = pd.concat([df_aux, df_aux['rank_metadata'].apply(pd.Series)], axis=1)

        df_final.to_csv('matches.csv')

        data_final_csv = df_final.to_csv()

        file_format = '.csv'

        AwsS3.upload_file(data_final_csv, path_write, file_format)

    def data_cleaner_guns():

        path_read = 'raw/trackergg/gun_report/'
        path_write = 'cleaned/trackergg/gun_report/'


        files = AwsS3.get_files_list(path_read)

        data = []
        
        for file in files:
            
            file = file.key
            data_s3 = AwsS3.get_file(path_read, file)
            data_json = json.loads(data_s3)
            weapons = data_json['data']

            for weapon in weapons:
                weapon_metadata = weapon["metadata"]
                weapon_stats = weapon["stats"]

                stat_dict = {}
                for stat, stat_data in weapon_stats.items():
                    stat_keys = weapon_stats.keys()
                    stat_columns = [f'{col}' for col in stat_keys]
                    stat_values = weapon_stats.values()
                    _stat_dict = {k: v for k, v in zip(stat_columns, stat_values)}
                    stat_dict.update(_stat_dict)
                
                row = {}
                row.update(weapon_metadata)
                row.update(stat_dict)

                data.append(row)

        df_final = pd.DataFrame(data)

        df_final.to_csv('guns.csv')

        data_final_csv = df_final.to_csv()

        file_format = '.csv'

        AwsS3.upload_file(data_final_csv, path_write, file_format)
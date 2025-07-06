
import pandas as pd


class Dataframe:
    def __init__(self, data: dict = None):
        self.data = {
            "name": [],
            "type": [],
            "year_built": [],
            "gross_tonnage": [],
            "deadweight": [],
            "length(m)": [],
            "beam(m)": [],
            "detail_link": [],
            "departure_date": [],
            "last_port_country": [],
            "last_port_name": [],
            "arrival_date": [],
            "destination_port_country": [],
            "destination_port_name": [],
            "destination_port_lat": [],
            "destination_port_lon": [],
            "reported_status": [],
            "report_date": []
        }

        if data:
            self.data = data
    

    def addNewRecord(self, record: dict):

        for k in self.data.keys():
            if k in record.keys():
                self.data[k].append(record[k])
            else:
                self.data[k].append(None)
    
    def toPandasDataFrame(self):
        return pd.DataFrame(self.data)
    

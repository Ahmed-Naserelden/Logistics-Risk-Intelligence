
from datetime import datetime

class Record:

    def __init__(self, data):
        self.record: dict = data
        self.report_date_time = datetime.now().strftime('')
        self.redefined_record = self.redefineRecode()

    @staticmethod
    def isValid(value):
        return value not in ('-', '')
    
    def getIfInRecord(self, key):
        # if key in self.record.keys:

        if key in self.record.keys():
            return self.record[key]
        return '-'

    def getLengthAndBeem(self) -> tuple[int, int]:
        
        if 'Size(m)' in self.record.keys():
            values = self.record['Size(m)']
            if '/' in values:
                v_list = values.split('/')
                return int(v_list[0]), int(v_list[-1])

        if 'Length / Beam' in self.record.keys():
            values = self.record['Length / Beam']
            if '/' in values:
                # values is a string like this: '229 / 32 m'
                v_list = values.split('/')

                v_list[-1] = v_list[-1][:v_list[-1].find('m')]

                return int(v_list[0]), int(v_list[-1]) 
        
        length, beam = '-', '-'

        if 'Length Overall(m)' in self.record.keys():
            length = self.record['Length Overall(m)']
        if 'Beam(m)'in self.record.keys():
            beam = self.record['Beam(m)']

        return length, beam
    
    def getLength(self) -> dict:
        return {'length(m)': self.getLengthAndBeem()[0]}
    
    def getBeam(self) -> dict:
        return {'beam(m)': self.getLengthAndBeem()[-1]}
    
    def getName(self) -> dict:
        name = self.getIfInRecord('vessel_name')
        if name == '-':
            name = self.getIfInRecord('Vessel Name')
        return {"name": name}

    def getType(self) -> dict:
        type = self.getIfInRecord('AIS Type')
        return {"type": type}
    
    def getFlage(self) -> dict:
        flage = self.getIfInRecord('Flage')
        if flage == '-':
            flage = self.getIfInRecord('AIS Flag')
        return {"flage": 'Egypt'}

    def getIMO(self) -> dict:
        imo = self.getIfInRecord('IMO number')
        if not Record.isValid(imo):
            imo = self.getIfInRecord('MMSI')
        return {"imo": imo}
    
    def getNavigationStatus(self) -> dict:
        reported_status = self.getIfInRecord('Navigation Status')
        return {"reported_status": reported_status}

    def getGrossTonnage(self) -> dict:
        gross_tonnage = self.getIfInRecord('GT')
        if gross_tonnage == '-':
            gross_tonnage = self.getIfInRecord('Gross Tonnage')
        return {"gross_tonnage": gross_tonnage}

    def getDeadWeight(self) -> dict:
        deadweight = self.getIfInRecord('DWT')
        if deadweight == '-':
            deadweight = self.getIfInRecord('Deadweight(t)')
        return {"deadweight": deadweight}

    def getLastPortCountry(self) -> dict:
        last_port_country = self.getIfInRecord('last_port_country')
        return {"last_port_country": last_port_country}

    def getYearBuilt(self) -> dict:
        year_built = self.getIfInRecord('Built')
        if year_built == '-':
            year_built = self.getIfInRecord('Year of Build')
        return {"year_built": year_built}

    def getDestinationPortCountry(self) -> dict:
        destination_port_country = self.getIfInRecord('destination_port_country')
        return {"destination_port_country": destination_port_country}
    
    def getDestinationPortName(self) -> dict:
        destination_port_name = self.getIfInRecord('destination_port_name')
        return {"destination_port_name": destination_port_name}

    def getAarrivalDate(self) -> dict:
        arrival_date = self.getIfInRecord('arrival_date')
        return {"arrival_date": arrival_date}

    def getLastPortName(self) -> dict:
        last_port_name = self.getIfInRecord('last_port_name')
        print("Last_PORT_NAME: ", last_port_name)
        return {"last_port_name": last_port_name}

    def getDepartureDate(self) -> dict:
        departure_date = self.getIfInRecord('departure_date')
        return {"departure_date": departure_date}

    def getReportDate(self) -> dict:
        return {"report_date": datetime.now().strftime('%Y-%m-%d')}

    def getDestinationPortLat(self) -> dict:
        destination_port_lat = self.getIfInRecord('destination_port_lat')
        return {"destination_port_lat": destination_port_lat}
    
    def getDestinationPortLon(self) -> dict: 
        destination_port_lon = self.getIfInRecord('destination_port_lon')
        return {"destination_port_lon": destination_port_lon}
    def getVesselURL(self) -> dict:
        url = self.getIfInRecord('Url')
        return {'url': url}
    def redefineRecode(self) -> dict:
        new_record = {
            **self.getVesselURL(),
            **self.getName(),
            **self.getType(),
            **self.getYearBuilt(),
            **self.getDeadWeight(),
            **self.getGrossTonnage(),
            **self.getLength(),
            **self.getBeam(),
            **self.getDepartureDate(),
            **self.getLastPortCountry(),
            **self.getLastPortName(),
            **self.getDestinationPortCountry(),
            **self.getDestinationPortName(),
            **self.getDestinationPortLat(),
            **self.getDestinationPortLon(),
            **self.getAarrivalDate(),
            **self.getNavigationStatus(),
            **self.getReportDate()
        }

        return new_record

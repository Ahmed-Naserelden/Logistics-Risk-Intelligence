import json
import requests
from bs4 import BeautifulSoup
from scrapper import Scrapper
import pandas as pd
from data_frame import Dataframe
from record import Record
from datetime import datetime
from portscraper import PortsLocationsScraper

class VesselURLSCrapper(Scrapper):
    """
    A class to scrape vessel URLs from a given URL.
    """

    def __init__(self, base_url: str, endpoint: str = ""):
        super().__init__(base_url, endpoint)
        self.getNumberOfPages = self.getNumberOfPages(endpoint)

    
    def get_all_vessel_urls(self) -> list:
        """
        Scrape vessel URLs from the base URL.
        Returns:
            list: A list of vessel URLs.
        """
        all_vessel_urls = []
        for page in range(1, self.getNumberOfPages + 1):
            vessel_urls = self.get_vessel_urls_in_page(page)
            all_vessel_urls.extend(vessel_urls)
        return all_vessel_urls

    def get_vessel_urls_in_page(self, page=None) -> list:
        """
        Scrape vessel URLs from the given URL.
        Args:
            url (str): The URL to scrape.
            page (int, optional): The page number to scrape. Defaults to None, which scrapes the first page.
        Returns:
            list: A list of vessel URLs.
        """

        if page is not None:
            url = f"{self.base_url}/{self.endpoint}&page={page}"
        else:
            url = f"{self.base_url}/{self.endpoint}"

        vessel_data = requests.get(url, headers=self.header).text
        soup = BeautifulSoup(vessel_data, 'html.parser')
        table = soup.find('table')

        rows = table.find_all('tr')[1:]  # Skip the header row

        # Extract vessel URLs from the table rows
        vessel_urls = [row.find('td').find('a').get('href') for row in rows]
        return vessel_urls


def divide_page_into_sections(soup: BeautifulSoup, element_name: str, value: str) -> BeautifulSoup:
    """
    Divide the page into sections.
    Args:
        soup (BeautifulSoup): The BeautifulSoup object containing the page.
    Returns:
        list: A list of sections containing vessel data.
    """

    # i want to find the parent fo child that contains this value "Vessel Particulars"
    html_element = soup.find(element_name, string=value)
    print("\n"*2)
    print("From divide_page_into_sections: ", html_element)
    print("\n"*2)
    parent = html_element.parent
    return parent


class VesselScraper(Scrapper):
    """
    A class to scrape vessel information from a given URL.
    """

    def __init__(self, base_url: str, endpoint: str = ""):
        super().__init__(base_url, endpoint)

    def get_page_soup(self, vessal_link) -> BeautifulSoup:
        """
        Get the BeautifulSoup object for the given URL.
        Args:
            url (str): The URL to scrape.
        Returns:
            BeautifulSoup: The BeautifulSoup object containing the page content.
        """
        url = f"{self.base_url}/{vessal_link}"
        response = requests.get(url, headers=self.header)
        if response.status_code != 200:
            raise Exception(f"Failed to retrieve page: {response.status_code}")
        return BeautifulSoup(response.text, 'html.parser')
    


    def extract_last_port_information(self, div: BeautifulSoup) -> dict:
        """
        Extract last port information from the given div.
        Args:
            div (BeautifulSoup): The BeautifulSoup object containing the last port information.
        Returns:
            dict: A dictionary with last port name, country. and departure_date.
        """

        last_port_name_county = div.find('a', class_='_npNa').text.strip().split(',')

        if last_port_name_county:
            last_port_name = last_port_name_county[0].strip()
            last_port_country = last_port_name_county[1].strip() if len(last_port_name_county) > 1 else ''
        

        departure_date_info = div.find('div', class_='_value').text.strip()

        if departure_date_info:
            departure_date = departure_date_info[departure_date_info.find(' ') + 1:].strip()
        
        return {
            "last_port_name": last_port_name,
            "last_port_country": last_port_country,
            "departure_date": departure_date
        }

    def extract_destination_port_information(self, div: BeautifulSoup) -> dict:
        """
        Extract destination port information from the given div.
        Args:
            div (BeautifulSoup): The BeautifulSoup object containing the destination port information.
        Returns:
            dict: A dictionary with destination port name, country, and arrival_date.
        """
        destination_port_lon, destination_port_lat = '-', '-'
        try:
            destination_port_name_country = div.find('div', class_='_3-Yih').text.strip().split(',')
        except:
            destination_port_name_country = div.find('a', class_='_npNa').text.strip().split(',')
            port_endpoint = div.find('a', class_='_npNa')['href']
            destination_port_lon, destination_port_lat = PortsLocationsScraper.get_port_location(port_endpoint)
            print("###################")
            print("Long: ", destination_port_lon)
            print("Lat: ", destination_port_lat)
            print("###################")

        if destination_port_name_country:
            destination_port_name = destination_port_name_country[0].strip()
            destination_port_country = destination_port_name_country[1].strip() if len(destination_port_name_country) > 1 else ''
        
        arrival_date_info = div.find('div', class_='_value').text.strip()
        if arrival_date_info:
            arrival_date = arrival_date_info[arrival_date_info.find(' ') + 1:].strip()  

        return {
            "destination_port_name": destination_port_name,
            "destination_port_country": destination_port_country,
            "destination_port_lat": destination_port_lat,
            "destination_port_lon": destination_port_lon,
            "arrival_date": arrival_date
        }

    def extract_life_way_information(self, div: BeautifulSoup) -> dict:
        
        labels = div.find_all('td', class_='n3')
        values = div.find_all('td', class_='v3')

        lables = [ lable.text.strip() for lable in labels]
        values = [ value.text.strip() for value in values]

        ret = {}
        for k, v in zip(lables, values):
            ret[k] = v
        # print(ret)

        return ret

    # get key, value pairs of Voyage Data Section
    def parse_section_1(self, section: BeautifulSoup) -> dict:

        """
        Parse the first section of the vessel page.
        Args:
            section (BeautifulSoup): The BeautifulSoup object containing the section.
        Returns:
            dict: A dictionary with parsed data from the section.
        """

        right_side = section

        try:
            destination_port_div, life_way_div, last_port_div = right_side.select(':scope > div')

            last_port_info = self.extract_last_port_information(last_port_div)
            
            life_way_info = self.extract_life_way_information(life_way_div)

            dest_port_info = self.extract_destination_port_information(destination_port_div)
            
            return {**last_port_info, **life_way_info, **dest_port_info}
        except:
            return {}

    # get key, value pairs of Vessel Particulars Section
    def parse_section_4(self, section: BeautifulSoup) -> dict:
        """
        Parse the fourth section of the vessel page.
        Args:
            section (BeautifulSoup): The BeautifulSoup object containing the section.
        Returns:
            dict: A dictionary with parsed data from the section.
        """
        
        data = {}
        # print(section.prettify())
        # Example parsing logic, adjust according to actual HTML structure
        labels = section.find_all('td', class_='tpc1')
        values = section.find_all('td', class_='tpc2')

        for label, value in zip(labels, values):
            label_text = label.get_text(strip=True)
            value_text = value.get_text(strip=True)
            data[label_text] = value_text
        
        return data

    def get_parent_of_element(self, soup: BeautifulSoup, value: str, ele_name: str = 'h2') -> BeautifulSoup:
        """
        Divide the page into sections.
        Args:
            soup (BeautifulSoup): The BeautifulSoup object containing the page.
        Returns:
            list: A list of sections containing vessel data.
        """

        # i want to find the parent fo child that contains this value "Vessel Particulars"
        html_ele = soup.find(ele_name, string=value)
        if not html_ele:
            return None

        parent = html_ele.parent
        return parent


    def parse_vessel_page_to_3_main_sections(self, soup: BeautifulSoup) -> dict:
        """
        Parse the vessel page to extract sections of interest.
        Args:
            soup (BeautifulSoup): The BeautifulSoup object containing the vessel page.
        Returns:
            list: A list of sections containing vessel data.
        """
        
        vessel_particulars = self.get_parent_of_element(soup, 'Vessel Particulars')
        voyage_data = self.get_parent_of_element(soup, 'Voyage Data')

        return {
            "vessel_particulars": vessel_particulars,
            "voyage_data": voyage_data
        }
    
    def operate(self):

        soup = self.get_page_soup(self.endpoint)

        sections  = self.parse_vessel_page_to_3_main_sections(soup)

        section1_info = {}
        try:
            section1 = sections["voyage_data"]
            # print(section1.prettify())
            if section1:
                section1_info = self.parse_section_1(section1)
            # print("section1_info: ", section1_info)
            
        except Exception as e:
            print("Erro Mesage: ", e)
            print("error in Voyage Data Section")
        
            
        section4_info = {}
        try:
            
            section4 = sections["vessel_particulars"]
            if section4:
                section4_info = self.parse_section_4(section4)
            # print("section4_info: ", section4_info)
        except Exception as e:
            print("Erro Mesage: ", e)
            print("error in Vessel Particulars Section")
        

        return section1_info | section4_info



def doWork(base_url, endpoint):
    pass

if __name__ == '__main__':
    base_url = "https://www.vesselfinder.com"

    endpoints = ["vessels?type=601&flag=EG", "vessels?type=4&flag=EG"]


    df = Dataframe()
    list_of_invalid = []

    for endpoint in endpoints:
        vessels_links = VesselURLSCrapper(base_url, endpoint).get_all_vessel_urls()
        print(f"Number of Links: {len(vessels_links)}")

        for _ in range(0, len(vessels_links)):

            vessels_link = vessels_links[_]        
            print(f"Now Scraping this, ", vessels_links[_])

            vs = VesselScraper(base_url=base_url, endpoint=vessels_link)
            rs = vs.operate()

            # print all key, value data
            print("*"*100)
            print(rs)
            print("*"*100)

            # to select the needed data
            record = Record(rs).redefineRecode()
            print(record)
            print("="*100)
            df.addNewRecord(record)

            if record == {}:
                list_of_invalid.append(vessels_links[_])
        # break
            
    print("Count: ", len(list_of_invalid))
    print(list_of_invalid)
    df.toPandasDataFrame().to_csv(f"/extraction/data/vessels/{datetime.now().strftime('%Y-%m-%d')}.csv")
    print(df.toPandasDataFrame().head())
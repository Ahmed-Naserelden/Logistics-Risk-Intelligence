

import json
import requests
from bs4 import BeautifulSoup



class PortsUrlsScraper:

    def __init__(self, url= "https://www.vesselfinder.com/ports"):
        self.base_url = url
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }

    

    def get_port_urls_in_page(self, page=1) -> list:
        """
        Get all port URLs from the VesselFinder ports page.
        Returns:
            list: A list of port URLs.
        """

        print("Scraping port URLs from page:", page)
        url = f'{self.base_url}?page={page}'
        response = requests.get(url, headers=self.headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        ports = {}
        for a_tag in soup.select('a[href^="/ports/"]'):
            ports[f'{a_tag.text.strip().split('\n')[0]}'] = {
                'lat': 0,  # Placeholder for latitude
                'lon': 0,  # Placeholder for longitude
                'url': a_tag['href']
            }

        return ports
    
    def get_ports_data(self, numberOfPages = 2) -> dict:
        
        ports = {}
        for page in range(1, numberOfPages):

            print(f"Scraping page {page}...")
            page_ports = self.get_port_urls_in_page(page)

            ports = {**ports, **page_ports}
            
        return ports


class PortsLocationsScraper:

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }
    base_url = "https://www.vesselfinder.com"
    def __init__(self, ports: dict):
        self.ports = ports
        
        
    @staticmethod
    def get_port_location(port_endpoint: str) -> dict:
        """
        Get the latitude and longitude of a port from its URL.
        Args:
            port_endpoint (str): The URL of the port.
        Returns:
            dict: A dictionary containing latitude and longitude.
        """
        response = requests.get(f'{PortsLocationsScraper.base_url}{port_endpoint}', headers=PortsLocationsScraper.headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        port_info = soup.find('p', class_='text1').text.strip().split('\n')[0]
        spectum  = port_info.split(',')
        lat = spectum[0].split(' ')[-1].strip()
        long = spectum[-1][:-1]
        print(lat, long)
        return long, lat

if __name__ == "__main__":

    ports = PortsUrlsScraper().get_ports_data()
    
    # print(len(ports))

    for port_name, port_info in ports.items():
        print(f"Port: {port_name}, URL: {port_info['url']}")
        lon, lat =PortsLocationsScraper.get_port_location(port_info['url'])
        ports[port_name]['lat'] = lat
        ports[port_name]['lon'] = lon


    

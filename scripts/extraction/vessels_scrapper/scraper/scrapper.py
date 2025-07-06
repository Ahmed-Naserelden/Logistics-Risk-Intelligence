
import requests
from bs4 import BeautifulSoup

class Scrapper:

    def __init__(self, base_url: str = "https://www.vesselfinder.com", endpoint: str = "vessels?type=601&flag=EG"):
        self.base_url = base_url
        self.endpoint = endpoint
        self.url = f"{self.base_url}/{self.endpoint}"
        self.header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    def getNumberOfPages(self, endpoint: str) -> int:

        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url, headers=self.header)
        soup = BeautifulSoup(response.text, 'html.parser')

        pagination = soup.find('nav', class_='pagination').find('span')

        # Extract the number of pages from the pagination text
        # Example pagination text: "Page 1 / 10"
        if not pagination:
            return 1
        numper_of_pages = int(pagination.text.strip().split('/')[-1])

        return numper_of_pages



if __name__ == '__main__':

    base_url = "https://www.vesselfinder.com"
    scrapper = Scrapper(base_url)
    
    # example usage
    number_of_pages = scrapper.getNumberOfPages("vessels?type=601&flag=EG")
    print(f"Number of pages: {number_of_pages}")


    
    


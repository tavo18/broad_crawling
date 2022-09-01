from scraping_script_with_api_and_signals import RunLinkSearch

links = [
        'https://www.lra.org/',
        'https://wcet2022.org/',
        'https://www.imts.com/',
        'https://www.mdmwest.com/en/home.html'
]


data = RunLinkSearch(start_urls = links)

print(data)
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from tld import get_tld
import pandas as pd
import re
from scrapy.http import Request
from scrapy.utils.spider import iterate_spider_output
from ..utils import PLATFORM_DOMAINS
from datetime import datetime

bottom = datetime(year=2022, month=8, day = 1)
top = datetime(year=2022, month=12, day = 31)

df = pd.read_excel('TSC_events.xlsx')
df = df.loc[(df.date >= bottom) & (df.date <= top)]

def links_as_lower(link):
    return link.lower()


platforms_rules = {
                    'mys': [r'(?P<link>https?://\w+.mapyourshow.com//?[8|7]_0/)'], 
                    'a2z': [r'(?P<link>https?://.*?/public/eventmap\.aspx).*', r'(?P<link>https?://.*?/public/exhibitors\.aspx).*',
                            r'(?P<link>https?://.*?/public/e_boothsales\.aspx).*', r'(?P<link>https?://.*?/public/e_login\.aspx).*',
                            r'(?P<link>https?://.*?/public/sendpage\.aspx).*', r'(?P<link>https?://.*?/public/sessions\.aspx).*'],
                    'eshow': [r'(?P<link>https?://s\d+\.goeshow\.com/.*?/\d+)'],
                    'ec1': [r'(?P<link>https?://.*/exfx.html).*',
                            r'(?P<link>https?://(?:www\.)?(?:.*\.)?expocad\.com/events/.*\.index.html)',
                            r'(?P<link>https?://(?:www\.)?expocad\.com/host/fx/.*?/.*?).*'],
                    'md': [r'(?P<link>https?://(?:.*)\.map-dynamics\.com/.*)'],
                    'ec2': [r'(?P<link>https?://(?:www\.)?expocadweb\.com/.*?/index5\.aspx)'],
                    'ch': [r'(?P<link>https?://(?:www\.)?conferenceharvester\.com/floorplan/floorplan\.asp\?eventkey=.*)'],
                    'escribe':[r'(?P<link>https?://(?:www\.)?eventscribe\.net/\d{4}/.*?/)'],
                    'expofp': [r'(?P<link>https?://(?:www\.)?.*?\.expofp\.com)'],
                    'ungerboeck': [r'(?P<link>https?://(?:www\.)?.*?\.ungerboeck\.com/prod/app85.cshtml).*',
                                   r'(?P<link>https?://(?:www\.)?.*?\.ungerboeck\.net/prod/app85.cshtml).*']
                }

def process_link(link):

    link = link.lower()
    link = re.sub(r'https?','https', link)
    
    for key in platforms_rules.keys():
        for regex in platforms_rules[key]:
            match = re.search(regex, link, re.I)
            if match:
                output_link = match.group('link')
                
                if key == 'a2z':
                    output_link = re.sub(r'/public/.*\.aspx.*','/public/exhibitors.aspx', output_link)
                
                return output_link

    return None


class FindLinks(CrawlSpider):
    name = 'FindLinks'
    # start_urls = df.Link.tolist()
    start_urls = [
        'https://vegas.insuretechconnect.com/'
    ]

    platforms_domains = PLATFORM_DOMAINS
    
    events_domains = []
    allowed_domains = platforms_domains #+ events_domains

    
    custom_settings = {
        'LOG_LEVEL': 'INFO',
        'DEPTH_LIMIT': 3,
        'COOKIES_ENABLED': False,
        'RETRY_ENABLED': False,
        'DOWNLOAD_TIMEOUT': 20,
        'REDIRECT_ENABLED': True,
        'DEPTH_PRIORITY': 1,
        'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue',
        'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue',
        'SCHEDULER_PRIORITY_QUEUE': 'scrapy.pqueues.DownloaderAwarePriorityQueue',
        'CONCURRENT_REQUESTS': 60,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 3,
        'MAX_REQUESTS_PER_DOMAIN': 75, # BEWARE - PLATFORM DOMAINS,
        'MAX_REQUESTS_PER_START_URL': 75
    }

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._compile_rules()

        # print(self.start_urls)

        urls = self.__dict__['start_urls'] if hasattr(self.__dict__, 'start_urls') else self.start_urls
        for url in urls:
            tld = get_tld(url, as_object=True)
            domain = tld.domain
            domain = '.'.join([domain, str(tld)])
            # domain = re.sub(r'(?:https?://)?(?:www\.)?', '', url).split('/')[0]
            # domain = '.'.join(domain.split('.')[-2:])
            self.allowed_domains.append(domain)
            self.events_domains.append(domain)
        
        # print(self.events_domains)

    def start_requests(self):
        """ 
        Added the start_url to the Request meta
        """
        for url in self.start_urls:
            yield Request(url, dont_filter=True, meta={'start_url': url})
    
    
    def _parse_response(self, response, callback, cb_kwargs, follow=True):
        """ 
        Internal method from CrawlSpider class.
        I've added the start_url to the meta argument so it can be
        accessed into the parsing method.
        That way I can know wich event website the platform link belongs to.
        """
        if callback:
            cb_res = callback(response, **cb_kwargs) or ()
            cb_res = self.process_results(response, cb_res)
            for request_or_item in iterate_spider_output(cb_res):
                yield request_or_item

        if follow and self._follow_links:
            for request_or_item in self._requests_to_follow(response):
                request_or_item.meta['start_url'] = response.meta['start_url']
                yield request_or_item
    
    rules = (
        Rule(LinkExtractor(unique=True, process_value=process_link), callback='parse_item'),
        Rule(LinkExtractor(unique=True, canonicalize=True, allow_domains = events_domains), follow=True),
    )


    
    def parse_item(self, response):

        if 'redirect_urls' in response.request.meta:
            output_link = response.request.meta['redirect_urls'][0]
        else:
            output_link = response.request.url

        print({'link': output_link, 'start_url': response.meta['start_url']})

        yield {'link': output_link, 'start_url': response.meta['start_url']}

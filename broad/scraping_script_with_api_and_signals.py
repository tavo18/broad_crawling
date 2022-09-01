# Run the spider with the internal API of Scrapy:
from scrapy import signals
from scrapy.crawler import Crawler, CrawlerProcess
from scrapy.utils.project import get_project_settings
from broad.spiders.FindLinks import FindLinks
import sys

settings = get_project_settings()
process = CrawlerProcess(settings)
broad_crawler = Crawler(FindLinks, settings)

has_links = False
data = []

def _handle_spider_opened(spider):
    print(f"Spider {spider.name} is started.")


def _handle_item_scraped(item):
    print(f"Item scraped: {item}.")
    global has_links
    has_links = True
    data.append({'start_url': item['start_url'], 'link': item['link']})


def _handle_spider_closed(spider):
    print(f"Spider {spider.name} is closed.")

def RunLinkSearch(start_urls = []):

    broad_crawler.signals.connect(
        _handle_spider_opened, signal=signals.spider_opened
    )

    broad_crawler.signals.connect(_handle_item_scraped, signal=signals.item_scraped)

    broad_crawler.signals.connect(
        _handle_spider_closed, signal=signals.spider_closed
    )


    process.crawl(broad_crawler, start_urls=start_urls)
    process.start()

    # return has_links, links

    return data


if __name__ == '__main__':
    args = sys.argv
    if len(args) < 2:
        raise "No link provided"
    else:
        # RunLinkSearch(args[1])
        print(RunLinkSearch([args[1]]))
    # main()
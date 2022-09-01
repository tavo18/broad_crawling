[Broad crawling](https://docs.scrapy.org/en/latest/topics/broad-crawls.html) optimized Scrapy project intented to find, given an event website, every possible platform link inside the original url.

Sample 1:
Given this start url as input: https://www.mdmwest.com/en/home.html
We expect to find this one: https://www.expocad.com/host/fx/informa/ana23/exfx.html
We expect this output: https://www.expocad.com/host/fx/informa/ana23/exfx.html

Sample 2:
Input: https://www.imts.com/
Output: https://imts22.mapyourshow.com/8_0/

Proper settings have been defined for this purpose, together with the redefinition of Scrapy native classes to obtain the desired behaviour.
It is based on CrawlerSpider implementation + LinkExtractors + Regular expressions.
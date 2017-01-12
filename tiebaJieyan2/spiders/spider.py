#-*-coding=utf-8-*-
import scrapy
import requests
from scrapy.selector import Selector
from tiebaJieyan2.items import  postsItem,userInfo
from json import loads
import time
from mysql_model import Mysql
from scrapy.http import Request
from tiebaJieyan2.common.logger import Logger
mysql = Mysql()

from re import compile
logger = Logger('postsSpider','postsSpider.log')
regex = compile('<[^>]+>')

class postsSpider(scrapy.Spider):

  
    name = "postsJieyan"
    # start_urls = ["http://tieba.baidu.com/p/4607897153"]
    #取出所有url
    start_urls = []
    
    def __init__(self,offset=0,maxRow=100,order='desc',*args, **kwargs):
        super(postsSpider, self).__init__(*args, **kwargs)
        offset = int(offset)
        maxRow = int(maxRow)
        db_order = order
        sql = "select thread_link from threads order by id %s limit %d,%d;"%(order,offset,maxRow)
        #返回查询结果
        data = mysql.find_data(sql)
        # print data
        # print page_start
        # print page_end
        self.start_urls = [dt[0] for dt in data]
        # ...


    def parse(self, response):
        logger.info('crawl url:%s'%response.url)
        commentUrl = "http://tieba.baidu.com/p/totalComment"
        selector = Selector(response)
        #获取下一页
        url_li = selector.xpath('//li[@class="l_pager pager_theme_4 pb_list_pager"]/a')
        #没有获取到url
        if url_li:
            url_next_link = url_li[-2].xpath('./@href').extract()[0].strip()
            # 提取下一页的文本
            url_next_text = url_li[-2].xpath('text()').extract()[0]
            #获取当前页码
            cur_pn = selector.xpath('//span[@class="tP"]/text()').extract()[0]
            # print url_next_link
        else:
            url_next_text = u'没有下一页'
            cur_pn = 1

        # print  u'当前页面是:%s'%cur_pn

        all_posts = selector.xpath('//div[@class="p_postlist"]/div[@class="l_post l_post_bright j_l_post clearfix  "]')
        # print u'主题中的中帖子数目是:%d'%(len(all_posts))

        #打开评论
        flag = True
        #该页所有评论
        comment_info = ''
        for post in all_posts:
            postItem = postsItem()
            #获取信息
            if post.xpath('./@data-field').extract():
                data_field = loads(post.xpath('./@data-field').extract()[0])
                #获取作者
                author_info = data_field['author']
                #作者id
                author_id = author_info['user_id']

                #作者名字
                author_name = author_info['user_name']

                #帖子内容
                post_content = data_field['content']


                #帖子主题id
                thread_id = post_content['thread_id']

                #帖子id
                post_id = post_content['post_id']
                # print post_id
                #forum_id
                forum_id = post_content['forum_id']

                #帖子内容
                content = regex.sub('',post_content['content'])
                # print content

                #获取楼层
                floor_no = post_content['post_no']
                # print u'楼层是:%s'%floor_no
                # 楼层号
                postItem['floor_no'] = floor_no

                #存入item
                # 存储到item
                postItem['thread_id'] = thread_id
                # 不同楼层帖子id
                postItem['post_id'] = post_id
                postItem['content'] = content
                # print  u'帖子内容是:%s' % content
                # 不同楼层用户id
                postItem['author_id'] = author_id
                postItem['author_name'] = author_name



                # 获取回复数目，所有都有该项
                commet_num = post_content['comment_num']
                # 回复数目
                postItem['comment_num'] = commet_num
                # print u'该楼有%d 条评论'%commet_num

                #内容默认为空
                # postItem['comment_info_json'] = 'null'
                # 判断该页否有回复，避免多次访问数据接口，一次性获得该页所有数据
                if int(commet_num) > 0 and flag:
                    # print u'该页存在评论'
                    #获取时间戳
                    now_time = int(time.time()*1000)

                    querystring = {"t":now_time,"tid": thread_id, "fid": forum_id, "pn": cur_pn }
                    headers = {
                        'cache-control': "no-cache",

                    }
                    #返回评论信息,并解析评论
                    comment_info = loads(requests.request("GET", commentUrl, headers=headers, params=querystring).text)
                    #保存json文件
                    # postItem['comment_info_json'] = comment_info
                    #关闭访问评论
                    flag = False


                # 该楼层是否有回复
                ct_author_id = ['%s' % author_id]
                ct_author_name = ['%s' % author_name]
                if int(commet_num) > 0:
                    #存在评论
                    # postItem['comment_info_json'] = comment_info
                    #获取当前帖子的评论
                    comment = comment_info['data']['comment_list']['%s'%post_id]['comment_info']
                    comment_content = ''
                    for ct in comment:
                        ct_author_name.append(ct['username'])
                        ct_author_id.append(ct['user_id'])
                        #时间戳转化为特定格式的时间
                        timeArray = time.localtime(ct['now_time'])
                        ct_time = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
                        # print u'回复时间是%s'%ct_time
                        try:
                        
                            ct_content = regex.sub('',ct['content'])
                            #添加评论内容
                            comment_content += ct['username']+': '+ ct_content + u'\n回复时间:%s'%ct_time+'\n---------------------------------------------\n'

                        except TypeError, e:
                            logger.error(e)
                            logger.error("TypeError:%s"%ct['content'])
                            
                    zip_authors = zip(ct_author_id,ct_author_name)
                else:
                    comment_content = 'null'

                postItem['comment_content'] = comment_content

                # print u'评论内容是:%s'%comment_content


                #------------------------至于id取到是否，才能进行其他项目存储
                #获取设备来源,部分页面没有设备来源，设置为空
                if post.xpath('.//div[@class="post-tail-wrap"]/span[2]/a/text()').extract():
                    shebei = post.xpath('.//div[@class="post-tail-wrap"]/span[2]/a/text()').extract()[0]
                else:
                    shebei = 'null'
                # print u'设备是:%s'%shebei
                # 设备
                postItem['shebei'] = shebei

                if shebei=='null':
                    #获取时间,每个楼层都有，如果程序没有获取到，则丢弃该item
                    if post.xpath('.//div[@class="post-tail-wrap"]/span[@class="tail-info"]')[1].xpath('text()').extract():
                        post_date = post.xpath('.//div[@class="post-tail-wrap"]/span[@class="tail-info"]')[1].xpath('text()').extract()[0]
                        # 时间
                        postItem['post_date'] = post_date
                else:
                    if post.xpath('.//div[@class="post-tail-wrap"]/span[@class="tail-info"]')[2].xpath('text()').extract():
                        post_date = post.xpath('.//div[@class="post-tail-wrap"]/span[@class="tail-info"]')[2].xpath('text()').extract()[0]
                        # 时间
                        postItem['post_date'] = post_date
                # print u'发布时间是:%s'%post_date

                # 实例化Item，存储用户信息
                if int(commet_num) > 0:
                    
                    for auhthor_id, author_name in zip_authors:
                        user_info = userInfo()
                        user_info['user_id'] = auhthor_id
                        user_info['user_name'] = author_name
                        yield user_info

                # 提交item
                yield postItem

        # 存在下一页再进行访问
        if url_next_text == u'下一页':
            print u'进入下一页'+url_next_link
            yield Request(url="http://tieba.baidu.com"+url_next_link, callback=self.parse)






























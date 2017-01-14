# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from twisted.enterprise import adbapi
#丢弃无用的pipline
from scrapy.exceptions import DropItem
from tiebaJieyan2.items import postsItem,userInfo
from tiebaJieyan2.common.logger import Logger
import time
import MySQLdb
import MySQLdb.cursors

from scrapy.utils.project import get_project_settings
from scrapy import log

#载入设置
SETTINGS = get_project_settings()

logger = Logger('postsJieyan','postsPipeline.log')
class postsPipeline(object):

    def __init__(self):
        #数据库链接操作
        self.dbpool = adbapi.ConnectionPool('MySQLdb',
                                            host=SETTINGS['DB_HOST'],
                                            user=SETTINGS['DB_USER'],
                                            passwd=SETTINGS['DB_PASSWD'],
                                            port=SETTINGS['DB_PORT'],
                                            db=SETTINGS['DB_DB'],
                                            charset='utf8',
                                            use_unicode=True,
                                            cursorclass=MySQLdb.cursors.DictCursor)

    def spider_closed(self, spider):
        """ Cleanup function, called after crawing has finished to close open
            objects.
            Close ConnectionPool. """
        self.dbpool.close()

    def process_item(self, item, spider):
        # run db query in thread pool
        if isinstance(item,postsItem):
            query = self.dbpool.runInteraction(self._conditional_insert, item)
            query.addErrback(self.handle_error)
            return item
        elif isinstance(item,userInfo):
            query = self.dbpool.runInteraction(self._conditional_insert2, item)
            query.addErrback(self.handle_error)
            return item



    def _conditional_insert(self, tx, item):
        # create record if doesn't exist.
        # all this block run on it's own thread
        
        sql1 = 'set names utf8mb4'
        tx.execute(sql1)
		#判断item是都否缺失项目，如果缺失就放弃该项
        try:
                
			args = (item['thread_id'],
					item['post_id'],
					item['content'],
					item['author_id'],
					item['comment_content'],
					item['shebei'],
					item['floor_no'],
					item['post_date'],
					item['comment_num']

					)
			sql = "insert into posts(thread_id,post_id,content,author_id,comment_content,shebei,floor_no,post_date,comment_num) VALUES(%s,%s,'%s',%s,'%s','%s','%s','%s',%d)"%args
			try:
				#执行插入操作
				tx.execute(sql)
				log.msg("Item stored in db: %s" % item, level=log.INFO)
			except MySQLdb.OperationalError,e:
				loger.error(e)                
				loger.error(u'insert failed,OperationalError:%s'%item)
			except MySQLdb.ProgrammingError,e:
				loger.error(e)
				loger.error(u'insert failed,ProgrammingError:%s'%item)
			except MySQLdb.DatabaseError,e:
				loger.warning(e)
				loger.warning(u'insert failed,DatabaseError:%s'%item)
				#避免重复存储，执行更新操作
				# log.msg("threads_id already stored in db: %s" % item, level=log.WARNING)
				#更新帖子信息
				args = (item['comment_content'],
						item['shebei'],
						item['comment_num'],
						item['post_id']
						)
				#sql后面
				sql2 = "UPDATE posts SET comment_content='%s', shebei='%s',comment_num=%d WHERE post_id='%s'" %args
				try:
					#执行更新操作
					tx.execute(sql2)
					log.msg("update item : %s" % item, level=log.WARNING)

				except MySQLdb.OperationalError,e:
					loger.error(e)                
					loger.error(u'UPDATE failed,OperationalError:%s'%item)
				except MySQLdb.ProgrammingError,e:
					loger.error(e)
					loger.error(u'UPDATE failed,ProgrammingError:%s'%item)
				except MySQLdb.DatabaseError,e:
					loger.error(e)
					loger.error(u'UPDATE failed,DatabaseError:%s'%item)
						   

        except KeyError:
            logger.error('error item:%s'%item)
            DropItem(u'missing thread_id:%s' % item)



    def _conditional_insert2(self, tx, item):
        # create record if doesn't exist.
        # all this block run on it's own thread
        sql1 = 'set names utf8mb4'
        tx.execute(sql1)
        # 判断item是都否缺失项目，如果缺失就放弃该项
        try:

			args = (item['user_id'],
					item['user_name'])
			sql = "insert into user(user_id,user_name) VALUES(%s,'%s')" % args
			try:
				#执行插入操作
				tx.execute(sql)
				log.msg("Item stored in db: %s" % item, level=log.INFO)
			except MySQLdb.OperationalError,e:
				loger.error(e)                
				loger.error(u'insert failed,OperationalError:%s'%item)
			except MySQLdb.ProgrammingError,e:
				loger.error(e)
				loger.error(u'insert failed,ProgrammingError:%s'%item)
			except MySQLdb.DatabaseError,e:
				loger.error(e)
				loger.error(u'insert failed,DatabaseError:%s'%item)
				# 避免重复存储
				# log.msg("threads_id already stored in db: %s" % item, level=log.WARNING)
				# 更新帖子信息
				args = (item['user_name'],
						item['user_id'])
				# sql后面
				sql2 = "UPDATE user SET user_name='%s' WHERE user_id='%s'"%args
				# 执行更新操作
				try:
						#执行更新操作
					tx.execute(sql2)
					log.msg("update item : %s" % item, level=log.WARNING)
				except MySQLdb.OperationalError,e:
					loger.error(e)                
					loger.error(u'UPDATE failed,OperationalError:%s'%item)
				except MySQLdb.ProgrammingError,e:
					loger.error(e)
					loger.error(u'UPDATE failed,ProgrammingError:%s'%item)
				except MySQLdb.DatabaseError,e:
					loger.error(e)
					loger.error(u'UPDATE failed,DatabaseError:%s'%item)     
					

        except KeyError:
            logger.error('error item:%s'%item)
            DropItem(u'missing thread_id:%s' % item)


    def handle_error(self, e):
        log.err(e)
# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals

import os
import json
import boto3
import datetime
import hashlib
import logging
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.exceptions import ElasticsearchException

import settings

logger = logging.getLogger(__name__)
logger.setLevel(settings.LOG_LEVEL)

RESULT_SUCCESS = {"acknowledged": True}

class ScrollError(ElasticsearchException):
    pass

class Event(dict):
    def __getattr__(self, name):
        if name in self:
            return self[name]
        return None

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        if name in self:
            del(self[name])

    def is_valid(self):
        if self.get('source_host') and self.get('source_index') and self.get('s3_bucket'):
            return True
        return False

    def Elasticsearch(self, host):
        return Elasticsearch(host, timeout=settings.TIMEOUT, send_get_body_as='POST')

    @property
    def source_host(self):
        return self.get('source_host')

    @property
    def source_index(self):
        return self.get('source_index')

    @property
    def source_client(self):
        return self.Elasticsearch(self.source_host)

    @property
    def scroll_id(self):
        return self.get('scroll_id', None)

    @property
    def scroll(self):
        return self.get('scroll', settings.DEFAULT_SCROLL)

    @property
    def scan_options(self):
        return self.get('scan_options', settings.DEFAULT_SCAN_OPTIONS)

    @property
    def bulk_options(self):
        return self.get('bulk_options', settings.DEFAULT_BULK_OPTIONS)

    @property
    def s3_bucket(self):
        return self.get('s3_bucket')

    @property
    def s3_prefix(self):
        return self.get('s3_prefix', '')


def lambda_handler(event, context):
    event = Event(event)
    if not event.is_valid():
        message = 'Invalid Parameters: {}'.format(event)
        logger.error(message)
        return {'error': message}

    scroll_id = event.scroll_id

    logger.debug('Scroll ID: {}'.format(scroll_id))

    if scroll_id is None:
        try:
            scroll_id = scan_search(
                event.source_client,
                index=event.source_index,
                scroll=event.scroll,
                **event.scan_options
            )
        except Exception as e:
            logger.error(e)
            return {'error': str(e)}

        if scroll_id:
            event.scroll_id = scroll_id
            invoke_reindex(event, context)
            return RESULT_SUCCESS
        else:
            message = 'Can not get the scroll_id: {source_host} {source_index}'.format(**event)
            logger.error(message)
            return {'error': message}

    docs, scroll_id = scroll_search(event.source_client, scroll_id, scroll=event.scroll)

    if scroll_id is None or not docs:
        logger.info('Finished: {}'.format(event))
        return RESULT_SUCCESS

    s3_put_object(event.s3_bucket, event.s3_prefix, docs)

    event.scroll_id = scroll_id
    invoke_reindex(event, context)

    return RESULT_SUCCESS

def scan_search(client, index, scroll='1m', size=10, **kwargs):
    kwargs['search_type'] = 'scan'
    kwargs['fields'] = ('_source', '_parent', '_routing', '_timestamp')
    response = client.search(index=index, scroll=scroll, size=size, **kwargs)
    return response.get('_scroll_id', None)


def scroll_search(client, scroll_id, scroll='1m', **kwargs):
    response = client.scroll(scroll_id, scroll=scroll, **kwargs)

    if response['_shards']['failed']:
        raise ScrollError(
            'Scroll request has failed on {} shards out of {}.'.format(
                response['_shards']['failed'], response['_shards']['total']
            )
        )

    docs = response['hits']['hits']
    scroll_id = response.get('_scroll_id', None)
    return docs, scroll_id


def invoke_reindex(event, context):
    if settings.DEBUG:
        return lambda_handler(event, context)

    client = boto3.client('lambda')
    client.invoke(
        FunctionName=context.function_name,
        InvocationType='Event',
        LogType='None',
        Payload=json.dumps(event)
    )

def s3_put_object(bucket, prefix, docs):
    def _create_object_key(prefix, body):
        key = os.path.join(
            prefix,
            'dumpdata-{:%Y-%m-%d-%H-%M-%S}-{}'.format(
                datetime.datetime.now(),
                hashlib.md5(body.encode('utf-8')).hexdigest()
            )
        )
        return key

    body = "\n".join([json.dumps(d, ensure_ascii=False) for d in docs])
    object_key = _create_object_key(prefix, body)

    if settings.DEBUG:
        logger.debug('S3 Bucket: {}'.format(bucket))
        logger.debug('S3 Object Key: {}'.format(object_key))
        logger.debug('S3 Docs: {}'.format(len(docs)))
        logger.debug('S3 Body: {}'.format(body.split('\n')[0]))
    else:
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket, Key=object_key, Body=body)

    logger.info('Put Object: s3://{}/{}'.format(bucket, object_key))

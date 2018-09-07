#!/opt/bin/python -u
import requests
import xml.etree.ElementTree as XMLTree
from datetime import datetime

try:
    from requests.packages.urllib3.exceptions import InsecureRequestWarning, InsecurePlatformWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    requests.packages.urllib3.disable_warnings(InsecurePlatformWarning)
except:
    pass


def get_datasets_schema(host, user, passwd, feed_name, last_serial_id, logger):
    url = "%s/ODFI/rest/query/%s?since_serial=%d&v=2" % (host, feed_name, last_serial_id)
    logger.info("odfi url: %s", url)
    r = requests.get(url, verify=False, auth=(user, passwd))
    r.raise_for_status()
    try:
        feed_ds = {}
        root = XMLTree.fromstring(r.content)
        for dataset in root:
            ds = {}
            if not dataset or dataset.get('status') != 'READY':
                return feed_ds
            ds['revision'] = int(dataset.get('revision'))
            ds['recordCount'] = int(dataset.get('recordCount'))
            ds['id'] = int(dataset.get('id'))
            ds['dataSize'] = int(dataset.get('dataSize'))
            ds['status'] = dataset.get('status')
            ds['serial'] = int(dataset.find('serial').text)
            ds['readableInterval'] = dataset.find('readableInterval').text
            ds['startTimestamp'] = datetime.strptime(dataset.find('startTimestamp').text,
                                                     '%Y-%m-%d %H:%M:%S %Z')
            ds['endTimestamp'] = datetime.strptime(dataset.find('endTimestamp').text,
                                                     '%Y-%m-%d %H:%M:%S %Z')
            ds['name'] = dataset.find('feed').get('name')
            ds['dateCreated'] = datetime.strptime(dataset.find('dateCreated').text,'%Y-%m-%d %H:%M:%S %Z')
            schema = {}
            schema_root = dataset.find('schema')
            schema['version'] = int(schema_root.get('version'))
            schema['locator'] = schema_root.get('locator')
            schema['name'] = schema_root.get('name')
            ds['schema'] = schema
            parts = []
            for part in dataset.find('parts'):
                part_dict = {}
                part_dict['locator'] = part.get('locator')
                part_dict['recordCount'] = int(part.get('recordCount'))
                part_dict['dataSize'] = int(part.get('dataSize'))
                part_dict['part_key'] = part.get('part_key', '000')
                part_dict['index'] = int(part.get('index'))
                part_dict['compressionType'] = part.get('compressionType')
                part_dict['digest'] = part.get('digest')
                parts.append(part_dict)
            parts = sorted(parts, key=lambda k: k['index'])
            ds['parts'] = parts
            if feed_ds.get(ds['startTimestamp']) is not None:
                if ds['serial'] >= feed_ds.get(ds['startTimestamp'])['serial']:
                    feed_ds[ds['startTimestamp']] = ds
            else:
                feed_ds[ds['startTimestamp']] = ds
        return feed_ds
    except Exception as err:
        logger.error("Exception occurred while downloading feed from url: %s \n Error:%s", url, err)
        raise Exception("Exception occurred during downloading %s" % err)

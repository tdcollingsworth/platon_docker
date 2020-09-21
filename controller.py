'''
python3 XXX.py --xxxxxx ...
'''

#==============================================================================
# PACKAGES
#==============================================================================

import argparse
import boto3
import botocore
from io import StringIO
import json
import os
import pandas
import re
import subprocess
import time
import utils

#==============================================================================
# FUNCTIONS
#==============================================================================

def runplaton(isolate):
    '''Docstring description of the function.
    '''

#    columns = ['Rep3',
#               'NT_Rep',
#               'RepA_N',
#               'Rep_trans',
#               'Inc18',
#               'Rep1',
#               'RepL',
#               'Rep2',
#               'Enterobacteriaceae']

#    PDF = pandas.DataFrame(columns=columns)

    if os.path.isfile(isolate+'_contigs.fasta'):

        try:
            p = subprocess.run(
            [
                '/TOOLS/platon/bin/platon'+\
                ' --db /TOOLS/platon_db'+\
#                ' --mode sensitivity'+\
                ' --prefix '+isolate+\
                ' '+isolate+'_contigs.fasta'
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True)
    
        except subprocess.CalledProcessError:
            error = utils.format_std_string(p.stderr)
            logger.error(error)

#    else:

#        PSeries = pandas.Series(data=['Assembly not available.'],
#                                 index=columns,
#                                 name=isolate)

#        PDF = PDF.append(PSeries)

    if os.path.isfile('/WORKSPACE/'+isolate+'.json'):

#        PSeries = pandas.Series(index=columns,
#                                 name=isolate)

        with open('/WORKSPACE/'++'.json') as f:
            data = json.load(f)

        print(json.dumps(data, indent=4, sort_keys=True))

#        gramPos = data['plasmidfinder']['results']['Gram Positive']

#        for k, v in gramPos.items():
#            if v != 'No hit found':
#                gramPosList = []
#                for ke, va in v.items():
#                    gramPosListList.append(ke)
#                PSeries[k] = (' ').join(gramPosList)
#            else:
#                PSeries[k] = 'No hit found.'

#        entero = data['plasmidfinder']['results']['Enterobacteriaceae']['enterobacteriaceae']

#        if entero != 'No hit found':
#            enteroList = []
#            for k, v in entero.items():
#                enteroList.append(k)
#            PSeries['Enterobacteriaceae'] = (' ').join(enteroList)
#        else:
#            PSeries['Enterobacteriaceae'] = 'No hit found.'

#        PDF = PDF.append(PSeries)

#    else:

#        PSeries = pandas.Series(data=['Results not available.'],
#                                 index=columns,
#                                 name=isolate)

#        PDF = PDF.append(PSeries)

    return None
#    return PDF

def getFiles(fileType,isolate,outFolder):
    """Docstring description of the function.
    """

    if fileType == 'contigs':

        bucketName = 'fastq.jmilabs.com'
        prefix = 'ngs_assemblies/'+isolate+'/'
        operationParameters = {'Bucket': bucketName,
                               'Prefix': prefix}
        suffix = '_contigs.fasta'

        client = boto3.client('s3')
        paginator = client.get_paginator('list_objects')
        pageIterator = paginator.paginate(**operationParameters)
    
        for page in pageIterator:

            try:
                for result in page['Contents']:
       
                    fileName = os.path.basename(result['Key'])
        
                    if fileName.startswith(isolate) and fileName.endswith(suffix):
                        try:
                            boto3.resource('s3').Bucket(bucketName).download_file(result['Key'],outFolder+fileName)
                        except botocore.exceptions.ClientError as e:
                            if e.response['Error']['Code'] == "404":
                                print("The object does not exist.")
                            else:
                                raise
        
            except KeyError:
                pass

    elif fileType == 'fastqs':
        queryString = "SELECT key FROM inventory"+\
                      " WHERE (dt = (SELECT CONCAT(CAST(CURRENT_DATE AS VARCHAR), '-00-00'))"+\
                      " OR dt = (SELECT CONCAT(CAST(CURRENT_DATE - INTERVAL '1' DAY AS VARCHAR), '-00-00')))"+\
                      " AND key LIKE '%.fastq.gz'"+\
                      " AND key LIKE '%"+isolate+"\_%' ESCAPE '\\'"+\
                      " ORDER BY last_modified_date Desc LIMIT 2;"
        region = 'us-east-2'
        database = 'ngsarchive'
        workgroup = 'molecular'
        bucketName = 'ngs-archive'

        client = boto3.client('athena',region_name=region)
    
        queryID = client.start_query_execution(
            QueryString=queryString,
            QueryExecutionContext={
            'Database': database,
            },
            WorkGroup=workgroup
        )['QueryExecutionId']
    
    
        queryStatus = None
        while queryStatus == 'QUEUED' or queryStatus == 'RUNNING' or queryStatus is None:
            queryStatus = client.get_query_execution(QueryExecutionId=queryID)['QueryExecution']['Status']['State']
            if queryStatus == 'FAILED' or queryStatus == 'CANCELLED':
                raise Exception('Athena query with the string "{}" failed or was cancelled'.format(queryString))
            time.sleep(1)

        paginator = client.get_paginator('get_query_results')
        operationParameters = {'QueryExecutionId': queryID}
        pageIterator = paginator.paginate(**operationParameters)
    
        results = [] 
        colNames = None
        for page in pageIterator:
            for row in page['ResultSet']['Rows']:
               colValues = [col.get('VarCharValue', None) for col in row['Data']]
               if not colNames:
                   colNames = colValues
               else:
                   results.append(dict(zip(colNames, colValues)))
   
        for result in results:

            fileName = os.path.basename(result['key'])
    
            try:
                boto3.resource('s3').Bucket(bucketName).download_file(result['key'],outFolder+fileName)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    print("The object does not exist.")
                else:
                    raise
    
    return None

def getArgs():
    """Docstring description of the function.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('--isolate', type=str,
                        nargs='?', default='18-PPD-01-8',
                        help='Isolate of interest.')

    args = parser.parse_args()
    return args

#==============================================================================
# MAIN
#==============================================================================

def main():
    ''' Docstring description of the main function.
    '''

    parseArgs=getArgs()

    getFiles('contigs',parseArgs.isolate,'/WORKSPACE/')

    PDF = runplaton(parseArgs.isolate)

#    output = StringIO()
#    PDF.to_csv(output,index_label='Isolate')
#    output.seek(0)
#    print(output.read())

#==============================================================================

if __name__ == '__main__':
    main()


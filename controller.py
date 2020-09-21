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

    columns = ['id',
               'inc_types',
               'plasmid_hits',
               'conjugation_hits',
               'mobilization_hits',
               'replication_hits',
               'amr_hits',
               'is_circular']

    PDF = pandas.DataFrame(columns=columns)

    if os.path.isfile(isolate+'_contigs.fasta'):

        try:
            p = subprocess.run(
            [
                '/TOOLS/platon/bin/platon'+\
                ' --db /TOOLS/platon_db'+\
                ' --prefix '+isolate+\
                ' --threads 8'+\
                ' '+isolate+'_contigs.fasta'
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True)
    
        except subprocess.CalledProcessError:
            error = utils.format_std_string(p.stderr)
            logger.error(error)

    else:

        PSeries = pandas.Series(data=['Assembly not available.'],
                                 index=columns,
                                 name=isolate)

        PDF = PDF.append(PSeries)

    if os.path.isfile('/WORKSPACE/'+isolate+'.json'):

        with open('/WORKSPACE/'+isolate+'.json') as f:
            data = json.load(f)

        for result,details in data.items():

            PSeries = pandas.Series(index=columns,
                                    name=isolate)

            PSeries['id']  = details['id']

            PSeries['is_circular'] = details['is_circular']

            for key,value in details.items():

                incTypes = []

                if key == 'inc_types':
                    for v in value:
                        incTypes.append(v['type'])

                if incTypes:
                    PSeries['inc_types'] = (' ').join(incTypes)


                plasmidHits = []

                if key == 'plasmid_hits':
                    for v in value:
                        plasmidHits.append(v['plasmid']['id'])
                
                if plasmidHits:
                    PSeries['plasmid_hits'] = (' ').join(plasmidHits)

                conjugationHits = []

                if key == 'conjugation_hits':
                    for v in value:
                        conjugationHits.append(v['type'])

                if conjugationHits:
                    PSeries['conjugation_hits'] = (' ').join(conjugationHits)

                mobilizationHits = []

                if key == 'mobilization_hits':
                    for v in value:
                         mobilizationHits.append(v['type'])
                    PSeries['mobilization_hits'] = (' ').join(mobilizationHits)

                replicationHits = []

                if key == 'replication_hits':
                    for v in value:
                        replicationHits.append(v['type'])

                if replicationHits:
                    PSeries['replication_hits'] = (' ').join(replicationHits)

                amrHits = []

                if key == 'amr_hits':
                    for v in value:
                        amrHits.append(v['type'])

                if amrHits:
                    PSeries['amr_hits'] = (' ').join(amrHits)


            PDF = PDF.append(PSeries)

    return PDF

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

    output = StringIO()
    PDF.to_csv(output,index_label='Isolate')
    output.seek(0)
    print(output.read())

#==============================================================================

if __name__ == '__main__':
    main()


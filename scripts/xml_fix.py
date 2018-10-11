#!/usr/bin/env python

import json
import os
import shutil
import yaml
import subprocess
import hashlib
import click
from overture_song.model import ApiConfig, SongError, FileUpdateRequest
from overture_song.client import Api, ManifestClient, StudyClient
from overture_song.tools import FileUploadClient
from overture_song.utils import setup_output_file_path

def generate_metadata_xml(xml_info, out_dir):

    bundle_id, ega_metadata_repo, project_code, output_file, ega_dataset_id, ega_analysis_id, ega_experiment_id, ega_run_id, ega_sample_id, ega_study_id  = xml_info.rstrip().split("\t")

    metadata_container = "quay.io/baminou/dckr_prepare_metadata_xml"
    subprocess.check_output(['docker', 'pull', metadata_container])

    try:
        subprocess.check_output(['docker','run',
                                 '-v', out_dir + ':/app',
                                 metadata_container,
                                  '-i',ega_metadata_repo,
                                  '-p',project_code,
                                  '-o',os.path.join('/app',output_file),
                                  '-d',ega_dataset_id,
                                  '-a',ega_analysis_id if ega_analysis_id else '',
                                  '-e',ega_experiment_id if ega_experiment_id else '',
                                  '-r',ega_run_id if ega_run_id else '',
                                  '-sa',ega_sample_id if ega_sample_id else '',
                                  '-st',ega_study_id if ega_study_id else ''])

    except Exception as e:
        click.echo('Error {}'.format(str(e)))
        with open('non_pcawg_fix.log', 'a') as f:
            f.write('{0}::{1}: {2}\n'.format(project_code, bundle_id, str(e)))

    return




def get_md5(fname):
    if not os.path.isfile(fname): return None
    with open(fname, 'r') as f: 
        xml_str = f.read()
    return hashlib.md5(xml_str.encode("utf-8")).hexdigest()


@click.command()
@click.option('--config', '-c', default='conf.yaml', type=click.File())
@click.option('--access_token', '-t', default=os.environ.get('ACCESSTOKEN',None))
def main(config, access_token):
    
    app_ctx = yaml.load(config)
    # app_ctx = replace_values(app_ctx)

    aws_song_file = app_ctx['aws']
    aws_song = {}
    with open(aws_song_file, 'r', newline='') as f:
        for l in f:
            projectCode, objectId, analysisId, fileName, songMd5, songFileSize = l.rstrip().split('\t')
            aws_song[objectId] = songMd5

    ega_xml_info = app_ctx['ega_xml_info']
    ega_xml = {}
    with open(ega_xml_info, 'r', newline='\n') as f:
        for l in f:
            analysisId = l.split('\t')[0]
            ega_xml[analysisId] = l.rstrip()


    if os.path.isfile('fix.log'):
        os.remove('fix.log')
    if os.path.isfile(app_ctx['xml_mismatch']):
        os.remove(app_ctx['xml_mismatch'])
    if not os.path.isdir(app_ctx['collab_xml_fix_dir']):
        os.mkdir(app_ctx['collab_xml_fix_dir'])
    if not os.path.isdir(app_ctx['aws_xml_fix_dir']):
        os.mkdir(app_ctx['aws_xml_fix_dir'])

    collab_song_file = app_ctx['collab']
    with open(collab_song_file, 'r', newline='') as f:
        for l in f:
            projectCode, objectId, analysisId, fileName, songMd5 = l.rstrip().split('\t')

            # only download if there is not local copy
            fpath = os.path.join(app_ctx['xml_dir'], fileName)
            if not os.path.isfile(fpath):
                # download the xml from collab
                subprocess.check_output(['score-client', '--profile', 'collab', 'download','--object-id', objectId, '--validate', 'false', '--force', '--output-dir', app_ctx['xml_dir']])

            # get file md5sum for the one downloaded from collab
            collabMd5 = get_md5(fpath)
            collabSize = os.path.getsize(fpath)


            # handle the ega xml
            if analysisId.startswith('EGA') and fileName.startswith('bundle'):
                # generate the xml from ega jobs
                if not ega_xml.get(analysisId):
                    click.echo('{}::{}: the ega transfer job is missing'.format(projectCode, analysisId))
                    with open('fix.log', 'a') as f:
                        f.write(
                            '{0}::{1}: the ega transfer job is missing in the completed folder\n'.format(projectCode,
                                                                                                         analysisId))
                    continue

                if not os.path.isdir(app_ctx['ega_xml_dir']): os.makedirs(app_ctx['ega_xml_dir'])

                fpath = os.path.join(app_ctx['ega_xml_dir'], fileName)
                if not os.path.isfile(fpath):
                    generate_metadata_xml(ega_xml[analysisId], app_ctx['ega_xml_dir'])

            # get file md5sum for the one to upload
            fileMd5 = get_md5(fpath)
            fileSize = os.path.getsize(fpath)

            if not fileMd5 == songMd5 or not fileMd5 == collabMd5:
                with open(app_ctx['xml_mismatch'], 'a') as f:
                    f.write('\t'.join([projectCode, analysisId, collabMd5, songMd5, fileMd5]))
                    f.write('\n')

            # skip the fix ones
            fixpath = os.path.join(app_ctx['collab_xml_fix_dir'], fileName + '.fix')
            if os.path.isfile(fixpath): continue

            # upload to collab
            subprocess.check_output(['score-client', '--profile', 'collab', 'upload', '--md5', fileMd5, '--file', fpath, '--object-id', objectId, '--force'])

            # copy xml to open meta bucket
            subprocess.check_output(['aws', '--endpoint-url', app_ctx['collab_endpoint_url'], '--profile', 'collab', 's3', 'cp', fpath, app_ctx['meta_bucket_url']+objectId])


            # update the collab song
            server_url = app_ctx['song']['collab']
            api_config = ApiConfig(server_url,projectCode,access_token)
            api = Api(api_config)


	        # check whether the collab-song payload need to be updated
            if not fileMd5 == songMd5:
            # update the file
                fileUpdate = FileUpdateRequest()
                fileUpdate.fileSize = fileSize
                fileUpdate.fileMd5sum = fileMd5
                api.update_file(objectId, fileUpdate)


            # publish the analysis
            if not api.get_analysis(analysisId).__dict__['analysisState'] == "PUBLISHED":
                try:
                    api.publish(analysisId)
                except:
                    with open('fix.log', 'a') as f:
                        f.write('{0}::{1}: can not be published\n'.format(projectCode, analysisId))

            with open(fixpath, 'w') as w: w.write('')

            # skip the fix ones
            fixpath = os.path.join(app_ctx['aws_xml_fix_dir'], fileName + '.fix')
            if os.path.isfile(fixpath): continue

            # check whether the object is aws approved
            if projectCode in app_ctx['aws_approved'] and objectId in aws_song:
	            # upload to aws
                subprocess.check_output(['score-client', '--profile', 'aws', 'upload', '--md5', fileMd5, '--file', fpath, '--object-id', objectId, '--force'])

	            # copy xml to open meta bucket
                subprocess.check_output(['aws', '--profile', 'amazon_pay', 's3', 'cp', fpath, app_ctx['meta_bucket_url']+objectId])

	            # update the aws song
                server_url = app_ctx['song']['aws']
                api_config = ApiConfig(server_url,projectCode,access_token)
                api = Api(api_config)


		        # check whether the aws-song payload need to be updated
                if not fileMd5 == aws_song[objectId]:
	                # update the file
                    fileUpdate = FileUpdateRequest()
                    fileUpdate.fileSize = fileSize
                    fileUpdate.fileMd5sum = fileMd5
                    api.update_file(objectId, fileUpdate)


	            # publish the analysis
                if not api.get_analysis(analysisId).__dict__['analysisState'] == "PUBLISHED":
                    try:
                        api.publish(analysisId)
                    except:
                        with open('fix.log', 'a') as f:
                            f.write('{0}::{1}: can not be published\n'.format(projectCode, analysisId))

                with open(fixpath, 'w') as w: w.write('')

            elif projectCode in app_ctx['aws_approved'] and (not objectId in aws_song):
                with open('fix.log', 'a') as f:
                    f.write('{0}::{1} object {2}: is missing from AWS\n'.format(projectCode, analysisId, objectId))

            elif (not projectCode in app_ctx['aws_approved']) and objectId in aws_song:
                with open('fix.log', 'a') as f:
                    f.write('{0}::{1} object {2}: is not allowed in AWS\n'.format(projectCode, analysisId, objectId))
            else:
                pass


    return

if __name__ == "__main__":
    main()

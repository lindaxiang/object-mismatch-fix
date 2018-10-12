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

def generate_metadata_xml(xml_info, app_ctx):
    out_dir = app_ctx['ega_xml_dir']

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
        with open(app_ctx['log'], 'a') as f:
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
@click.option('--profile', '-p', default='collab')
def main(config, access_token, profile):
    
    app_ctx = yaml.load(config)

    ega_xml_info = app_ctx['ega_xml_info']
    ega_xml = {}
    with open(ega_xml_info, 'r', newline='') as f:
        for l in f:
            analysisId = l.split('\t')[0]
            ega_xml[analysisId] = l.rstrip()


    if os.path.isfile(app_ctx['log']):
        os.remove(app_ctx['log'])
    if os.path.isfile(app_ctx['xml_mismatch']):
        os.remove(app_ctx['xml_mismatch'])
    if not os.path.isdir(app_ctx['xml_fix_dir']):
        os.mkdir(app_ctx['xml_fix_dir'])

    song_file = app_ctx[profile]
    with open(song_file, 'r', newline='') as f:
        for l in f:
            projectCode, objectId, analysisId, fileName, songMd5 = l.rstrip().split('\t')

            # double check for safety reason
            if profile == 'aws' and not projectCode in app_ctx['aws_approved']:
                with open(app_ctx['log'], 'a') as f:
                    f.write('{0}::{1} object {2}: is not allowed in AWS\n'.format(projectCode, analysisId, objectId))
                continue

            # only download if there is no local copy
            fpath = os.path.join(app_ctx['xml_dir'], fileName)
            if not os.path.isfile(fpath):
                # download the xml from collab
                subprocess.check_output(['score-client', '--profile', profile, 'download','--object-id', objectId, '--validate', 'false', '--force', '--output-dir', app_ctx['xml_dir']])

            # get file md5sum for the one downloaded by score client
            scoreMd5 = get_md5(fpath)
            scoreSize = os.path.getsize(fpath)


            # handle the ega xml
            if analysisId.startswith('EGA') and fileName.startswith('bundle'):
                # generate the xml from ega jobs
                if not ega_xml.get(analysisId):
                    click.echo('{}::{}: the ega transfer job is missing'.format(projectCode, analysisId))
                    with open(app_ctx['log'], 'a') as f:
                        f.write(
                            '{0}::{1}: the ega transfer job is missing in the completed folder\n'.format(projectCode,
                                                                                                         analysisId))
                    continue

                if not os.path.isdir(app_ctx['ega_xml_dir']): os.makedirs(app_ctx['ega_xml_dir'])

                fpath = os.path.join(app_ctx['ega_xml_dir'], fileName)
                if not os.path.isfile(fpath):
                    generate_metadata_xml(ega_xml[analysisId], app_ctx)

            # get file md5sum for the one to upload
            fileMd5 = get_md5(fpath)
            fileSize = os.path.getsize(fpath)

            if not fileMd5 == songMd5 or not fileMd5 == scoreMd5:
                with open(app_ctx['xml_mismatch'], 'a') as f:
                    f.write('\t'.join([projectCode, analysisId, scoreMd5, songMd5, fileMd5]))
                    f.write('\n')

            # skip the fix ones
            fixpath = os.path.join(app_ctx['xml_fix_dir'], fileName + '.fix')
            if os.path.isfile(fixpath): continue

            # upload to storage
            subprocess.check_output(['score-client', '--profile', profile, 'upload', '--md5', fileMd5, '--file', fpath, '--object-id', objectId, '--force'])

            # copy xml to open meta bucket
            if profile == 'collab':
                subprocess.check_output(['aws', '--endpoint-url', app_ctx[profile+'_endpoint_url'], '--profile', profile, 's3', 'cp', fpath, app_ctx['meta_bucket_url']+objectId])
            else:
                subprocess.check_output(['aws', '--profile', 'amazon_pay', 's3', 'cp', fpath, app_ctx['meta_bucket_url'] + objectId])

            # update the song
            server_url = app_ctx['song'][profile]
            api_config = ApiConfig(server_url,projectCode,access_token)
            api = Api(api_config)


	        # check whether the song payload need to be updated
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
                    with open(app_ctx['log'], 'a') as f:
                        f.write('{0}::{1}: can not be published\n'.format(projectCode, analysisId))

            with open(fixpath, 'w') as w: w.write('')

    return

if __name__ == "__main__":
    main()
